"""Consistent backups, immutable release assets, verified restore and retention."""
from __future__ import annotations
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
from contextlib import nullcontext
from datetime import datetime
from psycopg import sql
from common import ROOT, LOCAL, capture, db, digest, gh, gh_api, now, pg_env, pg_tool, read_json, run, table_metrics, write_json

TAG_PREFIX = 'catalog-'

def dump_identity_sequence(c, backup, major, sequence):
    """Read the sequence value saved by pg_dump; sequences are not MVCC data."""
    restore = pg_tool(c, 'pg_restore', major)
    entries = [line for line in capture([restore, '--list', backup]).splitlines()
               if ' SEQUENCE SET ' in line
               and line.split(' SEQUENCE SET ', 1)[1].split()[:2] == sequence.split('.')]
    if len(entries) != 1:
        raise RuntimeError('Backup must contain exactly one proxy identity sequence')
    with tempfile.NamedTemporaryFile(mode='w', suffix='.list') as selected:
        selected.write(entries[0] + '\n')
        selected.flush()
        script = capture([restore, '--use-list', selected.name, '--file=-', backup])
    values = re.findall(r"^SELECT pg_catalog\.setval\('[^']+', (-?\d+), (true|false)\);$", script, re.MULTILINE)
    if len(values) != 1:
        raise RuntimeError('Could not read the backed-up proxy identity sequence')
    return {'last_value': int(values[0][0]), 'is_called': values[0][1] == 'true'}


def make_snapshot(c, folder, report, snapshot_guard=nullcontext):
    folder = Path(folder)
    out = folder / 'snapshot'
    out.mkdir(exist_ok=True)
    existing = read_json(out / 'manifest.json')
    if existing:
        for asset in existing['assets']:
            p = out / asset['name']
            if not p.exists() or p.stat().st_size != asset['bytes'] or digest(p) != asset['sha256']:
                raise RuntimeError('Saved snapshot integrity failure: ' + asset['name'])
        return existing
    with db(c) as connection:
        size = connection.execute('SELECT pg_database_size(current_database()) AS n').fetchone()['n']
        if shutil.disk_usage(out).free < max(3 * 1024**3, size * 2):
            raise RuntimeError('Insufficient free disk space for snapshot and restore verification')
        with connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            connection.execute("SET LOCAL TIME ZONE 'UTC'")
            with snapshot_guard():
                paused_at = time.monotonic()
                if not connection.execute("SELECT pg_try_advisory_lock(hashtextextended('proxy:proxy_collection',0)) AS ok").fetchone()['ok']:
                    raise RuntimeError('Another collector is active; retry snapshot later')
                try:
                    if callable(report):
                        report = report(connection)
                    snapshot = connection.execute('SELECT pg_export_snapshot() AS id').fetchone()['id']
                finally:
                    connection.execute("SELECT pg_advisory_unlock(hashtextextended('proxy:proxy_collection',0))")
                pause_seconds = time.monotonic() - paused_at
            # Keep the MVCC snapshot open, while allowing new imports to commit.
            # The report and SQLite receipts were captured inside the same barrier.
            write_json(out / 'snapshot-boundary.json', {'imports_resumed_at': now(),
                       'import_pause_seconds': round(pause_seconds, 3)})
            backup = out / 'catalog.dump'
            temp = out / 'catalog.dump.partial'
            compression = c.get('snapshot_compression', 'zstd:1' if connection.info.server_version >= 180000 else '1')
            run([pg_tool(c,'pg_dump',connection.info.server_version//10000), '--format=custom', '--compress=' + compression, '--no-owner', '--no-acl',
                 '--snapshot', snapshot, '--file', temp], env=pg_env(c), stdout=subprocess.DEVNULL)
            metrics = table_metrics(connection)
            sequence = connection.execute("SELECT pg_get_serial_sequence('public.proxies','proxy_id') AS name").fetchone()['name']
            metrics['identity_sequence'] = dump_identity_sequence(c, temp, connection.info.server_version // 10000, sequence)
            temp.replace(backup)
            server_version = connection.execute('SHOW server_version').fetchone()['server_version']
    assets = []
    with backup.open('rb') as source:
        index = 0
        while block := source.read(c['chunk_bytes']):
            name = f'catalog.dump.part-{index:04d}'
            part = out / name
            part.write_bytes(block)
            assets.append({'name':name, 'bytes':part.stat().st_size, 'sha256':digest(part), 'role':'database'})
            index += 1
    for name in ('sources-before.jsonl.gz', 'source-results.jsonl.gz', 'candidates.json', 'validation.json', 'intake-results.jsonl.gz'):
        source = folder / name
        if source.exists():
            target = out / name
            shutil.copyfile(source, target)
            assets.append({'name':name, 'bytes':target.stat().st_size, 'sha256':digest(target), 'role':'evidence'})
    write_json(out / 'report.json', report)
    assets.append({'name':'report.json','bytes':(out/'report.json').stat().st_size,'sha256':digest(out/'report.json'),'role':'report'})
    manifest = {'format_version':1, 'created_at':now(), 'run_id':folder.name, 'repository':c['repository'],
                'code_commit':c.get('code_commit') or capture(['git','rev-parse','HEAD'],cwd=ROOT).strip(),
                'postgres_version':server_version, 'database_bytes':size, 'tables':metrics,
                'compression':compression,
                'import_pause_seconds':round(pause_seconds, 3),
                'dump_sha256':digest(backup), 'dump_bytes':backup.stat().st_size, 'assets':assets,
                'cache_policy':'Source payload and parser caches remain local; restore starts fresh downloads. Source records and run evidence are included.'}
    write_json(out/'manifest.json',manifest)
    return manifest

def release_record(c, tag):
    try:
        return gh_api(c, f'repos/{c["repository"]}/releases/tags/{tag}')
    except subprocess.CalledProcessError as exc:
        if '404' not in (exc.stderr or ''): raise
    # Drafts do not create the Git tag yet, so recover them by authenticated listing.
    page=1
    while True:
        records=gh_api(c,f'repos/{c["repository"]}/releases?per_page=100&page={page}')
        match=next((r for r in records if r['tag_name']==tag),None)
        if match: return match
        if len(records)<100: raise RuntimeError('Release not found: '+tag)
        page+=1

def publish_assets(c, folder, manifest, seed=False):
    folder = Path(folder)
    tag = TAG_PREFIX + folder.name
    journal = read_json(folder/'publication.json', {})
    if not journal:
        # A interrupted create is recovered by finding the tag; no replacement of other releases.
        releases = gh_api(c, f'repos/{c["repository"]}/releases?per_page=100')
        found = next((r for r in releases if r['tag_name'] == tag), None)
        if found is None:
            notes = folder/'release-notes.md'
            notes.write_text(f'Proxy catalog snapshot {folder.name}\n\nConfigurations: {manifest["tables"]["proxies"]["rows"]:,}\n\nDownload through `python catalog.py sync` to verify every file.\n')
            gh(c,'release','create',tag,'--repo',c['repository'],'--target',manifest['code_commit'],
               '--draft','--title',('Initial seed: ' if seed else '') + folder.name,'--notes-file',str(notes))
            found = release_record(c,tag)
        journal = {'tag':tag,'release_id':found['id'],'seed':seed,'created_at':now()}
        write_json(folder/'publication.json',journal)
    record = release_record(c,tag)
    files = [dict(x) for x in manifest['assets']]
    mpath = folder/'snapshot/manifest.json'
    files.append({'name':'manifest.json','bytes':mpath.stat().st_size,'sha256':digest(mpath)})
    for item in files:
        asset = next((a for a in record['assets'] if a['name']==item['name']),None)
        if asset and asset.get('digest') == 'sha256:'+item['sha256'] and asset['size']==item['bytes']:
            continue
        if asset:
            if not record['draft']:
                raise RuntimeError('Published asset differs from local snapshot; refusing replacement')
            gh(c,'api',f'repos/{c["repository"]}/releases/assets/{asset["id"]}','--method','DELETE')
        gh(c,'release','upload',tag,str(folder/'snapshot'/item['name']),'--repo',c['repository'])
        record = release_record(c,tag)
    # Check the complete remote set. Older GitHub servers without digests get a read-back check.
    for item in files:
        asset = next((a for a in record['assets'] if a['name']==item['name']),None)
        if not asset or asset['size']!=item['bytes'] or asset.get('state')!='uploaded':
            raise RuntimeError('Incomplete uploaded asset: '+item['name'])
        if asset.get('digest'):
            if asset['digest']!='sha256:'+item['sha256']:
                raise RuntimeError('Uploaded checksum mismatch: '+item['name'])
        else:
            check = folder/'remote-check'
            check.mkdir(exist_ok=True)
            gh(c,'release','download',tag,'--repo',c['repository'],'--pattern',item['name'],'--dir',str(check),'--clobber')
            if digest(check/item['name'])!=item['sha256']:
                raise RuntimeError('Downloaded checksum mismatch: '+item['name'])
    journal.update(assets_verified_at=now(),url=f'https://github.com/{c["repository"]}/releases/tag/{tag}',manifest_sha256=digest(mpath))
    write_json(folder/'publication.json',journal)
    return journal

def download(c, tag, destination, expected_manifest=None):
    destination = Path(destination)
    destination.mkdir(parents=True,exist_ok=True)
    record = release_record(c,tag)
    gh(c,'release','download',tag,'--repo',c['repository'],'--pattern','manifest.json','--dir',str(destination),'--clobber')
    if expected_manifest and digest(destination/'manifest.json') != expected_manifest:
        raise RuntimeError('Manifest checksum differs from the published Git reference')
    manifest = read_json(destination/'manifest.json')
    if manifest.get('repository') != c['repository'] or manifest.get('format_version') != 1:
        raise RuntimeError('Unexpected snapshot manifest')
    for asset in manifest['assets']:
        name = asset['name']
        if Path(name).name != name or name in ('','.','..'):
            raise RuntimeError('Invalid snapshot asset name')
        path = destination/name
        if path.exists() and path.stat().st_size==asset['bytes'] and digest(path)==asset['sha256']:
            continue
        gh(c,'release','download',tag,'--repo',c['repository'],'--pattern',name,'--dir',str(destination),'--clobber')
        if path.stat().st_size!=asset['bytes'] or digest(path)!=asset['sha256']:
            raise RuntimeError('Snapshot checksum mismatch: '+name)
    dump = destination/'catalog.dump'
    if not dump.exists() or digest(dump)!=manifest['dump_sha256']:
        partial = destination/'catalog.dump.partial'
        with partial.open('wb') as target:
            for asset in manifest['assets']:
                if asset['role']=='database':
                    with (destination/asset['name']).open('rb') as source:
                        shutil.copyfileobj(source,target,4*1024*1024)
        if partial.stat().st_size!=manifest['dump_bytes'] or digest(partial)!=manifest['dump_sha256']:
            raise RuntimeError('Reassembled backup checksum mismatch')
        partial.replace(dump)
    write_json(destination/'download-verification.json',{'verified_at':now(),'tag':tag,'dump_sha256':digest(dump)})
    return manifest

def restore_verify(c, destination, database_name=None, keep=False):
    destination = Path(destination)
    manifest = read_json(destination/'manifest.json')
    name = database_name or 'proxy_catalog_verify_' + manifest['dump_sha256'][:12]
    if not re.fullmatch(r'proxy_catalog_(verify|restore)_[a-z0-9_]+',name):
        raise ValueError('Restore names must begin proxy_catalog_verify_ or proxy_catalog_restore_')
    if digest(destination/'catalog.dump')!=manifest['dump_sha256']:
        raise RuntimeError('Backup checksum mismatch before restore')
    with db(c,dbname='postgres') as admin:
        if admin.execute('SELECT 1 FROM pg_database WHERE datname=%s',(name,)).fetchone():
            raise RuntimeError('Restore target already exists; use a new database name')
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    success = False
    try:
        run([pg_tool(c,'pg_restore',int(manifest['postgres_version'].split('.')[0])),'--jobs=4','--no-owner','--no-acl','--exit-on-error',
             '--dbname',name,destination/'catalog.dump'],env=pg_env(c,dbname=name),stdout=subprocess.DEVNULL)
        with db(c,dbname=name) as restored:
            restored.execute("SET TIME ZONE 'UTC'")
            actual = table_metrics(restored)
        if actual!=manifest['tables']:
            raise RuntimeError('Restored table counts, content fingerprints, or identity sequence differ')
        proof = {'verified_at':now(),'database':name,'tables':actual,'dump_sha256':manifest['dump_sha256'],'kept':keep}
        write_json(destination/'restore-verification.json',proof)
        success=True
        return proof
    finally:
        # Only remove the temporary database created by this exact invocation.
        if not keep:
            with db(c,dbname='postgres') as admin:
                admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))

def commit_publication(c, folder, journal, report):
    folder = Path(folder)
    tag = journal['tag']
    if not journal.get('assets_verified_at'):
        raise RuntimeError('Assets must be verified before publication')
    run(['git','fetch','origin','main'],cwd=ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    head=capture(['git','rev-parse','HEAD'],cwd=ROOT).strip()
    remote=capture(['git','rev-parse','origin/main'],cwd=ROOT).strip()
    if head!=remote:
        count=capture(['git','rev-list','--count','origin/main..HEAD'],cwd=ROOT).strip()
        message=capture(['git','log','-1','--format=%s'],cwd=ROOT).strip()
        changed=capture(['git','diff','--name-only','origin/main','HEAD'],cwd=ROOT).splitlines()
        own_pending=(count=='1' and message==f'Publish proxy catalog {folder.name}' and
                     all(p.startswith(('sources/','reports/')) or p=='latest.json' for p in changed))
        if not own_pending: raise RuntimeError('Local code and origin/main differ; update the checkout before publishing')
        latest = read_json(ROOT/'latest.json',{})
        if latest.get('tag') != tag or latest.get('manifest_sha256') != journal['manifest_sha256']:
            raise RuntimeError('Pending publication metadata does not match this snapshot')
        # Retry the existing commit; changing published_at here would create another
        # unpushed commit and prevent the next recovery attempt from recognizing it.
        run(['git','push','origin','HEAD:main'],cwd=ROOT)
        journal.update(metadata_pushed_at=now(),commit=head)
        write_json(folder/'publication.json',journal)
        return latest
    if release_record(c,tag)['draft']:
        gh(c,'release','edit',tag,'--repo',c['repository'],'--draft=false','--latest')
    registry = read_json(folder/'registry.json')
    if registry is not None:
        write_json(ROOT/'sources/registry.json',registry)
    if report.get('kind') != 'hourly_refresh':
        history = read_json(ROOT/'sources/discovery-history.json',[])
        if not any(x['run_id']==folder.name for x in history):
            history.append({'run_id':folder.name,'summary':report.get('research_summary'),
                            'validation':read_json(folder/'validation.json',[])})
        write_json(ROOT/'sources/discovery-history.json',history)
    write_json(ROOT/'reports'/f'{folder.name}.json',report)
    latest = {'tag':tag,'url':journal['url'],'manifest_sha256':journal['manifest_sha256'],
              'configurations':report['catalog_after'],'published_at':now(),'seed':journal.get('seed',False)}
    if report.get('kind') == 'hourly_refresh':
        latest.update(new_configurations=report['new_configurations'],
                      research_in_progress=any(not r['complete'] for r in report['research']),
                      pending_sources=report['pending_sources'],retry_sources=report['retry_sources'])
    write_json(ROOT/'latest.json',latest)
    staged = capture(['git','diff','--cached','--name-only'],cwd=ROOT).splitlines()
    if any(not (p.startswith('sources/') or p.startswith('reports/') or p=='latest.json') for p in staged):
        raise RuntimeError('Unrelated staged files; publication will not commit them')
    run(['git','add','sources','reports','latest.json'],cwd=ROOT,stdout=subprocess.DEVNULL)
    if capture(['git','diff','--cached','--name-only'],cwd=ROOT).strip():
        run(['git','commit','-m',f'Publish proxy catalog {folder.name}'],cwd=ROOT,stdout=subprocess.DEVNULL)
    run(['git','push','origin','HEAD:main'],cwd=ROOT)
    journal.update(metadata_pushed_at=now(),commit=capture(['git','rev-parse','HEAD'],cwd=ROOT).strip())
    write_json(folder/'publication.json',journal)
    return latest

def retained_tags(c, candidates, current_tag):
    from zoneinfo import ZoneInfo
    zone=ZoneInfo(c['timezone'])
    keep={current_tag}
    keep.update(r['tag_name'] for r in candidates[:c.get('continuous',{}).get('retain_hourly',0)])
    days=set(); weeks=set()
    for item in candidates:
        if item['name'].startswith('Initial seed:'):
            keep.add(item['tag_name'])
        at=datetime.fromisoformat(item['published_at'].replace('Z','+00:00')).astimezone(zone)
        day=at.date().isoformat(); week=at.strftime('%G-%V')
        if day not in days and len(days)<c['retain_daily']:
            days.add(day); keep.add(item['tag_name'])
        if week not in weeks and len(weeks)<c['retain_weekly']:
            weeks.add(week); keep.add(item['tag_name'])
    return keep


def prune(c, current_tag):
    """Retention only touches verified catalog releases created by this runner."""
    records=[]
    page=1
    while True:
        batch=gh_api(c,f'repos/{c["repository"]}/releases?per_page=100&page={page}')
        records.extend(batch)
        if len(batch)<100: break
        page+=1
    managed={j['tag'] for p in (LOCAL/'runs').glob('*/publication.json')
             if (j:=read_json(p,{})).get('metadata_pushed_at')}
    candidates=[r for r in records if r['tag_name'] in managed and not r['draft']
                and any(a['name']=='manifest.json' for a in r['assets'])]
    candidates.sort(key=lambda r:r['published_at'],reverse=True)
    keep=retained_tags(c,candidates,current_tag)
    for r in candidates:
        if r['tag_name'] not in keep:
            gh(c,'release','delete',r['tag_name'],'--repo',c['repository'],'--yes')
            local=LOCAL/'runs'/r['tag_name'][len(TAG_PREFIX):]/'snapshot'
            if local.is_dir(): shutil.rmtree(local)
