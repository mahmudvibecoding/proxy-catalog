"""Import saved discoveries and publish verified snapshots during research."""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import plistlib
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

from common import ROOT, LOCAL, config, db, digest, lock, now, read_json, run, write_json
import catalog
import publication

sys.path.insert(0, str(ROOT / 'vendor'))
import collect_proxies as collector

WORK = LOCAL / 'continuous'
LABEL = 'com.mahmud.proxy-catalog'


def settings(c):
    return {'enabled': False, 'intake_seconds': 300, 'publish_seconds': 3600,
            'retry_seconds': 300, 'terminal_retry_seconds': 86400,
            'retain_hourly': 24, 'local_snapshots': 2, 'min_free_bytes': 3 * 1024**3,
            'parse_workers': max(1, min(4, (os.cpu_count() or 2) // 2)),
            'import_workers': 4,
            'max_source_bytes': 256 * 1024**2, **c.get('continuous', {})}


def enabled(c):
    return settings(c)['enabled'] and not read_json(LOCAL / 'state.json', {}).get('disabled')


@contextmanager
def import_guard(home=WORK, shared=False):
    home.mkdir(parents=True, exist_ok=True)
    with (home / 'import.lock').open('a+') as stream, (home / 'import-gate.lock').open('a+') as gate:
        # A waiting publisher holds the entry gate, preventing a continuous
        # stream of shared imports from starving its exclusive snapshot lock.
        fcntl.flock(gate, fcntl.LOCK_EX)
        try:
            fcntl.flock(stream, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        finally:
            fcntl.flock(gate, fcntl.LOCK_UN)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


class Journal:
    def __init__(self, home=WORK):
        self.home = Path(home)
        self.home.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.home / 'queue.sqlite3', check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.mutex = threading.RLock()
        self.conn.executescript('''PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=30000;
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS inputs(run_id TEXT PRIMARY KEY,signature TEXT NOT NULL,
                sha256 TEXT NOT NULL,candidates INTEGER NOT NULL,read_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,run_id TEXT NOT NULL,url TEXT NOT NULL,
                version TEXT NOT NULL,body TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,
                result TEXT,inserted INTEGER NOT NULL DEFAULT 0,refreshed INTEGER NOT NULL DEFAULT 0,
                error TEXT,updated_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(state,retry_at,attempts);
            CREATE INDEX IF NOT EXISTS jobs_url ON jobs(url,state);
            CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(
                CASE WHEN json_extract(result,'$.status') IN ('downloaded','downloaded_partial')
                    THEN 0 ELSE 1 END,attempts) WHERE state IN ('pending','retry');
            CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY,job_id TEXT NOT NULL,
                inserted INTEGER NOT NULL,refreshed INTEGER NOT NULL,applied_at TEXT NOT NULL);
        ''')
        self.conn.commit()

    @contextmanager
    def transaction(self):
        with self.mutex:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                yield self.conn
            except BaseException:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()

    def get(self, key, default=None):
        with self.mutex:
            row = self.conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
            return json.loads(row[0]) if row else default

    @contextmanager
    def snapshot(self):
        with self.mutex:
            self.conn.execute('BEGIN')
            try:
                yield
            finally:
                self.conn.rollback()

    def set(self, key, value):
        with self.transaction() as conn:
            conn.execute('INSERT OR REPLACE INTO settings VALUES(?,?)', (key, json.dumps(value)))

    def enqueue(self, run_id, rows, signature, checksum):
        added = 0
        with self.transaction() as conn:
            for row in rows:
                # Notes/evidence can grow while the source URL and parser hints stay the same.
                version = hashlib.sha256(json.dumps([row['url'], row['kind'], row['protocol_hints']],
                                                   sort_keys=True).encode()).hexdigest()
                ident = hashlib.sha256((run_id + '\0' + version).encode()).hexdigest()
                cursor = conn.execute('''INSERT OR IGNORE INTO jobs
                    (id,run_id,url,version,body,updated_at) VALUES(?,?,?,?,?,?)''',
                    (ident, run_id, row['url'], version, json.dumps(row), now()))
                if cursor.rowcount:
                    added += 1
                    conn.execute("""UPDATE jobs SET state='superseded' WHERE url=?
                        AND id<>? AND state IN ('pending','retry')
                        AND coalesce(json_extract(result,'$.status'),'') NOT IN ('downloaded','downloaded_partial')""",
                        (row['url'], ident))
            conn.execute('INSERT OR REPLACE INTO inputs VALUES(?,?,?,?,?)',
                         (run_id, signature, checksum, len(rows), now()))
        return added

    def recover(self):
        with self.transaction() as conn:
            conn.execute("UPDATE jobs SET state='pending' WHERE state='leased'")

    def claim(self, at=None):
        with self.transaction() as conn:
            # Traverse ready jobs in priority order instead of sorting the entire backlog.
            row = conn.execute("""SELECT * FROM jobs j INDEXED BY jobs_claim
                WHERE state IN ('pending','retry') AND retry_at<=?
                AND NOT EXISTS(SELECT 1 FROM jobs x WHERE x.url=j.url AND x.state='leased')
                AND NOT EXISTS(SELECT 1 FROM jobs x WHERE x.url=j.url AND x.id<>j.id
                    AND x.state IN ('pending','retry') AND x.rowid<j.rowid
                    AND json_extract(x.result,'$.status') IN ('downloaded','downloaded_partial'))
                ORDER BY CASE WHEN json_extract(result,'$.status') IN ('downloaded','downloaded_partial')
                    THEN 0 ELSE 1 END,attempts,rowid LIMIT 1""", (time.time() if at is None else at,)).fetchone()
            if row:
                conn.execute("UPDATE jobs SET state='leased',attempts=attempts+1,updated_at=? WHERE id=?",
                             (now(), row['id']))
                return dict(row)

    def downloaded(self, job, result):
        with self.transaction() as conn:
            conn.execute('UPDATE jobs SET result=?,updated_at=? WHERE id=?',
                         (json.dumps(result, default=str), now(), job['id']))

    def finish(self, job, result, imported=False, retry_seconds=None, error=None):
        with self.transaction() as conn:
            previous = conn.execute('SELECT state FROM jobs WHERE id=?', (job['id'],)).fetchone()
            if previous is None or previous['state'] == 'imported':
                return
            partial = imported and result.get('status') == 'collected_partial'
            state = 'imported' if imported and not partial else ('retry' if retry_seconds is not None else 'empty')
            conn.execute('''UPDATE jobs SET state=?,result=?,inserted=?,refreshed=?,error=?,
                retry_at=?,updated_at=? WHERE id=?''',
                (state, json.dumps(result, default=str), result.get('inserted', 0),
                 result.get('refreshed', 0), error, time.time() + (retry_seconds or 0), now(), job['id']))
            if imported:
                receipt = conn.execute('INSERT OR IGNORE INTO receipts VALUES(?,?,?,?,?)',
                    (result['continuous_receipt'],job['id'],result.get('inserted',0),result.get('refreshed',0),now()))
                if receipt.rowcount:
                    row = conn.execute("SELECT value FROM settings WHERE key='revision'").fetchone()
                    revision = int(json.loads(row[0])) if row else 0
                    conn.execute("INSERT OR REPLACE INTO settings VALUES('revision',?)", (str(revision + 1),))

    def stats(self):
        with self.mutex:
            states = dict(self.conn.execute('SELECT state,count(*) FROM jobs GROUP BY state'))
            totals = self.conn.execute('SELECT coalesce(sum(inserted),0),coalesce(sum(refreshed),0) FROM receipts').fetchone()
            inputs = [dict(r) for r in self.conn.execute('SELECT * FROM inputs ORDER BY run_id')]
            return {'states': states, 'candidates': sum(states.values()), 'inserted': totals[0],
                    'refreshed': totals[1], 'revision': self.get('revision', 0), 'inputs': inputs}

    def export(self, path):
        with self.mutex, gzip.open(path, 'wt', encoding='utf-8', compresslevel=3) as out:
            for row in self.conn.execute('SELECT * FROM jobs ORDER BY run_id,url,version'):
                value = dict(row)
                value['body'] = json.loads(value['body'])
                value['result'] = json.loads(value['result']) if value['result'] else None
                out.write(json.dumps(value, ensure_ascii=False) + '\n')

    def close(self):
        self.conn.close()


def intake(journal, run_folder, force=False):
    with lock(journal.home / 'intake.lock'):
        return _intake(journal, run_folder, force)


def _intake(journal, run_folder, force=False):
    """Read an immutable copy; never write into the running researcher's workspace."""
    folder = Path(run_folder)
    source = folder / 'research' / 'candidates.json'
    if not source.exists():
        return {'run_id': folder.name, 'added': 0, 'waiting_for': 'saved_candidates'}
    stat = source.stat()
    signature = json.dumps([stat.st_ino, stat.st_size, stat.st_mtime_ns])
    with journal.mutex:
        prior = journal.conn.execute('SELECT signature FROM inputs WHERE run_id=?', (folder.name,)).fetchone()
    if prior and prior[0] == signature and not force:
        return {'run_id': folder.name, 'added': 0, 'unchanged': True}
    target = journal.home / 'intake' / folder.name / 'candidates.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix('.json.tmp')
    with source.open('rb') as incoming, temp.open('wb') as outgoing:
        before = os.fstat(incoming.fileno())
        shutil.copyfileobj(incoming, outgoing, 1024 * 1024)
        after = os.fstat(incoming.fileno())
        outgoing.flush()
        os.fsync(outgoing.fileno())
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        temp.unlink()
        raise RuntimeError('Candidate checkpoint changed during copying; retry intake')
    rows = catalog.candidate_rows(temp)
    checksum = digest(temp)
    temp.replace(target)
    added = journal.enqueue(folder.name, rows, signature, checksum)
    journal.set('last_intake_at', time.time())
    return {'run_id': folder.name, 'candidates': len(rows), 'added': added, 'sha256': checksum}


def intake_current(journal, force=False):
    state = read_json(LOCAL / 'state.json', {})
    run_id = state.get('active_run') or state.get('last_run')
    return intake(journal, LOCAL / 'runs' / run_id, force) if run_id else {'added': 0}


def configure_collector(c):
    collector.DB = dict(c['database'])
    collector.STORAGE = Path(c.get('storage', str(LOCAL / 'proxy-collection')))
    for sub in ('payloads', 'parsed', 'tmp', 'runs'):
        (collector.STORAGE / sub).mkdir(parents=True, exist_ok=True)


def apply_download(c, journal, job, downloaded, store=None):
    """PG receipt and source data commit together; an unacknowledged commit replays once."""
    result = dict(downloaded)
    row = json.loads(job['body'])
    if isinstance(result.get('finished_at'), str):
        result['finished_at'] = datetime.fromisoformat(result['finished_at'])
    receipt_id = hashlib.sha256((job['id'] + '\0' + result.get('content_sha256','') + '\0' +
                    result.get('finished_at', datetime.now(timezone.utc)).isoformat()).encode()).hexdigest()
    owned = store is None
    try:
        if result['status'] not in ('downloaded', 'downloaded_partial'):
            journal.finish(job, result, retry_seconds=settings(c)['retry_seconds'], error=result['status'])
            return result
        if store is None:
            store = collector.Store(acquire_lock=False)
        _, metrics = store.parsed_cache(result)
        if not metrics['unique_entries'] or metrics.get('warnings', {}).get('html_response'):
            result['status'] = 'no_valid_entries'
            journal.finish(job, result, retry_seconds=settings(c)['terminal_retry_seconds'], error=result['status'])
            return result
        # PG18 gets exact counts from RETURNING, so independent sources can
        # commit concurrently. Publishers and the legacy collector remain exclusive.
        parallel = store.writer.info.server_version >= 180000
        with import_guard(journal.home, shared=parallel):
            with store.writer.transaction():
                advisory = 'pg_advisory_xact_lock_shared' if parallel else 'pg_advisory_xact_lock'
                store.writer.execute(f"SELECT {advisory}(hashtextextended('proxy:proxy_collection',0))")
                previous = store.writer.execute('SELECT run_id,status,fetch_state FROM proxy_lists WHERE url=%s',
                                                (row['url'],)).fetchone()
                saved = previous['fetch_state'] if previous else {}
                if saved.get('continuous_receipt') == receipt_id:
                    outcome = {'status': previous['status'], 'unique_entries': saved.get('unique_entries', 0),
                               'inserted': saved.get('receipt_inserted', 0), 'refreshed': saved.get('receipt_refreshed', 0)}
                else:
                    if previous and previous['status'] == 'pending':
                        raise RuntimeError('Source belongs to an unfinished collector run')
                    store.run_id = uuid.uuid5(uuid.NAMESPACE_URL, 'proxy-catalog-source:' + job['id'])
                    store.writer.execute('''INSERT INTO proxy_lists(url,kind,protocol_hints,enabled,run_id,status)
                        VALUES(%s,%s,%s,true,%s,'pending') ON CONFLICT(url) DO UPDATE SET
                        kind=excluded.kind,protocol_hints=excluded.protocol_hints,enabled=true,
                        run_id=excluded.run_id,status='pending' ''',
                        (row['url'], row['kind'], row['protocol_hints'], store.run_id))
                    result['continuous_receipt'] = receipt_id
                    outcome = store.save(result)
            # The file lock covers the SQLite acknowledgement too, so publication sees matching counts.
            result.update(outcome)
            result['continuous_receipt'] = receipt_id
            journal.finish(job, result, imported=True,
                           retry_seconds=settings(c)['retry_seconds'] if result['status']=='collected_partial' else None)
        return result
    finally:
        if owned and store is not None:
            store.close()


class Importer:
    """Each import thread reuses its own connections and staging table."""
    def __init__(self, c, journal):
        self.c, self.journal = c, journal
        self.stores = {}

    def apply(self, job, result):
        ident = threading.get_ident()
        try:
            if result['status'] in ('downloaded', 'downloaded_partial') and ident not in self.stores:
                self.stores[ident] = collector.Store(acquire_lock=False)
            return apply_download(self.c, self.journal, job, result, self.stores.get(ident))
        except BaseException:
            store = self.stores.pop(ident, None)
            if store is not None:
                store.close()
            raise

    def close(self):
        # Called only after every submitted import has completed or been cancelled.
        for store in self.stores.values():
            store.close()
        self.stores.clear()


async def collect(c, journal, limit=None, once=False):
    configure_collector(c)
    await asyncio.to_thread(journal.recover)
    options = argparse.Namespace(concurrency=c['concurrency'], per_host=c['per_host'],
        github_concurrency=c['github_concurrency'], retries=c['retries'], max_bytes=settings(c)['max_source_bytes'])
    downloader = collector.Downloader(options)
    executor = ThreadPoolExecutor(max_workers=settings(c)['import_workers'], thread_name_prefix='proxy-import')
    parsers = ProcessPoolExecutor(max_workers=settings(c)['parse_workers'],
                                  mp_context=multiprocessing.get_context('spawn'))
    importer = Importer(c, journal)
    claim_lock = asyncio.Lock()
    claimed = finished = 0
    waiting_for_space = False
    stopping = asyncio.Event()

    async def monitor():
        while not stopping.is_set():
            stats = await asyncio.to_thread(journal.stats)
            await asyncio.to_thread(write_json, journal.home / 'collector-status.json',
                       {'at': now(), 'pid': os.getpid(), 'state': 'running', 'finished_this_process': finished,
                        'parse_workers': settings(c)['parse_workers'],
                        'import_workers': settings(c)['import_workers'], **stats})
            last_intake = await asyncio.to_thread(journal.get, 'last_intake_at', 0)
            if not once and time.time() - last_intake >= settings(c)['intake_seconds']:
                try:
                    await asyncio.to_thread(intake_current, journal)
                except (OSError, ValueError, RuntimeError) as exc:
                    write_json(journal.home / 'intake-error.json', {'at': now(), 'error': str(exc)})
                    await asyncio.to_thread(journal.set, 'last_intake_at', time.time())
            await asyncio.sleep(10)

    async def worker():
        nonlocal claimed, finished, waiting_for_space
        while not stopping.is_set() and (limit is None or claimed < limit):
            reserve = settings(c)['min_free_bytes'] + min(c['concurrency'], limit or c['concurrency']) * options.max_bytes
            if shutil.disk_usage(collector.STORAGE).free < reserve:
                waiting_for_space = True
                write_json(journal.home / 'storage-wait.json', {'at': now(), 'required_free_bytes': reserve,
                           'free_bytes': shutil.disk_usage(collector.STORAGE).free, 'state': 'waiting_for_disk_space'})
                return
            (journal.home / 'storage-wait.json').unlink(missing_ok=True)
            async with claim_lock:
                if limit is not None and claimed >= limit:
                    return
                job = await asyncio.to_thread(journal.claim)
                if job is None:
                    return
                claimed += 1
            result = {}
            try:
                saved = json.loads(job['result']) if job['result'] else None
                if saved and saved.get('status') in ('downloaded', 'downloaded_partial'):
                    result = saved
                else:
                    row = json.loads(job['body'])
                    result = await downloader.fetch({'list_url': row['url'], 'protocol_hints': row['protocol_hints']})
                    await asyncio.to_thread(journal.downloaded, job, result)
                loop = asyncio.get_running_loop()
                if result['status'] in ('downloaded', 'downloaded_partial'):
                    await loop.run_in_executor(parsers, collector.prepare_import, result, str(collector.STORAGE))
                submitted = executor.submit(importer.apply, job, result)
                future = asyncio.wrap_future(submitted)
                try:
                    await asyncio.shield(future)
                except asyncio.CancelledError:
                    # Queued imports already have durable downloads. Leave them
                    # for recovery; only wait for transactions currently running.
                    if not submitted.cancel():
                        await future
                    raise
            except (OSError, ValueError, RuntimeError, collector.psycopg.Error) as exc:
                # Keep downloaded pages when an import fails; retries can replay the PG receipt.
                await asyncio.to_thread(journal.finish, job, result,
                               retry_seconds=settings(c)['retry_seconds'], error=type(exc).__name__ + ': ' + str(exc)[:300])
            finished += 1

    reporter = asyncio.create_task(monitor())
    try:
        async with asyncio.TaskGroup() as group:
            for _ in range(c['concurrency']):
                group.create_task(worker())
    finally:
        stopping.set()
        reporter.cancel()
        await asyncio.gather(reporter, return_exceptions=True)
        await downloader.client.aclose()
        await asyncio.to_thread(parsers.shutdown, wait=True, cancel_futures=True)
        await asyncio.get_running_loop().run_in_executor(executor, importer.close)
        executor.shutdown(wait=True)
        write_json(journal.home / 'collector-status.json',
                   {'at': now(), 'pid': os.getpid(), 'state': 'waiting_for_disk_space' if waiting_for_space else 'idle', 'finished_this_process': finished,
                    **journal.stats()})
    return {'finished': finished, **journal.stats()}


def research_status(inputs):
    result = []
    for item in inputs:
        record = read_json(LOCAL / 'runs' / item['run_id'] / 'run.json', {})
        result.append({'run_id': item['run_id'], 'stage': record.get('stage'),
                       'session': record.get('discovery_session'),
                       'complete': 'discovery' in record.get('completed', []),
                       'candidate_checkpoint_at': item['read_at'], 'candidates': item['candidates']})
    return result


def prepare_publication(c, journal, folder, connection):
    # Held until the snapshot is complete. Network discovery and downloads continue.
    with journal.snapshot():
        queue = journal.stats()
        journal.export(folder / 'intake-results.jsonl.gz')
    previous = read_json(ROOT / 'latest.json', {})
    total = connection.execute('SELECT count(*) AS n FROM proxies').fetchone()['n']
    before = previous.get('configurations', total)
    if total < before:
        raise RuntimeError('Catalog count decreased since the last published snapshot')
    states = queue['states']
    report = {'run_id': folder.name, 'kind': 'hourly_refresh', 'created_at': now(),
        'catalog_before': before, 'new_configurations': total - before, 'catalog_after': total,
        'candidates': queue['candidates'], 'validated_sources': states.get('imported', 0),
        'pending_sources': states.get('pending', 0) + states.get('leased', 0),
        'retry_sources': states.get('retry', 0), 'superseded_sources': states.get('superseded', 0),
        'revision': queue['revision'], 'research': research_status(queue['inputs']),
        'research_summary': {'completion_status': 'incomplete' if any(not x['complete'] for x in research_status(queue['inputs'])) else 'complete'},
        'proxy_testing': 'stopped', 'collection': {'source_states': states,
            'inserted_since_worker_start': queue['inserted'], 'refreshed_since_worker_start': queue['refreshed']}}
    write_json(folder / 'report.json', report)
    catalog.archive_sources(c, folder / 'source-results.jsonl.gz', connection=connection)
    return report


def publish(c, journal, force=False, restore_audit=False):
    with lock(journal.home / 'publisher.lock'):
        state_path = journal.home / 'publication-state.json'
        state = read_json(state_path, {})
        due = time.time() - state.get('last_published_epoch', 0) >= settings(c)['publish_seconds']
        if not state.get('active') and not force and (not due or journal.get('revision', 0) <= state.get('published_revision', 0)):
            return {'state': 'unchanged', 'last_publication': state.get('last_publication')}
        if not state.get('active'):
            ident = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-hourly-' + uuid.uuid4().hex[:6]
            state['active'] = ident
            write_json(state_path, state)
        folder = LOCAL / 'runs' / state['active']
        folder.mkdir(parents=True, exist_ok=True)
        record = read_json(folder / 'hourly.json', {'id': folder.name, 'created_at': now(), 'completed': []})

        def stage(name, fn):
            if name in record['completed']:
                return
            record.update(stage=name, updated_at=now())
            write_json(folder / 'hourly.json', record)
            write_json(journal.home / 'publisher-status.json', {'at': now(), 'pid': os.getpid(),
                       'state': 'running', 'publication': folder.name, 'stage': name})
            fn()
            record['completed'].append(name)
            write_json(folder / 'hourly.json', record)

        def snapshot():
            with import_guard(journal.home):
                publication.make_snapshot(c, folder, lambda connection: prepare_publication(c, journal, folder, connection))

        stage('snapshot', snapshot)
        manifest = read_json(folder / 'snapshot/manifest.json')
        report = read_json(folder / 'report.json')
        stage('upload', lambda: publication.publish_assets(c, folder, manifest))
        receipt = read_json(folder / 'publication.json')
        week = datetime.now(timezone.utc).strftime('%G-%V')
        if restore_audit or state.get('last_restore_week') != week:
            stage('download_audit', lambda: publication.download(c, receipt['tag'], folder / 'downloaded', receipt['manifest_sha256']))
            stage('restore_audit', lambda: publication.restore_verify(c, folder / 'downloaded'))
            state['last_restore_week'] = week
        stage('publish', lambda: publication.commit_publication(c, folder, receipt, report))
        state.update(last_publication=folder.name, last_published_epoch=time.time(),
                     published_revision=report['revision'])
        write_json(state_path, state)
        stage('retention', lambda: publication.prune(c, receipt['tag']))
        cleanup_local(c, folder)
        state['active'] = None
        write_json(state_path, state)
        record.update(stage='completed', finished_at=now())
        write_json(folder / 'hourly.json', record)
        result = {'at': now(), 'state': 'published', 'tag': receipt['tag'], 'url': receipt['url'],
                  'configurations': report['catalog_after'], 'new_configurations': report['new_configurations'],
                  'revision': report['revision']}
        write_json(journal.home / 'publisher-status.json', result)
        return result


def cleanup_local(c, current):
    downloaded = current / 'downloaded'
    if (downloaded / 'restore-verification.json').exists():
        for name in ('restore-verification.json', 'download-verification.json'):
            shutil.copyfile(downloaded / name, current / name)
        shutil.rmtree(downloaded)
    (current / 'snapshot/catalog.dump').unlink(missing_ok=True)
    completed = []
    for path in (LOCAL / 'runs').glob('*/publication.json'):
        receipt = read_json(path, {})
        if receipt.get('metadata_pushed_at'):
            completed.append((receipt['metadata_pushed_at'], path.parent))
    completed.sort(reverse=True)
    keep = {folder for _, folder in completed[:settings(c)['local_snapshots']]}
    keep.add(current)
    for _, folder in completed:
        if folder not in keep and (folder / 'snapshot').is_dir():
            shutil.rmtree(folder / 'snapshot')


def install(c):
    if not settings(c)['enabled']:
        raise ValueError('Enable continuous publication in config.local.json first')
    WORK.mkdir(parents=True, exist_ok=True)
    for role in ('collect', 'publish'):
        label = LABEL + '.' + role
        path = Path.home() / 'Library/LaunchAgents' / (label + '.plist')
        data = {'Label': label, 'ProgramArguments': [str(ROOT / '.venv/bin/python'), str(ROOT / 'continuous.py'), role, '--check'],
            'WorkingDirectory': str(ROOT), 'RunAtLoad': True, 'StartInterval': 60, 'ProcessType': 'Background',
            'StandardOutPath': str(WORK / (role + '.log')), 'StandardErrorPath': str(WORK / (role + '-error.log')),
            'EnvironmentVariables': {'PATH': '/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin'},
            'AbandonProcessGroup': False, 'ExitTimeOut': 30}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(plistlib.dumps(data))
        path.chmod(0o600)
        subprocess.run(['/bin/launchctl', 'bootout', f'gui/{os.getuid()}/{label}'], capture_output=True)
        run(['/bin/launchctl', 'bootstrap', f'gui/{os.getuid()}', path])
    return {'installed': True, 'intake_seconds': settings(c)['intake_seconds'], 'publish_seconds': settings(c)['publish_seconds']}


def disable():
    for role in ('collect', 'publish'):
        label = LABEL + '.' + role
        subprocess.run(['/bin/launchctl', 'bootout', f'gui/{os.getuid()}/{label}'], capture_output=True)
        path = Path.home() / 'Library/LaunchAgents' / (label + '.plist')
        if path.exists():
            WORK.mkdir(parents=True, exist_ok=True)
            path.rename(WORK / ('disabled-' + role + '.plist'))


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('intake', 'collect', 'publish', 'status', 'install', 'disable'))
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--restore-audit', action='store_true')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--run', type=Path)
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error('--limit must be positive')
    c = config()
    if args.command == 'install':
        print(json.dumps(install(c))); return
    if args.command == 'disable':
        disable(); return
    if args.check and not enabled(c):
        return
    journal = Journal()
    awake = None
    try:
        with catalog.shutdown_signals():
            if args.command == 'status':
                value = {'queue': journal.stats(), 'collector': read_json(WORK / 'collector-status.json'),
                         'publisher': read_json(WORK / 'publisher-status.json'), 'publication': read_json(WORK / 'publication-state.json')}
            elif args.command == 'intake':
                with lock(WORK / 'collector.lock'):
                    value = intake(journal, args.run, args.force) if args.run else intake_current(journal, args.force)
            else:
                readiness = catalog.readiness(c)
                if not readiness['ready']:
                    print(json.dumps(readiness)); return
                awake = subprocess.Popen(['/usr/bin/caffeinate', '-i', '-w', str(os.getpid())],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if args.command == 'publish':
                    value = publish(c, journal, args.force, args.restore_audit)
                else:
                    with lock(WORK / 'collector.lock'):
                        if args.run:
                            intake(journal, args.run, args.force)
                        elif time.time() - journal.get('last_intake_at', 0) >= settings(c)['intake_seconds']:
                            intake_current(journal)
                        value = asyncio.run(collect(c, journal, args.limit, once=bool(args.limit)))
            print(json.dumps(value, default=str), flush=True)
    except BlockingIOError:
        print(json.dumps({'state': 'already_running'}), flush=True)
    except catalog.RunInterrupted:
        print(json.dumps({'state': 'interrupted', 'checkpoints_preserved': True}), flush=True)
    except Exception as exc:
        write_json(WORK / (args.command + '-failure.json'), {'at': now(), 'error': type(exc).__name__, 'detail': str(exc)[:500]})
        raise
    finally:
        if awake:
            awake.terminate()
        journal.close()


if __name__ == '__main__':
    main()
