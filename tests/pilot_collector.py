"""Explicit integration pilot; creates and drops its own empty verification DB."""
import argparse
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import json
import subprocess
import uuid
from psycopg import sql
from common import ROOT, LOCAL, config, db, digest, pg_env, pg_tool, run, write_json
from catalog import COLLECTOR

def main():
    c=config(); ident=uuid.uuid4().hex[:10]
    name='proxy_catalog_verify_pilot_'+ident
    folder=LOCAL/'collector-pilot'/ident; folder.mkdir(parents=True)
    with db(c) as source:
        version=source.info.server_version//10000
        feeds=source.execute("SELECT url,kind,protocol_hints FROM proxy_lists WHERE enabled AND status='collected' AND url LIKE 'https://raw.githubusercontent.com/%' ORDER BY (fetch_state->>'unique_entries')::bigint ASC,url LIMIT 2").fetchall()
        proxies=source.execute('SELECT p.* FROM proxies p JOIN proxy_stats s USING(proxy_id) ORDER BY p.proxy_id LIMIT 2').fetchall()
        stats=source.execute('SELECT * FROM proxy_stats WHERE proxy_id=ANY(%s) ORDER BY proxy_id',([p['proxy_id'] for p in proxies],)).fetchall()
        assert len(stats)==2,'Pilot requires existing statistics to verify preservation'
    schema=folder/'schema.sql'
    run([pg_tool(c,'pg_dump',version),'--schema-only','--no-owner','--no-acl','--file',schema],env=pg_env(c))
    with db(c,dbname='postgres') as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    try:
        env=pg_env(c,dbname=name); env['PROXY_STORAGE']=str(folder/'storage')
        run([str(Path(pg_tool(c,'pg_dump',version)).with_name('psql')),'-X','-v','ON_ERROR_STOP=1','-f',schema],env=env,stdout=subprocess.DEVNULL)
        with db(c,dbname=name) as temp:
            for table,rows in [('proxies',proxies),('proxy_stats',stats)]:
                for row in rows:
                    keys=list(row)
                    values=[json.dumps(row[k]) if isinstance(row[k],dict) else row[k] for k in keys]
                    temp.execute(sql.SQL('INSERT INTO {} ({}) OVERRIDING SYSTEM VALUE VALUES ({})').format(
                        sql.Identifier(table),sql.SQL(',').join(map(sql.Identifier,keys)),sql.SQL(',').join(sql.Placeholder()*len(keys))),values)
            temp.execute("SELECT setval(pg_get_serial_sequence('proxies','proxy_id'),(SELECT max(proxy_id) FROM proxies))")
            for row in feeds:
                temp.execute('INSERT INTO proxy_lists(url,kind,protocol_hints,enabled) VALUES(%s,%s,%s,true)',(row['url'],row['kind'],row['protocol_hints']))
        collection=str(uuid.uuid4())
        with (folder/'collector.log').open('w') as log:
            run([sys.executable,COLLECTOR,'--new-run-id',collection,'--take','1'],env=env,stdout=log,stderr=subprocess.STDOUT)
            with db(c,dbname=name) as temp:
                assert temp.execute("SELECT count(*) AS n FROM proxy_lists WHERE status='pending'").fetchone()['n']==1
            run([sys.executable,COLLECTOR,'--run-id',collection],env=env,stdout=log,stderr=subprocess.STDOUT)
            with db(c,dbname=name) as temp:
                assert temp.execute("SELECT count(*) AS n FROM proxy_lists WHERE status='pending'").fetchone()['n']==0
                before=temp.execute('SELECT proxy_id,connection_key FROM proxies ORDER BY proxy_id').fetchall()
                assert len(before)>len(proxies),'Expected at least one imported configuration'
            # Reparse the saved payloads: deterministic repeat-import check without source drift.
            run([sys.executable,COLLECTOR,'--run-id',collection,'--reparse'],env=env,stdout=log,stderr=subprocess.STDOUT)
            with db(c,dbname=name) as temp:
                after=temp.execute('SELECT proxy_id,connection_key FROM proxies ORDER BY proxy_id').fetchall()
                after_stats=temp.execute('SELECT * FROM proxy_stats ORDER BY proxy_id').fetchall()
                assert before==after,'Repeat import changed configuration IDs or added duplicates'
                assert after_stats==stats,'Collector modified existing proxy statistics'
                assert all(any(p['proxy_id']==r['proxy_id'] and p['connection_key']==r['connection_key'] for r in after) for p in proxies)
        proof={'database':name,'sources':len(feeds),'configurations':len(after),'resume_passed':True,
               'repeat_import_preserved_ids':True,'statistics_preserved':True,'live_database_untouched':True}
        write_json(folder/'result.json',proof); print(json.dumps(proof))
    finally:
        with db(c,dbname='postgres') as admin:
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))

if __name__=='__main__': main()
