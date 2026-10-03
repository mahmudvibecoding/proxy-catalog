"""Explicit shutdown/readiness pilot using a fake research CLI and a private PG cluster."""
from pathlib import Path
import json
import os
import plistlib
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common import ROOT, LOCAL, config, db, now, pg_tool, read_json, write_json
import catalog
import postgres_service


def wait_for(check, seconds=30):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        result=check()
        if result: return result
        time.sleep(0.1)
    raise AssertionError('Pilot condition did not become true')


def runner_pilot(folder):
    local=folder/'runner'; runfolder=local/'runs'/'pilot'; research=runfolder/'research'
    research.mkdir(parents=True)
    write_json(local/'state.json',{'active_run':'pilot'})
    write_json(runfolder/'run.json',{'id':'pilot','stage':'discovery','completed':['baseline'],
               'attempts':0,'issues':[],'discovery_session':'saved-pilot-session'})
    original=[{'url':'https://example.com/original'}]
    write_json(research/'candidates.json',original)
    fake=folder/'fake_codex.py'
    fake.write_text(f'#!{sys.executable}\n'+textwrap.dedent('''
        import json,os,sys,time
        from pathlib import Path
        root=Path.cwd()
        session=sys.argv[sys.argv.index('resume')+1]
        (root/'child.json').write_text(json.dumps({'pid':os.getpid(),'session':session}))
        print(json.dumps({'type':'thread.started','thread_id':session}),flush=True)
        print(json.dumps({'type':'turn.started'}),flush=True)
        p=root/'candidates.json'; rows=json.loads(p.read_text())
        if not any(x['url']=='https://example.com/saved-during-run' for x in rows):
            rows.append({'url':'https://example.com/saved-during-run'})
        tmp=p.with_suffix('.tmp'); tmp.write_text(json.dumps(rows)); tmp.replace(p)
        (root/'checkpoint-ready').touch()
        while not (root/'finish').exists():time.sleep(0.1)
        (root/'research_summary.json').write_text(json.dumps({'completion_status':'complete','summary':'Pilot completed'}))
        print(json.dumps({'type':'turn.completed','usage':{}}),flush=True)
    '''))
    fake.chmod(0o700)
    c={'timezone':'Asia/Tashkent','model':'gpt-6.1-sol','reasoning_effort':'max','codex':str(fake)}
    fixture=folder/'runner_fixture.py'
    fixture.write_text(textwrap.dedent(f'''
        import sys
        from pathlib import Path
        sys.path.insert(0,{str(ROOT)!r})
        import catalog
        from common import lock,write_json
        local=Path({str(local)!r})
        catalog.LOCAL=local
        catalog.STATE=local/'state.json'
        catalog.config=lambda:{c!r}
        catalog.lock=lambda:lock(local/'runner.lock')
        catalog.source_records=lambda *args,**kwargs:[]
        catalog.session_active=lambda:True
        catalog.readiness=lambda c:{{'ready':(local/'dependencies-ready').exists(),'waiting_for':'database'}}
        async def validation(c,folder):
            write_json(folder/'validation-reached.json',{{'reached':True}})
            raise RuntimeError('Pilot stops before any database import')
        catalog.validate_candidates=validation
        sys.exit(catalog.main())
    '''))
    command=[sys.executable,str(fixture)]
    with (folder/'runner.log').open('w') as output:
        subprocess.run(command+['check'],stdout=output,stderr=subprocess.STDOUT,check=True)
        assert read_json(runfolder/'run.json')['attempts']==0
        assert read_json(research/'candidates.json')==original
        (local/'dependencies-ready').touch()
        first=subprocess.Popen(command+['check'],stdout=output,stderr=subprocess.STDOUT)
        try:
            wait_for(lambda:(research/'checkpoint-ready').exists())
            child=read_json(research/'child.json')
            duplicate=subprocess.run(command+['check'],capture_output=True,text=True,check=True)
            assert 'Another runner is already active' in duplicate.stdout
            first.send_signal(signal.SIGTERM)
            assert first.wait(timeout=30)==128+signal.SIGTERM
            stopped=read_json(runfolder/'run.json')
            assert stopped['stage']=='discovery' and stopped['completed']==['baseline']
            assert stopped['discovery_session']=='saved-pilot-session'
            assert stopped['error_kind']=='interrupted'
            assert len(read_json(research/'candidates.json'))==2
            try:os.kill(child['pid'],0)
            except ProcessLookupError:pass
            else:raise AssertionError('Research child survived shutdown')
            (research/'finish').touch()
            second=subprocess.run(command+['resume'],stdout=output,stderr=subprocess.STDOUT,timeout=30)
            assert second.returncode!=0  # Deliberate stop at validation, before touching any database.
            resumed=read_json(runfolder/'run.json')
            assert 'discovery' in resumed['completed'] and resumed['stage']=='validation'
            assert read_json(research/'child.json')['session']=='saved-pilot-session'
            assert len(read_json(runfolder/'candidates.json'))==2
            assert read_json(runfolder/'validation-reached.json')['reached']
        finally:
            if first.poll() is None:first.terminate();first.wait(timeout=30)
    return {'dependency_wait_preserved_attempts':True,'duplicate_runner_rejected':True,
            'sigterm_preserved_discovery':True,'child_stopped':True,'same_session_resumed':True,
            'saved_candidates_preserved':True,'completion_required_before_validation':True}


def postgres_pilot(folder):
    c=config()
    with db(c) as connection: major=connection.info.server_version//10000
    initdb=pg_tool(c,'initdb',major); pg_ctl=pg_tool(c,'pg_ctl',major)
    label='com.mahmud.proxy-catalog.recovery-test-'+uuid.uuid4().hex[:8]
    target=f'gui/{os.getuid()}/{label}'
    with tempfile.TemporaryDirectory(prefix='pc-recovery-',dir='/tmp') as temp:
        temp=Path(temp); data=temp/'data'; socket=temp/'socket'; socket.mkdir()
        with (folder/'initdb.log').open('w') as output:
            subprocess.run([initdb,'-D',str(data),'-A','trust','--no-locale','--encoding=UTF8'],stdout=output,stderr=subprocess.STDOUT,check=True)
        with (data/'postgresql.conf').open('a') as f:
            f.write(f"\nlisten_addresses = ''\nunix_socket_directories = '{socket}'\n")
        test={'postgres_service':{'data_directory':str(data)},
              'database':{'dbname':'postgres','user':c['database']['user'],'host':str(socket),'port':5432}}
        configuration=temp/'config.json'; write_json(configuration,test)
        plist=temp/'service.plist'; plist.write_bytes(plistlib.dumps(postgres_service.launchagent(test,configuration,label,folder)))
        def ready():
            try:
                with db(test,connect_timeout=1) as connection:return connection.execute('SELECT 1 AS ok').fetchone()['ok']==1
            except Exception:return False
        # Start an existing server first: installing the service must not restart it.
        subprocess.run([pg_ctl,'-D',str(data),'-l',str(folder/'existing-postgres.log'),'-w','start'],capture_output=True,check=True)
        original_pid=int((data/'postmaster.pid').read_text().splitlines()[0])
        loaded=False
        try:
            subprocess.run(['/bin/launchctl','bootstrap',f'gui/{os.getuid()}',str(plist)],check=True)
            loaded=True
            wait_for(lambda:(folder/'postgres-service.log').exists() and 'existing_postgres_running' in (folder/'postgres-service.log').read_text())
            assert int((data/'postmaster.pid').read_text().splitlines()[0])==original_pid
            # Delay the database: the same service must start it when the existing server exits.
            subprocess.run([pg_ctl,'-D',str(data),'-m','fast','-w','stop'],capture_output=True,check=True)
            wait_for(ready)
            managed_pid=int((data/'postmaster.pid').read_text().splitlines()[0])
            assert managed_pid!=original_pid
            # Restart only this test LaunchAgent, using the installed service definition.
            subprocess.run(['/bin/launchctl','bootout',target],check=True)
            loaded=False
            wait_for(lambda:not (data/'postmaster.pid').exists())
            assert not ready()
            subprocess.run(['/bin/launchctl','bootstrap',f'gui/{os.getuid()}',str(plist)],check=True)
            loaded=True
            wait_for(ready)
            with db(test) as connection:
                actual=connection.execute("SELECT current_setting('data_directory') AS path").fetchone()['path']
                assert Path(actual).resolve()==data.resolve()
        finally:
            if loaded:subprocess.run(['/bin/launchctl','bootout',target],check=True)
            if subprocess.run([pg_ctl,'-D',str(data),'status'],capture_output=True).returncode==0:
                subprocess.run([pg_ctl,'-D',str(data),'-m','fast','-w','stop'],capture_output=True,check=True)
            wait_for(lambda:not (data/'postmaster.pid').exists())
    return {'existing_database_not_restarted':True,'database_started_when_absent':True,
            'managed_database_stopped_cleanly':True,'launchagent_restart_passed':True,
            'temporary_cluster_removed':True,'live_cluster_untouched':True}


def main():
    folder=LOCAL/'recovery-pilot'/uuid.uuid4().hex[:10];folder.mkdir(parents=True)
    proof={'at':now(),'runner':runner_pilot(folder),'postgres':postgres_pilot(folder),'full_mac_reboot_tested':False}
    write_json(folder/'result.json',proof)
    print(json.dumps({'result':str(folder/'result.json'),**proof}))

if __name__=='__main__':main()
