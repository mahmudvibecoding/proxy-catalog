"""Start the existing PostgreSQL cluster at login, without replacing its data."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import plistlib
import signal
import subprocess
import sys
import threading
from common import ROOT, LOCAL, now, pg_tool, read_json, run

LABEL='com.mahmud.proxy-catalog.postgres'

def settings(c):
    raw=c.get('postgres_service',{}).get('data_directory')
    if not raw or not Path(raw).is_absolute():
        raise ValueError('postgres_service.data_directory must name an existing absolute cluster path')
    directory=Path(raw).resolve()
    major=int((directory/'PG_VERSION').read_text().strip())
    if directory.stat().st_uid!=os.getuid(): raise ValueError('The cluster must belong to the current user')
    return directory,pg_tool(c,'postgres',major),pg_tool(c,'pg_ctl',major)

def launchagent(c, configuration=ROOT/'config.local.json', label=LABEL, log_directory=LOCAL):
    settings(c)  # Refuse invalid clusters before registering a persistent service.
    return {'Label':label,'ProgramArguments':[str(ROOT/'.venv/bin/python'),str(ROOT/'postgres_service.py'),
            '--config',str(Path(configuration).resolve())],
            'WorkingDirectory':str(ROOT),'RunAtLoad':True,'KeepAlive':True,'ThrottleInterval':10,
            'ProcessType':'Background','AbandonProcessGroup':False,'ExitTimeOut':60,
            'StandardOutPath':str(log_directory/'postgres-service.log'),
            'StandardErrorPath':str(log_directory/'postgres-service-error.log')}

def install(c):
    data=launchagent(c)
    path=Path.home()/'Library/LaunchAgents'/f'{LABEL}.plist'
    if path.exists() and plistlib.loads(path.read_bytes()).get('ProgramArguments')!=data['ProgramArguments']:
        raise RuntimeError(f'Refusing to replace an unrelated service: {path}')
    LOCAL.mkdir(exist_ok=True); path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(plistlib.dumps(data)); path.chmod(0o600)
    target=f'gui/{os.getuid()}/{LABEL}'
    loaded=subprocess.run(['/bin/launchctl','print',target],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0
    if not loaded: run(['/bin/launchctl','bootstrap',f'gui/{os.getuid()}',path])
    print(json.dumps({'postgres_service':str(path),'already_loaded':loaded}))

def disable():
    # The database may be shared with other local projects; disable it explicitly.
    run(['/bin/launchctl','bootout',f'gui/{os.getuid()}/{LABEL}'])
    path=Path.home()/'Library/LaunchAgents'/f'{LABEL}.plist'
    if path.exists(): path.rename(LOCAL/'disabled-postgres-launchagent.plist')
    print('PostgreSQL login service disabled; the existing cluster is retained.')

def serve(c):
    directory,postgres,pg_ctl=settings(c)
    host=c.get('database',{}).get('host','')
    if host.startswith('/'):
        Path(host).mkdir(parents=True,exist_ok=True)
    stopping=threading.Event()
    def shutdown(signum,frame): stopping.set()
    previous={sig:signal.signal(sig,shutdown) for sig in (signal.SIGINT,signal.SIGTERM)}
    child=None; observing=False
    def event(name,**data): print(json.dumps({'at':now(),'event':name,**data}),flush=True)
    try:
        while not stopping.is_set():
            if child is not None:
                if child.poll() is None:
                    stopping.wait(2); continue
                event('postgres_exited',pid=child.pid,returncode=child.returncode)
                child=None; observing=False
                if stopping.wait(5): break
            status=subprocess.run([pg_ctl,'-D',str(directory),'status'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            if status.returncode==0:
                if not observing: event('existing_postgres_running',data_directory=str(directory))
                observing=True
                stopping.wait(5); continue
            if status.returncode!=3:
                raise RuntimeError(f'Cannot inspect the existing PostgreSQL cluster (pg_ctl status {status.returncode})')
            # PostgreSQL's own PID-file lock also prevents a race with a manual start.
            # A separate process group lets us translate launchd's SIGTERM to a fast shutdown.
            # launchd has no shell locale; macOS locale discovery can otherwise start threads in postmaster.
            env=os.environ.copy(); env['LC_ALL']='C'; env['LANG']='C'
            child=subprocess.Popen([postgres,'-D',str(directory)],start_new_session=True,env=env)
            event('postgres_started',pid=child.pid,data_directory=str(directory))
    finally:
        if child is not None and child.poll() is None:
            child.send_signal(signal.SIGINT)
            try: child.wait(timeout=45)
            except subprocess.TimeoutExpired:
                event('fast_shutdown_timed_out',pid=child.pid)
                child.send_signal(signal.SIGQUIT); child.wait(timeout=10)
            event('postgres_stopped',pid=child.pid,returncode=child.returncode)
        for sig,handler in previous.items(): signal.signal(sig,handler)

def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=ROOT/'config.local.json')
    args=parser.parse_args()
    serve(read_json(args.config))

if __name__=='__main__': main()
