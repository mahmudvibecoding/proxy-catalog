"""Keep account credentials on the Mac and run extractor audits/tests on the server."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

from common import ROOT, capture, config, now, run, write_json
import remote

STAGE = '.local/extractor-agent/workspace'


class VerificationFailed(RuntimeError):
    """The candidate needs a coding turn, rather than another network retry."""


DEPLOY_SCRIPT = r'''
import hashlib,json,os,pathlib,shutil,sys
request=json.load(sys.stdin); root=pathlib.Path('/work').resolve()
home=root/'.local/extractor-agent/deployments'/request['revision']
home.mkdir(parents=True,exist_ok=True); record=home/'backup.json'
deployed=root/'.local/extractor-agent/deployed-files.json'
def checksum(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,value):
 p.parent.mkdir(parents=True,exist_ok=True); t=p.with_name(p.name+'.tmp')
 with open(t,'w',opener=lambda p,f:os.open(p,f,0o600)) as stream:
  json.dump(value,stream,sort_keys=True); stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
 os.replace(t,p)
def target(name):
 p=root/name
 if not p.resolve().is_relative_to(root) or p.is_symlink(): raise RuntimeError('Unsafe deployment path')
 return p
def config_revision(value):
 p=root/'config.local.json'; c=json.loads(p.read_text())
 if value is None: c.pop('code_commit',None)
 else: c['code_commit']=value
 write(p,c)
mode=request['mode']
if mode=='prepare':
 expected=request['files']
 if record.exists():
  saved=json.loads(record.read_text())
  if saved['expected']!=expected: raise RuntimeError('Deployment revision reused with changed code')
 else:
  old=json.loads(deployed.read_text()) if deployed.exists() else {}
  previous={}
  for name in set(expected)|set(old):
   p=target(name); backup=home/'files'/name
   if p.is_file():
    backup.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(p,backup)
    previous[name]=checksum(backup)
   else: previous[name]=None
  saved={'expected':expected,'previous':previous,'old_manifest':old,
         'old_revision':json.loads((root/'config.local.json').read_text()).get('code_commit')}
  write(record,saved)
elif mode=='verify':
 saved=json.loads(record.read_text())
 for name,digest in saved['expected'].items():
  if checksum(target(name))!=digest: raise RuntimeError('Deployment checksum mismatch: '+name)
 for name in set(saved['old_manifest'])-set(saved['expected']):
  p=target(name)
  if p.is_file(): p.unlink()
 config_revision(request['revision']); write(deployed,saved['expected'])
elif mode=='rollback':
 saved=json.loads(record.read_text())
 for name,digest in saved['previous'].items():
  p=target(name)
  if digest is None:
   if p.is_file(): p.unlink()
  else:
   backup=home/'files'/name
   if checksum(backup)!=digest: raise RuntimeError('Rollback backup checksum mismatch')
   p.parent.mkdir(parents=True,exist_ok=True); temp=p.with_name(p.name+'.rollback')
   shutil.copy2(backup,temp); os.replace(temp,p)
   if checksum(p)!=digest: raise RuntimeError('Rollback verification failed')
 config_revision(saved['old_revision']); write(deployed,saved['old_manifest'])
else: raise RuntimeError('Unknown deployment operation')
print(json.dumps({'revision':request['revision'],'operation':mode,'verified':True}))
'''


def code_path(name):
    path = Path(name)
    if path.is_absolute() or '..' in path.parts or any(part.startswith('.') for part in path.parts):
        return False
    if len(path.parts) == 1:
        return path.suffix == '.py' or name == 'requirements.txt'
    if path.parts[0] in ('vendor', 'tests'):
        return path.suffix in ('.py', '.json', '.txt', '.yaml', '.yml', '.csv', '.xml')
    if path.parts[0] == 'prompts':
        return path.suffix == '.md'
    return path.parts[0] == 'schemas' and path.suffix == '.sql'


def code_files(workspace):
    workspace = Path(workspace).resolve()
    names = capture(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=workspace).split('\0')
    result = {}
    for name in sorted(set(names)):
        if not name or not code_path(name):
            continue
        path = workspace / name
        if path.is_symlink() or not path.resolve().is_relative_to(workspace):
            raise ValueError('Code transfer refuses symlinks: ' + name)
        if not path.is_file():
            continue
        if path.stat().st_size > 2 * 1024**2:
            raise ValueError('Keep large evidence on the server: ' + name)
        result[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    if 'vendor/proxy_formats.py' not in result or 'requirements.txt' not in result:
        raise ValueError('Expected a proxy-catalog code workspace')
    return result


def command(c, arguments, *, stage=True):
    target = remote.remote_config(c)
    parts = ['docker', 'compose', '-f', 'compose.worker.yaml', 'exec', '-T']
    if stage:
        parts += ['-w', '/work/' + STAGE, '-e', 'PROXY_CATALOG_CONFIG=/work/config.local.json']
    parts += ['worker', *map(str, arguments)]
    return ['ssh', *remote.SSH_OPTIONS, target['host'],
            'cd ' + shlex.quote(target['directory']) + ' && ' + shlex.join(parts)]


def status(c):
    return json.loads(capture(command(c, ['python', 'extractor_worker.py', 'status'], stage=False)))


def sync(c, workspace):
    workspace = Path(workspace)
    files = code_files(workspace)
    script = 'from pathlib import Path; Path(' + repr('/work/' + STAGE) + ').mkdir(parents=True,exist_ok=True)'
    run(command(c, ['python', '-c', script], stage=False), stdout=subprocess.DEVNULL)
    remote.copy_files(c, workspace, STAGE, names=list(files))
    if code_files(workspace) != files:
        raise RuntimeError('Code changed during transfer; retry the snapshot')
    # Remove only files listed in the previous task-owned manifest. Keep audit data.
    script = '''import hashlib,json,pathlib,sys
p=pathlib.Path.cwd(); expected=json.load(sys.stdin); m=p/'.code-manifest.json'
old=json.loads(m.read_text()) if m.exists() else {}
for name in set(old)-set(expected):
 q=p/name
 if q.resolve().is_relative_to(p) and q.is_file(): q.unlink()
for name,checksum in expected.items():
 if hashlib.sha256((p/name).read_bytes()).hexdigest()!=checksum: raise RuntimeError('Transferred code differs: '+name)
m.write_text(json.dumps(expected,sort_keys=True)+'\\n')
print(json.dumps({'files':len(expected)}))'''
    result = subprocess.run(command(c, ['python', '-c', script]), input=json.dumps(files),
                            text=True, capture_output=True, check=True)
    return {**json.loads(result.stdout), 'sha256': hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()}


def verify(c, workspace):
    proof = sync(c, workspace)
    # The deployed worker already has these pinned dependencies. A dependency
    # change needs an isolated image build before it can pass this release gate.
    script = '''import hashlib,json,os,pathlib,subprocess,sys
p=pathlib.Path.cwd(); manifest=json.loads((p/'.code-manifest.json').read_text())
if (p/'requirements.txt').read_bytes()!=pathlib.Path('/work/requirements.txt').read_bytes():
 sys.stderr.write('Dependency changes require an isolated worker-image build\\n'); raise SystemExit(21)
env=dict(os.environ,PYTHONPATH='.:vendor',PROXY_CATALOG_CONFIG='/work/config.local.json',PROXY_TEST_DATABASE='1')
r=subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-v'],env=env,text=True,capture_output=True)
sys.stderr.write(r.stdout+r.stderr)
if r.returncode: raise SystemExit(20)
for name,checksum in manifest.items():
 if hashlib.sha256((p/name).read_bytes()).hexdigest()!=checksum:
  sys.stderr.write('Code changed during verification: '+name+'\\n'); raise SystemExit(22)
sys.path.insert(0,str(p/'vendor')); from proxy_formats import PARSER_VERSION
print(json.dumps({'tests_passed':True,'parser_version':PARSER_VERSION,'files':len(manifest)}))'''
    result = subprocess.run(command(c, ['python', '-c', script]), text=True, capture_output=True)
    if result.returncode in (20, 21, 22):
        write_json(Path(workspace) / '.local/extractor/verification-failure.json',
                   {'at': now(), 'exit_code': result.returncode, 'detail': result.stderr[-16000:]})
        raise VerificationFailed('Candidate verification failed; read .local/extractor/verification-failure.json')
    result.check_returncode()
    proof.update(json.loads(result.stdout), verified_at=now())
    return proof, result.stderr


def deploy(c, workspace, revision, proof):
    if not re.fullmatch(r'[a-f0-9]{40}', revision) or not proof.get('tests_passed'):
        raise ValueError('Deployment requires a verified Git revision')
    files = code_files(workspace)
    if hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest() != proof['sha256']:
        raise RuntimeError('Code differs from the verified snapshot')
    # Stop only this catalog's collection worker before replacing its modules.
    target = remote.remote_config(c)
    compose = ['docker', 'compose', '-f', 'compose.worker.yaml']
    def service(*args):
        return ['ssh', *remote.SSH_OPTIONS, target['host'], 'cd ' + shlex.quote(target['directory']) +
                ' && ' + shlex.join([*compose, *args])]
    def operation(mode):
        # This separate container remains available if a interrupted deployment
        # has deliberately left the collection worker stopped.
        parts = [*compose, 'exec', '-T', 'research', 'python', '-c', DEPLOY_SCRIPT]
        result = subprocess.run(['ssh', *remote.SSH_OPTIONS, target['host'],
                                 'cd ' + shlex.quote(target['directory']) + ' && ' + shlex.join(parts)],
                                input=json.dumps({'files': files, 'revision': revision, 'mode': mode}),
                                text=True, capture_output=True, check=True)
        return json.loads(result.stdout)
    operation('prepare')
    run(service('stop', '-t', '300', 'worker'), stdout=subprocess.DEVNULL)
    try:
        remote.copy_files(c, workspace, '', names=list(files))
        result = operation('verify')
    except BaseException:
        # A worker must never resume against a partially copied code tree.
        # If even rollback cannot be verified, preserve the stopped service and
        # let the saved pending deployment recover on the next invocation.
        operation('rollback')
        run(service('start', 'worker'), stdout=subprocess.DEVNULL)
        raise
    run(service('start', 'worker'), stdout=subprocess.DEVNULL)
    return result


def replay(c, urls, revision, parser_version):
    result = subprocess.run(command(c, ['python', 'extractor_worker.py', 'replay'], stage=False),
                            input=json.dumps({'urls': urls, 'revision': revision, 'parser_version': parser_version}),
                            text=True, capture_output=True, check=True)
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, default=ROOT / '.local/extractor-agent/workspace')
    parser.add_argument('command', choices=('status', 'sync', 'exec', 'verify'))
    parser.add_argument('arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    c = config()
    if args.command == 'status':
        print(json.dumps(status(c)))
    elif args.command == 'verify':
        proof, log = verify(c, args.workspace)
        write_json(args.workspace / '.local/extractor/verification.json', proof)
        print(log, end='')
        print(json.dumps(proof))
    else:
        proof = sync(c, args.workspace)
        if args.command == 'sync':
            print(json.dumps(proof))
        else:
            arguments = args.arguments[1:] if args.arguments[:1] == ['--'] else args.arguments
            if not arguments:
                raise ValueError('Expected a command to execute in the server workspace')
            raise SystemExit(subprocess.run(command(c, arguments)).returncode)


if __name__ == '__main__':
    main()
