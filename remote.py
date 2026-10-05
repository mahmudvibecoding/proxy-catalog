"""Mac-side SSH bridge; account authentication never leaves this machine."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess

from common import ROOT, LOCAL, capture, digest, lock, now, read_json, run, write_json

SSH_OPTIONS = ['-o', 'BatchMode=yes', '-o', 'ForwardAgent=no', '-o', 'ConnectTimeout=15',
               '-o', 'ServerAliveInterval=30', '-o', 'ServerAliveCountMax=3']


def remote_config(c):
    remote = c['remote']
    if not re.fullmatch(r'[a-z_][a-z0-9_-]*@[a-zA-Z0-9.-]+', remote['host']):
        raise ValueError('Expected an SSH user@host')
    if not re.fullmatch(r'/[a-zA-Z0-9_/-]+', remote['directory']) or '..' in Path(remote['directory']).parts:
        raise ValueError('Invalid worker directory')
    return remote


def ssh_command(c, arguments, *, research=None):
    remote = remote_config(c)
    command = ['docker', 'compose', '-f', 'compose.worker.yaml', 'exec', '-T']
    if research:
        if not re.fullmatch(r'[a-zA-Z0-9_-]+', research):
            raise ValueError('Invalid research run')
        command += ['-w', '/work/.local/runs/' + research + '/research']
    command += ['worker', *map(str, arguments)]
    return ['ssh', *SSH_OPTIONS, remote['host'],
            'cd ' + shlex.quote(remote['directory']) + ' && ' + shlex.join(command)]


def call(c, action, *arguments):
    return json.loads(capture(ssh_command(c, ['python', 'remote_worker.py', action, *arguments])))


def copy_files(c, source, destination, *, download=False, names=None):
    """Only explicit task directories are transferred; no home/config sync."""
    remote = remote_config(c)
    ssh = shlex.join(['ssh', *SSH_OPTIONS])
    remote_path = remote['host'] + ':' + remote['directory'] + '/' + destination.strip('/') + '/'
    args = ['/usr/bin/rsync', '-a', '--partial', '--delay-updates',
            '--rsync-path=runuser -u proxy-catalog -- rsync', '-e', ssh]
    source = Path(source)
    source.mkdir(parents=True, exist_ok=True)
    if names is not None:
        allowed = []
        for name in names:
            path = Path(name)
            if path.is_absolute() or '..' in path.parts or '\n' in name or '\r' in name:
                raise ValueError('Unsafe transfer path')
            allowed.append(name)
        # A unique list avoids sharing mutable transfer state with another job.
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w') as listed:
            listed.write('\n'.join(allowed) + '\n')
            listed.flush()
            args += ['--files-from=' + listed.name]
            args += [remote_path, str(source) + '/'] if download else [str(source) + '/', remote_path]
            run(args, stdout=subprocess.DEVNULL)
    else:
        args += [remote_path, str(source) + '/'] if download else [str(source) + '/', remote_path]
        run(args, stdout=subprocess.DEVNULL)


def sync_research(c, folder=None, force=False):
    with lock(LOCAL / 'continuous/bridge.lock'):
        state = read_json(LOCAL / 'state.json', {})
        ident = Path(folder).name if folder else state.get('active_run') or state.get('last_run')
        out = LOCAL / 'continuous/outbox'
        out.mkdir(parents=True, exist_ok=True)
        write_json(out / '.local/state.json', state)
        write_json(out / 'latest.json', read_json(ROOT / 'latest.json', {}))
        names = ['.local/state.json', 'latest.json']
        if ident:
            folder = Path(folder) if folder else LOCAL / 'runs' / ident
            if not re.fullmatch(r'[a-zA-Z0-9_-]+', ident):
                raise ValueError('Invalid research run')
            record = read_json(folder / 'run.json', {})
            name = '.local/runs/' + ident + '/run.json'
            write_json(out / name, record)
            names.append(name)
            if not c['remote'].get('research_on_server'):
                source = folder / 'research/candidates.json'
                if source.exists():
                    target = out / '.local/runs' / ident / 'research/candidates.json'
                    target.parent.mkdir(parents=True, exist_ok=True)
                    stat = source.stat()
                    if force or not target.exists() or target.stat().st_mtime_ns != stat.st_mtime_ns:
                        temp = target.with_suffix('.partial')
                        with source.open('rb') as incoming, temp.open('wb') as outgoing:
                            before = os.fstat(incoming.fileno())
                            shutil.copyfileobj(incoming, outgoing, 1024 * 1024)
                            after = os.fstat(incoming.fileno())
                        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                            raise RuntimeError('Research checkpoint changed during copy; retry')
                        os.utime(temp, ns=(before.st_atime_ns, before.st_mtime_ns))
                        temp.replace(target)
                    names.append(str(target.relative_to(out)))
        copy_files(c, out, '', names=names)
        status = call(c, 'status')
        write_json(LOCAL / 'continuous/remote-status.json', {'at': now(), 'host': c['remote']['host'], **status})
        if status.get('collector'):
            write_json(LOCAL / 'continuous/collector-status.json',
                       {**status['collector'], 'execution_host': c['remote']['host'], 'bridge_checked_at': now()})
        return {'execution_host': c['remote']['host'], **status}


class Journal:
    def __init__(self, c):
        self.c = c
        self.home = LOCAL / 'continuous'

    def stats(self):
        return call(self.c, 'status')['queue']

    def get(self, key, default=None):
        if key != 'revision':
            raise ValueError('Unsupported remote journal setting: ' + key)
        return self.stats().get(key, default)

    def close(self):
        pass


def snapshot(c, folder):
    result = call(c, 'snapshot', '--run', folder.name)
    names = ['report.json', 'snapshot/manifest.json'] + ['snapshot/' + a['name'] for a in result['manifest']['assets']]
    copy_files(c, folder, '.local/runs/' + folder.name, download=True, names=names)
    # Verify all received assets, including evidence and report, before upload.
    for asset in result['manifest']['assets']:
        path = folder / 'snapshot' / asset['name']
        if Path(asset['name']).name != asset['name'] or path.stat().st_size != asset['bytes'] or digest(path) != asset['sha256']:
            raise RuntimeError('Transferred snapshot asset mismatch: ' + asset['name'])
    if read_json(folder / 'snapshot/manifest.json') != result['manifest']:
        raise RuntimeError('Transferred snapshot manifest mismatch')
    return result['manifest']


def restore(c, folder, downloaded):
    checksum = read_json(downloaded / 'manifest.json')['dump_sha256']
    # download() already verified the GitHub copy byte-for-byte. Restore the
    # server copy with that same SHA-256, avoiding another large return transfer.
    proof = call(c, 'restore', '--run', folder.name, '--checksum', checksum)
    write_json(downloaded / 'restore-verification.json', proof)
    return proof


def acknowledge(c, folder):
    copy_files(c, folder, '.local/runs/' + folder.name, names=['publication.json'])
    return call(c, 'ack', '--run', folder.name)
