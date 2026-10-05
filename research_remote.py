"""Run research helpers on the worker while OpenAI/GitHub login stays on the Mac."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess

from common import ROOT, LOCAL, capture, config, run, write_json
import remote

CORE = ['candidates.json', 'research_summary.json', 'coverage.json', 'parser_gaps.json']
PRIVATE = {'auth.json', 'hosts.yml', 'config.local.json', 'events.jsonl', 'stderr.log', 'stdout.log',
           'id_rsa', 'id_ed25519', 'id_ecdsa', 'credentials.json', 'client_secret.json'}


def safe_paths(workspace, names):
    result = []
    for name in names:
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or '\n' in name or '\r' in name:
            raise ValueError('Research transfers must be relative paths')
        if any(p.startswith('.') for p in path.parts) or path.name in PRIVATE or path.suffix in ('.pem', '.key', '.env'):
            raise ValueError('Authentication, session, and hidden files are never transferred')
        source = workspace / path
        if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(workspace.resolve()):
            raise ValueError('Expected a regular file inside the research workspace')
        result.append(name)
    return result


def bootstrap(c, folder):
    workspace = folder / 'research'
    ident = folder.name
    if not re.fullmatch(r'[a-zA-Z0-9_-]+', ident):
        raise ValueError('Invalid research run')
    script = 'from pathlib import Path; p=Path(' + repr('/work/.local/runs/' + ident + '/research') + '); p.mkdir(parents=True,exist_ok=True); print(int((p/".remote-ready").exists()))'
    ready = capture(remote.ssh_command(c, ['python', '-c', script])).strip() == '1'
    if ready:
        return
    names = [name for name in ['known_sources.json', 'discovery_history.json', 'candidates.json', *CORE] if (workspace / name).is_file()]
    remote.copy_files(c, workspace, '.local/runs/' + ident + '/research', names=sorted(set(names)))
    script = 'from pathlib import Path; Path(' + repr('/work/.local/runs/' + ident + '/research/.remote-ready') + ').touch()'
    run(remote.ssh_command(c, ['python', '-c', script]), stdout=subprocess.DEVNULL)


def pull_core(c, folder):
    script = 'from pathlib import Path; import json; print(json.dumps([n for n in ' + repr(CORE) + ' if Path(n).is_file()]))'
    names = json.loads(capture(remote.ssh_command(c, ['python', '-c', script], research=folder.name)))
    if names:
        remote.copy_files(c, folder / 'research', '.local/runs/' + folder.name + '/research', download=True, names=names)


def remote_summary_signature(c, folder):
    script = ('from pathlib import Path; import json; p=Path("research_summary.json"); '
              's=p.stat() if p.exists() else None; '
              'print(json.dumps([s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns] if s else None))')
    return json.loads(capture(remote.ssh_command(c, ['python', '-c', script], research=folder.name)))


def instructions(folder):
    helper = ROOT / 'research_remote.py'
    return f'''

## Execution location selected by the user

The user moved heavy work to their server and requires OpenAI/GitHub account
authentication to remain on the Mac. Continue this same investigation and retain
all prior evidence. The authoritative research data is now in the server worker.
Only the OpenAI session, authenticated GitHub operations, and small code edits
and summaries belong on this Mac. Do not start local crawlers, parsers, archive
scans, bulk merges, compression jobs, or whole-workspace audits.

Run Python helpers and data inspection through:
`python3 {helper} --run {folder.name} exec -- python3 -u YOUR_SCRIPT.py`

This uploads your current root-level .py helpers and executes them in the server
research directory. All previous data and resumable journals are already there.
Use the same command with `python3 -c '...'` for remote inspection. The helper
uses SSH with agent forwarding disabled and transfers no authentication files.
SSH access itself remains on the Mac; the worker has no OpenAI or GitHub login.
Public HTTP downloads and public Git operations can run on the server.

Create or edit your own small scripts locally, then run them with the helper.
For a new small input file, explicitly upload it using:
`python3 {helper} --run {folder.name} push relative-file.json`
To retrieve a small result use `pull relative-file.json`. Do not pull large
frontiers, payload archives, or history caches onto the Mac; inspect them remotely.
The collection service consumes the server's candidates.json directly. Before
ending your turn, write research_summary.json on the server. The runner retrieves
the required final checkpoints automatically. Existing local bulk files are a
preserved pre-migration copy and must not overwrite newer server checkpoints.
The original public-source, no-proxy-testing, and untrusted-content boundaries apply.
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True)
    parser.add_argument('command', choices=('exec', 'push', 'pull'))
    parser.add_argument('arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-zA-Z0-9_-]+', args.run):
        raise ValueError('Invalid research run')
    c = config()
    folder = LOCAL / 'runs' / args.run
    workspace = folder / 'research'
    destination = '.local/runs/' + args.run + '/research'
    if args.command == 'exec':
        command = args.arguments[1:] if args.arguments[:1] == ['--'] else args.arguments
        if not command:
            raise ValueError('A remote command is required')
        names = safe_paths(workspace, [p.name for p in workspace.glob('*.py')])
        if names:
            remote.copy_files(c, workspace, destination, names=names)
        completed = subprocess.run(remote.ssh_command(c, command, research=args.run))
        raise SystemExit(completed.returncode)
    elif args.command == 'push':
        remote.copy_files(c, workspace, destination, names=safe_paths(workspace, args.arguments))
    else:
        # Download names are checked by copy_files; hidden/auth paths are denied too.
        if any(any(part.startswith('.') for part in Path(name).parts) or Path(name).name in PRIVATE for name in args.arguments):
            raise ValueError('Private files are not research outputs')
        remote.copy_files(c, workspace, destination, download=True, names=args.arguments)


if __name__ == '__main__':
    main()
