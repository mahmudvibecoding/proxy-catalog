"""Restartable GPT-6.1 Sol / max extractor developer with verified releases."""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import plistlib
import re
import subprocess
import tempfile
import time
import uuid

import catalog
from common import ROOT, LOCAL, capture, config, lock, now, read_json, run, write_json
import extractor_remote

WORK = LOCAL / 'extractor-agent'
WORKSPACE = WORK / 'workspace'
STATE = WORK / 'state.json'
LABEL = 'com.mahmud.proxy-catalog.extractor'
BRANCH = 'codex/extractor-development'
MODEL = 'gpt-6.1-sol'
EFFORT = 'max'


class NeedsAgent(RuntimeError):
    """A preserved code change needs correction or merging before it can ship."""



def settings(c):
    return {'enabled': False, 'idle_seconds': 900, 'retry_seconds': 60,
            **c.get('extractor_agent', {})}


def save(state, **values):
    WORK.mkdir(parents=True, exist_ok=True)
    with (WORK / 'state.lock').open('a+') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX)
        latest = read_json(STATE, {})
        if 'disabled' in values:
            state.clear()
            state.update(latest)
        elif latest.get('disabled'):
            state['disabled'] = True
        state.update(values, updated_at=now(), model=MODEL, reasoning_effort=EFFORT)
        with tempfile.NamedTemporaryFile('w', dir=WORK, prefix='state-', suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            try:
                json.dump(state, stream, indent=2, default=str)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
                os.replace(temporary, STATE)
            finally:
                temporary.unlink(missing_ok=True)


def signature(path):
    try:
        stat = Path(path).stat()
        return [stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]
    except FileNotFoundError:
        return None


def git(*args, cwd=WORKSPACE):
    return capture(['git', *args], cwd=cwd).strip()


def prepare_workspace():
    if not WORKSPACE.exists():
        WORK.mkdir(parents=True, exist_ok=True)
        run(['git', 'worktree', 'add', '-b', BRANCH, WORKSPACE, 'main'], cwd=ROOT,
            stdout=subprocess.DEVNULL)
    if git('branch', '--show-current') != BRANCH:
        raise RuntimeError('Extractor workspace is on an unexpected branch')
    common_dir = Path(git('rev-parse', '--git-common-dir'))
    if not common_dir.is_absolute():
        common_dir = WORKSPACE / common_dir
    expected = Path(git('rev-parse', '--git-common-dir', cwd=ROOT))
    if not expected.is_absolute():
        expected = ROOT / expected
    if common_dir.resolve() != expected.resolve():
        raise RuntimeError('Extractor workspace belongs to another repository')
    # An interrupted coding turn owns its edits; resume without resetting them.
    if not git('status', '--porcelain'):
        run(['git', 'merge', '--no-edit', 'main'], cwd=WORKSPACE, stdout=subprocess.DEVNULL)


def agent_command(c, session=None):
    git_dir = Path(git('rev-parse', '--git-common-dir', cwd=ROOT))
    if not git_dir.is_absolute():
        git_dir = ROOT / git_dir
    prompt = ('Read .local/extractor/TASK.md and .local/extractor/context.json in full. '
              'Continue the extractor development queue, preserve prior work, and deliver one tested '
              'improvement batch or a precise idle/blocked report in .local/extractor/report.json.')
    command = [c['codex'], 'exec', '--ignore-user-config', '-m', MODEL,
               '-c', f'model_reasoning_effort="{EFFORT}"', '-c', 'forced_login_method="chatgpt"',
               '-c', 'web_search="live"', '-c', 'approval_policy="never"',
               '--sandbox', 'workspace-write', '-c', 'sandbox_workspace_write.network_access=true',
               '--add-dir', str(git_dir.resolve()), '--json', '-C', str(WORKSPACE)]
    if session:
        command += ['resume', session, prompt]
    else:
        command += ['-o', str(WORKSPACE / '.local/extractor/final.txt'), prompt]
    return command


def agent_environment():
    env = os.environ.copy()
    for key in ('OPENAI_API_KEY', 'CODEX_API_KEY', 'CODEX_ACCESS_TOKEN', 'PROXY_CATALOG_CONFIG'):
        env.pop(key, None)
    env['PATH'] = '/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin'
    return env


def read_report(path):
    report = read_json(path)
    if not isinstance(report, dict) or report.get('status') not in ('ready', 'idle', 'incomplete', 'blocked'):
        raise RuntimeError('A fresh structured extractor report is required')
    if not isinstance(report.get('summary'), str) or not report['summary'].strip():
        raise RuntimeError('Extractor report needs a concrete summary')
    if report['status'] == 'ready':
        if not re.fullmatch(r'[a-f0-9]{40}', report.get('commit', '')):
            raise RuntimeError('Ready report requires a full Git commit')
        if type(report.get('parser_version')) is not int:
            raise RuntimeError('Ready report requires a parser version')
        if not isinstance(report.get('replay_urls'), list) or not report['replay_urls']:
            raise RuntimeError('Ready report requires affected source URLs')
        for url in report['replay_urls']:
            catalog.normalize_url(url)
    return report


def run_agent(c, state, gap):
    prepare_workspace()
    task_dir = WORKSPACE / '.local/extractor'
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / 'TASK.md').write_text((ROOT / 'prompts/extractor.md').read_text())
    write_json(task_dir / 'context.json', {'research': gap, 'primary_repository': str(ROOT),
        'remote_helper': str(ROOT / 'extractor_remote.py'), 'workspace': str(WORKSPACE),
        'last_release': state.get('last_release'), 'last_error': state.get('last_error'),
        'last_report': state.get('last_report'), 'checked_at': now()})
    report_path = task_dir / 'report.json'
    previous_report = signature(report_path)
    attempt = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:6]
    attempt_dir = WORK / 'attempts' / attempt
    attempt_dir.mkdir(parents=True)
    command = agent_command(c, state.get('session_id'))
    save(state, phase='running', pid=os.getpid(), attempt=attempt,
         attempts=state.get('attempts', 0) + 1, research=gap, last_error=None)
    turn_completed = False
    with (attempt_dir / 'events.jsonl').open('a') as events, (attempt_dir / 'stderr.log').open('a') as errors:
        child = subprocess.Popen(command, cwd=WORKSPACE, env=agent_environment(),
                                 stdout=subprocess.PIPE, stderr=errors, text=True)
        save(state, child_pid=child.pid)
        try:
            for line in child.stdout:
                events.write(line)
                events.flush()
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                kind = event.get('type')
                values = {'last_event_at': now(), 'last_event': kind}
                if kind == 'thread.started':
                    values['session_id'] = event['thread_id']
                elif kind == 'turn.completed':
                    turn_completed = True
                    values['usage'] = event.get('usage')
                elif kind in ('turn.started', 'turn.failed'):
                    turn_completed = False
                save(state, **values)
            code = child.wait()
        finally:
            catalog.stop_discovery_child(child)
            state.pop('child_pid', None)
            save(state)
    if code != 0 or not turn_completed:
        raise RuntimeError('Extractor turn interrupted; the same saved session will resume')
    if signature(report_path) == previous_report:
        raise RuntimeError('Extractor finished without a fresh report; the same session will resume')
    report = read_report(report_path)
    write_json(attempt_dir / 'report.json', report)
    save(state, last_report=report, last_report_path=str(attempt_dir / 'report.json'))
    return report


def allowed_change(name):
    return (name == 'vendor/proxy_formats.py' or
            (name.startswith('vendor/proxy_formats_') and name.endswith('.py')) or
            name.startswith('vendor/formats/') or name.startswith(('tests/fixtures/proxy/', 'tests/fixtures/proxy_format_gaps/')) or
            (name.startswith('tests/test_proxy') and name.endswith('.py')) or name.startswith('docs/parser/'))


def parser_version(path):
    for node in ast.parse(Path(path).read_text()).body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'PARSER_VERSION'
                                                for target in node.targets):
            value = ast.literal_eval(node.value)
            if type(value) is int:
                return value
    raise NeedsAgent('Parser must declare an integer PARSER_VERSION')


def promote(c, state, report):
    """Tests, main integration, deployment and replay can all be retried after interruption."""
    revision = report['commit']
    if git('status', '--porcelain') or git('rev-parse', 'HEAD') != revision:
        raise NeedsAgent('Ready report must match a clean extractor checkout')
    pending = state.get('pending_deployment')
    if pending:
        if pending['parser_commit'] != revision:
            raise RuntimeError('Finish the saved deployment before starting a different parser batch')
        return finish_release(c, state, report, pending['integrated_commit'], pending['proof'])
    changed = git('diff', '--name-only', 'main...HEAD').splitlines()
    if any(not allowed_change(name) for name in changed):
        raise NeedsAgent('Extractor changed files outside parser, parser tests or parser documentation')
    parser_changed = any(name.startswith('vendor/') for name in changed)
    already_integrated = subprocess.run(['git', 'merge-base', '--is-ancestor', revision, 'main'],
                                        cwd=ROOT, capture_output=True).returncode == 0
    if (parser_changed and not already_integrated
            and report['parser_version'] <= parser_version(ROOT / 'vendor/proxy_formats.py')):
        raise NeedsAgent('Increase PARSER_VERSION so replay cannot reuse the old parsed cache')
    save(state, phase='verifying', pending_report=report)
    proof, test_log = extractor_remote.verify(c, WORKSPACE)
    if proof['parser_version'] != report['parser_version']:
        raise NeedsAgent('Verified parser version differs from the report')
    release_dir = WORK / 'releases' / revision
    release_dir.mkdir(parents=True, exist_ok=True)
    (release_dir / 'tests.log').write_text(test_log)
    write_json(release_dir / 'verification.json', proof)
    # Publication uses this same lock while updating Git metadata on main.
    with lock(LOCAL / 'continuous/publisher.lock'):
        if git('status', '--porcelain', cwd=ROOT):
            raise RuntimeError('Primary checkout has edits; keep the tested extractor branch for retry')
        if git('branch', '--show-current', cwd=ROOT) != 'main':
            raise RuntimeError('Primary checkout must be on main')
        run(['git', 'fetch', 'origin', 'main'], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        run(['git', 'merge', '--ff-only', 'origin/main'], cwd=ROOT, stdout=subprocess.DEVNULL)
        # Metadata publication may have advanced main; unrelated code changes
        # require a fresh tested merge in the isolated workspace.
        base = git('merge-base', 'main', revision)
        main_changes = git('diff', '--name-only', base, 'main').splitlines()
        if any(extractor_remote.code_path(name) for name in main_changes):
            if subprocess.run(['git', 'merge-base', '--is-ancestor', revision, 'main'], cwd=ROOT).returncode:
                raise NeedsAgent('Main code advanced; merge main in the extractor workspace and retest')
        save(state, phase='integrating')
        run(['git', 'merge', '--no-ff', '--no-edit', revision], cwd=ROOT, stdout=subprocess.DEVNULL)
        if extractor_remote.code_files(ROOT) != extractor_remote.code_files(WORKSPACE):
            raise NeedsAgent('Integrated code differs from the tested workspace; retest the merged branch')
        integrated = git('rev-parse', 'HEAD', cwd=ROOT)
        save(state, integrated_commit=integrated, phase='pushing')
        run(['git', 'push', 'origin', 'HEAD:main'], cwd=ROOT, stdout=subprocess.DEVNULL)
    save(state, pending_deployment={'parser_commit': revision, 'integrated_commit': integrated, 'proof': proof})
    return finish_release(c, state, report, integrated, proof)


def finish_release(c, state, report, integrated, proof):
    # A disconnect may leave collection stopped. Resume deployment using the
    # saved tested manifest without first trying to run tests in that container.
    revision = report['commit']
    release_dir = WORK / 'releases' / revision
    release_dir.mkdir(parents=True, exist_ok=True)
    save(state, phase='deploying')
    deployment = extractor_remote.deploy(c, WORKSPACE, integrated, proof)
    write_json(release_dir / 'deployment.json', deployment)
    save(state, phase='replaying')
    replay = extractor_remote.replay(c, report['replay_urls'], revision, report['parser_version'])
    write_json(release_dir / 'replay.json', replay)
    release = {'at': now(), 'commit': integrated, 'parser_commit': revision,
               'parser_version': report['parser_version'], 'summary': report['summary'],
               'replay': replay, 'verification': proof}
    write_json(release_dir / 'release.json', release)
    state.pop('pending_report', None)
    state.pop('pending_deployment', None)
    save(state, phase='pending', last_release=release, last_error=None,
         next_check_at=time.time() + settings(c)['retry_seconds'])
    return release


def execute(c, *, force=False):
    WORK.mkdir(parents=True, exist_ok=True)
    with lock(WORK / 'runner.lock'), catalog.shutdown_signals():
        state = read_json(STATE, {})
        if state.get('disabled') or not settings(c)['enabled']:
            return {'phase': 'disabled'}
        if not force and time.time() < state.get('next_check_at', 0):
            return state
        awake = None
        try:
            awake = subprocess.Popen(['/usr/bin/caffeinate', '-i', '-w', str(os.getpid())],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if state.get('pending_report'):
                return promote(c, state, state['pending_report'])
            gap = extractor_remote.status(c)
            if not gap.get('available'):
                save(state, phase='waiting_for_gaps', research=gap,
                     next_check_at=time.time() + settings(c)['idle_seconds'])
                return state
            if not force and state.get('phase') == 'idle' and state.get('last_gap_signature') == gap['signature']:
                save(state, next_check_at=time.time() + settings(c)['idle_seconds'])
                return state
            report = run_agent(c, state, gap)
            if report['status'] == 'ready':
                return promote(c, state, report)
            phase = {'idle': 'idle', 'blocked': 'waiting_for_blocker', 'incomplete': 'pending'}[report['status']]
            delay = settings(c)['retry_seconds'] if phase == 'pending' else settings(c)['idle_seconds']
            save(state, phase=phase, last_gap_signature=gap['signature'], next_check_at=time.time() + delay)
            return state
        except catalog.RunInterrupted:
            save(state, phase='interrupted', next_check_at=time.time() + settings(c)['retry_seconds'])
            return state
        except (NeedsAgent, extractor_remote.VerificationFailed) as exc:
            # Resume the developer to fix test failures or merge an advanced
            # main branch. Retrying the same immutable report cannot fix either.
            if state.get('pending_deployment'):
                raise
            state.pop('pending_report', None)
            save(state, phase='pending', last_error=str(exc),
                 next_check_at=time.time() + settings(c)['retry_seconds'])
            return state
        except Exception as exc:
            message = f'{type(exc).__name__}: {exc}'
            # Command arguments may contain public proxy settings; keep logs
            # separately and expose only the failure category when appropriate.
            if isinstance(exc, subprocess.CalledProcessError):
                message = f'Subprocess failed with exit code {exc.returncode}'
                (WORK / 'last-command-error.log').write_text((exc.stdout or '') + (exc.stderr or ''))
            save(state, phase='retrying', last_error=message[:1000],
                 next_check_at=time.time() + settings(c)['retry_seconds'])
            raise
        finally:
            if awake:
                awake.terminate()


def install(c):
    if not settings(c)['enabled']:
        raise ValueError('Enable extractor_agent in config.local.json first')
    WORK.mkdir(parents=True, exist_ok=True)
    path = Path.home() / 'Library/LaunchAgents' / (LABEL + '.plist')
    value = {'Label': LABEL, 'ProgramArguments': [str(ROOT / '.venv/bin/python'), str(ROOT / 'extractor_agent.py'), 'run'],
        'WorkingDirectory': str(ROOT), 'RunAtLoad': True, 'StartInterval': 60, 'ProcessType': 'Standard',
        'StandardOutPath': str(WORK / 'runner.log'), 'StandardErrorPath': str(WORK / 'runner-error.log'),
        'EnvironmentVariables': {'PATH': '/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin'},
        'AbandonProcessGroup': False, 'ExitTimeOut': 30}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(value))
    path.chmod(0o600)
    state = read_json(STATE, {})
    save(state, disabled=False, next_check_at=0)
    subprocess.run(['/bin/launchctl', 'bootout', f'gui/{os.getuid()}/{LABEL}'], capture_output=True)
    run(['/bin/launchctl', 'bootstrap', f'gui/{os.getuid()}', path])
    return {'installed': True, 'label': LABEL, 'model': MODEL, 'reasoning_effort': EFFORT,
            'workspace': str(WORKSPACE), 'state': str(STATE)}


def disable():
    state = read_json(STATE, {})
    save(state, disabled=True)
    subprocess.run(['/bin/launchctl', 'bootout', f'gui/{os.getuid()}/{LABEL}'], capture_output=True)
    path = Path.home() / 'Library/LaunchAgents' / (LABEL + '.plist')
    if path.exists():
        path.rename(WORK / 'disabled.plist')
    return {'disabled': True, 'checkpoints_preserved': True}


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('run', 'status', 'install', 'disable', 'promote'))
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    c = config()
    try:
        if args.command == 'status':
            value = read_json(STATE, {'phase': 'not_started', 'model': MODEL, 'reasoning_effort': EFFORT})
        elif args.command == 'install':
            value = install(c)
        elif args.command == 'disable':
            value = disable()
        elif args.command == 'promote':
            with lock(WORK / 'runner.lock'):
                state = read_json(STATE, {})
                value = promote(c, state, read_report(WORKSPACE / '.local/extractor/report.json'))
        else:
            value = execute(c, force=args.force)
        print(json.dumps(value, default=str), flush=True)
    except BlockingIOError:
        print(json.dumps({'phase': 'already_running'}), flush=True)


if __name__ == '__main__':
    main()
