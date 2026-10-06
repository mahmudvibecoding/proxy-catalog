from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import common
import continuous
import extractor_agent as agent
import extractor_remote as bridge
import extractor_worker as worker
from common import read_json, write_json


class AgentRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.work = self.root / '.local/extractor-agent'
        self.workspace = self.work / 'workspace'
        self.workspace.mkdir(parents=True)
        (self.root / 'prompts').mkdir()
        (self.root / 'prompts/extractor.md').write_text('preserve the saved session')
        self.state = self.work / 'state.json'
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, value in [('ROOT', self.root), ('LOCAL', self.root / '.local'),
                            ('WORK', self.work), ('WORKSPACE', self.workspace), ('STATE', self.state)]:
            self.stack.enter_context(patch.object(agent, name, value))
        self.c = {'codex': 'codex', 'extractor_agent': {'enabled': True}}
        self.gap = {'available': True, 'signature': ['run', 1, 100, 5], 'run_id': 'run'}

    def test_resume_keeps_selected_model_and_never_uses_api_keys(self):
        with patch.object(agent, 'git', return_value=str(self.root / '.git')):
            command = agent.agent_command(self.c, 'saved-session')
        self.assertEqual(command[command.index('-m') + 1], 'gpt-6.1-sol')
        self.assertIn('model_reasoning_effort="max"', command)
        self.assertIn('forced_login_method="chatgpt"', command)
        self.assertEqual(command[command.index('resume') + 1], 'saved-session')
        self.assertIn('--ignore-user-config', command)
        self.assertEqual(command[command.index('--sandbox') + 1], 'workspace-write')
        self.assertNotIn('--add-dir', command)
        with patch.dict(os.environ, {'OPENAI_API_KEY': 'secret', 'CODEX_API_KEY': 'secret',
                                     'CODEX_ACCESS_TOKEN': 'secret', 'PROXY_CATALOG_CONFIG': 'another-job'}):
            env = agent.agent_environment()
        self.assertFalse({'OPENAI_API_KEY', 'CODEX_API_KEY', 'CODEX_ACCESS_TOKEN', 'PROXY_CATALOG_CONFIG'} & env.keys())

    def fake_turn(self, *, completed=True, exit_code=0, fresh=True):
        task = self.workspace / '.local/extractor'
        task.mkdir(parents=True, exist_ok=True)
        report = task / 'report.json'
        write_json(report, {'status': 'idle', 'summary': 'old'})
        def child(*args, **kwargs):
            if fresh:
                write_json(report, {'status': 'idle', 'summary': 'audited available examples'})
            events = [{'type': 'thread.started', 'thread_id': 'same-session'}]
            if completed:
                events.append({'type': 'turn.completed', 'usage': {}})
            proc = Mock(pid=123, stdout=[json.dumps(row) + '\n' for row in events])
            proc.wait.return_value = exit_code
            proc.poll.return_value = exit_code
            return proc
        with patch.object(agent, 'prepare_workspace'), patch.object(agent, 'agent_command', return_value=['codex']), \
             patch.object(agent.subprocess, 'Popen', side_effect=child):
            return agent.run_agent(self.c, {}, self.gap)

    def test_interrupted_turn_preserves_session_for_restart(self):
        with self.assertRaisesRegex(RuntimeError, 'same saved session'):
            self.fake_turn(completed=False, exit_code=1)
        state = read_json(self.state)
        self.assertEqual(state['session_id'], 'same-session')
        self.assertNotIn('child_pid', state)

    def test_success_requires_completed_turn_and_fresh_report(self):
        with self.assertRaises(RuntimeError):
            self.fake_turn(completed=False)
        with self.assertRaisesRegex(RuntimeError, 'fresh report'):
            self.fake_turn(fresh=False)
        self.assertEqual(self.fake_turn()['status'], 'idle')

    def test_unchanged_idle_inputs_do_not_consume_another_ai_turn(self):
        write_json(self.state, {'phase': 'idle', 'last_gap_signature': self.gap['signature']})
        with patch.object(agent.extractor_remote, 'status', return_value=self.gap), \
             patch.object(agent.subprocess, 'Popen'), patch.object(agent, 'run_agent') as start:
            state = agent.execute(self.c)
        start.assert_not_called()
        self.assertEqual(state['phase'], 'idle')
        self.assertGreater(state['next_check_at'], 0)

    def test_unfinished_release_retries_without_starting_another_agent(self):
        report = {'status': 'ready', 'commit': 'a' * 40, 'summary': 'saved fix'}
        write_json(self.state, {'pending_report': report, 'session_id': 'saved-session'})
        with patch.object(agent.subprocess, 'Popen'), \
             patch.object(agent, 'promote', side_effect=RuntimeError('network unavailable')), \
             patch.object(agent, 'run_agent') as start:
            with self.assertRaises(RuntimeError):
                agent.execute(self.c, force=True)
        start.assert_not_called()
        self.assertEqual(read_json(self.state)['pending_report'], report)
        with patch.object(agent.subprocess, 'Popen'), \
             patch.object(agent, 'promote', return_value={'released': True}) as finish, \
             patch.object(agent, 'run_agent') as start:
            self.assertEqual(agent.execute(self.c, force=True), {'released': True})
        finish.assert_called_once()
        start.assert_not_called()

    def test_disable_survives_a_late_running_process_checkpoint(self):
        write_json(self.state, {'disabled': True})
        state = {'disabled': False, 'phase': 'running'}
        agent.save(state, phase='interrupted')
        self.assertTrue(read_json(self.state)['disabled'])

    def test_simultaneous_checkpoints_leave_valid_state_and_preserve_disable(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(agent.save, {'marker': number}, phase='running') for number in range(30)]
            futures.append(pool.submit(agent.save, {}, disabled=True))
            for future in futures:
                future.result()
        state = read_json(self.state)
        self.assertTrue(state['disabled'])
        self.assertEqual(state['model'], 'gpt-6.1-sol')
        self.assertFalse(list(self.work.glob('state-*.tmp')))

    def test_correctable_release_failure_returns_to_same_agent_session(self):
        write_json(self.state, {'pending_report': {'status': 'ready'}, 'session_id': 'same-session'})
        with patch.object(agent.subprocess, 'Popen'), \
             patch.object(agent, 'promote', side_effect=agent.NeedsAgent('Merge main and retest')):
            result = agent.execute(self.c, force=True)
        self.assertEqual(result['phase'], 'pending')
        self.assertEqual(result['session_id'], 'same-session')
        self.assertNotIn('pending_report', result)

    def test_changed_parser_requires_a_new_cache_version_before_verification(self):
        revision = 'a' * 40
        (self.root / 'vendor').mkdir()
        (self.root / 'vendor/proxy_formats.py').write_text('PARSER_VERSION = 5\n')
        def git(*args, **kwargs):
            return {('status', '--porcelain'): '', ('rev-parse', 'HEAD'): revision,
                    ('diff', '--name-only', 'main...HEAD'): 'vendor/proxy_formats.py\n'}[args]
        with patch.object(agent, 'git', side_effect=git), \
             patch.object(agent.subprocess, 'run', return_value=Mock(returncode=1)), \
             patch.object(agent.extractor_remote, 'verify') as verify:
            with self.assertRaisesRegex(agent.NeedsAgent, 'Increase PARSER_VERSION'):
                agent.promote(self.c, {}, {'commit': revision, 'parser_version': 5})
        verify.assert_not_called()

    def test_untrusted_report_cannot_request_file_or_credential_source(self):
        path = self.root / 'report.json'
        for url in ('file:///etc/passwd', 'https://user:password@example.com/feed'):
            write_json(path, {'status': 'ready', 'summary': 'fix', 'commit': 'a' * 40,
                              'parser_version': 5, 'replay_urls': [url]})
            with self.assertRaises(ValueError):
                agent.read_report(path)

    def test_parser_agent_cannot_change_runner_or_credentials(self):
        for path in ('extractor_agent.py', 'common.py', 'config.local.json', 'requirements.txt', 'continuous.py'):
            self.assertFalse(agent.allowed_change(path))
        self.assertTrue(agent.allowed_change('vendor/proxy_formats.py'))
        self.assertTrue(agent.allowed_change('tests/test_proxy_format_gaps.py'))


class CandidateCommitTests(unittest.TestCase):
    def setUp(self):
        AgentRecoveryTests.setUp(self)
        def git(*args):
            return subprocess.check_output(['git', *args], cwd=self.workspace,
                                           text=True, stderr=subprocess.DEVNULL).strip()
        self.git = git
        git('init', '-b', agent.BRANCH)
        git('config', 'user.name', 'Extractor test')
        git('config', 'user.email', 'extractor@example.test')
        (self.workspace / '.gitignore').write_text('.local/\n')
        (self.workspace / 'vendor').mkdir()
        (self.workspace / 'vendor/proxy_formats.py').write_text('PARSER_VERSION = 5\n')
        git('add', '.')
        git('commit', '-m', 'Baseline')
        self.baseline = git('rev-parse', 'HEAD')
        self.report = {'status': 'ready', 'summary': 'Read complete ZIP members',
                       'baseline_commit': self.baseline, 'parser_version': 6,
                       'replay_urls': ['https://publisher.example/archive.zip']}
        (self.workspace / 'vendor/proxy_formats.py').write_text('PARSER_VERSION = 6\n')
        (self.workspace / 'tests').mkdir()
        (self.workspace / 'tests/test_proxy_candidate.py').write_text('# regression fixture\n')

    def test_runner_commits_scoped_uncommitted_report_and_preserves_session(self):
        path = self.workspace / '.local/extractor/report.json'
        write_json(path, self.report)
        state = {'session_id': 'saved-session'}
        ready = agent.commit_candidate(state, agent.read_report(path))
        self.assertEqual(ready['commit'], self.git('rev-parse', 'HEAD'))
        self.assertEqual(self.git('rev-parse', 'HEAD^'), self.baseline)
        self.assertEqual(self.git('status', '--porcelain'), '')
        self.assertEqual(state['pending_report'], ready)
        self.assertEqual(state['session_id'], 'saved-session')
        self.assertNotIn('pending_commit', state)
        self.assertEqual(read_json(path), ready)

    def test_commit_interruption_recovers_without_duplicate_commit(self):
        real_save = agent.save
        def interrupt(state, **values):
            if values.get('phase') == 'pending':
                raise agent.catalog.RunInterrupted(15)
            return real_save(state, **values)
        with patch.object(agent, 'save', side_effect=interrupt):
            with self.assertRaises(agent.catalog.RunInterrupted):
                agent.commit_candidate({}, self.report)
        revision = self.git('rev-parse', 'HEAD')
        state = read_json(self.state)
        self.assertIn('pending_commit', state)
        ready = agent.commit_candidate(state, state['pending_report'])
        self.assertEqual(ready['commit'], revision)
        self.assertEqual(self.git('rev-list', '--count', 'HEAD'), '2')

    def test_unrelated_changes_are_rejected_before_any_git_write(self):
        (self.workspace / 'config.local.json').write_text('{}\n')
        with self.assertRaisesRegex(agent.NeedsAgent, 'outside its scope'):
            agent.commit_candidate({}, self.report)
        self.assertEqual(self.git('diff', '--cached', '--name-only'), '')
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.baseline)

    def test_graceful_shutdown_keeps_commit_recovery_checkpoint(self):
        real_save = agent.save
        def interrupt(state, **values):
            if values.get('phase') == 'pending':
                raise agent.catalog.RunInterrupted(15)
            return real_save(state, **values)
        real_popen = subprocess.Popen
        def start(*args, **kwargs):
            if args[0][0] == '/usr/bin/caffeinate':
                return Mock()
            return real_popen(*args, **kwargs)
        with patch.object(agent, 'save', side_effect=interrupt), \
             patch.object(agent.extractor_remote, 'status', return_value=self.gap), \
             patch.object(agent, 'run_agent', return_value=self.report), \
             patch.object(agent.subprocess, 'Popen', side_effect=start):
            result = agent.execute(self.c, force=True)
        self.assertEqual(result['phase'], 'interrupted')
        state = read_json(self.state)
        self.assertIn('pending_commit', state)
        ready = agent.commit_candidate(state, state['pending_report'])
        self.assertEqual(ready['commit'], self.git('rev-parse', 'HEAD'))
        self.assertEqual(self.git('rev-list', '--count', 'HEAD'), '2')

    def test_symlink_fixture_cannot_be_committed(self):
        (self.workspace / 'tests/test_proxy_link.py').symlink_to(self.root / 'outside')
        with self.assertRaisesRegex(agent.NeedsAgent, 'ordinary workspace files'):
            agent.commit_candidate({}, self.report)
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.baseline)

    def test_changed_baseline_is_rejected_without_losing_edits(self):
        report = {**self.report, 'baseline_commit': 'a' * 40}
        with self.assertRaisesRegex(agent.NeedsAgent, 'baseline changed'):
            agent.commit_candidate({}, report)
        self.assertEqual((self.workspace / 'vendor/proxy_formats.py').read_text(), 'PARSER_VERSION = 6\n')


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.patch = patch.object(worker, 'LOCAL', self.root)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.journal = continuous.Journal(self.root / 'queue')
        self.addCleanup(self.journal.close)
        self.storage = self.root / 'payloads'
        self.storage.mkdir()
        self.c = {'storage': str(self.storage)}
        self.url = 'https://publisher.example/feed?protocol=http&page=2'
        self.row = {'url': self.url, 'kind': 'api_candidate', 'protocol_hints': ['http'],
                    'evidence_url': self.url, 'notes': 'source'}
        self.journal.enqueue('research', [self.row], 'first', 'first')
        body = b'8.8.8.8:80\n'
        payload = self.storage / 'body.gz'
        with gzip.open(payload, 'wb') as stream:
            stream.write(body)
        job = self.journal.claim()
        self.original = {'status': 'collected', 'payload_path': str(payload),
            'content_sha256': hashlib.sha256(body).hexdigest(), 'finished_at': '2026-10-06T00:00:00+00:00',
            'protocol_hints': ['http'], 'continuous_receipt': 'old-receipt', 'inserted': 1}
        self.journal.finish(job, self.original, imported=True)
        self.version = continuous.collector.PARSER_VERSION

    def replay(self, urls=None):
        return worker.replay(self.c, urls or [self.url], 'a' * 40, self.version, self.journal)

    def test_replay_reuses_saved_body_and_preserves_previous_receipt(self):
        proof = self.replay()
        self.assertEqual(proof['queued_jobs'], 1)
        self.assertEqual(proof['reused_payloads'], 1)
        job = self.journal.claim()
        result = json.loads(job['result'])
        self.assertEqual(result['status'], 'downloaded')
        self.assertTrue(result['reparse'])
        self.assertNotIn('continuous_receipt', result)
        self.assertEqual(self.journal.stats()['inserted'], 1)
        self.assertEqual(len(self.journal.stats()['inputs']), 1)
        self.assertEqual(self.replay(), proof)
        self.assertEqual(self.journal.stats()['candidates'], 2)

    def test_crash_before_receipt_does_not_duplicate_replay_jobs(self):
        with patch.object(worker, 'write_json', side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):
                self.replay()
        proof = self.replay()
        self.assertEqual(proof['queued_jobs'], 1)
        self.assertEqual(proof['reused_payloads'], 1)
        self.assertEqual(self.journal.stats()['candidates'], 2)

    def test_replay_never_reads_a_cache_outside_catalog_storage(self):
        outside = self.root / 'outside.gz'
        outside.write_bytes(b'not a source cache')
        result = dict(self.original, payload_path=str(outside))
        self.journal.conn.execute('UPDATE jobs SET result=?', (json.dumps(result),))
        self.journal.conn.commit()
        self.assertEqual(self.replay()['reused_payloads'], 0)
        self.assertIsNone(self.journal.claim()['result'])

    def test_wrong_version_rejects_before_queue_changes(self):
        with self.assertRaises(RuntimeError):
            worker.replay(self.c, [self.url], 'b' * 40, self.version + 1, self.journal)
        self.assertEqual(self.journal.stats()['candidates'], 1)

    def test_unknown_sources_are_reported_without_inventing_candidates(self):
        unknown = 'https://publisher.example/unknown'
        proof = self.replay([self.url, unknown])
        self.assertEqual(proof['unknown_urls'], [unknown])
        self.assertEqual(proof['queued_jobs'], 1)


class BridgeTests(unittest.TestCase):
    def test_transfer_allowlist_excludes_auth_and_large_research_directories(self):
        for path in ('config.local.json', '.env', '.local/runs/data.json', '../secret.py', '/tmp/code.py',
                     'secrets/auth.json', 'sources/registry.json'):
            self.assertFalse(bridge.code_path(path))
        self.assertTrue(bridge.code_path('vendor/proxy_formats.py'))
        self.assertTrue(bridge.code_path('tests/fixtures/proxy/example.json'))
        self.assertTrue(bridge.code_path('prompts/discover.md'))

    def test_remote_command_keeps_auth_on_mac_and_uses_separate_workspace(self):
        c = {'remote': {'host': 'root@worker.example', 'directory': '/opt/proxy-catalog-worker'}}
        command = bridge.command(c, ['python', '-c', 'print("literal; $(nothing)")'])
        self.assertIn('ForwardAgent=no', command)
        self.assertNotIn('-A', command)
        self.assertIn('/work/.local/extractor-agent/workspace', command[-1])
        self.assertIn('PROXY_CATALOG_CONFIG=/work/config.local.json', command[-1])
        self.assertIn("'print(\"literal; $(nothing)\")'", command[-1])

    def test_isolated_tests_can_use_existing_server_config_without_copying_it(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'server-config.json'
            expected = {'model': 'gpt-6.1-sol', 'reasoning_effort': 'max', 'marker': 'server'}
            write_json(path, expected)
            with patch.dict(os.environ, {'PROXY_CATALOG_CONFIG': str(path)}):
                self.assertEqual(common.config(), expected)

    def test_partial_deployment_restores_code_manifest_and_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'vendor').mkdir()
            old_parser = b'PARSER_VERSION = 4\n'
            new_parser = b'PARSER_VERSION = 5\n'
            (root / 'vendor/proxy_formats.py').write_bytes(old_parser)
            (root / 'obsolete.py').write_bytes(b'old helper\n')
            write_json(root / 'config.local.json', {'code_commit': 'b' * 40, 'marker': 'preserve'})
            old_manifest = {'vendor/proxy_formats.py': hashlib.sha256(old_parser).hexdigest(),
                            'obsolete.py': hashlib.sha256(b'old helper\n').hexdigest()}
            write_json(root / '.local/extractor-agent/deployed-files.json', old_manifest)
            expected = {'vendor/proxy_formats.py': hashlib.sha256(new_parser).hexdigest(),
                        'added.py': hashlib.sha256(b'new helper\n').hexdigest()}
            script = bridge.DEPLOY_SCRIPT.replace("pathlib.Path('/work')", 'pathlib.Path(' + repr(directory) + ')')
            def operation(mode, check=True):
                return subprocess.run([sys.executable, '-c', script], text=True, capture_output=True,
                    input=json.dumps({'revision': 'a' * 40, 'files': expected, 'mode': mode}), check=check)
            operation('prepare')
            (root / 'vendor/proxy_formats.py').write_bytes(b'partial file')
            (root / 'added.py').write_bytes(b'new helper\n')
            self.assertNotEqual(operation('verify', check=False).returncode, 0)
            operation('prepare')  # A retry must retain the original rollback copy.
            operation('rollback')
            self.assertEqual((root / 'vendor/proxy_formats.py').read_bytes(), old_parser)
            self.assertFalse((root / 'added.py').exists())
            self.assertEqual(read_json(root / '.local/extractor-agent/deployed-files.json'), old_manifest)
            self.assertEqual(read_json(root / 'config.local.json'), {'code_commit': 'b' * 40, 'marker': 'preserve'})
            (root / 'vendor/proxy_formats.py').write_bytes(new_parser)
            (root / 'added.py').write_bytes(b'new helper\n')
            operation('verify')
            self.assertFalse((root / 'obsolete.py').exists())
            self.assertEqual(read_json(root / '.local/extractor-agent/deployed-files.json'), expected)
            self.assertEqual(read_json(root / 'config.local.json')['code_commit'], 'a' * 40)
            self.assertEqual((root / 'config.local.json').stat().st_mode & 0o777, 0o600)

    def test_failed_rollback_keeps_worker_stopped(self):
        files = {'vendor/proxy_formats.py': 'digest'}
        proof = {'tests_passed': True, 'sha256': hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()}
        c = {'remote': {'host': 'root@worker.example', 'directory': '/opt/proxy-catalog-worker'}}
        with patch.object(bridge, 'code_files', return_value=files), \
             patch.object(bridge.subprocess, 'run', side_effect=[Mock(stdout='{}'), RuntimeError('rollback unavailable')]), \
             patch.object(bridge, 'run') as service, \
             patch.object(bridge.remote, 'copy_files', side_effect=RuntimeError('copy interrupted')):
            with self.assertRaisesRegex(RuntimeError, 'rollback unavailable'):
                bridge.deploy(c, Path('/workspace'), 'a' * 40, proof)
        self.assertEqual(service.call_count, 1)
        self.assertIn('stop', service.call_args.args[0][-1])


if __name__ == '__main__':
    unittest.main()
