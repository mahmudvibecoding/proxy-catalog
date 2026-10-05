import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import remote
import research_remote
from common import write_json


class RemoteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.c = {'remote': {'host': 'root@worker.example.com', 'directory': '/opt/proxy-catalog-worker',
                             'research_on_server': True}}

    def test_remote_commands_disable_agent_forwarding_and_quote_arguments(self):
        command = remote.ssh_command(self.c, ['python', '-c', 'print("literal; $(nothing)")'], research='run-1')
        self.assertIn('ForwardAgent=no', command)
        self.assertIn('BatchMode=yes', command)
        self.assertNotIn('-A', command)
        self.assertIn("'print(\"literal; $(nothing)\")'", command[-1])
        for host in ('root@host;touch /tmp/x', '-oProxyCommand=x', 'root@host\ncommand'):
            with self.assertRaises(ValueError):
                remote.ssh_command({'remote': dict(self.c['remote'], host=host)}, ['python'])

    def test_research_transfer_refuses_credentials_and_external_symlinks(self):
        workspace = self.root / 'research'
        workspace.mkdir()
        (workspace / 'helper.py').write_text('print(1)')
        (workspace / 'auth.json').write_text('{}')
        (workspace / '.env').write_text('secret')
        (workspace / 'outside').symlink_to(self.root / 'other.json')
        (self.root / 'other.json').write_text('{}')
        self.assertEqual(research_remote.safe_paths(workspace, ['helper.py']), ['helper.py'])
        for name in ('auth.json', '.env', '../other.json', 'outside', '/tmp/file', 'line\nbreak'):
            with self.assertRaises(ValueError):
                research_remote.safe_paths(workspace, [name])

    def test_bridge_does_not_overwrite_authoritative_server_candidates(self):
        local = self.root / '.local'
        folder = local / 'runs/run-1'
        write_json(local / 'state.json', {'active_run': 'run-1'})
        write_json(folder / 'run.json', {'stage': 'discovery'})
        write_json(folder / 'research/candidates.json', [{'url': 'stale local copy'}])
        status = {'queue': {'revision': 4}, 'collector': {'state': 'running'}}
        with patch.object(remote, 'LOCAL', local), patch.object(remote, 'ROOT', self.root), \
             patch.object(remote, 'copy_files') as copying, patch.object(remote, 'call', return_value=status):
            result = remote.sync_research(self.c)
        names = copying.call_args.kwargs['names']
        self.assertEqual(set(names), {'.local/state.json', 'latest.json', '.local/runs/run-1/run.json'})
        self.assertEqual(result['queue']['revision'], 4)

    def test_verified_download_is_restored_on_server_using_the_same_checksum(self):
        downloaded = self.root / 'downloaded'
        write_json(downloaded / 'manifest.json', {'dump_sha256': 'frozen-backup'})
        proof = {'dump_sha256': 'frozen-backup', 'tables': {'proxies': {'rows': 12}}}
        with patch.object(remote, 'call', return_value=proof) as calling:
            self.assertEqual(remote.restore(self.c, self.root / 'run-1', downloaded), proof)
        calling.assert_called_once_with(self.c, 'restore', '--run', 'run-1', '--checksum', 'frozen-backup')
        self.assertEqual(json.loads((downloaded / 'restore-verification.json').read_text()), proof)


if __name__ == '__main__':
    unittest.main()
