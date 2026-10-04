import asyncio
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid

from psycopg import sql
import catalog
import continuous as worker
import publication
from common import config, db, pg_env, pg_tool, read_json, write_json


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.journal = worker.Journal(self.root / 'worker')
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.journal.close)

    def row(self, url='https://example.test/list', hints=None):
        return {'url': url, 'kind': 'feed_candidate', 'protocol_hints': hints or [],
                'evidence_url': url, 'notes': 'Evidence'}

    def test_intake_works_during_incomplete_research_and_does_not_modify_it(self):
        folder = self.root / 'ongoing'
        path = folder / 'research/candidates.json'
        write_json(path, [self.row(), {'url': 'file:///private'}])
        write_json(folder / 'research/research_summary.json', {'completion_status': 'incomplete'})
        before = {p.name: p.read_bytes() for p in path.parent.iterdir()}
        result = worker.intake(self.journal, folder)
        self.assertEqual(result['added'], 1)
        self.assertEqual(before, {p.name: p.read_bytes() for p in path.parent.iterdir()})
        self.assertTrue(worker.intake(self.journal, folder)['unchanged'])
        write_json(path, [dict(self.row(), notes='Expanded evidence')])
        self.assertEqual(worker.intake(self.journal, folder)['added'], 0)
        write_json(path, [self.row(hints=['http'])])
        self.assertEqual(worker.intake(self.journal, folder)['added'], 1)
        self.assertEqual(self.journal.stats()['states'], {'pending': 1, 'superseded': 1})

    def test_incomplete_json_does_not_erase_the_previous_queue(self):
        folder = self.root / 'ongoing'
        path = folder / 'research/candidates.json'
        write_json(path, [self.row()])
        worker.intake(self.journal, folder)
        before = self.journal.stats()
        path.write_text('[{"url":')
        with self.assertRaises(json.JSONDecodeError):
            worker.intake(self.journal, folder)
        self.assertEqual(before, self.journal.stats())

    def test_recovery_keeps_download_and_prevents_same_url_overlap(self):
        self.journal.enqueue('run', [self.row()], 'a', 'a')
        first = self.journal.claim()
        self.journal.downloaded(first, {'status': 'downloaded', 'content_sha256': 'saved'})
        self.journal.enqueue('run', [self.row(hints=['http'])], 'b', 'b')
        self.assertIsNone(self.journal.claim())
        self.journal.recover()
        recovered = self.journal.claim()
        self.assertEqual(first['id'], recovered['id'])
        self.assertEqual(json.loads(recovered['result'])['content_sha256'], 'saved')

    def test_partial_imports_remain_pending_and_receipts_count_once(self):
        self.journal.enqueue('run', [self.row()], 'a', 'a')
        job = self.journal.claim()
        result = {'status': 'collected_partial', 'continuous_receipt': 'one', 'inserted': 2}
        self.journal.finish(job, result, imported=True, retry_seconds=0)
        self.journal.finish(job, result, imported=True, retry_seconds=0)
        self.assertEqual(self.journal.stats()['revision'], 1)
        self.assertEqual(self.journal.stats()['inserted'], 2)
        self.assertEqual(self.journal.stats()['states'], {'retry': 1})
        job = self.journal.claim()
        self.journal.finish(job, dict(result, status='collected', continuous_receipt='two', inserted=1), imported=True)
        self.assertEqual(self.journal.stats()['inserted'], 3)
        self.assertEqual(self.journal.stats()['revision'], 2)

    def test_failed_source_does_not_block_another_source(self):
        self.journal.enqueue('run', [self.row(), self.row('https://another.test/list')], 'a', 'a')
        failed = self.journal.claim()
        self.journal.finish(failed, {'status': 'http_404'}, retry_seconds=300, error='http_404')
        following = self.journal.claim()
        self.assertNotEqual(failed['url'], following['url'])

    def test_import_barrier_excludes_another_process(self):
        script = 'import sys; from pathlib import Path; import continuous;\nwith continuous.import_guard(Path(sys.argv[1])): print("entered",flush=True)'
        with worker.import_guard(self.journal.home):
            child = subprocess.Popen([os.sys.executable, '-c', script, str(self.journal.home)], stdout=subprocess.PIPE, text=True)
            try:
                with self.assertRaises(subprocess.TimeoutExpired):
                    child.communicate(timeout=0.2)
            finally:
                if child.poll() is not None:
                    self.fail('Import crossed the active snapshot barrier')
        self.assertEqual(child.communicate(timeout=5)[0].strip(), 'entered')


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.journal = worker.Journal(self.root / 'continuous')
        self.journal.set('revision', 1)
        self.c = {'continuous': {'enabled': True}, 'timezone': 'Asia/Tashkent', 'retain_daily': 7, 'retain_weekly': 4}
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.journal.close)

    def test_upload_failure_reuses_snapshot_and_keeps_previous_latest(self):
        latest = self.root / 'latest.json'
        write_json(latest, {'tag': 'old', 'configurations': 10})
        snapshots = []
        def snapshot(c, folder, report):
            snapshots.append(folder.name)
            write_json(folder / 'snapshot/manifest.json', {'assets': []})
            write_json(folder / 'report.json', {'revision': 1, 'catalog_after': 12, 'new_configurations': 2})
        def upload(c, folder, manifest):
            write_json(folder / 'publication.json', {'tag': 'catalog-' + folder.name, 'url': 'https://example.test/release', 'manifest_sha256': 'abc'})
        def commit(c, folder, receipt, report):
            write_json(latest, {'tag': receipt['tag'], 'configurations': report['catalog_after']})
        with ExitStack() as stack:
            stack.enter_context(patch.object(worker, 'LOCAL', self.root))
            stack.enter_context(patch.object(worker, 'ROOT', self.root))
            stack.enter_context(patch.object(publication, 'make_snapshot', side_effect=snapshot))
            uploading = stack.enter_context(patch.object(publication, 'publish_assets', side_effect=OSError('offline')))
            stack.enter_context(patch.object(publication, 'download'))
            stack.enter_context(patch.object(publication, 'restore_verify'))
            commit_mock = stack.enter_context(patch.object(publication, 'commit_publication', side_effect=commit))
            stack.enter_context(patch.object(publication, 'prune'))
            with self.assertRaises(OSError):
                worker.publish(self.c, self.journal)
            active = read_json(self.journal.home / 'publication-state.json')['active']
            self.assertEqual(read_json(latest)['tag'], 'old')
            commit_mock.assert_not_called()
            uploading.side_effect = upload
            result = worker.publish(self.c, self.journal)
            self.assertEqual(snapshots, [active])
            self.assertEqual(result['configurations'], 12)
            self.assertEqual(read_json(latest)['tag'], 'catalog-' + active)
            self.assertEqual(worker.publish(self.c, self.journal)['state'], 'unchanged')

    def test_retention_uses_distinct_days_and_weeks(self):
        at = datetime(2026, 10, 4, 8, tzinfo=timezone.utc)
        records = [{'tag_name': str(i), 'name': str(i), 'published_at': (at-timedelta(hours=i)).isoformat()}
                   for i in range(24*35)]
        records[-1]['name'] = 'Initial seed: old'
        c = dict(self.c, continuous={'retain_hourly': 24})
        kept = publication.retained_tags(c, records, 'current')
        self.assertTrue(set(map(str, range(24))) <= kept)
        self.assertIn('current', kept)
        self.assertIn(str(len(records)-1), kept)
        self.assertGreater(len(kept), 24)
        self.assertLess(len(kept), 24+7+4+2)

    def test_failed_git_push_retries_existing_commit_without_rewriting_latest(self):
        folder = self.root / 'publication-one'
        folder.mkdir()
        receipt = {'tag': 'catalog-publication-one', 'manifest_sha256': 'abc', 'assets_verified_at': 'done'}
        latest = {'tag': receipt['tag'], 'manifest_sha256': 'abc', 'published_at': 'original-time'}
        write_json(self.root / 'latest.json', latest)
        def capture(args, **kwargs):
            if args[1:3] == ['rev-parse', 'HEAD']: return 'new-head\n'
            if args[1:3] == ['rev-parse', 'origin/main']: return 'old-head\n'
            if args[1] == 'rev-list': return '1\n'
            if args[1] == 'log': return 'Publish proxy catalog publication-one\n'
            if args[1] == 'diff': return 'latest.json\nreports/publication-one.json\n'
            self.fail(args)
        with patch.object(publication, 'ROOT', self.root), patch.object(publication, 'capture', side_effect=capture), patch.object(publication, 'run') as commands:
            self.assertEqual(publication.commit_publication(self.c, folder, receipt, {}), latest)
        self.assertEqual(read_json(self.root / 'latest.json'), latest)
        self.assertEqual(sum(call.args[0][1] == 'push' for call in commands.call_args_list), 1)
        self.assertFalse(any(call.args[0][1] == 'commit' for call in commands.call_args_list))


@unittest.skipUnless(os.environ.get('PROXY_TEST_DATABASE'), 'Set PROXY_TEST_DATABASE=1 for isolated PostgreSQL tests')
class DatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.c = config()
        cls.name = 'proxy_catalog_verify_continuous_' + uuid.uuid4().hex[:10]
        cls.c = dict(cls.c, database=dict(cls.c['database'], dbname=cls.name), storage=str(cls.root/'cache'))
        original = config()
        with db(original) as conn:
            version = conn.info.server_version // 10000
        schema = cls.root / 'schema.sql'
        subprocess.run([pg_tool(original, 'pg_dump', version), '--schema-only', '--no-owner', '--no-acl', '--file', str(schema)],
                       env=pg_env(original), check=True, capture_output=True)
        with db(original, dbname='postgres') as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.name)))
        try:
            subprocess.run([str(Path(pg_tool(original, 'pg_dump', version)).with_name('psql')), '-X', '-v', 'ON_ERROR_STOP=1', '-f', str(schema)],
                           env=pg_env(cls.c), check=True, capture_output=True)
        except BaseException:
            cls.tearDownClass()
            raise
        worker.configure_collector(cls.c)

    @classmethod
    def tearDownClass(cls):
        with db(config(), dbname='postgres') as admin:
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(cls.name)))
        worker.configure_collector(config())
        cls.temp.cleanup()

    def setUp(self):
        with db(self.c) as conn:
            conn.execute('TRUNCATE proxies,proxy_stats,proxy_lists RESTART IDENTITY CASCADE')
        self.folder = self.root / uuid.uuid4().hex
        self.journal = worker.Journal(self.folder)
        self.addCleanup(self.journal.close)
        row = {'url': 'https://example.test/feed', 'kind': 'feed_candidate', 'protocol_hints': ['http'],
               'evidence_url': 'https://example.test/feed', 'notes': ''}
        self.journal.enqueue('research', [row], 'a', 'a')
        self.job = self.journal.claim()

    def result(self, text='http://8.8.8.8:8080\n', partial=False):
        checksum = hashlib.sha256(text.encode()).hexdigest()
        path = self.root / 'cache/payloads' / (checksum + '.gz')
        with gzip.open(path, 'wb') as out:
            out.write(text.encode())
        return {'list_url': 'https://example.test/feed', 'protocol_hints': ['http'], 'status': 'downloaded_partial' if partial else 'downloaded',
                'content_sha256': checksum, 'payload_path': str(path), 'finished_at': datetime.now(timezone.utc),
                'content_type': 'text/plain', 'attempts': 1}

    def snapshot(self):
        with db(self.c) as conn:
            return {'proxies': conn.execute('SELECT * FROM proxies ORDER BY proxy_id').fetchall(),
                    'stats': conn.execute('SELECT * FROM proxy_stats ORDER BY proxy_id').fetchall(),
                    'sources': conn.execute('SELECT * FROM proxy_lists ORDER BY url').fetchall(),
                    'sequence': conn.execute('SELECT last_value,is_called FROM proxies_proxy_id_seq').fetchone()}

    def test_lost_ack_replay_preserves_ids_statistics_and_timestamps(self):
        result = self.result()
        self.journal.downloaded(self.job, result)
        with patch.object(self.journal, 'finish', side_effect=RuntimeError('lost acknowledgement')):
            with self.assertRaisesRegex(RuntimeError, 'lost acknowledgement'):
                worker.apply_download(self.c, self.journal, self.job, result)
        with db(self.c) as conn:
            ident = conn.execute('SELECT proxy_id FROM proxies').fetchone()['proxy_id']
            conn.execute('INSERT INTO proxy_stats(proxy_id,connection_attempts,last_connection_attempt_at) VALUES(%s,7,now())', (ident,))
        before = self.snapshot()
        self.journal.recover()
        job = self.journal.claim()
        worker.apply_download(self.c, self.journal, job, json.loads(job['result']))
        self.assertEqual(before, self.snapshot())
        self.assertEqual(self.journal.stats()['inserted'], 1)
        self.assertEqual(self.journal.stats()['revision'], 1)
        worker.apply_download(self.c, self.journal, job, result)
        self.assertEqual(before, self.snapshot())
        self.assertEqual(self.journal.stats()['revision'], 1)

    def test_source_and_configurations_rollback_together(self):
        original = worker.collector.Store.save
        def fail(store, result):
            original(store, result)
            raise RuntimeError('after savepoint before outer commit')
        with patch.object(worker.collector.Store, 'save', fail):
            with self.assertRaises(RuntimeError):
                worker.apply_download(self.c, self.journal, self.job, self.result())
        snapshot = self.snapshot()
        self.assertEqual(snapshot['proxies'], [])
        self.assertEqual(snapshot['sources'], [])
        self.assertEqual(self.journal.stats()['revision'], 0)

    def test_process_crash_after_commit_resumes_from_durable_download(self):
        result = self.result()
        self.journal.downloaded(self.job, result)
        cpath = self.folder / 'test-config.json'
        write_json(cpath, self.c)
        script = '''import json,os,sys
from pathlib import Path
import continuous as worker
c=json.loads(Path(sys.argv[1]).read_text()); worker.configure_collector(c)
j=worker.Journal(Path(sys.argv[2])); j.recover(); job=j.claim()
j.finish=lambda *a,**k: os._exit(23)
worker.apply_download(c,j,job,json.loads(job['result']))
'''
        child = subprocess.run([os.sys.executable, '-c', script, str(cpath), str(self.folder)], capture_output=True)
        self.assertEqual(child.returncode, 23, child.stderr)
        before = self.snapshot()
        self.assertEqual(len(before['proxies']), 1)
        self.journal.recover()
        job = self.journal.claim()
        worker.apply_download(self.c, self.journal, job, json.loads(job['result']))
        self.assertEqual(before, self.snapshot())
        self.assertEqual(self.journal.stats()['inserted'], 1)
        self.assertEqual(self.journal.stats()['states'], {'imported': 1})

    def test_partial_then_complete_download_adds_only_new_configurations(self):
        worker.apply_download(self.c, self.journal, self.job, self.result(partial=True))
        self.assertEqual(self.journal.stats()['states'], {'retry': 1})
        job = self.journal.claim(at=time_in_future())
        worker.apply_download(self.c, self.journal, job, self.result('http://8.8.8.8:8080\nhttp://1.1.1.1:8081\n'))
        self.assertEqual(len(self.snapshot()['proxies']), 2)
        self.assertEqual(self.journal.stats()['inserted'], 2)
        self.assertEqual(self.journal.stats()['states'], {'imported': 1})

    def test_empty_or_failed_source_never_becomes_enabled(self):
        worker.apply_download(self.c, self.journal, self.job, self.result('<html>not a proxy list</html>'))
        self.assertEqual(self.snapshot()['sources'], [])
        self.assertEqual(self.journal.stats()['revision'], 0)


def time_in_future():
    return datetime.now(timezone.utc).timestamp() + 100000


if __name__ == '__main__':
    unittest.main()
