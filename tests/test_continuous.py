import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import PropertyMock, patch
import uuid

from psycopg import sql
import catalog
import continuous as worker
import publication
from proxy_formats import Proxy, canonical_json, pack_connection_settings
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

    def test_cached_retry_preserves_url_order_and_retry_deadline(self):
        self.journal.enqueue('run', [self.row()], 'a', 'a')
        first = self.journal.claim()
        cached = {'status': 'downloaded', 'content_sha256': 'saved'}
        self.journal.finish(first, cached, retry_seconds=300)
        self.journal.enqueue('run', [self.row(hints=['http']), self.row('https://another.test/list')], 'b', 'b')
        other = self.journal.claim()
        self.assertEqual(other['url'], 'https://another.test/list')
        self.assertIsNone(self.journal.claim())
        replay = self.journal.claim(at=time_in_future())
        self.assertEqual(replay['id'], first['id'])
        self.journal.finish(replay, dict(cached, status='collected', continuous_receipt='saved'), imported=True)
        self.assertEqual(json.loads(self.journal.claim()['body'])['protocol_hints'], ['http'])

    def test_concurrent_claims_do_not_duplicate_jobs(self):
        self.journal.enqueue('run', [self.row(f'https://example.test/{i}') for i in range(30)], 'a', 'a')
        second = worker.Journal(self.journal.home)
        self.addCleanup(second.close)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda i: (self.journal if i % 2 else second).claim(), range(40)))
        claimed = [r for r in results if r]
        self.assertEqual(len(claimed), 30)
        self.assertEqual(len({r['id'] for r in claimed}), 30)

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

    def test_publication_barrier_waits_for_all_shared_importers(self):
        ready = threading.Barrier(3)
        release = threading.Event()
        def importing():
            with worker.import_guard(self.journal.home, shared=True):
                ready.wait(timeout=5)
                release.wait(timeout=5)
        def publishing():
            with worker.import_guard(self.journal.home):
                return 'snapshot'
        with ThreadPoolExecutor(max_workers=3) as pool:
            workers = [pool.submit(importing) for _ in range(2)]
            try:
                ready.wait(timeout=5)
                publication = pool.submit(publishing)
                with self.assertRaises(TimeoutError):
                    publication.result(timeout=0.1)
            finally:
                release.set()
            self.assertEqual(publication.result(timeout=5), 'snapshot')
            for future in workers:
                future.result(timeout=5)


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
        def snapshot(c, folder, report, **kwargs):
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

    def test_binary_copy_preserves_every_transport_and_escaped_options(self):
        transports = ['unknown', 'http', 'https', 'socks4', 'socks5', 'shadowsocks', 'shadowsocksr',
                      'vmess', 'vless', 'trojan', 'hysteria', 'hysteria2', 'tuic', 'wireguard', 'ssh']
        proxies = [Proxy('2606:4700:4700::1111' if i % 2 else '8.8.8.8', 1000 + i, transport,
                         {'username': 'é\t\\', 'password': 'line\n\x00quote"',
                          'nested': {'zero\x00': [True, False, None, 1, 1.5]}})
                   for i, transport in enumerate(transports)]
        result = self.result()
        # Exercise the persisted parser-cache contract for all database transports.
        cache = worker.collector.parsed_cache_path(worker.collector.PARSER_VERSION, result['content_sha256'], result['protocol_hints'])
        summary = {'entries_found': len(proxies), 'unique_entries': len(proxies),
                   'duplicates_in_list': 0, 'invalid_entries': 0, 'warnings': {}}
        with gzip.open(cache, 'wt', encoding='utf-8') as out:
            out.write(canonical_json(summary) + '\n')
            for proxy in proxies:
                out.write(canonical_json(proxy.as_dict()) + '\n')
        cache.with_suffix('.copy-v1.gz').unlink(missing_ok=True)
        try:
            worker.apply_download(self.c, self.journal, self.job, result)
            rows = self.snapshot()['proxies']
            expected = {p.key: (p.address, p.port, pack_connection_settings(p.protocol, p.settings)) for p in proxies}
            actual = {bytes(r['connection_key']): (r['address'], r['port'], r['connection_settings']) for r in rows}
            self.assertEqual(actual, expected)
            self.assertEqual(len(rows), len(transports))
        finally:
            cache.unlink(missing_ok=True)
            cache.with_suffix('.copy-v1.gz').unlink(missing_ok=True)

    def test_parallel_pipeline_respects_limit_reuses_writer_and_keeps_loop_responsive(self):
        self.journal.finish(self.job, {'status': 'test_setup'})
        rows = [{'url': f'https://example.test/feed/{i}', 'kind': 'feed_candidate',
                 'protocol_hints': ['http']} for i in range(12)]
        self.journal.enqueue('research', rows, 'b', 'b')
        payload = self.result('http://9.9.9.9:9090\n')
        class Downloader:
            def __init__(self, options): self.client = self
            async def aclose(self): pass
            async def fetch(self, row):
                await asyncio.sleep(0.01)
                return dict(payload, list_url=row['list_url'])
        original_claim = self.journal.claim
        ticks = []
        def slow_claim():
            time.sleep(0.08)
            return original_claim()
        async def scenario():
            async def heartbeat():
                for _ in range(15):
                    ticks.append(time.monotonic())
                    await asyncio.sleep(0.02)
            pulse = asyncio.create_task(heartbeat())
            outcome = await worker.collect(dict(self.c, concurrency=8, continuous={'parse_workers': 2, 'import_workers': 1}),
                                           self.journal, limit=5, once=True)
            await pulse
            return outcome
        with patch.object(worker.collector, 'Downloader', Downloader), \
             patch.object(self.journal, 'claim', side_effect=slow_claim), \
             patch.object(worker.collector, 'Store', wraps=worker.collector.Store) as stores:
            outcome = asyncio.run(scenario())
        self.assertEqual(outcome['finished'], 5)
        self.assertEqual(outcome['states'], {'empty': 1, 'imported': 5, 'pending': 7})
        self.assertEqual(stores.call_count, 1)
        self.assertEqual(outcome['inserted'], 1)
        self.assertEqual(len(self.snapshot()['sources']), 5)
        self.assertLess(max(b - a for a, b in zip(ticks, ticks[1:])), 0.075)

    def test_reused_writer_counts_overlapping_batches_and_preserves_newer_observations(self):
        importer = worker.Importer(self.c, self.journal)
        self.addCleanup(importer.close)
        def payload(start, stop):
            return ''.join(f'http://8.8.{i // 256}.{i % 256}:8080\n' for i in range(start, stop))
        first = self.result(payload(0, 2000))
        outcome = importer.apply(self.job, first)
        self.assertEqual((outcome['inserted'], outcome['refreshed']), (2000, 0))
        ids = {r['connection_key']: r['proxy_id'] for r in self.snapshot()['proxies']}
        row = json.loads(self.job['body'])
        self.journal.enqueue('next', [row], 'b', 'b')
        second = self.result(payload(1000, 2500))
        second['finished_at'] = first['finished_at'] + timedelta(seconds=1)
        outcome = importer.apply(self.journal.claim(), second)
        self.assertEqual((outcome['inserted'], outcome['refreshed']), (500, 1000))
        before = self.snapshot()['proxies']
        self.journal.enqueue('older', [row], 'c', 'c')
        older = self.result(payload(1200, 1201))
        older['finished_at'] = first['finished_at'] - timedelta(seconds=1)
        outcome = importer.apply(self.journal.claim(), older)
        self.assertEqual((outcome['inserted'], outcome['refreshed']), (0, 0))
        after = self.snapshot()['proxies']
        self.assertEqual(before, after)
        self.assertEqual(len(after), 2500)
        self.assertTrue(all(r['proxy_id'] == ids[r['connection_key']] for r in after if r['connection_key'] in ids))
        self.assertEqual(self.journal.stats()['inserted'], 2500)

    def test_pre_pg18_counting_remains_supported(self):
        store = worker.collector.Store(acquire_lock=False)
        self.addCleanup(store.close)
        with patch.object(type(store.writer.info), 'server_version', new_callable=PropertyMock, return_value=170000):
            result = worker.apply_download(self.c, self.journal, self.job, self.result(), store)
        self.assertEqual(result['inserted'], 1)
        self.assertEqual(result['refreshed'], 0)

    def test_parallel_imports_deduplicate_and_keep_latest_observations(self):
        self.journal.finish(self.job, {'status': 'test_setup'})
        rows = [{'url': f'https://example.test/parallel/{i}', 'kind': 'feed_candidate',
                 'protocol_hints': ['http']} for i in range(8)]
        self.journal.enqueue('parallel', rows, 'p', 'p')
        jobs = [self.journal.claim() for _ in rows]
        at = datetime.now(timezone.utc)
        results = []
        for i, job in enumerate(jobs):
            # Reverse every other source's input order to exercise lock ordering.
            addresses = list(range(150)) + list(range(150 + i * 20, 170 + i * 20))
            if i % 2: addresses.reverse()
            result = self.result(''.join(f'http://8.8.{n // 256}.{n % 256}:8080\n' for n in addresses))
            result.update(list_url=job['url'], finished_at=at + timedelta(seconds=i))
            self.journal.downloaded(job, result)
            worker.collector.prepare_import(result)
            results.append(result)
        importer = worker.Importer(self.c, self.journal)
        self.addCleanup(importer.close)
        with ThreadPoolExecutor(max_workers=4) as pool:
            outcomes = list(pool.map(lambda pair: importer.apply(*pair), zip(jobs, results)))
        self.assertEqual(sum(r['inserted'] for r in outcomes), 310)
        saved = self.snapshot()
        self.assertEqual(len(saved['proxies']), 310)
        self.assertEqual(len(saved['sources']), 8)
        self.assertEqual(self.journal.stats()['states'], {'empty': 1, 'imported': 8})
        for proxy in saved['proxies']:
            n = int(proxy['address'].split('.')[-2]) * 256 + int(proxy['address'].split('.')[-1])
            latest = 7 if n < 150 else (n - 150) // 20
            self.assertEqual(proxy['last_seen_at'], at + timedelta(seconds=latest))
        # Replaying any source after its receipt was committed changes no rows.
        for job, result in zip(jobs, results):
            importer.apply(job, result)
        self.assertEqual(saved, self.snapshot())
        self.assertEqual(self.journal.stats()['inserted'], 310)

    def test_shutdown_keeps_queued_downloads_and_finishes_active_transaction(self):
        self.journal.finish(self.job, {'status': 'test_setup'})
        rows = [{'url': f'https://example.test/stop/{i}', 'kind': 'feed_candidate',
                 'protocol_hints': ['http']} for i in range(6)]
        self.journal.enqueue('shutdown', rows, 's', 's')
        payload = self.result('http://1.1.1.1:8080\n')
        started, release = threading.Event(), threading.Event()
        entered = []
        original = worker.Importer.apply
        def pausing_import(importer, job, result):
            entered.append(job['id'])
            started.set()
            if not release.wait(timeout=10):
                raise RuntimeError('Test did not release importer')
            return original(importer, job, result)
        class Downloader:
            def __init__(self, options): self.client = self
            async def aclose(self): pass
            async def fetch(self, row): return dict(payload, list_url=row['list_url'])
        c = dict(self.c, concurrency=6, continuous={'parse_workers': 1, 'import_workers': 1})
        async def scenario():
            task = asyncio.create_task(worker.collect(c, self.journal, once=True))
            try:
                for _ in range(200):
                    if started.is_set(): break
                    await asyncio.sleep(0.01)
                self.assertTrue(started.is_set())
                await asyncio.sleep(0.15)
                task.cancel()
                asyncio.get_running_loop().call_later(0.1, release.set)
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        with patch.object(worker.collector, 'Downloader', Downloader), \
             patch.object(worker, 'ProcessPoolExecutor', side_effect=lambda **kw: ThreadPoolExecutor(max_workers=1)), \
             patch.object(worker.Importer, 'apply', pausing_import):
            asyncio.run(scenario())
        self.assertEqual(len(entered), 1)
        self.assertEqual(self.journal.stats()['states'], {'empty': 1, 'imported': 1, 'leased': 5})
        self.journal.recover()
        remaining = [self.journal.claim() for _ in range(5)]
        self.assertTrue(all(json.loads(job['result'])['status'] == 'downloaded' for job in remaining))
        for job in remaining:
            worker.apply_download(self.c, self.journal, job, json.loads(job['result']))
        self.assertEqual(self.journal.stats()['states'], {'empty': 1, 'imported': 6})
        self.assertEqual(self.journal.stats()['inserted'], 1)

    def test_fast_snapshot_restores_rows_statistics_and_identity_sequence(self):
        worker.apply_download(self.c, self.journal, self.job,
                              self.result('http://8.8.8.8:8080\nsocks5://1.1.1.1:1080\n'))
        with db(self.c) as conn:
            conn.execute('INSERT INTO proxy_stats(proxy_id,connection_attempts,last_connection_attempt_at) SELECT proxy_id,7,now() FROM proxies')
        folder = self.folder / 'snapshot-test'
        folder.mkdir()
        manifest = publication.make_snapshot(dict(self.c, chunk_bytes=1024), folder, {'kind': 'test'})
        self.assertEqual(manifest['tables']['proxies']['rows'], 2)
        self.assertEqual(manifest['tables']['proxy_stats']['rows'], 2)
        proof = publication.restore_verify(self.c, folder / 'snapshot')
        self.assertEqual(proof['tables'], manifest['tables'])

    def test_snapshot_allows_imports_during_dump_and_restores_the_captured_state(self):
        worker.apply_download(self.c, self.journal, self.job, self.result())
        with db(self.c) as conn:
            conn.execute('INSERT INTO proxy_stats(proxy_id,connection_attempts,last_connection_attempt_at) SELECT proxy_id,7,now() FROM proxies')
        row = {'url': 'https://example.test/after-snapshot', 'kind': 'feed_candidate', 'protocol_hints': ['http']}
        self.journal.enqueue('during-backup', [row], 'b', 'b')
        job = self.journal.claim()
        result = dict(self.result('http://1.1.1.1:8081\n'), list_url=row['url'])
        worker.collector.prepare_import(result)
        folder = self.folder / 'concurrent-snapshot'
        folder.mkdir()
        original_run = publication.run
        observed = {}
        def report(connection):
            with self.journal.snapshot():
                return {'revision': self.journal.stats()['revision'],
                        'catalog_after': connection.execute('SELECT count(*) AS n FROM proxies').fetchone()['n']}
        with ThreadPoolExecutor(max_workers=1) as pool:
            def run_with_import(args, **kwargs):
                if Path(args[0]).name == 'pg_dump':
                    # This must finish while pg_dump's exported snapshot remains open.
                    imported = pool.submit(worker.apply_download, self.c, self.journal, job, result)
                    observed['imported'] = imported.result(timeout=5)
                    with db(self.c) as conn:
                        conn.execute('UPDATE proxy_stats SET connection_attempts=9')
                    outcome = original_run(args, **kwargs)
                    with db(self.c) as conn:
                        observed['dump_sequence'] = conn.execute('SELECT last_value,is_called FROM proxies_proxy_id_seq').fetchone()
                        conn.execute("SELECT nextval('proxies_proxy_id_seq') FROM generate_series(1,10)")
                    return outcome
                return original_run(args, **kwargs)
            with patch.object(publication, 'run', side_effect=run_with_import):
                manifest = publication.make_snapshot(dict(self.c, chunk_bytes=1024), folder, report,
                                                      snapshot_guard=lambda: worker.import_guard(self.folder))
        self.assertEqual(observed['imported']['inserted'], 1)
        self.assertEqual(len(self.snapshot()['proxies']), 2)
        self.assertEqual(self.journal.stats()['revision'], 2)
        self.assertEqual(read_json(folder / 'snapshot/report.json'), {'revision': 1, 'catalog_after': 1})
        self.assertEqual(manifest['tables']['proxies']['rows'], 1)
        self.assertEqual(manifest['tables']['proxy_lists']['rows'], 1)
        self.assertEqual(manifest['tables']['identity_sequence'], observed['dump_sequence'])
        proof = publication.restore_verify(self.c, folder / 'snapshot')
        self.assertEqual(proof['tables'], manifest['tables'])


def time_in_future():
    return datetime.now(timezone.utc).timestamp() + 100000


if __name__ == '__main__':
    unittest.main()
