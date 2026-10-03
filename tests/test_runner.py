import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
import subprocess
import sys
import catalog
from common import lock, read_json, write_json
import publication

class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.c={'timezone':'Asia/Tashkent','model':'gpt-6.1-sol','reasoning_effort':'max','codex':'codex'}

    def tearDown(self): self.tmp.cleanup()

    def test_one_success_per_local_day(self):
        at=datetime(2026,10,3,20,tzinfo=timezone.utc)  # October 4 in Tashkent
        self.assertFalse(catalog.due(self.c,{'last_completed_day':'2026-10-04'},True,at))
        self.assertTrue(catalog.due(self.c,{'last_completed_day':'2026-10-03'},True,at))
        self.assertFalse(catalog.due(self.c,{},False,at))
        self.assertFalse(catalog.due(self.c,{'disabled':True,'active_run':'x'},True,at))
        self.assertTrue(catalog.due(self.c,{'last_completed_day':'2026-10-04','active_run':'x'},True,at))

    def test_lock_excludes_another_process_and_releases_after_exit(self):
        path=self.root/'lock'
        script='from common import lock; import sys\nwith lock(sys.argv[1]): pass'
        with lock(path):
            result=subprocess.run([sys.executable,'-c',script,str(path)],capture_output=True,cwd=catalog.ROOT)
            self.assertNotEqual(result.returncode,0)
        result=subprocess.run([sys.executable,'-c',script,str(path)],capture_output=True,cwd=catalog.ROOT)
        self.assertEqual(result.returncode,0)

    def test_candidate_dedup_keeps_meaningful_query(self):
        path=self.root/'candidates.json'
        write_json(path,[{'url':'https://EXAMPLE.com/list?key=a#top'}, {'url':'https://example.com/list?key=a'},
                         {'url':'https://example.com/list?key=b'}])
        rows=catalog.candidate_rows(path)
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0]['url'],'https://example.com/list?key=a')
        with self.assertRaises(ValueError): catalog.normalize_url('file:///tmp/private')
        with self.assertRaises(ValueError): catalog.normalize_url('https://user:pass@example.com')

    def test_invalid_discovery_json_does_not_silently_pass(self):
        path=self.root/'candidates.json'; path.write_text('{bad')
        with self.assertRaises(json.JSONDecodeError): catalog.candidate_rows(path)

    def test_one_bad_candidate_does_not_discard_good_findings(self):
        path=self.root/'candidates.json'
        write_json(path,[{'url':'file:///private'},{'url':'https://example.com/feed'}])
        self.assertEqual(len(catalog.candidate_rows(path)),1)
        self.assertEqual(len(read_json(path.with_suffix('.rejected.json'))),1)

    def test_command_preserves_model_and_has_no_deadline(self):
        for session in (None,'session-id'):
            cmd=catalog.discovery_command(self.c,self.root,session)
            self.assertEqual(cmd[cmd.index('-m')+1],'gpt-6.1-sol')
            self.assertIn('model_reasoning_effort="max"',cmd)
            self.assertIn('web_search="live"',cmd)
            self.assertNotIn('--timeout',cmd)
            self.assertIn('workspace-write',cmd)
            self.assertIn('forced_login_method="chatgpt"',cmd)

    def test_corrupt_saved_snapshot_stops_before_upload(self):
        snapshot=self.root/'snapshot'; snapshot.mkdir()
        (snapshot/'part').write_bytes(b'changed')
        write_json(snapshot/'manifest.json',{'assets':[{'name':'part','bytes':7,'sha256':'incorrect'}]})
        with self.assertRaisesRegex(RuntimeError,'integrity'):
            publication.make_snapshot({},self.root,{})

    def test_restore_cannot_target_live_database(self):
        write_json(self.root/'manifest.json',{'dump_sha256':'x'})
        with self.assertRaises(ValueError): publication.restore_verify({},self.root,'proxy')

    def test_draft_release_recovers_after_create_without_journal(self):
        error=subprocess.CalledProcessError(1,['gh','api'],stderr='HTTP 404')
        draft={'id':123,'draft':True,'tag_name':'catalog-test'}
        with patch.object(publication,'gh_api',side_effect=[error,[draft]]):
            self.assertEqual(publication.release_record({'repository':'owner/repo'},'catalog-test'),draft)

    def test_release_lookup_does_not_treat_auth_failure_as_missing(self):
        error=subprocess.CalledProcessError(1,['gh','api'],stderr='HTTP 401')
        with patch.object(publication,'gh_api',side_effect=error):
            with self.assertRaises(subprocess.CalledProcessError):
                publication.release_record({'repository':'owner/repo'},'catalog-test')

    def test_resume_skips_completed_discovery_collection_and_upload(self):
        record={'started_at':'x','completed':['baseline','discovery','validation','source_import','collection',
                    'report','snapshot','upload','publish','retention']}
        write_json(self.root/'report.json',{'catalog_after':12})
        write_json(self.root/'snapshot/manifest.json',{})
        write_json(self.root/'publication.json',{'tag':'catalog-test'})
        state=self.root/'state.json'
        write_json(state,{'last_restore_week':datetime.now(timezone.utc).strftime('%G-%V')})
        with patch.object(catalog,'STATE',state), patch.object(catalog,'discover') as discovery, patch.object(catalog,'collect') as collection, patch.object(publication,'publish_assets') as upload:
            catalog.pipeline(self.c,self.root,record)
        discovery.assert_not_called(); collection.assert_not_called(); upload.assert_not_called()
        self.assertEqual(record['stage'],'completed')

if __name__=='__main__': unittest.main()
