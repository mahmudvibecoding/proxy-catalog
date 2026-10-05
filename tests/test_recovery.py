import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from contextlib import ExitStack
from datetime import datetime, timezone
import httpx
import psycopg
import catalog
from common import read_json, write_json, lock
import postgres_service


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.c={'timezone':'Asia/Tashkent','model':'gpt-6.1-sol','reasoning_effort':'max','codex':'fake-codex'}

    def tearDown(self): self.temp.cleanup()

    def test_database_not_ready_does_not_probe_network(self):
        with patch.object(catalog,'db',side_effect=psycopg.OperationalError('starting up')), patch.object(catalog.httpx,'Client') as client:
            result=catalog.readiness(self.c)
        self.assertFalse(result['ready'])
        self.assertEqual(result['waiting_for'],'database')
        client.assert_not_called()

    def test_offline_is_readiness_failure(self):
        with patch.object(catalog,'db'), patch.object(catalog.httpx,'Client') as client:
            client.return_value.__enter__.return_value.get.side_effect=httpx.ConnectError('offline')
            result=catalog.readiness(self.c)
        self.assertEqual(result['waiting_for'],'internet')

    def test_dependencies_do_not_consume_attempts_and_ready_retries_same_run(self):
        folder=self.root/'runs'/'saved-run'; folder.mkdir(parents=True)
        state=self.root/'state.json'
        write_json(state,{'active_run':'saved-run','retry_after':'2099-01-01T00:00:00+00:00'})
        record={'id':'saved-run','stage':'discovery','completed':['baseline'],'attempts':7,
                'discovery_session':'existing-session','issues':[]}
        write_json(folder/'run.json',record)
        with ExitStack() as stack:
            for name,value in [('LOCAL',self.root),('STATE',state)]: stack.enter_context(patch.object(catalog,name,value))
            stack.enter_context(patch.object(catalog,'lock',side_effect=lambda:lock(self.root/'lock')))
            stack.enter_context(patch.object(catalog,'session_active',return_value=True))
            ready=stack.enter_context(patch.object(catalog,'readiness',return_value={'ready':False,'waiting_for':'database'}))
            pipeline=stack.enter_context(patch.object(catalog,'pipeline',side_effect=catalog.DiscoveryIncomplete('temporary failure')))
            stack.enter_context(patch.object(catalog.subprocess,'Popen'))
            catalog.execute(self.c,'check')
            self.assertEqual(read_json(folder/'run.json'),record)
            self.assertIsNone(read_json(state)['retry_after'])
            pipeline.assert_not_called()
            ready.return_value={'ready':True}
            with self.assertRaises(catalog.DiscoveryIncomplete): catalog.execute(self.c,'check')
        saved=read_json(folder/'run.json')
        self.assertEqual(saved['discovery_session'],'existing-session')
        self.assertEqual(saved['attempts'],8)
        self.assertEqual(saved['completed'],['baseline'])
        retry=datetime.fromisoformat(read_json(state)['retry_after'])-datetime.now(timezone.utc)
        self.assertGreater(retry.total_seconds(),0)
        self.assertLessEqual(retry.total_seconds(),60)

    def test_discovery_failure_never_advances_to_validation_or_collection(self):
        record={'stage':'discovery','completed':['baseline'],'issues':[],'discovery_session':'saved'}
        research=self.root/'research'; research.mkdir()
        data=[{'url':'https://example.com/already-saved'}]
        write_json(research/'candidates.json',data)
        with patch.object(catalog,'discover',side_effect=catalog.DiscoveryIncomplete('interrupted')), patch.object(catalog,'validate_candidates') as validate, patch.object(catalog,'collect') as collect:
            with self.assertRaises(catalog.DiscoveryIncomplete): catalog.pipeline(self.c,self.root,record)
        self.assertEqual(record['completed'],['baseline'])
        self.assertEqual(record['discovery_session'],'saved')
        self.assertEqual(read_json(research/'candidates.json'),data)
        validate.assert_not_called(); collect.assert_not_called()

    def discovery(self, completed=True, status='complete', exit_code=0, fresh=True, clock_skew=False):
        workspace=self.root/'research'; workspace.mkdir(exist_ok=True)
        (workspace/'.git').mkdir(exist_ok=True)
        write_json(workspace/'candidates.json',[{'url':'https://example.com/saved'}])
        summary=workspace/'research_summary.json'
        write_json(summary,{'completion_status':'complete','summary':'old'})
        os.utime(summary,(1,1))
        record={'discovery_session':'saved-session'}
        def child(*args,**kwargs):
            if fresh: write_json(summary,{'completion_status':status,'summary':'current'})
            if clock_skew: os.utime(summary,(1,1))
            events=[{'type':'thread.started','thread_id':'saved-session'}]
            if completed: events.append({'type':'turn.completed','usage':{}})
            proc=Mock(pid=123,stdout=[json.dumps(x)+'\n' for x in events])
            proc.wait.return_value=exit_code; proc.poll.return_value=exit_code
            return proc
        with patch.object(catalog,'source_records',return_value=[]), patch.object(catalog.subprocess,'Popen',side_effect=child):
            catalog.discover(self.c,self.root,record)
        return record

    def test_nonzero_exit_preserves_incomplete_discovery(self):
        with self.assertRaises(catalog.DiscoveryIncomplete): self.discovery(exit_code=1)

    def test_zero_exit_without_completed_turn_is_not_completion(self):
        with self.assertRaises(catalog.DiscoveryIncomplete): self.discovery(completed=False)

    def test_incomplete_summary_is_not_completion(self):
        with self.assertRaises(catalog.DiscoveryIncomplete): self.discovery(status='incomplete')

    def test_stale_completed_summary_is_not_completion(self):
        with self.assertRaises(catalog.DiscoveryIncomplete): self.discovery(fresh=False)

    def test_fresh_completion_preserves_session_and_candidates(self):
        result=self.discovery()
        self.assertEqual(result['discovery_session'],'saved-session')
        self.assertEqual(result['research_summary']['summary'],'current')
        self.assertEqual(read_json(self.root/'candidates.json')[0]['url'],'https://example.com/saved')

    def test_fresh_completion_handles_filesystem_or_remote_clock_skew(self):
        result=self.discovery(clock_skew=True)
        self.assertEqual(result['research_summary']['summary'],'current')

    def test_postgres_service_refuses_missing_cluster_without_creating_it(self):
        missing=self.root/'not-a-cluster'
        with self.assertRaises(FileNotFoundError):
            postgres_service.settings({'postgres_service':{'data_directory':str(missing)}})
        self.assertFalse(missing.exists())


if __name__=='__main__': unittest.main()
