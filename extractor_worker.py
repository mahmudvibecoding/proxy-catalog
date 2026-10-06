"""Small server-side status and idempotent replay operations for parser development."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

import catalog
import continuous
from common import LOCAL, config, lock, now, read_json, write_json


def gap_status():
    state = read_json(LOCAL / 'state.json', {})
    ident = state.get('active_run') or state.get('last_run')
    if not ident or not re.fullmatch(r'[A-Za-z0-9_-]+', ident):
        return {'available': False, 'reason': 'no_research_run'}
    path = LOCAL / 'runs' / ident / 'research/parser_gaps.json'
    try:
        stat = path.stat()
    except FileNotFoundError:
        return {'available': False, 'run_id': ident, 'reason': 'no_parser_gaps'}
    return {'available': True, 'run_id': ident, 'path': str(path),
            'signature': [ident, stat.st_ino, stat.st_size, stat.st_mtime_ns],
            'bytes': stat.st_size, 'checked_at': now()}


def replay(c, urls, revision, expected_version, journal=None):
    """Queue only named existing sources, retaining caches and prior import receipts."""
    with lock(LOCAL / 'extractor-agent/replay.lock'):
        return _replay(c, urls, revision, expected_version, journal)


def _replay(c, urls, revision, expected_version, journal=None):
    if not re.fullmatch(r'[a-f0-9]{40}', revision):
        raise ValueError('Expected a full Git commit for replay')
    if expected_version != continuous.collector.PARSER_VERSION:
        raise RuntimeError('Deployed parser version differs from the replay request')
    if not isinstance(urls, list) or any(not isinstance(url, str) for url in urls):
        raise ValueError('replay_urls must be an array of URLs')
    urls = sorted({catalog.normalize_url(url) for url in urls})
    checksum = hashlib.sha256(json.dumps(urls).encode()).hexdigest()
    run_id = f'parser-v{expected_version}-{revision[:16]}'
    home = LOCAL / 'extractor-agent/replays'
    receipt = home / (revision + '.json')
    previous = read_json(receipt)
    if previous:
        if previous['urls_sha256'] != checksum or previous['parser_version'] != expected_version:
            raise RuntimeError('A replay revision cannot be reused with different inputs')
        return previous
    owned = journal is None
    journal = journal or continuous.Journal()
    try:
        rows, saved, missing = [], {}, []
        for url in urls:
            with journal.mutex:
                job = journal.conn.execute(
                    'SELECT body,result FROM jobs WHERE url=? ORDER BY updated_at DESC,rowid DESC LIMIT 1',
                    (url,)).fetchone()
            if job is None:
                missing.append(url)
                continue
            row = json.loads(job['body'])
            row['url'] = url
            rows.append(row)
            result = json.loads(job['result']) if job['result'] else {}
            storage = Path(c.get('storage', str(LOCAL / 'proxy-collection'))).resolve()
            path = Path(result.get('payload_path', '/nonexistent'))
            if (path.is_file() and path.resolve().is_relative_to(storage)
                    and re.fullmatch(r'[a-f0-9]{64}', result.get('content_sha256', ''))
                    and result.get('finished_at')):
                partial = result.get('status') in ('downloaded_partial', 'collected_partial')
                saved[url] = {**result, 'status': 'downloaded_partial' if partial else 'downloaded',
                              'list_url': url, 'protocol_hints': row['protocol_hints'], 'reparse': True}
                saved[url].pop('continuous_receipt', None)
        added = journal.enqueue(run_id, rows, checksum, checksum, record_input=False)
        reused = 0
        with journal.transaction() as conn:
            for url, result in saved.items():
                changed = conn.execute("""UPDATE jobs SET result=?,updated_at=?
                    WHERE run_id=? AND url=? AND state IN ('pending','retry') AND result IS NULL""",
                    (json.dumps(result), now(), run_id, url))
                reused += changed.rowcount
            queued = conn.execute('SELECT count(*) FROM jobs WHERE run_id=?', (run_id,)).fetchone()[0]
            cached = conn.execute("""SELECT count(*) FROM jobs WHERE run_id=?
                AND json_extract(result,'$.payload_path') IS NOT NULL""", (run_id,)).fetchone()[0]
        proof = {'requested_at': now(), 'revision': revision, 'parser_version': expected_version,
                 'run_id': run_id, 'urls_sha256': checksum, 'requested_urls': len(urls),
                 'queued_jobs': queued, 'new_jobs': added, 'reused_payloads': cached,
                 'unknown_urls': missing}
        write_json(receipt, proof)
        return proof
    finally:
        if owned:
            journal.close()


if __name__ == '__main__':
    import argparse
    import sys
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('status', 'replay'))
    args = parser.parse_args()
    if args.command == 'status':
        print(json.dumps(gap_status()))
    else:
        request = json.load(sys.stdin)
        print(json.dumps(replay(config(), request['urls'], request['revision'], request['parser_version'])))
