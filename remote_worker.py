"""Credential-free collection and backup commands for the Linux worker."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import time

import catalog
import continuous
import publication
from common import LOCAL, config, digest, lock, now, read_json, write_json


def run_folder(ident):
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-hourly-[a-f0-9]{6}', ident):
        raise ValueError('Invalid snapshot identifier')
    return LOCAL / 'runs' / ident


def snapshot(c, journal, ident):
    folder = run_folder(ident)
    folder.mkdir(parents=True, exist_ok=True)
    with lock(journal.home / 'publisher.lock'):
        manifest = publication.make_snapshot(c, folder,
            lambda connection: continuous.prepare_publication(c, journal, folder, connection),
            snapshot_guard=lambda: continuous.import_guard(journal.home))
    return {'manifest': manifest, 'report': read_json(folder / 'report.json')}


def restore(c, journal, ident, checksum):
    folder = run_folder(ident) / 'snapshot'
    manifest = read_json(folder / 'manifest.json')
    if not manifest or manifest['dump_sha256'] != checksum:
        raise RuntimeError('Downloaded and server backup checksums differ')
    with lock(journal.home / 'restore.lock'):
        return publication.restore_verify(c, folder)


def acknowledge(c, journal, ident):
    folder = run_folder(ident)
    receipt = read_json(folder / 'publication.json', {})
    if not receipt.get('metadata_pushed_at') or not receipt.get('assets_verified_at'):
        raise RuntimeError('Missing verified publication receipt')
    continuous.cleanup_local(c, folder)
    return {'acknowledged': ident, 'at': now()}


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('collect', 'status', 'snapshot', 'restore', 'ack',
                                           'research-prepare', 'research-finish', 'research-baseline'))
    parser.add_argument('--run')
    parser.add_argument('--checksum')
    args = parser.parse_args()
    c = config()
    journal = continuous.Journal()
    try:
        if args.command == 'collect':
            with lock(journal.home / 'collector.lock'), catalog.shutdown_signals():
                while True:
                    result = asyncio.run(continuous.collect(c, journal))
                    print(json.dumps(result, default=str), flush=True)
                    time.sleep(5)
        elif args.command == 'status':
            print(json.dumps({'queue': journal.stats(),
                              'collector': read_json(journal.home / 'collector-status.json')}, default=str))
        elif args.command == 'snapshot':
            print(json.dumps(snapshot(c, journal, args.run), default=str))
        elif args.command == 'restore':
            print(json.dumps(restore(c, journal, args.run, args.checksum), default=str))
        elif args.command == 'ack':
            print(json.dumps(acknowledge(c, journal, args.run), default=str))
        elif args.command == 'research-prepare':
            print(json.dumps(prepare_research(c, args.run)))
        elif args.command == 'research-finish':
            print(json.dumps(finish_research(c, args.run)))
        elif args.command == 'research-baseline':
            print(json.dumps(baseline_research(c, args.run)))
    except catalog.RunInterrupted:
        print(json.dumps({'state': 'interrupted', 'checkpoints_preserved': True}), flush=True)
    finally:
        journal.close()


def research_folder(ident):
    if not re.fullmatch(r'[a-zA-Z0-9_-]+', ident):
        raise ValueError('Invalid research identifier')
    folder = LOCAL / 'runs' / ident
    (folder / 'research').mkdir(parents=True, exist_ok=True)
    return folder


def prepare_research(c, ident):
    folder = research_folder(ident)
    write_json(folder / 'research/known_sources.json', catalog.source_records(c))
    return {'prepared': ident}


def finish_research(c, ident):
    folder = research_folder(ident)
    rows = catalog.candidate_rows(folder / 'research/candidates.json')
    write_json(folder / 'candidates.json', rows)
    return {'candidates': len(rows)}


def baseline_research(c, ident):
    folder = research_folder(ident)
    before = catalog.totals(c)
    started_at = now()
    catalog.archive_sources(c, folder / 'sources-before.jsonl.gz')
    return {'before': before, 'collection_started_at': started_at}


if __name__ == '__main__':
    main()
