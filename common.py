"""Local configuration, durable state, and process helpers."""
from __future__ import annotations
import contextlib
import ctypes
import fcntl
import hashlib
import json
import os
from pathlib import Path
import plistlib
import subprocess
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parent
LOCAL = ROOT / '.local'
LABEL = 'com.mahmud.proxy-catalog.daily'

def now():
    return datetime.now(timezone.utc).isoformat()

def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default

def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('w') as f:
        json.dump(value, f, indent=2, ensure_ascii=False, default=str)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)

def config():
    c = read_json(ROOT / 'config.local.json')
    if c is None:
        raise RuntimeError('Create config.local.json from config.example.json first')
    if c['model'] != 'gpt-6.1-sol' or c['reasoning_effort'] != 'max':
        raise RuntimeError('This runner requires the user-selected gpt-6.1-sol / max')
    return c

def day(c, at=None):
    return (at or datetime.now(timezone.utc)).astimezone(ZoneInfo(c['timezone'])).date().isoformat()

def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        while block := f.read(4 * 1024 * 1024):
            h.update(block)
    return h.hexdigest()

def run(args, **kwargs):
    return subprocess.run([str(x) for x in args], check=True, text=True, **kwargs)

def capture(args, **kwargs):
    return run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs).stdout

def db(c, dbname=None, autocommit=True):
    opts = dict(c['database'])
    if dbname:
        opts['dbname'] = dbname
    connection = psycopg.connect(**opts, autocommit=autocommit, row_factory=dict_row)
    return connection

def pg_env(c, dbname=None):
    env = os.environ.copy()
    for key, pgkey in [('dbname','PGDATABASE'),('user','PGUSER'),('host','PGHOST'),('port','PGPORT')]:
        env[pgkey] = str(c['database'][key])
    if dbname:
        env['PGDATABASE'] = dbname
    env['PROXY_STORAGE'] = c.get('storage', str(LOCAL / 'proxy-collection'))
    env['PGOPTIONS'] = '-c timezone=UTC'
    return env

@contextlib.contextmanager
def lock(path=LOCAL / 'runner.lock'):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open('a+') as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield f
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)

def session_active():
    root = plistlib.loads(subprocess.check_output(['/usr/sbin/ioreg','-a','-n','Root','-d','1']))
    if isinstance(root, list):
        root = root[0]
    unlocked = root.get('IOConsoleLocked') is False
    on_console = any(x.get('kCGSSessionOnConsoleKey') and x.get('kCGSSessionUserIDKey') == os.getuid()
                     for x in root.get('IOConsoleUsers', []))
    cg = ctypes.CDLL('/System/Library/Frameworks/CoreGraphics.framework/CoreGraphics')
    cg.CGMainDisplayID.restype = ctypes.c_uint32
    cg.CGDisplayIsAsleep.argtypes = [ctypes.c_uint32]
    cg.CGDisplayIsAsleep.restype = ctypes.c_bool
    display_awake = not cg.CGDisplayIsAsleep(cg.CGMainDisplayID())
    return bool(unlocked and on_console and display_awake)

def gh(c, *args):
    return capture(['/opt/homebrew/bin/gh', *args], cwd=ROOT).strip()

def gh_api(c, endpoint, *args):
    return json.loads(gh(c, 'api', endpoint, *args))

def table_metrics(connection):
    """Same-version PostgreSQL content fingerprints complement backup SHA-256."""
    result = {}
    for table in ('proxies', 'proxy_stats', 'proxy_lists'):
        result[table] = connection.execute(f'''SELECT count(*) AS rows,
            coalesce(sum(hashtextextended(row_to_json(t)::text, 0)::numeric),0)::text AS content_fingerprint
            FROM public.{table} AS t''').fetchone()
    seq=connection.execute("SELECT pg_get_serial_sequence('public.proxies','proxy_id') AS name").fetchone()['name']
    result['identity_sequence'] = connection.execute(
        sql.SQL('SELECT last_value, is_called FROM {}').format(sql.Identifier(*seq.split('.')))).fetchone()
    return result
