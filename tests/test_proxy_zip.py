"""ZIP reproductions preserve complete settings and reject unrelated members."""
import io
import json
from pathlib import Path
import stat
import struct
import unittest
from unittest.mock import patch
import warnings
import zipfile

from proxy_formats import parse_proxies


def make_zip(members, compression=zipfile.ZIP_DEFLATED):
    buffer = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        with zipfile.ZipFile(buffer, 'w', compression=compression) as archive:
            for name, body in members:
                archive.writestr(name, body)
    return buffer.getvalue()


class ProxyZipTests(unittest.TestCase):
    def test_cached_member_shapes_preserve_complete_settings_and_roles(self):
        fixture = json.loads((Path(__file__).parent / 'fixtures/proxy_format_gaps/zip_members.json').read_text())
        for compression in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            with self.subTest(compression=compression):
                body = make_zip([(m['name'], m['body']) for m in fixture['members']], compression)
                result = parse_proxies(body, content_type='application/octet-stream')
                self.assertEqual(result.entries_found, fixture['expected_entries_found'])
                actual = [p.as_dict() for p in result.proxies.values()]
                self.assertCountEqual(actual, fixture['expected'])
                self.assertEqual(result.formats['zip_archive'], 1)
                self.assertEqual(result.formats['zip_text_member'], 4)
                self.assertFalse(result.warnings)

    def test_duplicate_names_are_read_individually_and_connections_deduplicate(self):
        result = parse_proxies(make_zip([
            ('list.txt', 'http://8.8.8.8:80\n'),
            ('list.txt', 'http://1.1.1.1:80\n'),
            ('copy.TXT', 'http://8.8.8.8:80\n'),
        ]))
        self.assertEqual(result.entries_found, 3)
        self.assertEqual(len(result.proxies), 2)

    def test_source_protocol_hint_applies_to_plain_member_records(self):
        result = parse_proxies(make_zip([('proxies.txt', '8.8.8.8:1080\n')]), hints=('socks5',))
        self.assertEqual(next(iter(result.proxies.values())).as_dict(), {
            'address': '8.8.8.8', 'port': 1080, 'protocol': 'socks5', 'settings': {},
        })

    def test_relay_ip_pools_without_protocols_are_not_complete_proxy_records(self):
        body = make_zip([('pool.txt', '8.8.8.8:443\n1.1.1.1 443\n'),
                         ('pool.json', '{"ip":"9.9.9.9","port":443}')])
        for hints in ((), ('http', 'socks5')):
            with self.subTest(hints=hints):
                result = parse_proxies(body, hints=hints)
                self.assertFalse(result.proxies)
                self.assertEqual(result.entries_found, 0)
                self.assertEqual(result.invalid['missing_zip_protocol'], 3)
        # Existing plaintext semantics remain available to their source caller.
        self.assertEqual(len(parse_proxies(b'8.8.8.8:443').proxies), 1)

    def test_unsafe_names_special_files_and_documentation_are_ignored(self):
        names = ('../list.txt', '/list.txt', 'C:/list.txt', 'folder\\list.txt',
                 '.hidden/list.txt', '__MACOSX/list.txt', 'docs/list.txt',
                 'tests/list.txt', 'sample.json', 'LICENSE.txt')
        link = zipfile.ZipInfo('link.txt')
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        members = [(name, 'http://8.8.8.8:80\n') for name in names]
        members += [(link, 'http://8.8.8.8:80\n')]
        result = parse_proxies(make_zip(members))
        self.assertFalse(result.proxies)
        self.assertEqual(result.formats['zip_text_member'], 0)

    def test_malformed_archive_never_falls_back_to_scanning_binary_bytes(self):
        for body in (b'PK\x03\x04\nhttp://8.8.8.8:80\n',
                     make_zip([('list.txt', 'http://8.8.8.8:80')])[:-30]):
            with self.subTest(body=body[:4]):
                result = parse_proxies(body)
                self.assertFalse(result.proxies)
                self.assertEqual(result.warnings['invalid_zip_archive'], 1)

    def test_bad_crc_does_not_import_partial_member_or_drop_valid_neighbor(self):
        damaged = b'http://8.8.8.8:80\n'
        body = make_zip([('damaged.txt', damaged), ('valid.txt', 'http://1.1.1.1:80\n')], zipfile.ZIP_STORED)
        body = body.replace(damaged, b'http://9.9.9.9:80\n', 1)
        result = parse_proxies(body)
        self.assertEqual([p.address for p in result.proxies.values()], ['1.1.1.1'])
        self.assertEqual(result.warnings['invalid_zip_member'], 1)

    def test_encrypted_and_unsupported_members_are_skipped_without_keys(self):
        for field in ('encrypted', 'compression'):
            with self.subTest(field=field):
                body = bytearray(make_zip([('skip.txt', 'http://8.8.8.8:80\n'),
                                          ('valid.txt', 'http://1.1.1.1:80\n')]))
                central = body.index(b'PK\x01\x02')
                if field == 'encrypted':
                    struct.pack_into('<H', body, 6, 1)
                    struct.pack_into('<H', body, central + 8, 1)
                    warning = 'encrypted_zip_member'
                else:
                    struct.pack_into('<H', body, 8, 99)
                    struct.pack_into('<H', body, central + 10, 99)
                    warning = 'unsupported_zip_compression'
                result = parse_proxies(bytes(body))
                self.assertEqual([p.address for p in result.proxies.values()], ['1.1.1.1'])
                self.assertEqual(result.warnings[warning], 1)

    def test_member_and_total_expansion_limits_preserve_valid_small_members(self):
        small = 'http://8.8.8.8:80\n'
        with patch('proxy_formats.ZIP_MAX_MEMBER_BYTES', 32):
            result = parse_proxies(make_zip([('large.txt', 'x' * 64), ('small.txt', small)]))
        self.assertEqual(len(result.proxies), 1)
        self.assertEqual(result.warnings['zip_member_size_limit'], 1)
        with patch('proxy_formats.ZIP_MAX_TOTAL_BYTES', len(small)):
            result = parse_proxies(make_zip([('first.txt', small), ('second.txt', 'http://1.1.1.1:80\n')]))
        self.assertEqual([p.address for p in result.proxies.values()], ['8.8.8.8'])
        self.assertEqual(result.warnings['zip_expansion_limit'], 1)

    def test_member_count_limit_rejects_before_parsing_any_records(self):
        body = make_zip([(f'{i}.txt', 'http://8.8.8.8:80') for i in range(3)])
        with patch('proxy_formats.ZIP_MAX_MEMBERS', 2):
            result = parse_proxies(body)
        self.assertFalse(result.proxies)
        self.assertEqual(result.warnings['zip_member_count_limit'], 1)

    def test_binary_nested_and_malformed_text_members_are_not_proxy_records(self):
        nested = make_zip([('list.txt', 'http://8.8.8.8:80')])
        result = parse_proxies(make_zip([
            ('binary.txt', b'\x00http://8.8.8.8:80'),
            ('invalid-utf8.txt', b'\xffhttp://8.8.8.8:80'),
            ('nested.zip', nested),
            ('malformed.json', '{"outbounds": [}'),
            ('incomplete.json', '{"type":"http","server":"8.8.8.8"}'),
            ('web.txt', '<html><body>http://8.8.8.8:80</body></html>'),
        ]))
        self.assertFalse(result.proxies)
        self.assertEqual(result.warnings['binary_zip_member'], 2)

    def test_empty_archive_is_valid(self):
        result = parse_proxies(make_zip([]))
        self.assertFalse(result.proxies)
        self.assertEqual(result.formats['zip_archive'], 1)
        self.assertFalse(result.warnings)


if __name__ == '__main__':
    unittest.main()
