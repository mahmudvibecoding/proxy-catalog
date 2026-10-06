"""Sanitized reproductions of confirmed recorded parser gaps."""
import copy
import json
import unittest

from proxy_formats import parse_proxies


def compact_page(rows, protocol='http'):
    # Native /api.php columns are IP, port, country, and first_seen.
    return {
        'updated': 1700000000, 'proto': protocol, 'cc': '', 'q': '',
        'sort': 'first_seen', 'dir': 'desc', 'total': len(rows),
        'filtered': len(rows), 'page': 1, 'per': 1000, 'pages': 1,
        'new_threshold': 1699900000, 'rows': rows,
    }


def parse(value, hints=()):
    return parse_proxies(json.dumps(value).encode(), hints=hints)


class CompactProxyRowGapTests(unittest.TestCase):
    def test_native_rows_use_the_declared_protocol(self):
        for protocol in ('http', 'https', 'socks4', 'socks5'):
            with self.subTest(protocol=protocol):
                result = parse(compact_page([
                    ['8.8.8.8', 8080, 'US', 1700000000],
                    ['1.1.1.1', '1080', '', 1699999999],
                ], protocol), hints=('socks4',))
                self.assertEqual(result.entries_found, 2)
                self.assertEqual({(p.address, p.port, p.protocol) for p in result.proxies.values()}, {
                    ('8.8.8.8', 8080, protocol), ('1.1.1.1', 1080, protocol),
                })
                self.assertTrue(all(p.settings == {} for p in result.proxies.values()))
                self.assertEqual(result.formats['compact_proxy_rows'], 1)

    def test_country_dates_and_response_metadata_do_not_change_connections(self):
        document = compact_page([
            ['8.8.8.8', 8080, 'US', 1700000000],
            ['8.8.8.8', 8080, 'GB', 1700000010],
        ])
        document['q'] = '9.9.9.9:3128'
        document['metadata'] = {'uri': 'socks5://9.9.9.9:1080'}
        result = parse(document)
        self.assertEqual(result.entries_found, 2)
        self.assertEqual(len(result.proxies), 1)
        self.assertEqual(next(iter(result.proxies.values())).settings, {})

    def test_invalid_rows_are_not_guessed_or_recursively_scanned(self):
        result = parse(compact_page([
            ['8.8.8.8', 8080, 'US', 1700000000],
            ['1.1.1.1', 1080],  # Missing the documented column layout.
            ['1.1.1.1', 1080, 'US', 1700000000, 'password'],
            {'ip': '1.1.1.1', 'port': 1080},
            ['http://1.1.1.1:1080', 1080, 'US', 1700000000],
            ['1.1.1.1', 1080, {'uri': 'http://9.9.9.9:80'}, 1700000000],
            ['1.1.1.1', 1080, 'US', True],
        ]))
        self.assertEqual(result.entries_found, 1)
        self.assertEqual(result.invalid['invalid_compact_row'], 5)
        self.assertEqual(result.invalid['invalid_address_or_port'], 1)

    def test_normal_endpoint_validation_also_applies_to_rows(self):
        result = parse(compact_page([
            ['2606:4700:4700::1111', 1080, 'US', 1700000000],
            ['127.0.0.1', 8080, '', 1700000000],
            ['10.0.0.1', 8080, '', 1700000000],
            ['metadata.google.internal', 8080, '', 1700000000],
            ['example.com', 8080, '', 1700000000],
            ['999.1.1.1', 8080, '', 1700000000],
            ['8.8.8.8', True, '', 1700000000],
            ['8.8.8.8', 65536, '', 1700000000],
        ]))
        self.assertEqual(len(result.proxies), 1)
        self.assertEqual(next(iter(result.proxies.values())).address, '2606:4700:4700::1111')
        self.assertEqual(result.invalid['invalid_address_or_port'], 7)

    def test_missing_protocol_and_incomplete_envelopes_are_not_inferred(self):
        document = compact_page([['8.8.8.8', 8080, 'US', 1700000000]])
        for field in ('proto', 'page', 'per', 'total', 'pages'):
            with self.subTest(field=field):
                incomplete = copy.deepcopy(document)
                incomplete.pop(field)
                self.assertEqual(len(parse(incomplete, hints=('http',)).proxies), 0)
        self.assertEqual(len(parse(document['rows'], hints=('http',)).proxies), 0)

    def test_invalid_envelopes_and_configured_transports_are_rejected(self):
        document = compact_page([['8.8.8.8', 8080, 'US', 1700000000]])
        for field, value in (
            ('proto', 'trojan'), ('proto', 'dns'), ('proto', 'unknown'),
            ('page', 0), ('page', True), ('per', 0), ('total', -1),
            ('pages', '1'), ('rows', {'ip': '8.8.8.8', 'port': 8080}),
        ):
            with self.subTest(field=field, value=value):
                malformed = copy.deepcopy(document)
                malformed[field] = value
                result = parse(malformed)
                self.assertEqual(len(result.proxies), 0)
                self.assertEqual(result.warnings['invalid_compact_proxy_page'], 1)

    def test_nested_sample_and_service_roles_do_not_become_proxy_rows(self):
        document = compact_page([['8.8.8.8', 8080, 'US', 1700000000]])
        for role in ('dns', 'ntp', 'inbounds', 'listeners', 'examples', 'metadata'):
            with self.subTest(role=role):
                self.assertEqual(len(parse({role: document}).proxies), 0)
        document['type'] = 'dns'
        self.assertEqual(len(parse(document).proxies), 0)

    def test_empty_native_page_is_valid(self):
        result = parse(compact_page([]))
        self.assertEqual(len(result.proxies), 0)
        self.assertEqual(result.formats['compact_proxy_rows'], 1)
        self.assertFalse(result.invalid)
        self.assertFalse(result.warnings)


if __name__ == '__main__':
    unittest.main()
