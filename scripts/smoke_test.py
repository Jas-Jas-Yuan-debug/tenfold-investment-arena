# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

#!/usr/bin/env python3
"""Read-only integration check for a running Tenfold localhost server."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
import sys
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

SNAPSHOT_COUNTS = {'SSE': 2319, 'SZSE': 2904, 'BSE': 348,
                   'NASDAQ': 4333, 'NYSE': 2837, 'CRYPTO': 2}
SECRET_FIELDS = {'apikey', 'api_key', 'ai_api_key', 'search_api_key',
                 'authorization', 'access_token', 'secret'}


def assert_no_credentials(value):
    if isinstance(value, dict):
        for key, item in value.items():
            assert str(key).lower() not in SECRET_FIELDS, f'Credential field exposed: {key}'
            assert_no_credentials(item)
    elif isinstance(value, list):
        for item in value:
            assert_no_credentials(item)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:8787')
    parser.add_argument('--expect-snapshot', action='store_true',
                        help='Require the initial 12,743-instrument counts exactly.')
    parser.add_argument('--timeout', type=float, default=30)
    args = parser.parse_args()
    base = args.base_url.rstrip('/')
    parsed = urlparse(base)
    assert parsed.scheme == 'http' and parsed.hostname in {'127.0.0.1', 'localhost', '::1'}, 'Use an HTTP localhost URL'
    assert not parsed.username and not parsed.password and not parsed.query and not parsed.fragment, 'Use a plain localhost base URL'

    def get(path, **params):
        url = base + path + ('?' + urlencode(params) if params else '')
        request = Request(url, headers={'Accept': 'application/json'})
        with urlopen(request, timeout=args.timeout) as response:
            assert response.status == 200, f'{path}: HTTP {response.status}'
            assert response.headers.get('Cache-Control') == 'no-store', f'{path}: API response must not be cached'
            raw = response.read().decode('utf-8')
        value = json.loads(raw)
        assert_no_credentials(value)
        for setting in ('AI_API_KEY', 'SEARCH_API_KEY'):
            secret = os.getenv(setting)
            if secret:
                assert json.dumps(secret) not in raw, f'{setting} exposed in {path}'
        return value

    health = get('/api/health')
    assert health.get('ok') is True, 'Health failed'
    state = get('/api/state')
    provider = get('/api/provider')
    agents = state['agents']
    assert len(agents) == 10 and len({a['id'] for a in agents}) == 10, 'Expected ten distinct investors'
    assert Counter(a['model'] for a in agents) == {'gpt-6-astra': 5, 'gpt-6.1-sol': 5}, 'Unexpected model assignments'
    assert all(a['effort'] == 'max' and a['initialCash'] == 800000 for a in agents), 'Incorrect effort or initial wallets'
    assert state['totals']['initialCapital'] == 8000000, 'Incorrect initial capital'
    assert 60 <= state['runtime']['intervalSeconds'] <= 3600, 'Invalid scheduler interval'
    assert all(key in state['runtime'] for key in ('running', 'inProgress', 'round')), 'Missing runtime status'
    assert {row['id'] for row in state['markets']} == set(SNAPSHOT_COUNTS), 'Unexpected markets'

    counts, unique_ids = {}, set()
    for market in SNAPSHOT_COUNTS:
        coverage = state['coverage'][market]
        assert coverage.get('complete') is True, f'{market}: incomplete directory ({coverage.get("status")})'
        count = coverage['count']
        assert isinstance(count, int) and count > 0, f'{market}: invalid count'
        assert count == coverage['expectedCount'], f'{market}: source count mismatch'
        local_ids, offset = set(), 0
        while offset < count:
            page = get('/api/instruments', market=market, limit=200, offset=offset)
            assert page['total'] == count, f'{market}: pagination count changed; retry after refresh'
            rows = page['items']
            assert rows, f'{market}: empty page before reaching declared count'
            assert all(row['market'] == market and row['id'].startswith(market + ':') for row in rows), f'{market}: mixed catalog rows'
            ids = {row['id'] for row in rows}
            assert len(ids) == len(rows) and not (ids & local_ids), f'{market}: duplicate instrument IDs'
            assert not (ids & unique_ids), 'Duplicate IDs across exchanges'
            local_ids.update(ids)
            offset += len(rows)
        assert len(local_ids) == count, f'{market}: not all rows returned'
        unique_ids.update(local_ids)
        counts[market] = count
    assert get('/api/instruments', limit=1)['total'] == len(unique_ids), 'Full catalog total does not match exchange totals'
    assert health['catalogSize'] == len(unique_ids), 'Health catalog count differs from catalog API'
    assert counts['CRYPTO'] == 2 and {'CRYPTO:BTC', 'CRYPTO:ETH'} <= unique_ids, 'Expected BTC and ETH'
    if args.expect_snapshot:
        assert counts == SNAPSHOT_COUNTS, f'Initial snapshot changed: {counts}'
    for agent in agents:
        detail = get('/api/agents/' + agent['id'])
        assert detail['agent']['id'] == agent['id'], 'Investor detail returned another account'

    warnings = [f'{market}: {state["coverage"][market]["status"]}' for market in counts
                if state['coverage'][market].get('status') not in {'complete', 'complete_cached'}]
    result = {'ok': True, 'catalogSize': len(unique_ids), 'counts': counts,
              'agents': len(agents), 'providerConnected': bool(provider.get('connected')),
              'running': bool(state['runtime']['running']), 'warnings': warnings}
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'Smoke check failed: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)
