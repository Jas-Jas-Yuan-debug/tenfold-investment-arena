# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import httpx
import pytest

from arena import markets
from arena.markets import MarketService


NOW = datetime(2026, 10, 4, 9, 30, tzinfo=timezone.utc)


@pytest.fixture
def service(tmp_path, monkeypatch):
    monkeypatch.setattr(markets, '_now', lambda: NOW)
    value = MarketService(tmp_path)
    yield value
    asyncio.run(value.close())


def response(payload=None, text=None):
    return httpx.Response(200, json=payload) if text is None else httpx.Response(200, text=text)


@pytest.mark.parametrize('market', ['SSE', 'SZSE', 'BSE', 'NASDAQ', 'NYSE', 'CRYPTO'])
def test_october_4_only_crypto_open(service, market):
    status = service.market_status(market, NOW)
    assert status['isOpen'] == (market == 'CRYPTO')
    if market in ('SSE', 'SZSE', 'BSE'):
        assert status['nextOpen'] == '2026-10-08T01:30:00Z'


@pytest.mark.parametrize('day', range(1, 8))
def test_china_national_day_holiday(service, day):
    status = service.market_status('SSE', f'2026-10-{day:02}T02:00:00Z')
    assert not status['isOpen']
    assert status['reason'] == 'Exchange holiday'
    assert status['nextOpen'] == '2026-10-08T01:30:00Z'


@pytest.mark.parametrize('hour,minute,second,is_open', [
    (1, 29, 59, False), (1, 30, 0, True), (3, 29, 59, True),
    (3, 30, 0, False), (4, 59, 59, False), (5, 0, 0, True),
    (6, 59, 59, True), (7, 0, 0, False),
])
def test_china_session_and_lunch_boundaries(service, hour, minute, second, is_open):
    stamp = datetime(2026, 10, 8, hour, minute, second, tzinfo=timezone.utc)
    status = service.market_status('SSE', stamp)
    assert status['isOpen'] == is_open
    if hour in (3, 4) and not is_open:
        assert status['nextOpen'] == '2026-10-08T05:00:00Z'


def test_us_early_close_and_thanksgiving(service):
    assert not service.market_status('NYSE', '2026-11-26T16:00:00Z')['isOpen']
    assert service.market_status('NYSE', '2026-11-27T17:59:59Z')['isOpen']
    status = service.market_status('NYSE', '2026-11-27T18:00:00Z')
    assert not status['isOpen']
    assert status['nextOpen'] == '2026-11-30T14:30:00Z'


@pytest.mark.parametrize('day,opening', [
    ('2026-03-06', '14:30'), ('2026-03-09', '13:30'),
    ('2026-10-30', '13:30'), ('2026-11-02', '14:30'),
])
def test_us_opening_tracks_dst(service, day, opening):
    stamp = datetime.fromisoformat(f'{day}T{opening}:00+00:00')
    assert not service.market_status('NASDAQ', stamp - timedelta(seconds=1))['isOpen']
    assert service.market_status('NASDAQ', stamp)['isOpen']


def test_calendar_failure_closes_market(service, monkeypatch):
    def fail(*args):
        raise ValueError('fixture calendar unavailable')
    monkeypatch.setattr(service, '_session', fail)
    status = service.market_status('NYSE', NOW)
    assert not status['isOpen']
    assert 'calendarError' in status
    assert service.market_status('CRYPTO', NOW)['isOpen']


def china_rows(count=50):
    return [{'code': f'{920000 + index:06}', 'symbol': f'bj{920000 + index:06}', 'name': f'Company {index}'}
            for index in range(count)]


@pytest.mark.parametrize('defect', ['none', 'missing', 'duplicate', 'prefix', 'changed_count'])
def test_china_catalog_checks_count_identity_and_pagination(service, monkeypatch, defect):
    rows = china_rows(101)
    if defect == 'missing':
        rows.pop()
    elif defect == 'duplicate':
        rows[-1] = rows[0]
    elif defect == 'prefix':
        rows[0]['symbol'] = 'sh' + rows[0]['code']
    count_calls = 0
    pages = []

    async def fake_get(url, **kwargs):
        nonlocal count_calls
        if 'StockCount' in url:
            count_calls += 1
            return response('102' if defect == 'changed_count' and count_calls > 1 else '101')
        page = kwargs['params']['page']
        pages.append(page)
        return response(rows[(page - 1) * 100:page * 100])

    monkeypatch.setattr(service, '_get', fake_get)
    if defect == 'none':
        instruments, metadata = asyncio.run(service._china_catalog('BSE', 'hs_bjs'))
        assert len(instruments) == metadata['count'] == metadata['expectedCount'] == 101
        assert metadata['complete'] and metadata['pages'] == 2
        assert sorted(pages) == [1, 2] and count_calls == 2
    else:
        with pytest.raises(ValueError):
            asyncio.run(service._china_catalog('BSE', 'hs_bjs'))


def us_directory(market, duplicate=False, trailer=True):
    if market == 'NASDAQ':
        header = 'Symbol|Security Name|Test Issue|ETF'
        rows = [f'T{i:04}|Company {i}|N|N' for i in range(1000)]
        rows += ['GOOG|Alphabet Class C|N|N', 'GOOGL|Alphabet Class A|N|N',
                 'ETF1|ETF fixture|N|Y', 'TEST1|Test fixture|Y|N']
    else:
        header = 'ACT Symbol|Security Name|Exchange|CQS Symbol|Test Issue|ETF'
        rows = [f'T{i:04}|Company {i}|N|T{i:04}|N|N' for i in range(1000)]
        rows += ['BRK.B|Class B fixture|N|BRK.B|N|N', 'BAC$B|Preferred fixture|N|BACpB|N|N',
                 'ETF1|ETF fixture|N|ETF1|N|Y', 'TEST1|Test fixture|N|TEST1|Y|N',
                 'AMEX|American fixture|A|AMEX|N|N', 'ARCA|Arca fixture|P|ARCA|N|N']
    if duplicate:
        rows.append(rows[0])
    if trailer:
        rows.append('File Creation Time: 1002202621:31|' + '|' * (header.count('|') - 1))
    return '\n'.join([header] + rows)


@pytest.mark.parametrize('market', ['NASDAQ', 'NYSE'])
def test_us_directory_filters_etfs_tests_and_other_exchanges(service, monkeypatch, market):
    async def fake_get(*args, **kwargs):
        return response(text=us_directory(market))
    monkeypatch.setattr(service, '_get', fake_get)
    rows, coverage = asyncio.run(service._us_catalog(market, 'https://fixture.example/directory'))
    symbols = {row['symbol']: row for row in rows}
    assert len(symbols) == 1002
    assert not {'ETF1', 'TEST1', 'AMEX', 'ARCA'} & symbols.keys()
    assert coverage['complete'] and coverage['sourceDate'] == '1002202621:31'
    if market == 'NYSE':
        assert symbols['BRK.B']['id'] == 'NYSE:BRK.B'
        assert symbols['BRK.B']['quoteSymbol'] == 'usBRK.B'
        assert symbols['BRK.B']['yahooSymbol'] == 'BRK-B'
        assert symbols['BAC$B']['quoteSymbol'] == 'usBACpB'
        assert symbols['BAC$B']['yahooSymbol'] == 'BAC-PB'


@pytest.mark.parametrize('duplicate,trailer', [(True, True), (False, False)])
def test_us_directory_rejects_duplicates_or_missing_trailer(service, monkeypatch, duplicate, trailer):
    async def fake_get(*args, **kwargs):
        return response(text=us_directory('NYSE', duplicate, trailer))
    monkeypatch.setattr(service, '_get', fake_get)
    with pytest.raises(ValueError):
        asyncio.run(service._us_catalog('NYSE', 'https://fixture.example/directory'))


def test_checked_in_catalog_coverage_is_internally_consistent():
    saved = json.loads((Path(__file__).resolve().parents[1] / 'data' / 'market_catalog.json').read_text())
    rows = saved['instruments']
    assert saved['version'] == 1
    assert len({row['id'] for row in rows}) == len(rows)
    counts = Counter(row['market'] for row in rows)
    assert set(counts) == set(markets.MARKET_NAMES)
    for market, count in counts.items():
        coverage = saved['coverage'][market]
        assert count == coverage['count'] == coverage['expectedCount']
        assert coverage['complete']
    assert {r['symbol'] for r in rows if r['market'] == 'CRYPTO'} == {'BTC', 'ETH'}


@pytest.mark.parametrize('bad_count', [False, True])
def test_refresh_failure_preserves_cache_and_labels_invalid_counts(service, monkeypatch, bad_count):
    row = service._instrument('920001', 'Fixture', 'BSE', quoteSymbol='bj920001')
    service.cache_path.write_text(json.dumps({'version': 1, 'instruments': [row], 'coverage': {
        'BSE': {'count': 2 if bad_count else 1, 'expectedCount': 1, 'complete': True,
                'asOf': '2026-10-01T00:00:00Z'}
    }}))

    async def unavailable(*args):
        raise RuntimeError('fixture offline')
    monkeypatch.setattr(service, '_china_catalog', unavailable)
    monkeypatch.setattr(service, '_us_catalog', unavailable)
    asyncio.run(service.initialize())
    assert service.catalog[row['id']] == row
    assert service.coverage['BSE']['status'] == 'stale_cache'
    assert service.coverage['BSE']['complete'] == (not bad_count)
    assert service.coverage['NASDAQ']['status'] == 'unavailable'
    assert service.coverage['CRYPTO']['complete']


def test_search_pagination_alias_and_ambiguous_symbols(service):
    rows = [service._instrument('600001', 'Alpha', 'SSE'),
            service._instrument('000001', 'Beta', 'SZSE'),
            service._instrument('DUP', 'One', 'NYSE', 'USD'),
            service._instrument('DUP', 'Two', 'NASDAQ', 'USD')]
    service.catalog = {r['id']: r for r in rows}
    result = asyncio.run(service.search(market='A股', limit=1, offset=1))
    assert result['total'] == 2 and result['items'][0]['id'] == 'SZSE:000001'
    assert service._resolve('nyse:dup')['id'] == 'NYSE:DUP'
    with pytest.raises(ValueError, match='ambiguous'):
        service._resolve('DUP')


@pytest.mark.parametrize('price', [0, -1, 'NaN', 'inf'])
def test_quote_rejects_invalid_prices(service, price):
    instrument = service._instrument('BTC', 'Bitcoin', 'CRYPTO', 'USD', 'crypto')
    with pytest.raises(ValueError):
        service._quote_result(instrument, price, NOW, 'fixture', 'https://fixture.example')


def test_quote_uses_provider_timestamp_and_rejects_future(service):
    instrument = service._instrument('BTC', 'Bitcoin', 'CRYPTO', 'USD', 'crypto')
    result = service._quote_result(instrument, 1, NOW - timedelta(seconds=80), 'fixture', 'https://fixture.example')
    assert result['timestamp'] == '2026-10-04T09:28:40Z' and result['delaySeconds'] == 80
    with pytest.raises(ValueError, match='future'):
        service._quote_result(instrument, 1, NOW + timedelta(minutes=3), 'fixture', 'https://fixture.example')


def test_crypto_falls_back_with_original_timestamp(service, monkeypatch):
    instrument = service._instrument('BTC', 'Bitcoin', 'CRYPTO', 'USD', 'crypto')
    requested = []

    async def fake_get(url, **kwargs):
        requested.append(url)
        if 'coinbase' in url:
            raise httpx.ConnectError('fixture outage')
        return response({'bitcoin': {'usd': 62000, 'last_updated_at': int(NOW.timestamp()) - 42,
                                     'usd_24h_change': 1.5}})
    monkeypatch.setattr(service, '_get', fake_get)
    row = asyncio.run(service._fetch_quote(instrument))
    assert row['price'] == 62000 and row['delaySeconds'] == 42
    assert row['source'].startswith('CoinGecko') and len(requested) == 2


@pytest.mark.parametrize('kind,market,symbol,age,fetches', [
    ('crypto', 'CRYPTO', 'BTC', 4, 0), ('crypto', 'CRYPTO', 'BTC', 6, 1),
    ('stock', 'NASDAQ', 'AAPL', 6, 0), ('stock', 'NASDAQ', 'AAPL', 31, 1),
])
def test_live_quote_refresh_intervals_preserve_source_time(service, monkeypatch, kind, market, symbol, age, fetches):
    instrument = service._instrument(symbol, symbol, market, 'USD', kind)
    service.catalog[instrument['id']] = instrument
    quote = service._quote_result(instrument, 100, NOW - timedelta(seconds=60), 'fixture', 'https://fixture.example')
    service._quotes[instrument['id']] = (NOW - timedelta(seconds=age), quote)
    calls = []
    async def fetch(row):
        calls.append(row)
        return quote
    monkeypatch.setattr(service, '_fetch_quote', fetch)
    actual = asyncio.run(service.get_quote(instrument['id']))
    assert len(calls) == fetches
    assert actual['timestamp'] == quote['timestamp']


def test_equity_fallback_and_total_outage(service, monkeypatch):
    instrument = service._instrument('AAPL', 'Apple', 'NASDAQ', 'USD')

    async def unavailable(*args):
        raise ValueError('fixture outage')

    async def yahoo(*args):
        return service._quote_result(instrument, 200, NOW, 'Yahoo fixture', 'https://fixture.example')
    monkeypatch.setattr(service, '_tencent_quote', unavailable)
    monkeypatch.setattr(service, '_yahoo_quote', yahoo)
    assert asyncio.run(service._fetch_quote(instrument))['price'] == 200
    monkeypatch.setattr(service, '_yahoo_quote', unavailable)
    with pytest.raises(ValueError, match='Quote unavailable'):
        asyncio.run(service._fetch_quote(instrument))


def test_tencent_rejects_different_instrument(service, monkeypatch):
    instrument = service._instrument('600001', 'Fixture', 'SSE', quoteSymbol='sh600001')
    fields = ['1', 'Fixture', '600002', '10'] + [''] * 36
    fields[30] = '20261004093000'

    async def fake_get(*args, **kwargs):
        return response(text='v_sh600001="' + '~'.join(fields) + '";')
    monkeypatch.setattr(service, '_get', fake_get)
    with pytest.raises(ValueError, match='symbol mismatch'):
        asyncio.run(service._tencent_quote(instrument))


def test_fx_validates_base_and_retains_daily_timestamp(service, monkeypatch):
    payload = {'result': 'success', 'base_code': 'USD', 'rates': {'CNY': 7.1},
               'time_last_update_unix': int(NOW.timestamp()) - 86400}

    async def fake_get(*args, **kwargs):
        return response(payload)
    monkeypatch.setattr(service, '_get', fake_get)
    row = asyncio.run(service.get_fx())
    assert row['rate'] == 7.1 and row['delaySeconds'] == 86400
    assert row['timestamp'] == '2026-10-03T09:30:00Z'
    assert row['updateFrequency'] == 'daily'
    service._fx = None
    payload['base_code'] = 'EUR'
    with pytest.raises(ValueError, match='unavailable'):
        asyncio.run(service.get_fx())
