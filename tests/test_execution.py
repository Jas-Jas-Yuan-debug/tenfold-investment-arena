# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from arena.execution import TradeExecutor
from arena.store import Store


class Markets:
    def __init__(self):
        self.catalog = {
            'CRYPTO:BTC': {'id': 'CRYPTO:BTC', 'symbol': 'BTC', 'name': 'Bitcoin', 'kind': 'crypto', 'market': 'CRYPTO', 'currency': 'USD'},
            'NASDAQ:AAPL': {'id': 'NASDAQ:AAPL', 'symbol': 'AAPL', 'kind': 'stock', 'market': 'NASDAQ', 'currency': 'USD'},
            'SSE:600000': {'id': 'SSE:600000', 'symbol': '600000', 'kind': 'stock', 'market': 'SSE', 'currency': 'CNY'},
            'SSE:688001': {'id': 'SSE:688001', 'symbol': '688001', 'kind': 'stock', 'market': 'SSE', 'currency': 'CNY'},
            'BSE:920001': {'id': 'BSE:920001', 'symbol': '920001', 'kind': 'stock', 'market': 'BSE', 'currency': 'CNY'},
        }
        self.open = True
        self.quote_age = 0
        self.fx_age = 0
        self.bad_quote = False
        self.bad_fx = False
        self.quote_calls = 0
        self.price = 100

    def market_status(self, market):
        return {'id': market, 'isOpen': self.open, 'reason': 'Test weekend'}

    async def get_quote(self, key):
        self.quote_calls += 1
        if self.bad_quote:
            raise ValueError('Quote feed unavailable')
        return {'instrumentId': key, 'price': self.price, 'currency': self.catalog[key]['currency'],
                'timestamp': (datetime.now(timezone.utc) - timedelta(seconds=self.quote_age)).isoformat(), 'source': 'Fixture vendor'}

    async def get_fx(self):
        if self.bad_fx:
            raise ValueError('FX feed unavailable')
        return {'rate': 7, 'timestamp': (datetime.now(timezone.utc) - timedelta(seconds=self.fx_age)).isoformat(), 'source': 'Fixture daily FX'}


@pytest.fixture
def setup(tmp_path):
    store, markets = Store(tmp_path / 'ledger.db'), Markets()
    return store, markets, TradeExecutor(store, markets)


def execute(executor, action='buy', key='CRYPTO:BTC', quantity=1, **kwargs):
    return asyncio.run(executor.execute('astra-1', action, key, quantity, **kwargs))


def test_crypto_fraction_and_valuation_use_cny_fx(setup):
    store, markets, executor = setup
    trade = execute(executor, quantity=0.1)
    assert trade['amountCny'] == 70
    markets.price = 110
    executor._mark_cache.clear()
    agents = asyncio.run(executor.portfolios())
    agent = next(a for a in agents if a['id'] == 'astra-1')
    assert agent['cash'] == 799930
    assert agent['equity'] == 800007
    assert agent['positions'][0]['valueCny'] == 77
    assert agent['pnl'] == 7
    assert len(store.equity_history()) == 10  # Reads never append snapshots.


@pytest.mark.parametrize('condition', ['closed', 'quote_stale', 'fx_stale', 'missing_quote', 'missing_fx', 'fractional_stock', 'bad_lot'])
def test_invalid_market_orders_leave_balances_unchanged(setup, condition):
    store, markets, executor = setup
    key, quantity = 'CRYPTO:BTC', 1
    if condition == 'closed':
        markets.open = False
    elif condition == 'quote_stale':
        markets.quote_age = 121
    elif condition == 'fx_stale':
        markets.fx_age = 4 * 86400 + 10
    elif condition == 'missing_quote':
        markets.bad_quote = True
    elif condition == 'missing_fx':
        markets.bad_fx = True
    elif condition == 'fractional_stock':
        key, quantity = 'NASDAQ:AAPL', 0.5
    elif condition == 'bad_lot':
        key, quantity = 'SSE:600000', 101
    with pytest.raises(ValueError):
        execute(executor, key=key, quantity=quantity)
    assert store.get_agent('astra-1')['cash'] == 800000
    assert store.positions() == []
    assert store.count_trades() == 0
    assert store.events()[0]['kind'] == 'order_rejected'


def test_board_lots_and_same_day_t1(setup):
    store, markets, executor = setup
    execute(executor, key='SSE:600000', quantity=100)
    with pytest.raises(ValueError, match=r'T\+1'):
        execute(executor, action='sell', key='SSE:600000', quantity=100)
    with pytest.raises(ValueError, match='at least 200'):
        execute(executor, key='SSE:688001', quantity=199)
    execute(executor, key='SSE:688001', quantity=201)
    execute(executor, key='BSE:920001', quantity=101)
    assert store.count_trades() == 3


def test_daily_weekend_fx_and_delayed_equity_accepted(setup):
    _, markets, executor = setup
    markets.fx_age = 3 * 86400
    markets.quote_age = 19 * 60
    execute(executor, key='NASDAQ:AAPL')
    with pytest.raises(ValueError, match='stale'):
        execute(executor)


def test_missing_valuation_is_unknown_and_cache_is_labeled(setup):
    store, markets, executor = setup
    execute(executor)
    markets.bad_quote = True
    executor._mark_cache.clear()
    agent = asyncio.run(executor.portfolios())[0]
    assert agent['equity'] is None and agent['pnl'] is None
    assert agent['knownEquity'] == 799300
    assert not agent['valuationComplete']
    assert agent['positions'][0]['valueCny'] is None
    assert 'unavailable' in agent['valuationWarnings'][0]

    markets.bad_quote = False
    markets.quote_age = 3600
    executor._failed_marks.clear()
    agent = asyncio.run(executor.portfolios())[0]
    assert agent['equity'] == 800000 and agent['isEstimated']
    assert 'Historical quote' in agent['valuationWarnings'][0]


def test_cached_portfolio_reads_avoid_repeated_quote_requests(setup):
    _, markets, executor = setup
    execute(executor)
    calls = markets.quote_calls
    asyncio.run(executor.portfolios())
    asyncio.run(executor.portfolios())
    assert markets.quote_calls == calls


def test_many_positions_fetch_with_bounded_concurrency(setup):
    store, markets, executor = setup
    for index in range(24):
        key = f'NASDAQ:TEST{index}'
        markets.catalog[key] = {'id': key, 'symbol': f'TEST{index}', 'name': 'Fixture', 'kind': 'stock', 'market': 'NASDAQ', 'currency': 'USD'}
        store.record_trade(agent_id='astra-1', action='buy', instrument_id=key, quantity='1',
            price='100', currency='USD', fx='7', reason='', source='fixture',
            quote_timestamp=datetime.now(timezone.utc).isoformat(), acquired_date='2026-10-04')
    original = markets.get_quote
    active, peak = 0, 0

    async def slow_quote(key):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.005)
            return await original(key)
        finally:
            active -= 1

    markets.get_quote = slow_quote
    rows = asyncio.run(executor.portfolios())
    assert peak <= 8
    assert len(rows[0]['positions']) == 24
    assert rows[0]['equity'] == 800000
