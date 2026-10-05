# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from arena.execution import TradeExecutor
from arena.store import Store


UTC = timezone.utc
CLOCK = datetime(2023, 6, 1, 13, 30, tzinfo=UTC)


class LiveGuard:
    def __init__(self):
        self.open_market = None

    def market_status(self, market):
        return {'id': market, 'isOpen': market == self.open_market}


class HistoricalMarkets:
    def __init__(self):
        self.main = LiveGuard()
        self.active = True
        self.clock = CLOCK
        self.catalog = {
            'NASDAQ:AAPL': {'id': 'NASDAQ:AAPL', 'symbol': 'AAPL', 'kind': 'stock', 'market': 'NASDAQ', 'currency': 'USD'},
            'SSE:600000': {'id': 'SSE:600000', 'symbol': '600000', 'kind': 'stock', 'market': 'SSE', 'currency': 'CNY'},
            'CRYPTO:BTC': {'id': 'CRYPTO:BTC', 'symbol': 'BTC', 'kind': 'crypto', 'market': 'CRYPTO', 'currency': 'USD'}}
        self.price = 100
        self.quote_age = 0
        self.quote_valid = True
        self.fx_age = 12 * 86400
        self.fx_available_future = False
        self.fx_hook = None
        self.quote_hook = None
        self.quote_calls = 0

    def clock_now(self):
        return self.clock

    def market_status(self, market):
        return {'id': market, 'isOpen': self.active, 'reason': 'Fixture replay gate'}

    async def get_quote(self, key):
        self.quote_calls += 1
        clock = self.clock
        crypto = self.catalog[key]['kind'] == 'crypto'
        stamp = clock - timedelta(seconds=self.quote_age)
        row = {'instrumentId': key, 'price': self.price, 'currency': self.catalog[key]['currency'],
            'timestamp': stamp.isoformat(), 'source': 'Genuine dated fixture', 'sourceUrl': 'https://fixture.example/history',
            'replay': True, 'replayTime': clock.isoformat(), 'world': 'historical', 'validForExecution': self.quote_valid,
            'validUntil': (stamp + timedelta(hours=2 if crypto else 6.5)).isoformat(),
            'barType': 'hourly_close' if crypto else 'daily_open',
            'executionPolicy': 'historical_completed_hour' if crypto else 'historical_daily_open'}
        if self.quote_hook:
            self.quote_hook()
        return row

    async def get_fx(self):
        row = {'rate': 7, 'timestamp': (self.clock - timedelta(seconds=self.fx_age)).isoformat(),
            'source': 'Dated historical FX fixture', 'replay': True,
            'conservativeAvailableAt': (self.clock + timedelta(seconds=1) if self.fx_available_future
                                       else self.clock - timedelta(days=1)).isoformat()}
        if self.fx_hook:
            self.fx_hook()
        return row


@pytest.fixture
def setup(tmp_path):
    store = Store(tmp_path / 'historical-ledger.db')
    wallets = {agent['id']: 2000 for agent in store.list_agents()}
    store.seed_wallets(wallets, world_id='historical-test')
    markets = HistoricalMarkets()
    return store, markets, TradeExecutor(store, markets)


def execute(executor, key='NASDAQ:AAPL', action='buy', quantity=1):
    return asyncio.run(executor.execute('astra-1', action, key, quantity))


def test_2023_stock_price_uses_replay_clock_not_current_date(setup):
    store, markets, executor = setup
    markets.quote_age = 3600
    trade = execute(executor)
    assert trade['amountCny'] == 700 and store.get_agent('astra-1')['cash'] == 1300
    assert trade['world'] == 'historical' and trade['worldId'] == 'historical-test'
    assert trade['asOf'] == CLOCK.isoformat()
    assert trade['quoteTimestamp'] == (CLOCK - timedelta(hours=1)).isoformat()
    assert trade['fxTimestamp'] == (CLOCK - timedelta(days=12)).isoformat()
    assert store.trades()[0]['timestamp'] == CLOCK.isoformat()
    assert store.events()[0]['details']['world'] == 'historical'


@pytest.mark.parametrize('case', ['expired_stock', 'ineligible', 'future_quote', 'stale_crypto', 'fx_too_old', 'fx_unavailable', 'inactive', 'main_open'])
def test_invalid_historical_orders_preserve_wallets_and_positions(setup, case):
    store, markets, executor = setup
    key = 'NASDAQ:AAPL'
    if case == 'expired_stock':
        markets.quote_age = 6.5 * 3600
    elif case == 'ineligible':
        markets.quote_valid = False
    elif case == 'future_quote':
        markets.quote_age = -1
    elif case == 'stale_crypto':
        key, markets.quote_age = 'CRYPTO:BTC', 7201
    elif case == 'fx_too_old':
        markets.fx_age = 14 * 86400 + 1
    elif case == 'fx_unavailable':
        markets.fx_available_future = True
    elif case == 'inactive':
        markets.active = False
    elif case == 'main_open':
        markets.main.open_market = 'SSE'
    with pytest.raises(ValueError):
        execute(executor, key)
    assert store.get_agent('astra-1')['cash'] == 2000
    assert store.positions() == [] and store.count_trades() == 0
    assert store.events()[0]['details']['world'] == 'historical'


@pytest.mark.parametrize('venue', ['SSE', 'SZSE', 'BSE', 'NASDAQ', 'NYSE'])
def test_any_live_stock_venue_reopening_during_fx_io_prevents_commit(setup, venue):
    store, markets, executor = setup
    markets.fx_hook = lambda: setattr(markets.main, 'open_market', venue)
    with pytest.raises(ValueError, match='reopened'):
        execute(executor)
    assert store.count_trades() == 0 and store.get_agent('astra-1')['cash'] == 2000


def test_historical_world_closing_during_fx_io_prevents_commit(setup):
    store, markets, executor = setup
    markets.fx_hook = lambda: setattr(markets, 'active', False)
    with pytest.raises(ValueError, match='closed'):
        execute(executor)
    assert store.count_trades() == 0


def test_unavailable_main_calendar_cannot_be_assumed_closed(setup):
    store, markets, executor = setup
    markets.main.market_status = lambda market: {'isOpen': False, 'calendarError': 'Unavailable fixture calendar'}
    with pytest.raises(ValueError, match='cannot be verified'):
        execute(executor)
    assert store.count_trades() == 0


def test_replay_clock_advancing_during_io_requires_fresh_quote(setup):
    store, markets, executor = setup
    markets.fx_hook = lambda: setattr(markets, 'clock', CLOCK + timedelta(days=1))
    with pytest.raises(ValueError, match='earlier replay clock'):
        execute(executor)
    assert store.count_trades() == 0


def test_historical_crypto_completed_hour_within_two_hours_is_eligible(setup):
    _, markets, executor = setup
    markets.quote_age = 90 * 60
    trade = execute(executor, 'CRYPTO:BTC')
    assert trade['amountCny'] == 700


def test_daily_open_source_timestamps_must_match_exchange_session_when_available(setup):
    store, markets, executor = setup
    markets.main._session = lambda market, day: (CLOCK, CLOCK + timedelta(hours=6.5))
    markets.quote_age = 1
    with pytest.raises(ValueError, match='does not match its exchange session'):
        execute(executor)
    assert store.count_trades() == 0


def test_a_share_t1_follows_replay_shanghai_date(setup):
    store, markets, executor = setup
    markets.price = 10
    markets.clock = datetime(2023, 6, 1, 1, 30, tzinfo=UTC)
    execute(executor, 'SSE:600000', quantity=100)
    lot = store.db.execute('SELECT acquired_date FROM lots').fetchone()
    assert lot['acquired_date'] == '2023-06-01'
    with pytest.raises(ValueError, match=r'T\+1'):
        execute(executor, 'SSE:600000', 'sell', 100)
    markets.clock += timedelta(days=1)
    execute(executor, 'SSE:600000', 'sell', 100)
    assert store.get_agent('astra-1')['cash'] == 2000


def test_branch_pnl_uses_seeded_capital_and_zero_baseline_pct_is_unknown(tmp_path):
    store = Store(tmp_path / 'historical-zero.db')
    wallets = {agent['id']: 2000 for agent in store.list_agents()}
    wallets['astra-1'] = 0
    store.seed_wallets(wallets, world_id='historical-zero')
    executor = TradeExecutor(store, HistoricalMarkets())
    rows = asyncio.run(executor.portfolios())
    zero = next(row for row in rows if row['id'] == 'astra-1')
    regular = next(row for row in rows if row['id'] == 'astra-2')
    assert zero['equity'] == zero['pnl'] == 0 and zero['pnlPct'] is None
    assert regular['equity'] == 2000 and regular['pnl'] == regular['pnlPct'] == 0


def test_advancing_replay_clock_invalidates_portfolio_price_cache(setup):
    _, markets, executor = setup
    execute(executor)
    first = asyncio.run(executor.portfolios())[0]
    assert first['equity'] == 2000 and first['pnl'] == 0
    markets.clock += timedelta(days=1)
    markets.price = 110
    second = asyncio.run(executor.portfolios())[0]
    assert second['equity'] == 2070 and second['pnl'] == 70
    assert second['asOf'] == markets.clock.isoformat()


def test_closed_world_can_still_value_positions(setup):
    _, markets, executor = setup
    execute(executor)
    markets.active = False
    rows = asyncio.run(executor.portfolios())
    assert rows[0]['equity'] == 2000 and rows[0]['valuationComplete']
