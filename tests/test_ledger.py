# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest

from arena.store import Store


def record(store, action='buy', quantity='1', price='100', **kwargs):
    return store.record_trade(agent_id='astra-1', action=action,
        instrument_id='CRYPTO:BTC', quantity=quantity, price=price, currency='USD',
        fx='7', reason=kwargs.pop('reason', 'verified research'),
        quote_timestamp='2026-10-04T00:00:00+00:00', source='test quote',
        acquired_date=kwargs.pop('acquired_date', '2026-10-04'), **kwargs)


def test_seed_and_restart_preserve_balances(tmp_path):
    path = tmp_path / 'ledger.db'
    store = Store(path)
    agents = store.list_agents()
    assert len(agents) == 10
    assert all(a['cash'] == 800_000 and a['effort'] == 'max' for a in agents)
    assert sum(a['model'] == 'gpt-6-astra' for a in agents) == 5
    assert sum(a['model'] == 'gpt-6.1-sol' for a in agents) == 5
    record(store)
    store.close()
    store = Store(path)
    assert store.get_agent('astra-1')['cash'] == 799_300
    assert store.positions('astra-1')[0]['quantity'] == 1
    assert store.count_trades() == 1


def test_weighted_average_basis_and_realized_profit(tmp_path):
    store = Store(tmp_path / 'ledger.db')
    record(store, quantity='0.1', price='100')
    record(store, quantity='0.2', price='200')
    assert store.positions('astra-1')[0]['costBasisCny'] == 350
    sold = record(store, action='sell', quantity='0.15', price='300')
    assert sold['realizedPnl'] == 140
    assert store.positions('astra-1')[0]['costBasisCny'] == 175
    record(store, action='sell', quantity='0.15', price='300')
    assert store.positions('astra-1') == []
    assert store.get_agent('astra-1')['cash'] == 800_280
    assert store.db.execute('SELECT COUNT(*) FROM lots').fetchone()[0] == 0


def test_concurrent_orders_cannot_overspend_two_connections(tmp_path):
    path = tmp_path / 'ledger.db'
    first, second = Store(path), Store(path)

    def buy(store):
        try:
            record(store, price='90000')  # 630,000 VC per order.
            return True
        except ValueError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(buy, [first, second]))
    assert sorted(results) == [False, True]
    assert first.get_agent('astra-1')['cash'] == 170_000
    assert first.count_trades() == 1
    assert first.positions('astra-1')[0]['quantity'] == 1


@pytest.mark.parametrize('quantity', ['NaN', 'Infinity', '-1', '0', '0.000000001', '1e100', True])
def test_invalid_amounts_leave_ledger_unchanged(tmp_path, quantity):
    store = Store(tmp_path / 'ledger.db')
    with pytest.raises(ValueError):
        record(store, quantity=quantity)
    assert store.get_agent('astra-1')['cash'] == 800_000
    assert store.count_trades() == 0
    assert store.positions() == []


def test_oversell_and_t1_are_atomic(tmp_path):
    store = Store(tmp_path / 'ledger.db')
    record(store)
    with pytest.raises(ValueError, match='Insufficient holdings'):
        record(store, action='sell', quantity='2')
    with pytest.raises(ValueError, match=r'T\+1'):
        record(store, action='sell', enforce_t1=True)
    assert store.get_agent('astra-1')['cash'] == 799_300
    assert store.positions('astra-1')[0]['quantity'] == 1
    record(store, action='sell', enforce_t1=True, acquired_date='2026-10-05')
    assert store.get_agent('astra-1')['cash'] == 800_000


def test_order_idempotence_allows_different_orders_in_one_round(tmp_path):
    store = Store(tmp_path / 'ledger.db')
    record(store, round_id='round-1')
    with pytest.raises(ValueError, match='Duplicate'):
        record(store, round_id='round-1')
    record(store, quantity='2', round_id='round-1')
    record(store, round_id='round-1:tool-2')
    assert store.count_trades() == 3


def test_settings_messages_history_and_explicit_reset(tmp_path):
    store = Store(tmp_path / 'ledger.db')
    store.set_setting('intervalSeconds', 300)
    record(store)
    store.add_message('astra-1', 'sol-1', 'distraction', 'Question my assumptions.')
    store.add_message('human', 'all', 'tip', 'Review primary sources.')
    assert len(store.messages(agent_id='sol-1')) == 2
    assert len(store.messages(agent_id='astra-2')) == 1
    store.add_event('research', 'astra-1', 'Read source', {'url': 'https://example.com'})
    assert store.events()[0]['details']['url'] == 'https://example.com'
    store.snapshot_equity('astra-1', Decimal('800005'))
    assert store.equity_history('astra-1')[-1]['equity'] == 800005
    store.reset()
    assert store.get_setting('intervalSeconds') == 300
    assert store.count_trades() == 0
    assert store.positions() == store.messages() == store.events() == []
    assert len(store.equity_history()) == 10
    assert all(a['cash'] == 800_000 for a in store.list_agents())


def test_quiet_investor_trade_history_survives_busy_global_feed(tmp_path):
    store = Store(tmp_path / 'ledger.db')
    original = record(store, quantity='0.00000001')
    for number in range(101):
        store.record_trade(agent_id='sol-1', action='buy', instrument_id='CRYPTO:ETH',
            quantity='0.01', price='100', currency='USD', fx='7', reason=f'other investor {number}',
            quote_timestamp='2026-10-04T00:00:00+00:00', source='test quote', acquired_date='2026-10-04')
    assert len(store.trades(limit=100)) == 100
    assert all(t['agentId'] == 'sol-1' for t in store.trades(limit=100))
    assert store.trades(limit=100, agent_id='astra-1') == [original]
    assert store.trades(limit=100, agent_id='astra-1')[0]['quantity'] == 0.00000001
    store.close()
