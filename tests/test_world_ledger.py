# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from arena.store import AGENTS, Store


def wallets(amount=800000):
    return {agent[0]: amount for agent in AGENTS}


def main_position(store):
    return store.record_trade(agent_id='astra-1', action='buy', instrument_id='CRYPTO:BTC',
        quantity='1', price='100', currency='USD', fx='7', reason='main position', source='fixture',
        quote_timestamp=datetime.now(timezone.utc).isoformat(), acquired_date='2026-10-04')


def test_historical_funding_clones_cash_and_persists_baselines_without_main_changes(tmp_path):
    main = Store(tmp_path / 'main-test.db')
    main_position(main)
    balances = {agent['id']: agent['cash'] for agent in main.list_agents()}
    branch_path = tmp_path / 'historical-2023.db'
    branch = Store(branch_path)
    result = branch.seed_wallets(balances, world_id='historical-2023-1')
    assert result['wallets'] == balances
    assert result['initialCapital'] == 7999300
    assert branch.positions() == []
    assert branch.get_agent('astra-1')['cash'] == 799300
    assert branch.get_agent('astra-1')['initialCash'] == 799300
    assert branch.equity_history('astra-1')[0]['equity'] == 799300
    assert main.get_agent('astra-1')['cash'] == 799300
    assert len(main.positions()) == 1
    main_events = main.events()
    assert not any(event['kind'] == 'world_funded' for event in main_events)
    main_position(branch)
    assert branch.seed_wallets(balances, world_id='historical-2023-1') == result
    assert branch.get_agent('astra-1')['cash'] == 798600  # Retry does not refill spent cash.
    branch.close()
    reopened = Store(branch_path)
    assert reopened.get_agent('astra-1')['initialCash'] == 799300
    assert reopened.get_agent('astra-1')['cash'] == 798600
    assert reopened.get_setting('branchWorldId') == 'historical-2023-1'
    main.close()
    reopened.close()


def test_zero_cash_and_microvc_branch_wallets_are_allowed(tmp_path):
    branch = Store(tmp_path / 'historical-zero.db')
    initial = wallets(0)
    initial['sol-3'] = Decimal('0.000001')
    branch.seed_wallets(initial, world_id='historical-zero')
    assert branch.get_agent('astra-1')['initialCash'] == 0
    assert branch.get_agent('astra-1')['cash'] == 0
    assert branch.get_agent('sol-3')['cash'] == 0.000001
    assert len(branch.equity_history()) == 10


def test_wallet_funding_rejects_main_path_and_nonpristine_or_invalid_branch(tmp_path):
    store = Store(tmp_path / 'test.db')
    original_path = store.path
    store.path = __import__('pathlib').Path(__file__).resolve().parents[1] / 'data' / 'arena.db'
    with pytest.raises(ValueError, match='Main wallets'):
        store.seed_wallets(wallets(), world_id='historical-guard')
    store.path = original_path
    with pytest.raises(ValueError, match='separate world'):
        store.seed_wallets(wallets(), world_id='main')
    with pytest.raises(ValueError, match='all ten'):
        store.seed_wallets({'astra-1': 1}, world_id='historical-missing')
    for amount in [-1, True, 'Infinity', 'NaN', '1000000001']:
        with pytest.raises(ValueError):
            store.seed_wallets(wallets(amount), world_id='historical-invalid')
    assert all(agent['cash'] == 800000 for agent in store.list_agents())
    main_position(store)
    with pytest.raises(ValueError, match='pristine'):
        store.seed_wallets(wallets(), world_id='historical-not-empty')
    assert store.get_setting('branchWorldId') is None


def test_net_profit_transfer_is_exact_and_keeps_main_positions_unchanged(tmp_path):
    main = Store(tmp_path / 'main-test.db')
    main_position(main)
    before_positions = main.positions()
    before_trades = main.trades()
    transfer = main.apply_world_delta('astra-1', Decimal('123.456789'), 'historical-2023', 'choice-astra-1',
        baseline_equity=800000, final_equity=Decimal('800123.456789'), details={'settlement': 'genuine historical quotes'})
    assert transfer['status'] == 'applied'
    assert transfer['deltaVC'] == 123.456789
    assert transfer['cashBefore'] == 799300
    assert transfer['cashAfter'] == 799423.456789
    assert transfer['baselineEquity'] == 800000
    assert transfer['finalEquity'] == 800123.456789
    assert main.get_agent('astra-1')['cash'] == 799423.456789
    assert main.positions() == before_positions
    assert main.trades() == before_trades
    retry = main.apply_world_delta('astra-1', Decimal('123.456789'), 'historical-2023', 'choice-astra-1',
        baseline_equity=800000, final_equity=Decimal('800123.456789'))
    assert retry == transfer
    assert len(main.world_transfers()) == 1
    assert main.events()[0]['kind'] == 'world_transfer'
    with pytest.raises(ValueError, match='already made'):
        main.apply_world_delta('astra-1', 500, 'historical-2023', 'different-choice')
    assert main.get_agent('astra-1')['cash'] == 799423.456789


def test_full_loss_transfer_or_atomic_failed_choice_without_silent_cap(tmp_path):
    main = Store(tmp_path / 'main-test.db')
    main_position(main)
    failed = main.apply_world_delta('astra-1', -800000, 'historical-full-loss', 'astra-loss',
        baseline_equity=800000, final_equity=0)
    assert failed['status'] == 'failed'
    assert failed['deltaVC'] == -800000
    assert 'insufficient cash' in failed['error']
    assert failed['cashBefore'] == failed['cashAfter'] == 799300
    assert main.get_agent('astra-1')['cash'] == 799300
    assert len(main.positions()) == 1
    retry = main.apply_world_delta('astra-1', -800000, 'historical-full-loss', 'astra-loss',
        baseline_equity=800000, final_equity=0)
    assert retry == failed
    assert len(main.world_transfers()) == 1
    assert main.events()[0]['kind'] == 'world_transfer_failed'
    # Main cash later increasing does not turn the consumed failed choice into a credit/debit.
    main.apply_world_delta('astra-1', 1000, 'historical-other-world', 'other-choice')
    assert main.apply_world_delta('astra-1', -800000, 'historical-full-loss', 'astra-loss', baseline_equity=800000, final_equity=0) == failed
    assert main.get_agent('astra-1')['cash'] == 800300
    applied = main.apply_world_delta('sol-1', -800000, 'historical-full-loss', 'same-batch-choice', baseline_equity=800000, final_equity=0)
    assert applied['status'] == 'applied'
    assert main.get_agent('sol-1')['cash'] == 0


def test_concurrent_retries_and_losses_cannot_duplicate_or_overspend(tmp_path):
    path = tmp_path / 'concurrent-main.db'
    first, second = Store(path), Store(path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda store: store.apply_world_delta('astra-1', 100, 'historical-profit', 'one-choice'), [first, second]))
    assert results[0] == results[1]
    assert first.get_agent('astra-1')['cash'] == 800100
    assert len(first.world_transfers()) == 1
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(first.apply_world_delta, 'astra-1', -600000, 'historical-loss-1', 'loss-choice-1'),
                   pool.submit(second.apply_world_delta, 'astra-1', -600000, 'historical-loss-2', 'loss-choice-2')]
        losses = [future.result() for future in futures]
    assert sorted(result['status'] for result in losses) == ['applied', 'failed']
    assert first.get_agent('astra-1')['cash'] == 200100
    assert len(first.world_transfers()) == 3


def test_invalid_delta_or_falsified_baseline_does_not_create_choice(tmp_path):
    main = Store(tmp_path / 'main-test.db')
    for value in ['NaN', 'Infinity', True, Decimal('1000000001')]:
        with pytest.raises(ValueError):
            main.apply_world_delta('astra-1', value, 'historical-invalid', 'choice')
    for identifier in ['', '../arena.db', 'bad id', 'x' * 201]:
        with pytest.raises(ValueError):
            main.apply_world_delta('astra-1', 1, identifier, 'choice')
    with pytest.raises(ValueError, match='final equity minus'):
        main.apply_world_delta('astra-1', 100, 'historical-false-mark', 'choice', baseline_equity=800000, final_equity=800200)
    with pytest.raises(ValueError, match='both historical'):
        main.apply_world_delta('astra-1', 100, 'historical-one-mark', 'choice', baseline_equity=800000)
    assert main.world_transfers() == []
    assert main.get_agent('astra-1')['cash'] == 800000
    branch = Store(tmp_path / 'historical.db')
    branch.seed_wallets(wallets(), world_id='historical-branch')
    with pytest.raises(ValueError, match='main-world ledger'):
        branch.apply_world_delta('astra-1', 100, 'historical-other', 'choice')


def test_speaking_announcements_disabled_across_cycles_restarts_and_worlds(tmp_path):
    path = tmp_path / 'chat.db'
    store = Store(path)
    assert store.announce_speaking_session('startup-1') is None
    for number in range(1, 4):
        for agent in AGENTS:
            store.open_round_room(f'{agent[0]}-cycle-{number}', number, agent_id=agent[0])
    assert store.messages() == []
    store.close()
    reopened = Store(path)
    assert reopened.announce_speaking_session('restart-3') is None
    assert reopened.messages() == []
    branch = Store(tmp_path / 'historical-chat.db')
    branch.seed_wallets(wallets(), world_id='historical-chat-2023')
    assert branch.announce_speaking_session('activation-1', world_id='historical-chat-2023') is None
    with pytest.raises(ValueError, match='does not match'):
        branch.announce_speaking_session('bad-activation', world_id='main')
    reopened.close()
    branch.close()


def test_legacy_announcements_removed_without_removing_other_chat(tmp_path):
    path = tmp_path / 'legacy-chat.db'
    old = Store(path)
    old.add_message('astra-1', 'sol-1', 'message', 'Unchanged private note')
    old.close()
    with sqlite3.connect(path) as db:
        db.execute('DROP TRIGGER speaking_invitations_disabled')
        for index in range(3):
            db.execute('INSERT INTO messages(sender,recipient,kind,content,timestamp,scope,world_id) VALUES(?,?,?,?,?,?,?)', ('arena', 'all', 'invitation', 'Anyone want to speak?', f'2026-10-04T00:00:0{index}+00:00', 'public', 'main'))
    migrated = Store(path)
    assert len(migrated.messages()) == 1
    assert len(migrated.messages(include_archived=True)) == 1
    assert migrated.announce_speaking_session('current-activation') is None
    assert len(migrated.messages(agent_id='sol-1')) == 1
    assert migrated.messages(agent_id='sol-2') == []
    migrated.close()


def test_sqlite_guard_blocks_all_old_server_invites_and_preserves_chat(tmp_path):
    path = tmp_path / 'legacy-running-server.db'
    store = Store(path)
    for index in range(3):
        room = store.open_round_room(f'old-cycle-{index}', index + 1, agent_id='astra-1')
        with sqlite3.connect(path) as old_connection:
            old_connection.execute('INSERT INTO messages(sender,recipient,kind,content,timestamp,scope,room_id) VALUES(?,?,?,?,?,?,?)',
                ('arena', 'all', 'invitation', 'Anyone want to speak?', datetime.now(timezone.utc).isoformat(), 'public', room['id']))
    assert store.messages(include_archived=True) == []
    with sqlite3.connect(path) as old_connection:
        old_connection.execute('INSERT INTO messages(sender,recipient,kind,content,timestamp,scope) VALUES(?,?,?,?,?,?)',
            ('sol-1', 'all', 'public_speech', 'An investor speech', datetime.now(timezone.utc).isoformat(), 'speech'))
        old_connection.execute('INSERT INTO messages(sender,recipient,kind,content,timestamp,scope) VALUES(?,?,?,?,?,?)',
            ('human', 'all', 'message', 'A human message', datetime.now(timezone.utc).isoformat(), 'public'))
    assert {message['content'] for message in store.messages()} == {'An investor speech', 'A human message'}
    store.close()
