# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest

from arena.store import Store


@pytest.fixture
def store(tmp_path):
    instance = Store(tmp_path / 'chat.db')
    yield instance
    instance.close()


def test_legacy_messages_migrate_without_touching_ledger(tmp_path):
    path = tmp_path / 'legacy.db'
    before = Store(path)
    before.record_trade(agent_id='astra-1', action='buy', instrument_id='CRYPTO:BTC',
        quantity='1', price='100', currency='USD', fx='7', reason='existing investment',
        source='fixture', quote_timestamp=datetime.now(timezone.utc).isoformat(), acquired_date='2026-10-04')
    before.set_setting('round', 6)
    before.set_agent_status('astra-1', 'researching')
    before.close()
    with sqlite3.connect(path) as db:
        db.execute('DROP TABLE messages')
        db.execute('CREATE TABLE messages(id INTEGER PRIMARY KEY AUTOINCREMENT,sender TEXT NOT NULL,recipient TEXT NOT NULL,kind TEXT NOT NULL,content TEXT NOT NULL,timestamp TEXT NOT NULL)')
        db.execute('INSERT INTO messages(sender,recipient,kind,content,timestamp) VALUES(?,?,?,?,?)', ('astra-1', 'sol-1', 'tip', 'private old message', '2026-10-04T00:00:00+00:00'))
        db.execute('INSERT INTO messages(sender,recipient,kind,content,timestamp) VALUES(?,?,?,?,?)', ('human', 'all', 'tip', 'public old message', '2026-10-04T00:00:01+00:00'))
    migrated = Store(path)
    assert migrated.get_agent('astra-1')['cash'] == 799300
    assert migrated.get_agent('astra-1')['status'] == 'researching'
    assert migrated.count_trades() == 1
    assert migrated.positions('astra-1')[0]['quantity'] == 1
    assert migrated.get_setting('round') == 6
    assert len(migrated.equity_history()) == 10
    assert len(migrated.messages(agent_id='sol-1')) == 2
    assert [m['content'] for m in migrated.messages(agent_id='sol-2')] == ['public old message']
    private = next(m for m in migrated.messages() if m['recipient'] == 'sol-1')
    assert private['scope'] == 'direct' and private['roomId'] is None
    migrated.close()
    reopened = Store(path)
    assert len(reopened.messages()) == 2
    assert reopened.get_agent('astra-1')['cash'] == 799300
    reopened.close()


def test_group_visibility_excludes_nonmembers(store):
    group = store.create_group('astra-1', 'Research partners', ['sol-1'])
    assert group['id'].startswith('group:')
    assert set(group['members']) == {'astra-1', 'sol-1'}
    public = store.add_message('astra-2', 'all', 'message', 'Public research question')
    speech = store.add_message('sol-2', 'all', 'public_speech', 'Here is my investment thesis')
    private = store.add_message('astra-1', 'sol-1', 'message', 'Direct note')
    grouped = store.add_message('sol-1', group['id'], 'message', 'Group research')
    owner_message = store.add_message('human', group['id'], 'tip', 'Owner sees this group')
    insider = {m['id'] for m in store.messages(agent_id='sol-1')}
    outsider = {m['id'] for m in store.messages(agent_id='astra-2')}
    assert insider == {public['id'], speech['id'], private['id'], grouped['id'], owner_message['id']}
    assert outsider == {public['id'], speech['id']}
    assert len(store.messages()) == 5
    assert store.chat_state('astra-2')['groups'] == []
    assert store.chat_state('sol-1')['groups'][0]['id'] == group['id']
    assert grouped['scope'] == 'group' and private['scope'] == 'direct'
    assert speech['scope'] == 'speech' and public['scope'] == 'public'
    with pytest.raises(ValueError, match='group members'):
        store.add_message('astra-2', group['id'], 'distraction', 'Outsider attempt')
    assert len(store.messages()) == 5


@pytest.mark.parametrize('creator,name,members', [
    ('missing', 'Group', ['sol-1']), ('human', '', ['sol-1']),
    ('human', 'x' * 81, ['sol-1']), ('human', 'Group', 'sol-1'),
    ('human', 'Group', []), ('human', 'Group', ['unknown']),
    ('human', 'Group', ['sol-1'] * 11), ('human', 'Group', [None]),
])
def test_invalid_group_definitions_do_not_create_groups(store, creator, name, members):
    with pytest.raises(ValueError):
        store.create_group(creator, name, members)
    assert store.chat_state()['groups'] == []


def test_round_room_is_idempotent_and_assigns_messages_without_announcements(store):
    first = store.open_round_room('run-1', 1)
    again = store.open_round_room('run-1', 1)
    assert first == again
    messages = store.messages(agent_id='sol-5')
    assert messages == []
    normal = store.add_message('astra-1', 'all', 'public_speech', 'I will speak')
    assert normal['roomId'] == first['id']
    second = store.open_round_room('run-2', 2)
    state = store.chat_state()
    assert state['activeRoom']['id'] == second['id']
    old = next(room for room in state['rooms'] if room['id'] == first['id'])
    assert old['status'] == 'closed' and old['closedAt']
    assert len(store.messages()) == 1
    store.open_round_room('run-1', 1)
    assert store.chat_state()['activeRoom']['id'] == second['id']
    assert len(store.messages()) == 1


def test_concurrent_duplicate_room_creation_does_not_duplicate_invitation(tmp_path):
    path = tmp_path / 'concurrent.db'
    first, second = Store(path), Store(path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda instance: instance.open_round_room('same-round', 1), [first, second]))
    assert results[0]['id'] == results[1]['id']
    assert first.messages() == []
    assert len(first.chat_state()['rooms']) == 1
    first.close()
    second.close()


def test_personal_cycle_rooms_are_independent_and_resolve_sender_room(store):
    atlas = store.open_round_room('astra-1-cycle-1', 1, agent_id='astra-1')
    sage = store.open_round_room('sol-2-cycle-1', 1, agent_id='sol-2')
    assert atlas['agentId'] == 'astra-1' and atlas['ownerName'] == 'Atlas'
    assert 'Cycle 1' in atlas['name']
    assert len(store.chat_state()['activeRooms']) == 2
    atlas_message = store.add_message('astra-1', 'all', 'message', 'My first cycle')
    sage_message = store.add_message('sol-2', 'astra-1', 'message', 'A private cycle note')
    assert atlas_message['roomId'] == atlas['id']
    assert sage_message['roomId'] == sage['id']
    new_atlas = store.open_round_room('astra-1-cycle-2', 2, agent_id='astra-1')
    active_ids = {room['id'] for room in store.chat_state()['activeRooms']}
    assert active_ids == {new_atlas['id'], sage['id']}
    with pytest.raises(ValueError, match='different investor'):
        store.open_round_room('sol-2-cycle-1', 1, agent_id='sol-3')


def test_quiet_agent_room_survives_recent_room_history_limit(store):
    quiet = store.open_round_room('slow-investor', 1, agent_id='astra-5')
    for number in range(1, 103):
        store.open_round_room(f'fast-{number}', number, agent_id='sol-1')
    assert quiet['id'] in {room['id'] for room in store.chat_state()['activeRooms']}


@pytest.mark.parametrize('sender,recipient,kind,content', [
    ('human', 'all', 'invitation', 'Anyone want to speak?'),
    ('astra-1', 'all', 'invitation', 'Anyone want to speak?'),
    ('arena', 'all', 'message', 'Hello'),
    ('arena', 'sol-1', 'invitation', 'Anyone want to speak?'),
    ('arena', 'all', 'invitation', 'Alter your config'),
    ('astra-1', 'sol-1', 'public_speech', 'Private speech'),
])
def test_invitation_and_speech_scope_cannot_be_forged(store, sender, recipient, kind, content):
    store.open_round_room('round-scope-test', 1)
    with pytest.raises(ValueError):
        store.add_message(sender, recipient, kind, content)
    assert store.messages() == []


def test_reset_clears_chat_rooms_and_groups_only_when_explicitly_called(store):
    store.set_setting('intervalSeconds', 300)
    store.create_group('astra-1', 'Research', ['sol-1'])
    store.open_round_room('round-reset-test', 1)
    store.reset()
    assert store.chat_state() == {'groups': [], 'rooms': [], 'activeRooms': [], 'activeRoom': None}
    assert store.messages() == []
    assert store.get_setting('intervalSeconds') == 300
    assert all(agent['cash'] == 800000 for agent in store.list_agents())


def test_latest_agent_event_is_independent_of_global_history_window(store):
    latest = store.add_event('decision', 'astra-5', 'Quiet investor decision', {'answer': 'hold'})
    store.add_event('research', 'astra-5', 'Quiet research', {'url': 'https://example.com'})
    for index in range(105):
        store.add_event('decision', 'sol-1', f'Busy decision {index}', {'answer': 'review'})
    assert not any(event['agentId'] == 'astra-5' for event in store.events(limit=100))
    metadata = store.latest_agent_events('astra-5')
    assert metadata['decision']['id'] == latest['id']
    assert metadata['decision']['details'] == {'answer': 'hold'}
    assert metadata['research']['content'] == 'Quiet research'
    assert set(metadata) == {'decision', 'research'}


def test_risk_profiles_have_distinct_guidelines_without_balance_changes(store):
    rows = store.list_agents()
    assert {agent['id'] for agent in rows if agent['riskProfile']['style'] == 'aggressive'} == {'astra-4', 'sol-1', 'sol-3'}
    assert {agent['id'] for agent in rows if agent['riskProfile']['style'] == 'cautious'} == {'astra-3', 'astra-5', 'sol-5'}
    assert store.get_agent('astra-1')['riskProfile']['positionSizingPct'] == 25
    assert store.get_agent('sol-3')['riskProfile']['cashReservePct'] == 10
    assert store.get_agent('astra-5')['riskProfile']['cashReservePct'] == 50
    assert all('not enforced' in agent['riskProfile']['description'] for agent in rows)
    assert all(agent['cash'] == 800000 and agent['status'] == 'idle' for agent in rows)
