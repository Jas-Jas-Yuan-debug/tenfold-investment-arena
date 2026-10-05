# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Isolated continuous-cycle, public-response and knowledge checkpoint checks."""
import asyncio
import copy
from datetime import datetime, timedelta, timezone
import json

import pytest

from arena.execution import TradeExecutor
from arena.runtime import Runtime
from arena.store import Store


STAMP = '2026-10-04T08:00:00+00:00'


class ObservedMarkets:
    def __init__(self):
        self.refreshes = 0
        self.catalog = {f'CRYPTO:{s}': {'id': f'CRYPTO:{s}', 'symbol': s, 'name': s,
                        'market': 'CRYPTO', 'kind': 'crypto', 'currency': 'USD'} for s in ('BTC', 'ETH')}

    def market_status(self, market):
        return {'id': market, 'isOpen': market == 'CRYPTO', 'reason': 'Fixture session'}

    async def overview(self):
        self.refreshes += 1
        return {'featured': [
            {'instrumentId': f'CRYPTO:{symbol}', 'currency': 'USD', 'price': price + self.refreshes,
             'timestamp': STAMP, 'source': 'Observed fixture exchange', 'sourceUrl': 'https://example.com/quote'}
            for symbol, price in (('BTC', 100), ('ETH', 50))],
            'fx': {'rate': 7, 'timestamp': STAMP, 'source': 'Observed fixture FX'}, 'errors': []}

    async def history(self, instrument_id):
        return [{'timestamp': f'2026-10-04T{n % 24:02d}:00:00+00:00', 'price': 100 + n} for n in range(100)]


@pytest.fixture
async def arena(tmp_path, monkeypatch):
    store = Store(tmp_path / 'continuous.db')
    markets = ObservedMarkets()
    runtime = Runtime(store, markets, TradeExecutor(store, markets), 'http://127.0.0.1:8317/v1', 'fixture-secret')

    async def search(query):
        return {'query': query, 'source': 'Observed fixture search', 'fetchedAt': STAMP,
                'results': [{'title': 'Observed public article', 'url': 'https://example.com/article',
                             'snippet': 'Supplied source evidence.', 'publishedAt': 'Sun, 04 Oct 2026 08:00:00 GMT'}]}

    monkeypatch.setattr(runtime, 'web_search', search)
    yield store, markets, runtime
    await runtime.close()
    store.close()


def call(name, args, identifier='fixture-call'):
    return {'id': identifier, 'type': 'function', 'function': {'name': name, 'arguments': json.dumps(args)}}


async def test_each_response_publishes_public_note_refreshes_then_preserves_tool_order(arena, monkeypatch):
    store, markets, runtime = arena
    requests = []

    async def model(agent, messages, tools_enabled=True):
        requests.append(copy.deepcopy(messages))
        if len(requests) == 1:
            store.add_message('human', 'astra-1', 'message', 'New inbox evidence: https://example.com/update')
            return {'role': 'assistant', 'content': 'Evidence is limited; checking my portfolio before risking cash.',
                    'reasoning_content': 'PRIVATE_SCRATCHPAD_DO_NOT_PUBLISH',
                    'tool_calls': [call('portfolio', {})]}
        return {'role': 'assistant', 'content': 'Conclusion: hold cash. Assumption: evidence is incomplete. Risk: avoid concentration.',
                'reasoning_content': 'PRIVATE_SCRATCHPAD_DO_NOT_PUBLISH'}

    monkeypatch.setattr(runtime, '_model_call', model)
    report = await runtime._run_investor(store.get_agent('astra-1'), 'cycle-fixture')
    assert report['status'] == 'completed'
    assert [m['role'] for m in requests[1][-3:]] == ['assistant', 'tool', 'user']
    refresh = json.loads(requests[1][-1]['content'].split('\n', 1)[1])
    assert refresh['afterResponse'] == 1
    assert refresh['quotes'][0]['price'] == 102
    assert refresh['quotes'][0]['timestamp'] == STAMP
    assert refresh['fx']['timestamp'] == STAMP
    assert any('New inbox evidence' in m['content'] for m in refresh['newMessages'])
    assert refresh['sharedResearch'][0]['results'][0]['publishedAt'] == 'Sun, 04 Oct 2026 08:00:00 GMT'
    saved = store.get_setting('investorKnowledge:astra-1')
    assert saved['afterResponse'] == 2 and saved['quotes'][0]['price'] == 103
    assert saved['portfolio']['cash'] == 800000
    events = store.events(100)
    responses = [e for e in events if e['kind'] == 'response_completed']
    assert len(responses) == 2
    assert {e['details']['kind'] for e in responses} == {'intermediate', 'final'}
    assert 'PRIVATE_SCRATCHPAD' not in json.dumps(events)
    assert len([e for e in events if e['kind'] == 'knowledge_updated']) == 3
    state = runtime.status()['agentStates']['astra-1']
    assert state['responseCount'] == 2 and state['knowledgeUpdateCount'] == 3
    assert state['completedCycles'] == 1 and state['status'] == 'ready_for_next_cycle'
    assert state['lastResponseAt'] and state['knowledgeUpdatedAt']


async def test_empty_tool_response_is_labeled_without_inventing_an_explanation(arena, monkeypatch):
    store, _, runtime = arena
    count = 0

    async def model(agent, messages, tools_enabled=True):
        nonlocal count
        count += 1
        return {'role': 'assistant', 'content': None, 'tool_calls': [call('portfolio', {})]} if count == 1 else {
            'role': 'assistant', 'content': 'Hold cash after checking observed evidence.'}

    monkeypatch.setattr(runtime, '_model_call', model)
    await runtime._run_investor(store.get_agent('astra-1'), 'cycle-fixture')
    intermediate = next(e for e in store.events() if e['kind'] == 'response_completed' and e['details']['kind'] == 'intermediate')
    assert intermediate['details']['explanation'] is None
    assert intermediate['details']['toolActions'] == [{'name': 'portfolio'}]


async def test_independent_agent_starts_second_cycle_before_slow_agent_finishes(arena, monkeypatch):
    store, _, runtime = arena
    second_fast, release = asyncio.Event(), asyncio.Event()
    counts = {}

    async def model(agent, messages, tools_enabled=True):
        counts[agent['id']] = counts.get(agent['id'], 0) + 1
        if agent['id'] == 'astra-1' and counts[agent['id']] == 1:
            return {'role': 'assistant', 'content': 'Hold cash; first independent decision completed.'}
        if agent['id'] == 'astra-1':
            second_fast.set()
        await release.wait()
        return {'role': 'assistant', 'content': 'Hold cash after observed evidence.'}

    monkeypatch.setattr(runtime, '_model_call', model)
    runtime._next_round = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    await runtime.start()
    try:
        await asyncio.wait_for(second_fast.wait(), 2)
        status = runtime.status()
        assert status['mode'] == 'continuous' and status['nextRound'] is None
        assert status['completedDecisions'] == 1
        assert status['agentStates']['astra-1']['cycle'] == 2
        assert status['agentStates']['sol-1']['completedCycles'] == 0
        assert any(r['agentId'] == 'astra-1' and r['cycle'] == 2 for r in status['currentRoundReports'])
        assert (await runtime.run_round())['skipped'] is True
        active = store.chat_state()['activeRooms']
        assert len(active) == 10 and any(r['agentId'] == 'astra-1' and r['number'] == 2 for r in active)
        assert not any(m['kind'] == 'invitation' for m in store.messages())
        await runtime.pause()
        assert not runtime.status()['inProgress'] and not runtime.status()['running']
        assert store.get_setting('runtimeSnapshot')['running'] is False
        assert all(a['status'] == 'idle' for a in store.list_agents())
    finally:
        release.set()


async def test_restoring_checkpoint_does_not_mutate_db_status_and_keeps_real_counters(arena):
    store, markets, _ = arena
    store.set_agent_status('astra-1', 'generating_response')
    store.set_setting('runtimeSnapshot', {'running': True, 'round': 3, 'lastRound': STAMP,
        'nextRound': (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        'lastRoundReports': [{'agentId': 'astra-1', 'status': 'completed', 'decision': 'Actual prior public decision.', 'orders': 0}],
        'agentStates': {'astra-1': {'cycle': 7, 'completedCycles': 4, 'responseCount': 9, 'knowledgeUpdateCount': 12}}})
    restored = Runtime(store, markets, TradeExecutor(store, markets), 'http://127.0.0.1:8317/v1', 'fixture-secret')
    try:
        status = restored.status()
        assert store.get_agent('astra-1')['status'] == 'generating_response'
        assert status['round'] == 3 and status['lastRound'] == STAMP
        assert status['agentStates']['astra-1']['cycle'] == 7
        assert status['agentStates']['astra-1']['responseCount'] == 9
        assert status['agentStates']['sol-1']['cycle'] == 3
        assert status['agentStates']['sol-1']['completedCycles'] == 0
        assert status['agentStates']['astra-1']['lastExplanation'] == 'Actual prior public decision.'
        status['agentStates']['astra-1']['responseCount'] = 999
        assert restored.status()['agentStates']['astra-1']['responseCount'] == 9
        await restored.close()
        assert store.get_setting('runtimeSnapshot')['running'] is True
    finally:
        await restored.close()


async def test_explicit_reset_clears_knowledge_but_not_plain_pause(arena, monkeypatch):
    store, _, runtime = arena

    async def model(agent, messages, tools_enabled=True):
        return {'role': 'assistant', 'content': 'Hold cash.'}

    monkeypatch.setattr(runtime, '_model_call', model)
    await runtime._run_investor(store.get_agent('astra-1'), 'cycle-fixture')
    await runtime.pause()
    assert store.get_setting('investorKnowledge:astra-1')['afterResponse'] == 1
    runtime.reset_status()
    assert store.get_setting('investorKnowledge:astra-1') is None
    assert runtime.status()['completedDecisions'] == 0
    assert runtime.status()['agentStates']['astra-1']['responseCount'] == 0


async def test_private_groups_inboxes_history_and_risk_styles(arena):
    store, markets, runtime = arena
    hidden = store.create_group('sol-1', 'Private evidence', ['sol-2'])
    store.add_message('sol-1', hidden['id'], 'message', 'Private conversation.')
    agent = store.get_agent('astra-1')
    context = runtime._context(agent)
    assert hidden['id'] not in json.dumps(context)
    knowledge = await runtime._refresh_knowledge(agent, 'cycle-fixture', 0)
    assert hidden['id'] not in json.dumps(knowledge)
    budget = {'tools': 0, 'orders': 0, 'searches': 1, 'messages': 0, 'groups': 0,
              'researchOk': True, 'successfulOrders': 0, 'results': {}}
    history = await runtime._dispatch_tool(agent, 'cycle-fixture', 'get_price_history', {'instrument_id': 'CRYPTO:BTC'}, budget, 'history')
    assert len(history['points']) == 72 and history['points'][-1]['price'] == 199
    assert history['points'][-1]['timestamp'] == '2026-10-04T03:00:00+00:00'
    assert 'source' not in history
    group = await runtime._dispatch_tool(agent, 'cycle-fixture', 'create_group', {'name': 'Publicly invited members', 'members': ['sol-3']}, budget, 'group1')
    assert 'astra-1' in group['members'] and 'sol-3' in group['members']
    await runtime._dispatch_tool(agent, 'cycle-fixture', 'send_message', {'recipient': group['id'], 'kind': 'message', 'content': 'Share evidence?'}, budget, 'groupmessage')
    assert any(m['recipient'] == group['id'] for m in store.messages(agent_id='sol-3'))
    assert '40%' in runtime._risk_instructions(store.get_agent('sol-3'))
    assert '10% of equity' in runtime._risk_instructions(store.get_agent('astra-5'))
    assert '25%' in runtime._risk_instructions(store.get_agent('astra-1'))
