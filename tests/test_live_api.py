# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Offline API/stream regressions; never start a live investor or provider call."""
from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timezone

from fastapi.testclient import TestClient
import pytest

from arena.store import Store


@pytest.fixture
def live_api(monkeypatch, tmp_path):
    import app as module

    ledger = Store(tmp_path / 'live-api.db')

    class Runtime:
        def __init__(self):
            self.data = {'running': True, 'inProgress': True, 'round': 2,
                         'mode': 'continuous', 'agentStates': {}}
            self.pause_calls = self.close_calls = 0

        def status(self):
            return copy.deepcopy(self.data)

        async def pause(self):
            self.pause_calls += 1

        async def close(self):
            self.close_calls += 1

    class Markets:
        catalog = {}

        async def overview(self):
            return {'markets': [], 'coverage': {}, 'featured': []}

    class Executor:
        calls = 0

        async def portfolios(self):
            self.calls += 1
            return [{**a, 'positions': ledger.positions(a['id']),
                     'equity': a['cash'], 'pnl': 0, 'pnlPct': 0}
                    for a in ledger.list_agents()]

    runtime, markets, executor = Runtime(), Markets(), Executor()

    async def provider(force=False):
        return {'connected': True, 'models': ['gpt-6-astra', 'gpt-6.1-sol'],
                'baseUrl': 'http://127.0.0.1:8317/v1'}

    for name, value in {'store': ledger, 'runtime': runtime, 'markets': markets,
                        'executor': executor, 'check_provider': provider,
                        'stream_revision': 0, 'stream_signature': None,
                        'portfolio_cache': {}, 'manual_round_tasks': set()}.items():
        monkeypatch.setattr(module, name, value)
    client = TestClient(module.app)
    yield module, ledger, runtime, executor, client
    client.close()
    ledger.close()


def decode_frame(frame):
    lines = dict(line.split(': ', 1) for line in frame.strip().splitlines())
    assert lines['event'] == 'update'
    update = json.loads(lines['data'])
    assert int(lines['id']) == update['revision']
    return update


def record_buy(ledger, quantity=1):
    return ledger.record_trade(agent_id='astra-1', action='buy', instrument_id='CRYPTO:BTC',
        quantity=quantity, price=100, currency='USD', fx=7, reason='Isolated test trade',
        quote_timestamp=datetime.now(timezone.utc).isoformat(), source='Fixture quote',
        acquired_date='2026-10-04')


def test_live_snapshot_observes_phase_response_knowledge_and_message_changes(live_api):
    module, ledger, runtime, executor, _ = live_api
    first = module.live_update()
    assert module.live_update()['revision'] == first['revision']
    runtime.data['agentStates']['astra-1'] = {'status': 'waiting_for_model', 'responseCount': 0}
    phase = module.live_update()
    assert phase['revision'] > first['revision']
    assert next(a for a in phase['agents'] if a['id'] == 'astra-1')['status'] == 'waiting_for_model'
    response = ledger.add_event('response_completed', 'astra-1', 'Verify current sources before buying.',
                               {'kind': 'intermediate', 'responseNumber': 1})
    changed = module.live_update()
    assert changed['revision'] > phase['revision']
    atlas = next(a for a in changed['agents'] if a['id'] == 'astra-1')
    assert atlas['lastExplanation'] == response['content']
    assert atlas['lastResponseAt'] == response['timestamp']
    assert atlas['lastResponseKind'] == 'intermediate'
    knowledge = ledger.add_event('knowledge_updated', 'astra-1', 'Quotes and inbox refreshed.', {})
    changed_again = module.live_update()
    assert changed_again['revision'] > changed['revision']
    atlas = next(a for a in changed_again['agents'] if a['id'] == 'astra-1')
    assert atlas['knowledgeUpdatedAt'] == knowledge['timestamp']
    ledger.add_message('human', 'all', 'message', 'Please assess this new information.')
    assert module.live_update()['revision'] > changed_again['revision']
    assert executor.calls == 0  # SSE metadata never owns quote/network work.
    assert first['backgroundService']['continuesWithoutBrowser'] is True
    assert first['totals']['equity'] == 8000000


def test_quiet_investor_explanation_survives_busy_shared_event_feed(live_api):
    module, ledger, _, _, client = live_api
    explanation = 'Keep the previous evidence-based decision visible.'
    ledger.add_event('decision', 'astra-1', explanation, {'orders': 0, 'roundId': 'old-cycle'})
    ledger.add_event('response_completed', 'astra-1', explanation, {'kind': 'final'})
    for number in range(1100):
        ledger.add_event('research', 'sol-1', f'Another investor research event {number}', {})
    update = module.live_update()
    assert all(e['agentId'] != 'astra-1' for e in update['events'])
    atlas = next(a for a in update['agents'] if a['id'] == 'astra-1')
    assert atlas['lastDecision']['content'] == atlas['lastExplanation'] == explanation
    detail = client.get('/api/agents/astra-1')
    assert detail.status_code == 200
    assert detail.json()['agent']['lastDecision']['content'] == explanation


def test_changed_holdings_never_reuse_old_portfolio_equity(live_api):
    module, ledger, _, _, _ = live_api
    atlas = ledger.get_agent('astra-1')
    atlas.update(positions=[], equity=800000, pnl=0, pnlPct=0, valuationComplete=True)
    module.portfolio_cache['astra-1'] = {'signature': module.portfolio_signature(atlas),
                                        'agent': atlas, 'timestamp': '2026-10-04T00:00:00+00:00'}
    record_buy(ledger)
    update = module.live_update()
    current = next(a for a in update['agents'] if a['id'] == 'astra-1')
    assert current['cash'] == 799300
    assert current['equity'] is None and not current['valuationComplete']
    assert update['totals']['equity'] is None
    assert current['positions'][0]['quantity'] == 1
    assert next(a for a in update['agents'] if a['id'] == 'sol-1')['equity'] == 800000


@pytest.mark.parametrize('close_mode', ['disconnect', 'browser_close'])
async def test_stream_disconnect_does_not_cancel_or_pause_investors(live_api, close_mode):
    module, _, runtime, _, _ = live_api

    class Request:
        headers = {}
        disconnected = False

        async def is_disconnected(self):
            return self.disconnected

    request = Request()
    release = asyncio.Event()
    investor = asyncio.create_task(release.wait())
    generator = module.stream_updates(request)
    try:
        initial = decode_frame(await anext(generator))
        assert initial['runtime']['running'] is True
        if close_mode == 'disconnect':
            request.disconnected = True
            with pytest.raises(StopAsyncIteration):
                await anext(generator)
        else:
            await generator.aclose()
        assert not investor.done()
        assert runtime.pause_calls == runtime.close_calls == 0
        assert runtime.data['running'] is True
    finally:
        await generator.aclose()
        release.set()
        await investor


async def test_last_event_id_suppresses_duplicate_but_sends_next_full_snapshot(live_api):
    module, ledger, _, _, _ = live_api
    previous = module.live_update()

    class Request:
        headers = {'last-event-id': str(previous['revision'])}
        checks = 0

        async def is_disconnected(self):
            self.checks += 1
            if self.checks == 2:
                ledger.add_message('human', 'all', 'message', 'A newly arrived message')
            return False

    request = Request()
    generator = module.stream_updates(request)
    try:
        update = decode_frame(await asyncio.wait_for(anext(generator), 2))
        assert request.checks == 2  # The identical initial revision was skipped.
        assert update['revision'] > previous['revision']
        assert update['messages'][0]['content'] == 'A newly arrived message'
        assert len(update['agents']) == 10  # Reconnect delivers current state, not event replay.
    finally:
        await generator.aclose()


async def test_stream_endpoint_declares_unbuffered_non_cached_sse(live_api):
    module, _, _, _, _ = live_api

    class Request:
        headers = {}

        async def is_disconnected(self):
            return True

    response = await module.stream(Request())
    assert response.media_type == 'text/event-stream'
    assert response.headers['cache-control'] == 'no-store'
    assert response.headers['x-accel-buffering'] == 'no'
    await response.body_iterator.aclose()


async def test_normal_public_response_metadata_omits_credentials_and_private_reasoning(live_api, monkeypatch):
    module, ledger, _, executor, _ = live_api
    from arena.runtime import Runtime

    model_runtime = Runtime(ledger, module.markets, executor,
                            'http://127.0.0.1:8317/v1', 'fixture-model-secret')
    monkeypatch.setattr(module, 'runtime', model_runtime)
    try:
        model_runtime._record_response('astra-1', 'fixture-cycle', 0, {
            'role': 'assistant', 'content': 'Public investment explanation.',
            'reasoning_content': 'private-scratchpad-must-not-be-visible',
            'reasoning': {'text': 'private-scratchpad-must-not-be-visible'}, 'tool_calls': []})
        payload = json.dumps(module.live_update())
        assert 'Public investment explanation.' in payload
        for forbidden in ['fixture-model-secret', 'private-scratchpad-must-not-be-visible',
                          'reasoning_content', 'Authorization', 'apiKey']:
            assert forbidden not in payload
    finally:
        await model_runtime.close()


@pytest.mark.parametrize('body,status', [
    ({'name': ' ', 'members': ['astra-1']}, 400),
    ({'name': 'Research', 'members': ['missing']}, 400),
    ({'name': 'Research', 'members': []}, 422),
    ({'name': 'x' * 81, 'members': ['astra-1']}, 422),
])
def test_group_api_rejects_invalid_groups_without_creating_records(live_api, body, status):
    _, ledger, _, _, client = live_api
    assert client.post('/api/chat/groups', json=body).status_code == status
    assert ledger.chat_state()['groups'] == []


def test_group_api_and_group_messages_preserve_members_and_scope(live_api):
    module, ledger, _, _, client = live_api
    before = module.live_update()['revision']
    response = client.post('/api/chat/groups', json={'name': ' Research partners ',
        'members': ['astra-1', 'sol-1', 'astra-1']})
    assert response.status_code == 200
    group = response.json()
    assert group['name'] == 'Research partners'
    assert group['creator'] == 'human'
    assert group['members'] == ['astra-1', 'sol-1']
    assert module.live_update()['revision'] > before
    assert client.get('/api/chat').json()['groups'][0]['id'] == group['id']
    sent = client.post('/api/messages', json={'recipient': group['id'], 'kind': 'message',
                                            'content': 'Evaluate this research together.'})
    assert sent.status_code == 200 and sent.json()['scope'] == 'group'
    assert ledger.messages(agent_id='sol-1')[0]['id'] == sent.json()['id']
    assert ledger.messages(agent_id='sol-2') == []


def test_public_speech_api_attaches_room_and_rejects_invalid_routing(live_api):
    _, ledger, _, _, client = live_api
    room = ledger.open_round_room('fixture-cycle', 3, agent_id='astra-1')
    spoken = client.post('/api/messages', json={'recipient': 'all', 'kind': 'public_speech',
        'content': 'I would like to share my thesis.', 'roomId': room['id']})
    assert spoken.status_code == 200
    assert spoken.json()['scope'] == 'speech' and spoken.json()['roomId'] == room['id']
    assert spoken.json()['id'] in {m['id'] for m in ledger.messages(agent_id='sol-5')}
    original_count = len(ledger.messages())
    for body, status in [
        ({'recipient': 'astra-1', 'kind': 'public_speech', 'content': 'Invalid private speech'}, 400),
        ({'recipient': 'all', 'kind': 'message', 'content': 'Bad room', 'roomId': 'missing'}, 400),
        ({'recipient': 'missing', 'kind': 'message', 'content': 'Bad recipient'}, 404),
        ({'sender': 'astra-1', 'recipient': 'all', 'kind': 'message', 'content': 'Spoofed sender'}, 400),
    ]:
        assert client.post('/api/messages', json=body).status_code == status
    assert len(ledger.messages()) == original_count
