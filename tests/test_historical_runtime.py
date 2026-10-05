# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Historical-world boundaries and merge review checks without live providers."""
import asyncio
from datetime import datetime, timedelta, timezone
import json

import httpx
import pytest

from arena.runtime import Runtime
from arena.store import Store


class ReplayMarkets:
    def __init__(self):
        self.time = datetime(2023, 6, 2, 12, tzinfo=timezone.utc)
        self.catalog = {'CRYPTO:BTC': {'id': 'CRYPTO:BTC', 'symbol': 'BTC', 'market': 'CRYPTO',
                        'currency': 'USD', 'kind': 'crypto'},
                        'CRYPTO:ETH': {'id': 'CRYPTO:ETH', 'symbol': 'ETH', 'market': 'CRYPTO',
                        'currency': 'USD', 'kind': 'crypto'}}

    def clock_now(self):
        return self.time

    def market_status(self, market):
        return {'id': market, 'isOpen': market == 'CRYPTO', 'reason': 'Observed replay fixture'}

    async def get_quote(self, instrument_id):
        return {'instrumentId': instrument_id, 'price': 25000, 'currency': 'USD',
                'timestamp': self.time.isoformat(), 'source': 'Dated fixture historical quote'}

    async def get_fx(self):
        return {'rate': 7, 'timestamp': self.time.isoformat(), 'source': 'Dated fixture historical FX'}

    async def overview(self):
        return {'featured': [await self.get_quote(s) for s in self.catalog], 'fx': await self.get_fx(), 'errors': []}

    async def history(self, instrument_id):
        return [{'timestamp': (self.time - timedelta(days=1)).isoformat(), 'price': 24000},
                {'timestamp': self.time.isoformat(), 'price': 25000},
                {'timestamp': (self.time + timedelta(days=1)).isoformat(), 'price': 26000},
                {'timestamp': '2026-10-04T12:00:00+00:00', 'price': 120000}]


class Executor:
    def __init__(self, store):
        self.store = store
        self.orders = []

    async def portfolios(self):
        return [{**a, 'positions': [], 'equity': a['cash'], 'isEstimated': False} for a in self.store.list_agents()]

    async def execute(self, *args, **kwargs):
        self.orders.append((args, kwargs))
        return {'fixtureOrder': True}


def historical_research():
    return {'query': 'Bitcoin news', 'source': 'Dated fixture search', 'fetchedAt': '2026-10-04T08:00:00+00:00',
            'results': [{'title': 'Publication before replay time', 'url': 'https://example.com/2023',
                         'snippet': 'Observed dated historical evidence.', 'publishedAt': 'Fri, 02 Jun 2023 08:00:00 GMT'},
                        {'title': 'Future replay day', 'url': 'https://example.com/future',
                         'publishedAt': 'Sat, 03 Jun 2023 08:00:00 GMT'},
                        {'title': 'Present-day contamination', 'url': 'https://example.com/2026',
                         'publishedAt': 'Sun, 04 Oct 2026 08:00:00 GMT'},
                        {'title': 'Undated summary', 'url': 'https://example.com/undated', 'snippet': 'No publication date.'}]}


@pytest.fixture
async def replay(tmp_path):
    store = Store(tmp_path / 'historical.db')
    markets = ReplayMarkets()
    executor = Executor(store)
    runtime = Runtime(store, markets, executor, 'http://127.0.0.1:8317/v1', 'fixture-secret',
                      world_context={'worldId': 'replay-2023', 'name': '2023 replay', 'isHistorical': True})
    yield store, markets, executor, runtime
    await runtime.close()
    store.close()


async def test_historical_search_restricts_query_and_rejects_future_undated_articles(replay, monkeypatch):
    _, markets, _, runtime = replay
    queries = []

    async def search(query, **kwargs):
        queries.append(query)
        return historical_research()

    monkeypatch.setattr('arena.runtime.web_search', search)
    result = await runtime.web_search('Bitcoin latest news 2026-10-04 when:3d')
    assert queries == ['Bitcoin news after:2023-01-01 before:2023-06-03']
    assert [r['url'] for r in result['results']] == ['https://example.com/2023']
    assert result['results'][0]['publishedAt'] == 'Fri, 02 Jun 2023 08:00:00 GMT'
    assert result['fetchedAt'] == '2026-10-04T08:00:00+00:00'
    assert result['asOf'] == markets.time.isoformat() and result['worldId'] == 'replay-2023'


async def test_historical_mandatory_context_knowledge_and_events_use_replay_clock(replay, monkeypatch):
    store, markets, _, runtime = replay
    requests, queries = [], []
    store.set_setting('investorKnowledge:astra-1', {'worldId': 'main', 'sourceTime': '2026-10-04T12:00:00+00:00',
                                                'futureSecret': 'FUTURE_MAIN_KNOWLEDGE'})

    async def search(query):
        queries.append(query)
        return historical_research()

    async def model(agent, messages, tools_enabled=True):
        requests.append(messages)
        return {'role': 'assistant', 'content': 'Hold cash after reading the dated 2023 article. Risk: limited evidence.'}

    monkeypatch.setattr(runtime, 'web_search', search)
    monkeypatch.setattr(runtime, '_model_call', model)
    report = await runtime._run_investor(store.get_agent('astra-1'), 'historical-cycle')
    assert report['status'] == 'completed' and report['worldId'] == 'replay-2023'
    assert all('before:2023-06-03' in q and '2026' not in q and 'latest' not in q for q in queries)
    text = json.dumps(requests)
    assert 'FUTURE_MAIN_KNOWLEDGE' not in text and 'Present-day contamination' not in text
    assert 'isolated 2023 historical replay' in text
    assert runtime._context(store.get_agent('astra-1'))['timeUtc'] == markets.time.isoformat()
    knowledge = store.get_setting('investorKnowledge:astra-1')
    assert knowledge['worldId'] == 'replay-2023' and knowledge['sourceTime'] == markets.time.isoformat()
    assert knowledge['quotes'][0]['timestamp'] == markets.time.isoformat()
    assert datetime.fromisoformat(knowledge['updatedAt']).year != 2023
    assert all(e['details']['worldId'] == 'replay-2023' and e['details']['asOf'].startswith('2023-') for e in store.events())


async def test_future_quotes_history_and_stopped_world_orders_are_rejected(replay, monkeypatch):
    store, markets, executor, runtime = replay
    agent = store.get_agent('astra-1')
    budget = {'researchOk': True, 'orders': 0, 'results': {}, 'successfulOrders': 0}
    with pytest.raises(ValueError, match='stopped'):
        await runtime._dispatch_tool(agent, 'historical-cycle', 'place_order',
            {'action': 'buy', 'instrument_id': 'CRYPTO:BTC', 'quantity': 0.1, 'reason': 'Dated evidence.'}, budget, 'order')
    assert executor.orders == [] and store.count_trades() == 0
    class OpenMainVenue:
        def market_status(self, market):
            return {'id': market, 'isOpen': market == 'NASDAQ'}

    markets.main = OpenMainVenue()
    runtime._running = True
    with pytest.raises(ValueError, match='reopened'):
        await runtime._dispatch_tool(agent, 'historical-cycle', 'place_order',
            {'action': 'buy', 'instrument_id': 'CRYPTO:BTC', 'quantity': 0.1, 'reason': 'Dated evidence.'}, budget, 'reopened')
    assert executor.orders == []
    runtime._running = False
    result = await runtime._dispatch_tool(agent, 'historical-cycle', 'get_price_history',
                                         {'instrument_id': 'CRYPTO:BTC'}, budget, 'history')
    assert [p['price'] for p in result['points']] == [24000, 25000]

    async def future_quote(instrument_id):
        return {'instrumentId': instrument_id, 'price': 120000, 'currency': 'USD',
                'timestamp': '2026-10-04T12:00:00+00:00', 'source': 'Incorrect current-world source'}

    monkeypatch.setattr(markets, 'get_quote', future_quote)
    with pytest.raises(ValueError, match='replay clock'):
        await runtime._dispatch_tool(agent, 'historical-cycle', 'get_quote', {'instrument_id': 'CRYPTO:BTC'}, budget, 'future')


async def test_merge_review_is_one_tools_disabled_request_without_balance_mutation(replay):
    store, _, _, runtime = replay
    calls = []

    def handle(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={'choices': [{'message': {'role': 'assistant',
            'content': json.dumps({'merge': True, 'explanation': 'Observed delta and documented replay costs justify this virtual merge.'}),
            'reasoning_content': 'PRIVATE_SCRATCHPAD'}}]})

    await runtime._client.aclose()
    runtime._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    before = store.get_agent('astra-1')['cash']
    result = await runtime.decide_world_merge(store.get_agent('astra-1'), {'deltaCny': 100,
               'deadline': (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()})
    assert len(calls) == 1 and calls[0]['tool_choice'] == 'none' and calls[0]['reasoning_effort'] == 'max'
    assert result['merge'] is True and result['status'] == 'decided' and result['modelDecisionReceived']
    assert store.get_agent('astra-1')['cash'] == before and store.count_trades() == 0
    assert 'PRIVATE_SCRATCHPAD' not in json.dumps(store.events())


async def test_merge_review_failure_has_no_retry_or_default_approval(replay):
    store, _, _, runtime = replay
    count = 0

    def handle(request):
        nonlocal count
        count += 1
        return httpx.Response(503)

    await runtime._client.aclose()
    runtime._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    result = await runtime.decide_world_merge(store.get_agent('astra-1'), {'deltaCny': 100})
    assert count == 1 and result['merge'] is False and result['status'] == 'failed'
    assert result['modelDecisionReceived'] is False and result['explanation'] is None


async def test_late_or_missing_merge_review_never_approves(replay, monkeypatch):
    store, _, _, runtime = replay
    wall = datetime(2026, 10, 4, 12, tzinfo=timezone.utc)
    deadline = wall + timedelta(minutes=5)

    def now():
        return wall

    async def late(agent, messages, **kwargs):
        nonlocal wall
        wall += timedelta(minutes=6)
        return {'role': 'assistant', 'content': '{"merge":true,"explanation":"An actual but late public choice."}'}

    monkeypatch.setattr('arena.runtime._now', now)
    monkeypatch.setattr(runtime, '_model_call', late)
    result = await runtime.decide_world_merge(store.get_agent('astra-1'), {'deadline': deadline.isoformat()})
    assert result['status'] == 'expired' and result['merge'] is False and result['modelRequestedMerge'] is True
    assert result['explanation'] == 'An actual but late public choice.'
    calls = 0

    async def missing(agent, messages, **kwargs):
        nonlocal calls
        calls += 1
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime, '_model_call', missing)
    result = await runtime.decide_world_merge(store.get_agent('astra-1'), {'deadline': (wall + timedelta(seconds=.01)).isoformat()})
    assert calls == 1 and result['status'] == 'timeout' and not result['merge'] and not result['modelDecisionReceived']


async def test_cycle_completion_hook_is_external_and_has_no_investor_barrier(replay, monkeypatch):
    store, markets, _, runtime = replay
    seen = []

    async def search(query):
        return historical_research()

    async def model(agent, messages, tools_enabled=True):
        return {'role': 'assistant', 'content': 'Hold cash after dated evidence.'}

    async def hook(agent_id, report):
        seen.append((agent_id, report))
        markets.time += timedelta(days=1)
        runtime._running = False

    monkeypatch.setattr(runtime, 'web_search', search)
    monkeypatch.setattr(runtime, '_model_call', model)
    runtime.on_cycle_complete = hook
    runtime._running = True
    await runtime._investor_loop(store.get_agent('astra-1'))
    assert len(seen) == 1 and seen[0][0] == 'astra-1'
    assert seen[0][1]['asOf'] == '2023-06-02T12:00:00+00:00'
    assert markets.time.day == 3 and runtime.status()['agentStates']['sol-1']['completedCycles'] == 0


async def test_two_worlds_share_three_provider_slots(replay, tmp_path):
    store, _, _, historical = replay
    main_store = Store(tmp_path / 'main.db')
    main = Runtime(main_store, ReplayMarkets(), Executor(main_store), 'http://127.0.0.1:8317/v1', 'fixture-secret',
                   model_slots=historical._model_slots, search_slots=historical._search_slots)
    active = peak = 0
    third, release = asyncio.Event(), asyncio.Event()

    async def handle(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 3:
            third.set()
        try:
            await release.wait()
            return httpx.Response(200, json={'choices': [{'message': {'role': 'assistant', 'content': 'Observed public response.'}}]})
        finally:
            active -= 1

    await historical._client.aclose()
    await main._client.aclose()
    historical._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    main._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    tasks = [asyncio.create_task(r._model_call(s.get_agent('astra-1'), [{'role': 'user', 'content': 'Review.'}]))
             for r, s in [(historical, store), (main, main_store)] * 4]
    try:
        await asyncio.wait_for(third.wait(), 2)
        assert active == peak == 3
        release.set()
        await asyncio.gather(*tasks)
        assert peak == 3 and main._search_slots is historical._search_slots
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await main.close()
        main_store.close()
