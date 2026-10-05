# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Branch/cache/control regressions with isolated ledgers and mocked HTTP only."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import HTTPException
import httpx
import pytest

from arena.remote import RemoteRuntime, RemoteRuntimeError
from arena.store import AGENTS, Store


@pytest.fixture
def worlds_api(monkeypatch, tmp_path):
    import app as module
    main = Store(tmp_path / 'main.db')
    branch = Store(tmp_path / 'historical.db')
    branch.seed_wallets({agent[0]: 800000 for agent in AGENTS}, world_id='historical-test-1')
    branch.record_trade(agent_id='astra-1', action='buy', instrument_id='CRYPTO:BTC',
        quantity=1, price=100, currency='USD', fx=7, reason='Isolated fixture',
        quote_timestamp='2023-03-20T13:00:00Z', source='Fixture historical quote',
        acquired_date='2023-03-20', timestamp='2023-03-20T13:30:00Z')

    class Source:
        clock = datetime(2023, 3, 20, 13, 30, tzinfo=timezone.utc)
        catalog = {}

        def clock_now(self):
            return self.clock

        async def search(self, **kwargs):
            return {'items': [{'id': 'fixture-old-branch'}], 'total': 1}

        async def overview(self):
            return {'markets': [], 'coverage': {}, 'featured': [], 'replayTime': self.clock.isoformat()}

    source = Source()

    class Broker:
        async def portfolios(self):
            result = []
            for agent in branch.list_agents():
                positions = branch.positions(agent['id'])
                agent.update(positions=positions, equity=agent['cash'] + (1400 if positions else 0),
                             valuationComplete=True, asOf=source.clock.isoformat())
                result.append(agent)
            return result

    class Worker:
        def status(self):
            return {'running': True, 'inProgress': True, 'agentStates': {}}

    controller = SimpleNamespace(historical_store=branch, historical_markets=source,
        historical_executor=Broker(), historical_runtime=Worker(), branch_id='historical-test-1')
    controller.status = lambda: {'main': {'id': 'main', 'stockMarketsOpen': False},
        'historical': {'id': controller.branch_id, 'phase': 'active', 'replayTime': source.clock.isoformat()},
        'merge': None}

    async def provider(force=False):
        return {'connected': True, 'models': []}

    for name, value in {'store': main, 'world_controller': controller,
        'world_portfolio_caches': {}, 'world_stream_states': {}, 'portfolio_cache': {},
        'check_provider': provider}.items():
        monkeypatch.setattr(module, name, value)
    yield module, main, branch, source, controller
    main.close()
    branch.close()


async def test_instrument_search_rejects_retired_branch_result(worlds_api, monkeypatch):
    module, _, _, source, controller = worlds_api

    async def swapping_search(**kwargs):
        controller.branch_id = 'historical-test-2'
        return {'items': [{'id': 'must-not-label-as-new-branch'}], 'total': 1}

    monkeypatch.setattr(source, 'search', swapping_search)
    with pytest.raises(HTTPException) as error:
        await module.instruments('', '', 50, 0, 'historical')
    assert error.value.status_code == 409


async def test_state_rechecks_branch_after_final_provider_await(worlds_api, monkeypatch):
    module, _, _, _, controller = worlds_api

    async def swapping_provider(force=False):
        controller.branch_id = 'historical-test-2'
        return {'connected': True, 'models': []}

    monkeypatch.setattr(module, 'check_provider', swapping_provider)
    with pytest.raises(HTTPException) as error:
        await module.state('historical')
    assert error.value.status_code == 409


async def test_replay_clock_advance_invalidates_cached_holding_values(worlds_api):
    module, main, _, source, controller = worlds_api
    state = await module.state('historical')
    atlas = next(agent for agent in state['agents'] if agent['id'] == 'astra-1')
    assert atlas['equity'] == 800700
    assert atlas['valuationComplete'] is True
    cache = module.world_portfolio_caches[controller.branch_id]['astra-1']
    assert cache['replayTime'] == source.clock.isoformat()
    source.clock += timedelta(days=1)
    stale = module.live_update('historical')
    atlas = next(agent for agent in stale['agents'] if agent['id'] == 'astra-1')
    assert atlas['equity'] is None and atlas['valuationComplete'] is False
    assert stale['totals']['equity'] is None
    assert all(agent['cash'] == 800000 for agent in main.list_agents())
    assert main.positions() == []
    refreshed = await module.state('historical')
    atlas = next(agent for agent in refreshed['agents'] if agent['id'] == 'astra-1')
    assert atlas['equity'] == 800700 and atlas['valuationComplete'] is True
    assert module.world_portfolio_caches[controller.branch_id]['astra-1']['replayTime'] == source.clock.isoformat()


async def test_state_does_not_return_complete_old_marks_when_clock_moves_during_provider_await(worlds_api, monkeypatch):
    module, _, _, source, _ = worlds_api

    async def advance_provider(force=False):
        source.clock += timedelta(days=1)
        return {'connected': True, 'models': []}

    monkeypatch.setattr(module, 'check_provider', advance_provider)
    payload = await module.state('historical')
    atlas = next(agent for agent in payload['agents'] if agent['id'] == 'astra-1')
    assert atlas['equity'] is None and atlas['valuationComplete'] is False
    assert payload['totals']['equity'] is None


@pytest.mark.parametrize('status_code', [101, 301, 302, 307, 400, 503])
async def test_remote_controls_require_a_successful_http_response(status_code):
    calls = []

    def response(request):
        calls.append((request.method, str(request.url)))
        return httpx.Response(status_code, json={'detail': 'fixture upstream rejection'},
                              headers={'location': 'http://127.0.0.1:9999/redirect'})

    remote = RemoteRuntime('http://127.0.0.1:8787')
    await remote._client.aclose()
    remote._client = httpx.AsyncClient(transport=httpx.MockTransport(response), follow_redirects=False)
    try:
        with pytest.raises(RemoteRuntimeError) as error:
            await remote.pause()
        assert error.value.status_code == (status_code if status_code >= 400 else 502)
        assert calls == [('POST', 'http://127.0.0.1:8787/api/simulation/pause')]
    finally:
        await remote.close()
