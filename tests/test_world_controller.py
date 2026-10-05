# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Controller lifecycle/permission tests with isolated ledgers and no network."""
import asyncio
import copy
from datetime import datetime, timedelta, timezone

import pytest

from arena.store import Store
from arena.worlds import CHECKPOINT_KEY, MERGE_WINDOW, REPLAY_INTERVAL, STOCK_VENUES, WorldController


UTC = timezone.utc
REAL_NOW = datetime(2026, 10, 4, 10, tzinfo=UTC)
REPLAY_NOW = datetime(2023, 2, 1, 1, 30, tzinfo=UTC)


class Clock:
    def __init__(self):
        self.value = REAL_NOW

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


class MainMarkets:
    def __init__(self):
        self.catalog = {}
        self.open_markets = set()
        self.unknown = set()
        self.calendar_errors = set()

    def market_status(self, market):
        if market in self.unknown:
            raise ValueError('Fixture calendar unavailable')
        result = {'id': market, 'isOpen': market in self.open_markets, 'reason': 'Fixture regular hours'}
        if market in self.calendar_errors:
            result.update(isOpen=False, calendarError='Unavailable exchange calendar')
        return result


class MainRuntime:
    def __init__(self):
        self.running = True
        self.api_connected = True
        self.remote_connected = True

    def status(self):
        return {'running': self.running, 'apiConnected': self.api_connected, 'remoteConnected': self.remote_connected}

    async def start(self):
        raise AssertionError('Historical controller must not start the main investors')

    async def pause(self):
        raise AssertionError('Historical controller must not interrupt the main investors')


class HistoricalSource:
    def __init__(self, main, data_dir=None, replay_time=None):
        self.main = main
        self.catalog = {}
        self.active = False
        self.clock = datetime.fromisoformat(replay_time.replace('Z', '+00:00')) if replay_time else REPLAY_NOW
        self.advanced = 0
        self.closed = False
        self.exhausted = False

    async def initialize(self):
        pass

    def clock_now(self):
        return self.clock

    def advance_session(self):
        if self.exhausted:
            raise ValueError('Replay ended')
        self.advanced += 1
        self.clock += timedelta(days=1)
        return self.clock

    def metadata(self):
        return {'world': 'historical', 'replay': True, 'active': self.active,
                'replayTime': self.clock.isoformat(), 'clockAdvanceIntervalSeconds': 300}

    async def close(self):
        self.closed = True


class HistoricalRuntime:
    def __init__(self, store, source, executor, base_url, api_key, *, world_context=None, on_cycle_complete=None):
        self.store, self.source = store, source
        self.world_context = world_context
        self.on_cycle_complete = on_cycle_complete
        self.running = False
        self.started = 0
        self.paused = 0
        self.closed = 0
        self.calls = []
        self.choices = {}
        self.choice_hook = None
        self.block_agent = None
        self.review_started = asyncio.Event()
        self.review_release = asyncio.Event()
        self.pause_started = asyncio.Event()
        self.pause_release = asyncio.Event()
        self.block_pause = False

    def status(self):
        return {'running': self.running, 'apiConnected': True}

    async def start(self):
        self.running = True
        self.started += 1

    async def pause(self):
        self.paused += 1
        self.pause_started.set()
        if self.block_pause:
            await self.pause_release.wait()
        self.running = False

    async def close(self):
        self.closed += 1
        self.running = False

    async def decide_world_merge(self, agent, summary):
        self.calls.append((agent['id'], copy.deepcopy(summary)))
        if agent['id'] == self.block_agent:
            self.review_started.set()
            await self.review_release.wait()
        if self.choice_hook:
            self.choice_hook(agent['id'])
        return copy.deepcopy(self.choices.get(agent['id'], {
            'status': 'decided', 'modelDecisionReceived': True, 'merge': False,
            'explanation': 'Keep this historical simulation isolated.'}))


class HistoricalExecutor:
    def __init__(self, store, source, deltas):
        self.store, self.source, self.deltas = store, source, deltas
        self.invalidated = 0
        self.incomplete = set()

    def invalidate_marks(self):
        self.invalidated += 1

    async def portfolios(self):
        rows = self.store.list_agents()
        for row in rows:
            baseline = row['initialCash']
            row.update(equity=baseline + self.deltas.get(row['id'], 0),
                       positions=self.store.positions(row['id']), valuationComplete=row['id'] not in self.incomplete,
                       valuationWarnings=[])
        return rows


class Rig:
    def __init__(self, tmp_path):
        self.store = Store(tmp_path / 'main.db')
        self.markets, self.runtime, self.clock = MainMarkets(), MainRuntime(), Clock()
        self.data_dir = tmp_path / 'data'
        self.deltas = {}
        self.controllers = []

    def controller(self):
        def execution(store, source):
            return HistoricalExecutor(store, source, self.deltas)
        value = WorldController(self.store, self.markets, self.runtime, 'http://unused.invalid/v1', 'unused',
            self.data_dir, now=self.clock, historical_factory=HistoricalSource,
            runtime_factory=HistoricalRuntime, executor_factory=execution)
        self.controllers.append(value)
        return value

    def main_buy(self, agent_id='astra-1', quantity=10, price=100):
        return self.store.record_trade(agent_id=agent_id, action='buy', instrument_id='SSE:600000',
            quantity=quantity, price=price, currency='CNY', fx=1, reason='Isolated fixture position',
            quote_timestamp=self.clock().isoformat(), source='Fixture', acquired_date=self.clock().date().isoformat())


async def reviews_finished(controller):
    tasks = list(controller._reviews.values())
    if tasks:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=3)


def test_closed_live_stocks_create_isolated_cash_copy_without_moving_main_holdings(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        rig.main_buy()
        before_agents, before_positions = rig.store.list_agents(), rig.store.positions()
        c = rig.controller()
        await c.tick()
        assert c.checkpoint['phase'] == 'active' and c.historical_markets.active
        assert c.historical_store.path != rig.store.path
        assert c.historical_store.get_agent('astra-1')['cash'] == 799000
        assert c.historical_store.get_agent('astra-1')['initialCash'] == 799000
        assert c.historical_store.positions() == []
        assert rig.store.list_agents() == before_agents and rig.store.positions() == before_positions
        assert c.historical_runtime.started == 1
        assert c.status()['main']['allStockMarketsClosed']
        await c.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('venue', STOCK_VENUES)
def test_any_open_stock_exchange_prevents_activation(tmp_path, venue):
    async def scenario():
        rig = Rig(tmp_path)
        rig.markets.open_markets.add(venue)
        c = rig.controller()
        await c.tick()
        assert c.historical_store is None and c.status()['main']['stockMarketsOpen']
        await c.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('unknown_kind', ['raised', 'calendar_error'])
def test_unknown_calendar_does_not_count_as_confirmed_closed(tmp_path, unknown_kind):
    async def scenario():
        rig = Rig(tmp_path)
        getattr(rig.markets, 'unknown' if unknown_kind == 'raised' else 'calendar_errors').add('NASDAQ')
        c = rig.controller()
        await c.tick()
        assert c.historical_store is None
        assert c.status()['main']['phase'] == 'calendar_unavailable'
        await c.close()
    asyncio.run(scenario())


def test_disconnected_main_runtime_service_does_not_start_historical_workers(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        rig.runtime.remote_connected = False
        c = rig.controller()
        await c.tick()
        assert c.historical_store is None
        await c.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('venue', STOCK_VENUES)
def test_any_live_reopen_closes_source_immediately_and_opens_exact_five_minute_window(tmp_path, venue):
    async def scenario():
        rig = Rig(tmp_path)
        c = rig.controller()
        await c.tick()
        worker = c.historical_runtime
        worker.block_pause = True
        rig.markets.open_markets.add(venue)
        reopen = asyncio.create_task(c.tick())
        await asyncio.wait_for(worker.pause_started.wait(), 3)
        assert c.historical_markets.active is False
        assert c.checkpoint['phase'] == 'merge_window'
        merge = c.checkpoint['merge']
        assert datetime.fromisoformat(merge['deadlineAt']) - datetime.fromisoformat(merge['openedAt']) == timedelta(seconds=MERGE_WINDOW)
        worker.pause_release.set()
        await reopen
        await reviews_finished(c)
        assert len(c.checkpoint['merge']['decisions']) == 10
        assert worker.paused == 1 and not worker.running
        assert c.status()['merge']['remainingSeconds'] == MERGE_WINDOW
        await c.close()
    asyncio.run(scenario())


def test_explicit_ai_merge_transfers_only_pnl_and_never_historical_positions(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        rig.main_buy()
        c = rig.controller()
        await c.tick()
        before_positions = rig.store.positions()
        rig.deltas['astra-1'] = 123.456789
        c.historical_store.record_trade(agent_id='astra-1', action='buy', instrument_id='NASDAQ:AAPL', quantity=1,
            price=100, currency='USD', fx=7, reason='Historical fixture position',
            quote_timestamp=REPLAY_NOW.isoformat(), source='Dated fixture', acquired_date='2023-02-01')
        c.historical_runtime.choices['astra-1'] = {'status': 'decided', 'modelDecisionReceived': True,
                                                'merge': True, 'explanation': 'Transfer this virtual net gain.'}
        rig.markets.open_markets.add('NYSE')
        await c.tick()
        await reviews_finished(c)
        decision = next(row for row in c.checkpoint['merge']['decisions'] if row['agentId'] == 'astra-1')
        assert decision['status'] == 'merged' and decision['deltaVC'] == 123.456789
        assert rig.store.get_agent('astra-1')['cash'] == 799123.456789
        assert rig.store.positions() == before_positions
        assert len(rig.store.world_transfers()) == 1
        assert all(row['status'] == 'declined' for row in c.checkpoint['merge']['decisions'] if row['agentId'] != 'astra-1')
        await c.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('choice', [
    {'status': 'decided', 'modelDecisionReceived': True, 'merge': 'true'},
    {'status': 'decided', 'modelDecisionReceived': True, 'merge': 1},
    {'status': 'decided', 'modelDecisionReceived': 'true', 'merge': True},
    {'status': 'failed', 'modelDecisionReceived': True, 'merge': True},
    {'status': 'decided', 'merge': True},
    {'status': 'timeout', 'modelDecisionReceived': False, 'merge': True},
    None,
])
def test_malformed_or_missing_permission_never_applies_delta(tmp_path, choice):
    async def scenario():
        rig = Rig(tmp_path)
        rig.deltas['astra-1'] = 100
        c = rig.controller()
        await c.tick()
        c.historical_runtime.choices['astra-1'] = choice
        rig.markets.open_markets.add('NASDAQ')
        await c.tick()
        await reviews_finished(c)
        assert rig.store.get_agent('astra-1')['cash'] == 800000
        assert rig.store.world_transfers() == []
        decision = next(row for row in c.checkpoint['merge']['decisions'] if row['agentId'] == 'astra-1')
        assert decision['status'] in {'failed', 'expired'}
        await c.close()
    asyncio.run(scenario())


def test_response_at_deadline_cannot_merge_even_if_model_claims_approval(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        rig.deltas['astra-1'] = 100
        c = rig.controller()
        await c.tick()
        worker = c.historical_runtime
        worker.choices['astra-1'] = {'status': 'decided', 'modelDecisionReceived': True, 'merge': True}
        worker.choice_hook = lambda agent_id: rig.clock.advance(MERGE_WINDOW) if agent_id == 'astra-1' else None
        rig.markets.open_markets.add('NYSE')
        await c.tick()
        await reviews_finished(c)
        assert rig.store.get_agent('astra-1')['cash'] == 800000 and rig.store.world_transfers() == []
        assert next(row for row in c.checkpoint['merge']['decisions'] if row['agentId'] == 'astra-1')['status'] == 'expired'
        await c.tick()
        assert c.checkpoint['phase'] == 'closed' and c.status()['merge']['remainingSeconds'] == 0
        await c.close()
    asyncio.run(scenario())


def test_insufficient_main_cash_records_failed_choice_without_overdrawing(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        rig.deltas['astra-1'] = -20000
        c = rig.controller()
        await c.tick()
        rig.main_buy(quantity=7900, price=100)
        c.historical_runtime.choices['astra-1'] = {'status': 'decided', 'modelDecisionReceived': True,
                                                'merge': True, 'explanation': 'Apply the historical loss.'}
        rig.markets.open_markets.add('NYSE')
        await c.tick()
        await reviews_finished(c)
        assert rig.store.get_agent('astra-1')['cash'] == 10000
        receipt = rig.store.world_transfers()[0]
        assert receipt['status'] == 'failed' and receipt['deltaVC'] == -20000
        assert next(row for row in c.checkpoint['merge']['decisions'] if row['agentId'] == 'astra-1')['status'] == 'failed'
        await c.close()
    asyncio.run(scenario())


def test_incomplete_historical_valuation_does_not_request_merge_approval(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        c = rig.controller()
        await c.tick()
        c.historical_executor.incomplete.add('astra-1')
        rig.markets.open_markets.add('NYSE')
        await c.tick()
        await reviews_finished(c)
        assert 'astra-1' not in [agent_id for agent_id, _ in c.historical_runtime.calls]
        assert next(row for row in c.checkpoint['merge']['decisions'] if row['agentId'] == 'astra-1')['status'] == 'failed'
        await c.close()
    asyncio.run(scenario())


def test_replay_clock_advances_every_300_seconds_without_restarting_personalized_cycles(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        c = rig.controller()
        await c.tick()
        original = c.historical_markets.clock_now()
        await c._cycle_completed('astra-1', {'status': 'completed'})
        await c._cycle_completed('sol-1', {'status': 'completed'})
        assert c.historical_markets.advanced == 0
        rig.clock.advance(REPLAY_INTERVAL - 1)
        await c.tick()
        assert c.historical_markets.clock_now() == original
        rig.clock.advance(1)
        await c.tick()
        assert c.historical_markets.advanced == 1
        assert c.historical_runtime.started == 1 and c.historical_runtime.paused == 0
        assert c.historical_executor.invalidated == 1
        assert c.checkpoint['nextAdvanceAt'] == (rig.clock() + timedelta(seconds=300)).isoformat()
        assert c.checkpoint['replayTime'] == c.historical_markets.clock_now().isoformat()
        await c.close()
    asyncio.run(scenario())


def test_replay_exhaustion_closes_execution_without_wrapping_backward(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        c = rig.controller()
        await c.tick()
        original = c.historical_markets.clock_now()
        c.historical_markets.exhausted = True
        rig.clock.advance(REPLAY_INTERVAL)
        await c.tick()
        assert c.checkpoint['phase'] == 'replay_exhausted' and not c.historical_markets.active
        assert c.historical_markets.clock_now() == original and not c.historical_runtime.running
        await c.tick()
        assert c.checkpoint['phase'] == 'replay_exhausted'
        await c.close()
    asyncio.run(scenario())


def test_pause_restart_preserves_same_branch_balances_positions_and_original_funding(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        rig.main_buy()
        c = rig.controller()
        await c.tick()
        branch_id = c.checkpoint['id']
        c.historical_store.record_trade(agent_id='astra-1', action='buy', instrument_id='SSE:600000', quantity=100,
            price=10, currency='CNY', fx=1, reason='Historical fixture', quote_timestamp=REPLAY_NOW.isoformat(),
            source='Fixture', acquired_date='2023-02-01')
        branch_cash = c.historical_store.get_agent('astra-1')['cash']
        branch_positions = c.historical_store.positions()
        rig.main_buy(quantity=20)
        rig.runtime.running = False
        await c.tick()
        assert c.checkpoint['phase'] == 'paused' and not c.historical_markets.active
        await c.close()
        rig.runtime.running = True
        restored = rig.controller()
        await restored.start()
        assert restored.checkpoint['id'] == branch_id and restored.checkpoint['phase'] == 'active'
        assert restored.historical_store.get_agent('astra-1')['cash'] == branch_cash
        assert restored.historical_store.get_agent('astra-1')['initialCash'] == 799000
        assert restored.historical_store.positions() == branch_positions
        assert len([event for event in restored.historical_store.events() if event['kind'] == 'world_funded']) == 1
        await restored.close()
    asyncio.run(scenario())


def test_existing_main_receipt_recovers_without_double_credit_or_new_model_call(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        rig.deltas['astra-1'] = 100
        c = rig.controller()
        await c.tick()
        c.historical_runtime.choices['astra-1'] = {'status': 'decided', 'modelDecisionReceived': True, 'merge': True}
        rig.markets.open_markets.add('NYSE')
        await c.tick()
        await reviews_finished(c)
        decision = next(row for row in c.checkpoint['merge']['decisions'] if row['agentId'] == 'astra-1')
        decision['status'] = 'approved'
        decision.pop('receipt')
        c._persist()
        await c.close()
        restored = rig.controller()
        await restored.start()
        assert rig.store.get_agent('astra-1')['cash'] == 800100
        assert len(rig.store.world_transfers()) == 1
        assert next(row for row in restored.checkpoint['merge']['decisions'] if row['agentId'] == 'astra-1')['status'] == 'merged'
        assert restored.historical_runtime.calls == []
        await restored.close()
    asyncio.run(scenario())


def test_interrupted_started_review_is_not_repeated_or_treated_as_approval(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        rig.deltas['astra-1'] = 100
        c = rig.controller()
        await c.tick()
        worker = c.historical_runtime
        worker.block_agent = 'astra-1'
        rig.markets.open_markets.add('NYSE')
        await c.tick()
        await asyncio.wait_for(worker.review_started.wait(), 3)
        other_tasks = [task for agent_id, task in c._reviews.items() if agent_id != 'astra-1']
        await asyncio.gather(*other_tasks)
        assert next(row for row in c.checkpoint['merge']['decisions'] if row['agentId'] == 'astra-1').get('requestStartedAt')
        await c.close()
        restored = rig.controller()
        await restored.start()
        await reviews_finished(restored)
        interrupted = next(row for row in restored.checkpoint['merge']['decisions'] if row['agentId'] == 'astra-1')
        assert interrupted['status'] == 'expired'
        assert restored.historical_runtime.calls == []
        assert rig.store.get_agent('astra-1')['cash'] == 800000 and rig.store.world_transfers() == []
        await restored.close()
    asyncio.run(scenario())


def test_restart_during_interrupted_freeze_recovers_ten_choices_with_original_deadline(tmp_path):
    async def scenario():
        rig = Rig(tmp_path)
        c = rig.controller()
        await c.tick()
        worker = c.historical_runtime
        worker.block_pause = True
        rig.markets.open_markets.add('NYSE')
        reopening = asyncio.create_task(c.tick())
        await asyncio.wait_for(worker.pause_started.wait(), 3)
        original_deadline = c.checkpoint['merge']['deadlineAt']
        assert c.checkpoint['merge']['decisions'] == []
        reopening.cancel()
        await asyncio.gather(reopening, return_exceptions=True)
        await c.close()
        rig.clock.advance(30)
        restored = rig.controller()
        await restored.start()
        await reviews_finished(restored)
        assert len(restored.checkpoint['merge']['decisions']) == 10
        assert restored.checkpoint['merge']['deadlineAt'] == original_deadline
        assert not restored.historical_markets.active
        assert rig.store.world_transfers() == []
        await restored.close()
    asyncio.run(scenario())
