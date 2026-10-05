# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Persistent, isolated 2023 paper world and deadline-guarded net-PnL review.

The controller never starts, pauses, or cancels the main world's investors.
Its wall clock governs the five-minute choice window; its source clock governs
historical prices. No historical positions are transferred to the main ledger.
"""
from __future__ import annotations

import asyncio
import contextlib
import copy
from datetime import datetime, timedelta, timezone
import math
from pathlib import Path
import re
import uuid

from .execution import TradeExecutor
from .historical import HistoricalMarketService
from .runtime import Runtime
from .store import Store, micros, money

STOCK_VENUES = ('SSE', 'SZSE', 'BSE', 'NASDAQ', 'NYSE')
CHECKPOINT_KEY = 'historicalWorldController'
REPLAY_INTERVAL = 300
MERGE_WINDOW = 300


def utcnow():
    return datetime.now(timezone.utc)


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('World timestamp must specify its timezone')
    return result.astimezone(timezone.utc)


class WorldController:
    def __init__(self, main_store, main_markets, main_runtime, base_url, api_key,
                 data_dir, *, now=utcnow, historical_factory=HistoricalMarketService,
                 runtime_factory=Runtime, executor_factory=TradeExecutor):
        self.main_store, self.main_markets, self.main_runtime = main_store, main_markets, main_runtime
        self.base_url, self.api_key = base_url, api_key
        self.data_dir = Path(data_dir) / 'worlds'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.now = now
        self.historical_factory, self.runtime_factory = historical_factory, runtime_factory
        self.executor_factory = executor_factory
        self.historical_store = self.historical_markets = None
        self.historical_executor = self.historical_runtime = None
        saved = main_store.get_setting(CHECKPOINT_KEY, {})
        self.checkpoint = copy.deepcopy(saved) if isinstance(saved, dict) else {}
        self._task = None
        self._reviews = {}
        self._lock = asyncio.Lock()
        self._closed = False
        self._error = None

    def stock_statuses(self):
        rows = []
        for venue in STOCK_VENUES:
            try:
                status = self.main_markets.market_status(venue)
                if not isinstance(status.get('isOpen'), bool) or status.get('calendarError'):
                    raise ValueError('Calendar did not return a known trading state')
                rows.append({**status, 'id': venue})
            except Exception:
                rows.append({'id': venue, 'isOpen': None, 'reason': 'Stock calendar unavailable'})
        return rows

    def _persist(self):
        if self.historical_markets:
            self.checkpoint['replayTime'] = self.historical_markets.clock_now().isoformat()
        self.main_store.set_setting(CHECKPOINT_KEY, self.checkpoint)

    def status(self):
        statuses = self.stock_statuses()
        any_open = any(row['isOpen'] is True for row in statuses)
        all_closed = all(row['isOpen'] is False for row in statuses)
        historical = {key: copy.deepcopy(value) for key, value in self.checkpoint.items()
                      if key not in {'merge', 'baselineWallets', 'finalPortfolios'}}
        historical.setdefault('id', None)
        historical.setdefault('phase', 'preparing' if all_closed and self.main_runtime.status().get('running') else 'inactive')
        historical.setdefault('name', 'Historical 2023')
        if self.historical_markets:
            historical.update(self.historical_markets.metadata())
        if self._error:
            historical['error'] = self._error
        merge = copy.deepcopy(self.checkpoint.get('merge') or {'openedAt': None, 'deadlineAt': None, 'decisions': []})
        deadline = merge.get('deadlineAt')
        merge['remainingSeconds'] = max(0, int((timestamp(deadline) - self.now()).total_seconds())) if deadline else 0
        return {'main': {'id': 'main', 'name': 'Main live world', 'phase': 'stock_trading_open' if any_open else
                         ('stock_trading_closed' if all_closed else 'calendar_unavailable'),
                         'stockMarketsOpen': any_open, 'allStockMarketsClosed': all_closed, 'markets': statuses},
                'historical': historical, 'merge': merge}

    async def start(self):
        if self._task and not self._task.done():
            return
        branch_id = self.checkpoint.get('id')
        if branch_id:
            if not re.fullmatch(r'historical-2023-[a-f0-9]{32}', branch_id):
                raise ValueError('Invalid saved historical branch identifier')
            await self._load_branch(branch_id, restoring=True)
            if self.checkpoint.get('phase') == 'merge_window' and not self.checkpoint.get('merge', {}).get('decisions'):
                await self._freeze_final_portfolios()
            self._recover_receipts()
        await self.tick()
        self._task = asyncio.create_task(self._loop(), name='historical-world-controller')

    async def _load_branch(self, branch_id, restoring=False):
        path = self.data_dir / branch_id
        ledger_path = path / 'arena.db'
        if restoring and not ledger_path.exists():
            raise ValueError('Saved historical ledger is missing; funding cannot be recreated')
        ledger = Store(ledger_path)
        if restoring and ledger.get_setting('branchWorldId') != branch_id:
            ledger.close()
            raise ValueError('Saved historical ledger identity does not match its checkpoint')
        source = self.historical_factory(self.main_markets, data_dir=path / 'prices',
                                         replay_time=self.checkpoint.get('replayTime'))
        source.active = self.checkpoint.get('phase') == 'active'
        execution = self.executor_factory(ledger, source)
        investor = self.runtime_factory(ledger, source, execution, self.base_url, self.api_key,
                      world_context={'worldId': branch_id, 'name': 'Historical 2023', 'isHistorical': True,
                                     'sessionId': branch_id}, on_cycle_complete=self._cycle_completed)
        self.historical_store, self.historical_markets = ledger, source
        self.historical_executor, self.historical_runtime = execution, investor
        await source.initialize()
        if restoring:
            baselines = ledger.get_setting('branchInitialCapital')
            if not isinstance(baselines, dict) or baselines != self.checkpoint.get('baselineWallets'):
                raise ValueError('Saved historical funding does not match its original baseline')

    async def _create_branch(self):
        await self._dispose_branch()
        wallets = {agent['id']: agent['cash'] for agent in self.main_store.list_agents()}
        self.checkpoint = {'id': 'historical-2023-' + uuid.uuid4().hex, 'name': 'Historical 2023',
                           'phase': 'preparing', 'startedAt': self.now().isoformat(),
                           'baselineWallets': wallets, 'baselineCapital': money(sum(micros(v) for v in wallets.values())),
                           'fundingPolicy': 'Copy available main cash into isolated virtual wallets; leave main balances and holdings unchanged',
                           'mergePolicy': 'Agent-approved net historical profit/loss only; main holdings unchanged',
                           'nextAdvanceAt': (self.now() + timedelta(seconds=REPLAY_INTERVAL)).isoformat(),
                           'merge': {'openedAt': None, 'deadlineAt': None, 'decisions': []}}
        await self._load_branch(self.checkpoint['id'])
        self.historical_store.seed_wallets(wallets, world_id=self.checkpoint['id'])
        self.checkpoint['phase'] = 'active'
        self.historical_markets.active = True
        self._persist()
        self.main_store.add_event('historical_world_opened', None, 'Isolated 2023 world activated while all live stock exchanges are closed',
                                  {'worldId': self.checkpoint['id'], 'replayTime': self.checkpoint['replayTime'],
                                   'baselineCapital': self.checkpoint['baselineCapital']})
        await self.historical_runtime.start()

    async def _cycle_completed(self, agent_id, report):
        # Decision cycles remain independent of both market-clock advancement
        # and other investors; this hook records the current source clock only.
        if self.checkpoint.get('phase') == 'active':
            self._persist()

    def _recover_receipts(self):
        merge = self.checkpoint.get('merge') or {}
        receipts = {row['agentId']: row for row in self.main_store.world_transfers(world_id=self.checkpoint['id'])}
        for decision in merge.get('decisions', []):
            receipt = receipts.get(decision['agentId'])
            if receipt:
                decision.update(status='merged' if receipt['status'] == 'applied' else 'failed', receipt=receipt)
            elif decision.get('status') == 'approved':
                # A crash between the persisted choice and receipt must never
                # create a new approval or apply after the original deadline.
                if merge.get('deadlineAt') and self.now() < timestamp(merge['deadlineAt']):
                    self._apply_choice(decision)
                else:
                    decision.update(status='expired', error='Approval was not committed before the original deadline')
            elif decision.get('status') == 'pending' and decision.get('requestStartedAt'):
                decision.update(status='expired', error='Interrupted review was not repeated; no recorded approval')
        self._persist()

    async def tick(self):
        async with self._lock:
            if self._closed:
                return
            statuses = self.stock_statuses()
            any_open = any(row['isOpen'] is True for row in statuses)
            all_closed = all(row['isOpen'] is False for row in statuses)
            phase = self.checkpoint.get('phase')
            if phase == 'merge_window':
                if self.now() >= timestamp(self.checkpoint['merge']['deadlineAt']):
                    await self._finish_window()
                else:
                    self._launch_reviews()
                return
            if any_open and phase in {'active', 'paused', 'replay_exhausted'}:
                await self._open_merge_window()
                return
            main_status = self.main_runtime.status()
            running = bool(main_status.get('running')) and main_status.get('remoteConnected', True) is not False
            if phase in {'active', 'paused'}:
                if not all_closed or not running:
                    self.historical_markets.active = False
                    if phase == 'active':
                        self.checkpoint['phase'] = 'paused'
                        self._persist()
                        await self.historical_runtime.pause()
                    return
                if phase == 'paused':
                    self.checkpoint['phase'] = 'active'
                    self.checkpoint['nextAdvanceAt'] = (self.now() + timedelta(seconds=REPLAY_INTERVAL)).isoformat()
                    self.historical_markets.active = True
                    self._persist()
                    await self.historical_runtime.start()
                elif not self.historical_runtime.status().get('running'):
                    # Startup recovery resumes the same funded branch.
                    self.historical_markets.active = True
                    await self.historical_runtime.start()
                if self.now() >= timestamp(self.checkpoint['nextAdvanceAt']):
                    try:
                        self.historical_markets.advance_session()
                    except ValueError:
                        self.historical_markets.active = False
                        self.checkpoint['phase'] = 'replay_exhausted'
                        self._persist()
                        await self.historical_runtime.pause()
                        return
                    self.historical_executor.invalidate_marks()
                    self.checkpoint['nextAdvanceAt'] = (self.now() + timedelta(seconds=REPLAY_INTERVAL)).isoformat()
                    self.historical_store.add_event('replay_clock_advanced', None, 'Historical price clock advanced to the next dated stock-session open',
                           {'worldId': self.checkpoint['id'], 'asOf': self.historical_markets.clock_now().isoformat()})
                    self._persist()
                return
            if all_closed and running and phase not in {'replay_exhausted', 'preparing', 'unavailable'}:
                await self._create_branch()

    async def _open_merge_window(self):
        # Close execution BEFORE any await, even when an order is awaiting I/O.
        self.historical_markets.active = False
        opened = self.now()
        self.checkpoint['phase'] = 'merge_window'
        self.checkpoint['endedAt'] = opened.isoformat()
        self.checkpoint['merge'] = {'openedAt': opened.isoformat(),
                                   'deadlineAt': (opened + timedelta(seconds=MERGE_WINDOW)).isoformat(), 'decisions': []}
        self._persist()
        await self.historical_runtime.pause()
        await self._freeze_final_portfolios()
        self.main_store.add_event('historical_merge_window', None, 'Live stocks reopened; historical trading stopped and a five-minute AI merge-choice window opened',
                   {'worldId': self.checkpoint['id'], 'replayTime': self.checkpoint['replayTime'],
                    'deadlineAt': self.checkpoint['merge']['deadlineAt']})
        self._launch_reviews()

    async def _freeze_final_portfolios(self):
        """Resume interrupted preparation at the frozen clock and same deadline."""
        self.historical_markets.active = False
        # Freeze genuine marks at the last source clock, with no liquidation.
        self.historical_executor.invalidate_marks()
        try:
            final = await asyncio.wait_for(self.historical_executor.portfolios(), timeout=10)
        except Exception:
            final = [{**agent, 'equity': None, 'valuationComplete': False} for agent in self.historical_store.list_agents()]
        self.checkpoint['finalPortfolios'] = final
        self.checkpoint['merge']['decisions'] = [self._new_decision(agent) for agent in final]
        self._persist()

    def _new_decision(self, agent):
        baseline = self.checkpoint['baselineWallets'][agent['id']]
        equity = agent.get('equity')
        valid = isinstance(equity, (int, float)) and math.isfinite(equity) and agent.get('valuationComplete', True)
        return {'agentId': agent['id'], 'status': 'pending' if valid else 'failed',
                'decisionId': 'merge-' + uuid.uuid4().hex, 'baselineEquity': baseline,
                'finalEquity': equity if valid else None,
                'deltaVC': money(micros(equity) - micros(baseline)) if valid else None,
                'explanation': None, **({} if valid else {'error': 'Genuine complete ending valuation unavailable; no merge approval requested'})}

    def _launch_reviews(self):
        for decision in self.checkpoint['merge']['decisions']:
            agent_id = decision['agentId']
            if decision['status'] == 'pending' and not decision.get('requestStartedAt') and agent_id not in self._reviews:
                self._reviews[agent_id] = asyncio.create_task(self._review(decision), name='historical-merge-' + agent_id)

    async def _review(self, decision):
        if self._closed or self.now() >= timestamp(self.checkpoint['merge']['deadlineAt']):
            return
        decision['requestStartedAt'] = self.now().isoformat()
        self._persist()
        agent = self.historical_store.get_agent(decision['agentId'])
        portfolio = next(row for row in self.checkpoint.get('finalPortfolios', []) if row['id'] == agent['id'])
        summary = {'worldId': self.checkpoint['id'], 'replayTime': self.checkpoint['replayTime'],
                   'deadlineAt': self.checkpoint['merge']['deadlineAt'],
                   'mergePolicy': self.checkpoint['mergePolicy'], 'agent': agent,
                   'baselineEquity': decision['baselineEquity'], 'finalEquity': decision['finalEquity'],
                   'deltaVC': decision['deltaVC'], 'endingPositions': portfolio.get('positions', [])[:50],
                   'valuationWarnings': portfolio.get('valuationWarnings', []),
                   'mainCurrentCash': self.main_store.get_agent(agent['id'])['cash'],
                   'historicalData': self.historical_markets.metadata()}
        try:
            choice = await self.historical_runtime.decide_world_merge(agent, summary)
            if self._closed:
                return
            decision['explanation'] = choice.get('explanation')
            decision['decidedAt'] = choice.get('decidedAt', self.now().isoformat())
            if self.now() >= timestamp(self.checkpoint['merge']['deadlineAt']) or self.checkpoint['phase'] != 'merge_window':
                decision.update(status='expired', error='AI response arrived after the merge deadline')
            elif choice.get('modelDecisionReceived') is True and choice.get('status') == 'decided' and choice.get('merge') is True:
                decision['status'] = 'approved'
                self._persist()
                self._apply_choice(decision)
            elif choice.get('modelDecisionReceived') is True and choice.get('status') == 'decided' and choice.get('merge') is False:
                decision['status'] = 'declined'
            else:
                decision.update(status='expired' if choice.get('status') in {'expired', 'timeout'} else 'failed',
                                error=choice.get('error') or 'No timely valid AI approval received')
            self._persist()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            decision.update(status='failed', error=type(exc).__name__ + ': merge review could not complete')
            self._persist()

    def _apply_choice(self, decision):
        if self.now() >= timestamp(self.checkpoint['merge']['deadlineAt']):
            decision['status'] = 'expired'
            return
        receipt = self.main_store.apply_world_delta(decision['agentId'], decision['deltaVC'], self.checkpoint['id'],
                    decision['decisionId'], baseline_equity=decision['baselineEquity'], final_equity=decision['finalEquity'],
                    deadline_at=self.checkpoint['merge']['deadlineAt'], now=self.now,
                    details={'explanation': decision['explanation'], 'decidedAt': decision.get('decidedAt'),
                             'deadlineAt': self.checkpoint['merge']['deadlineAt'], 'replayTime': self.checkpoint['replayTime']})
        decision.update(status='merged' if receipt['status'] == 'applied' else 'failed', receipt=receipt)
        if receipt.get('error'):
            decision['error'] = receipt['error']

    async def _finish_window(self):
        for task in self._reviews.values():
            if not task.done():
                task.cancel()
        if self._reviews:
            await asyncio.gather(*self._reviews.values(), return_exceptions=True)
        for decision in self.checkpoint['merge']['decisions']:
            if decision['status'] in {'pending', 'approved'}:
                decision.update(status='expired', error='Five-minute window elapsed without a committed approval')
        self.checkpoint['phase'] = 'closed'
        self.checkpoint['closedAt'] = self.now().isoformat()
        self._persist()
        # Keep source and ledger available for the final dashboard. No workers.
        await self.historical_runtime.close()

    async def _loop(self):
        while not self._closed:
            await asyncio.sleep(1)
            try:
                await self.tick()
                self._error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._error = type(exc).__name__ + ': historical controller update failed'

    async def _dispose_branch(self):
        tasks = list(self._reviews.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._reviews = {}
        if self.historical_runtime:
            await self.historical_runtime.close()
        if self.historical_markets:
            await self.historical_markets.close()
        if self.historical_store:
            self.historical_store.close()
        self.historical_store = self.historical_markets = None
        self.historical_executor = self.historical_runtime = None

    async def close(self):
        self._closed = True
        self._persist()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._dispose_branch()
