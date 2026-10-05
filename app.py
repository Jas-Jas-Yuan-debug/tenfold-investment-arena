# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import math
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent
DATA = Path(os.getenv('ARENA_DATA_DIR', str(ROOT / 'data')))
DATA.mkdir(parents=True, exist_ok=True)
# Read checkout-local credentials only on the server. No credentials in any API response.
if (ROOT / '.env').exists():
    for line in (ROOT / '.env').read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            os.environ.setdefault(key.strip(), value.strip())
BASE_URL = os.getenv('AI_BASE_URL', 'http://127.0.0.1:8317/v1').rstrip('/')
API_KEY = os.getenv('AI_API_KEY', '')

from arena.markets import MarketService
from arena.store import Store
from arena.execution import TradeExecutor
from arena.runtime import Runtime
from arena.remote import RemoteRuntime, RemoteRuntimeError

COMPANION = os.getenv('ARENA_COMPANION') == '1'
ENABLE_WORLDS = os.getenv('ARENA_ENABLE_WORLDS') == '1'

store = Store(DATA / 'arena.db')
markets = MarketService(DATA)
executor = TradeExecutor(store, markets)
runtime = RemoteRuntime(os.getenv('MAIN_RUNTIME_URL', 'http://127.0.0.1:8787')) if COMPANION else Runtime(store, markets, executor, BASE_URL, API_KEY)
world_controller = None
if ENABLE_WORLDS:
    from arena.worlds import WorldController
    world_controller = WorldController(store, markets, runtime, BASE_URL, API_KEY, DATA)
provider_cache = {'connected': False, 'models': [], 'baseUrl': BASE_URL, 'error': 'Checking connection', 'checkedAt': None}
provider_lock = asyncio.Lock()
manual_round_tasks: set[asyncio.Task] = set()
stream_revision = 0
stream_signature = None
portfolio_cache = {}
world_portfolio_caches = {}
world_stream_states = {}
SERVER_INSTANCE = uuid.uuid4().hex

def worlds_metadata():
    if world_controller is not None:
        metadata = world_controller.status()
        return metadata.get('worlds', metadata) if isinstance(metadata, dict) else {}
    status_fn = getattr(markets, 'market_status', None)
    open_stock = any(status_fn(m).get('isOpen') for m in ('SSE', 'SZSE', 'BSE', 'NASDAQ', 'NYSE')) if status_fn else None
    return {'main': {'id': 'main', 'name': 'Main world', 'stockMarketsOpen': open_stock,
                     'phase': 'active' if runtime.status().get('running') else 'paused'},
            'historical': {'id': None, 'name': '2023 historical world', 'phase': 'inactive'}, 'merge': None}

def components(world='main'):
    if world == 'main':
        return store, markets, executor, runtime, 'main'
    if world != 'historical':
        raise HTTPException(422, 'Choose main or historical')
    if world_controller is None:
        raise HTTPException(503, 'Historical world is not enabled')
    values = [getattr(world_controller, 'historical_' + name, None) for name in ('store', 'markets', 'executor', 'runtime')]
    if any(value is None for value in values):
        phase = worlds_metadata().get('historical', {}).get('phase', 'inactive')
        raise HTTPException(503, 'Historical world is ' + str(phase))
    branch_id = worlds_metadata().get('historical', {}).get('id') or 'historical'
    return (*values, str(branch_id))

def cache_for(world, branch_id):
    return portfolio_cache if world == 'main' else world_portfolio_caches.setdefault(branch_id, {})

def world_fields(world, branch_id):
    return {'worldId': world, 'branchId': branch_id, 'worlds': worlds_metadata(),
            'serverInstance': SERVER_INSTANCE + ':' + branch_id}

def verify_branch(world, branch_id):
    if components(world)[-1] != branch_id:
        raise HTTPException(409, 'The historical branch changed during this request; refresh the selected world')

def require_main_control(world):
    if world != 'main':
        raise HTTPException(403, 'Historical activation, clock and merge are managed by the world controller')

def portfolio_signature(agent):
    return (agent.get('cash'), tuple(sorted((p['instrumentId'], p['quantity'], p.get('costBasisCny'))
                                           for p in agent.get('positions', []))))

def normalized_clock(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    return value.astimezone(timezone.utc).isoformat() if isinstance(value, datetime) and value.tzinfo is not None else None

def source_clock(source):
    clock = getattr(source, 'clock_now', None)
    return normalized_clock(clock()) if callable(clock) else None

def portfolio_cache_entry(agent, source, requested_clock=None):
    replay_time = normalized_clock(agent.get('asOf') or agent.get('replayTime')) or requested_clock
    return {'signature': portfolio_signature(agent), 'agent': agent,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'replayTime': replay_time if callable(getattr(source, 'clock_now', None)) else None}

def enrich_agents(agents, status, events=None, ledger=None):
    """Expose recorded public explanations, never private model reasoning fields."""
    ledger = ledger or store
    events = events if events is not None else ledger.events(limit=1000)
    live = status.get('agentStates', {})
    result = []
    for agent in agents:
        row = dict(agent)
        getter = getattr(ledger, 'latest_agent_events', None)
        latest = getter(agent['id']) if getter else {}
        if not latest:
            for event in events:
                if event.get('agentId') == agent['id'] and event['kind'] not in latest:
                    latest[event['kind']] = event
        decision = latest.get('decision')
        if decision:
            row['lastDecision'] = {'content': decision['content'], 'timestamp': decision['timestamp'],
                                   **(decision.get('details') or {})}
        response = latest.get('response_completed')
        if response:
            row['lastResponseAt'] = response['timestamp']
            row['lastExplanation'] = response['content']
            row['lastResponseKind'] = (response.get('details') or {}).get('kind')
        knowledge = latest.get('knowledge_updated')
        if knowledge:
            row['knowledgeUpdatedAt'] = knowledge['timestamp']
        row.update(live.get(agent['id'], {}))
        result.append(row)
    return result

def totals_for(agents, ledger=None):
    ledger = ledger or store
    equities = [a.get('equity') for a in agents]
    complete = len(equities) == 10 and all(isinstance(v, (int, float)) and math.isfinite(v) for v in equities)
    equity = sum(equities) if complete else None
    baseline = sum(a.get('initialCash', 800000) for a in agents)
    return {'initialCapital': baseline, 'equity': equity, 'cash': sum(a.get('cash', 0) for a in agents),
            'pnl': equity - baseline if equity is not None else None,
            'pnlPct': (equity / baseline - 1) * 100 if equity is not None and baseline else None,
            'agentCount': len(agents), 'tradeCount': ledger.count_trades(), 'valuationComplete': complete}

def live_update(world='main'):
    """A cheap ledger/status snapshot; streams never own investor tasks."""
    global stream_revision, stream_signature
    ledger, source, _, worker, branch_id = components(world)
    cache = cache_for(world, branch_id)
    replay_time = source_clock(source) if world == 'historical' else None
    status = worker.status()
    events, messages, trades = ledger.events(limit=100), ledger.messages(limit=100), ledger.trades(limit=100)
    positions = ledger.positions()
    agents = []
    for agent in ledger.list_agents():
        agent['positions'] = [p for p in positions if p['agentId'] == agent['id']]
        cached = cache.get(agent['id'])
        if cached and cached['signature'] == portfolio_signature(agent) and (world == 'main' or cached.get('replayTime') == replay_time):
            agent = {**cached['agent'], **agent, 'positions': cached['agent'].get('positions', []),
                     'valuationAsOf': cached['timestamp']}
        elif not agent['positions']:
            baseline = agent.get('initialCash', 800000)
            agent.update(equity=agent['cash'], knownEquity=agent['cash'], pnl=agent['cash']-baseline,
                         pnlPct=(agent['cash']/baseline-1)*100 if baseline else None, valuationComplete=True)
        else:
            agent.update(equity=None, pnl=None, pnlPct=None, valuationComplete=False)
        agents.append(agent)
    agents = enrich_agents(agents, status, events, ledger)
    chat_fn = getattr(ledger, 'chat_state', None)
    chat = chat_fn() if chat_fn else {'groups': [], 'rooms': [], 'activeRoom': None}
    metadata = world_fields(world, branch_id)
    signature_status = {k: v for k, v in status.items() if k != 'asOf'} if world == 'main' else status
    signature = hashlib.sha256(json.dumps({'runtime': signature_status, 'agents': agents, 'chat': chat, 'worlds': metadata['worlds'],
        'events': [e['id'] for e in events], 'messages': [m['id'] for m in messages],
        'trades': [t['id'] for t in trades]}, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    if world == 'main':
        if signature != stream_signature:
            stream_revision += 1
            stream_signature = signature
        revision = stream_revision
    else:
        previous = world_stream_states.setdefault(branch_id, {'revision': 0, 'signature': None})
        if signature != previous['signature']:
            previous.update(revision=previous['revision'] + 1, signature=signature)
        revision = previous['revision']
    return {**metadata, 'revision': revision, 'runtime': status, 'agents': agents,
            'events': events, 'messages': messages, 'trades': trades, 'chat': chat,
            'totals': totals_for(agents, ledger), 'backgroundService': {
                'manager': os.getenv('ARENA_PROCESS_MANAGER', 'standalone'),
                'continuesWithoutBrowser': True}, 'serverTime': datetime.now(timezone.utc).isoformat()}

async def stream_updates(request, world='main'):
    last = request.headers.get('last-event-id')
    last_id = int(last) if last and last.isdigit() else -1
    try:
        last_instance = world_fields(world, components(world)[-1])['serverInstance']
    except HTTPException:
        last_instance = None
    heartbeat = asyncio.get_running_loop().time()
    while not await request.is_disconnected():
        try:
            update = live_update(world)
        except HTTPException as exc:
            yield 'event: world_unavailable\ndata: ' + json.dumps({'worldId': world, 'detail': exc.detail, 'worlds': worlds_metadata()}) + '\n\n'
            await asyncio.sleep(2)
            continue
        if update['revision'] != last_id or update['serverInstance'] != last_instance:
            last_id = update['revision']
            last_instance = update['serverInstance']
            yield 'id: ' + str(last_id) + '\nevent: update\ndata: ' + json.dumps(update, ensure_ascii=False, allow_nan=False) + '\n\n'
        elif asyncio.get_running_loop().time() - heartbeat >= 15:
            yield ': keep-alive\n\n'
            heartbeat = asyncio.get_running_loop().time()
        await asyncio.sleep(0.25)

def finish_manual_round(task):
    manual_round_tasks.discard(task)
    if not task.cancelled():
        error = task.exception()
        if error:
            with contextlib.suppress(Exception):
                store.add_event('error', None, 'Manual round failed: ' + type(error).__name__)

async def cancel_manual_rounds():
    tasks = [task for task in manual_round_tasks if not task.done()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def check_provider(force=False):
    now = datetime.now(timezone.utc)
    checked = provider_cache.get('checkedAt')
    if not force and checked and (now - datetime.fromisoformat(checked)).total_seconds() < 45:
        return provider_cache.copy()
    async with provider_lock:
        try:
            async with httpx.AsyncClient(timeout=8, trust_env=False) as client:
                response = await client.get(BASE_URL + '/models', headers={'Authorization': 'Bearer ' + API_KEY})
                response.raise_for_status()
                ids = [row['id'] for row in response.json().get('data', [])]
                missing = [m for m in ['gpt-6-astra', 'gpt-6.1-sol'] if m not in ids]
                provider_cache.update(connected=not missing, models=ids, error=('Missing models: ' + ', '.join(missing)) if missing else None)
        except Exception as exc:
            provider_cache.update(connected=False, error=f'{type(exc).__name__}: local model provider unavailable')
        provider_cache['checkedAt'] = now.isoformat()
    return provider_cache.copy()

async def refresh_loop():
    last_catalog_refresh = asyncio.get_running_loop().time()
    last_account_refresh = last_catalog_refresh
    while True:
        await asyncio.sleep(5)
        if isinstance(runtime, RemoteRuntime):
            await runtime.refresh()
        with contextlib.suppress(Exception):
            for agent in await executor.portfolios():
                portfolio_cache[agent['id']] = portfolio_cache_entry(agent, markets)
        if world_controller is not None:
            with contextlib.suppress(Exception):
                ledger, branch_source, branch_executor, _, branch_id = components('historical')
                cache = cache_for('historical', branch_id)
                requested_clock = source_clock(branch_source)
                rows = await branch_executor.portfolios()
                verify_branch('historical', branch_id)
                for agent in rows:
                    cache[agent['id']] = portfolio_cache_entry(agent, branch_source, requested_clock)
        if asyncio.get_running_loop().time() - last_account_refresh >= 60:
            with contextlib.suppress(Exception):
                await check_provider()
            with contextlib.suppress(Exception):
                for agent in await executor.portfolios():
                    equity = agent.get('equity')
                    if not isinstance(runtime, RemoteRuntime) and isinstance(equity, (int, float)) and math.isfinite(equity):
                        store.snapshot_equity(agent['id'], equity)
            last_account_refresh = asyncio.get_running_loop().time()
        if asyncio.get_running_loop().time() - last_catalog_refresh >= 21600:
            with contextlib.suppress(Exception):
                await markets.initialize()
            last_catalog_refresh = asyncio.get_running_loop().time()

@asynccontextmanager
async def lifespan(app):
    await markets.initialize()
    await check_provider(True)
    if isinstance(runtime, RemoteRuntime):
        await runtime.refresh()
    if world_controller is not None:
        await world_controller.start()
    task = asyncio.create_task(refresh_loop())
    saved = store.get_setting('runtimeSnapshot', {})
    if not isinstance(runtime, RemoteRuntime) and isinstance(saved, dict) and saved.get('running') and API_KEY:
        await runtime.start()
    yield
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    await cancel_manual_rounds()
    if world_controller is not None:
        await world_controller.close()
    await runtime.close()
    close = getattr(markets, 'close', None)
    if close:
        await close()

app = FastAPI(title='Tenfold · AI Investment Arena', lifespan=lifespan)

@app.middleware('http')
async def localhost_boundary(request: Request, call_next):
    host = request.url.hostname
    if host not in {'127.0.0.1', 'localhost', '::1', 'testserver'}:
        return JSONResponse({'detail': 'Localhost access only'}, status_code=403)
    if request.method in {'POST', 'PUT', 'PATCH', 'DELETE'}:
        origin = request.headers.get('origin')
        if origin and origin != str(request.base_url).rstrip('/'):
            return JSONResponse({'detail': 'Same-origin requests only'}, status_code=403)
        fetch_site = request.headers.get('sec-fetch-site')
        if fetch_site and fetch_site not in {'same-origin', 'none'}:
            return JSONResponse({'detail': 'Same-origin requests only'}, status_code=403)
        if request.url.path.startswith('/api/') and request.headers.get('content-type', '').split(';')[0] != 'application/json':
            return JSONResponse({'detail': 'JSON required'}, status_code=415)
    response = await call_next(request)
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['Referrer-Policy'] = 'same-origin'
    response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
    if request.url.path.startswith('/api/'):
        response.headers['Cache-Control'] = 'no-store'
    return response

@app.get('/')
async def index():
    return FileResponse(ROOT / 'static' / 'index.html')

@app.get('/api/health')
async def health():
    return {'ok': True, 'time': datetime.now(timezone.utc).isoformat(), 'catalogSize': len(markets.catalog),
            'runtime': runtime.status(), 'companion': isinstance(runtime, RemoteRuntime), 'worlds': worlds_metadata()}

@app.get('/api/provider')
async def provider():
    return await check_provider()

@app.get('/api/state')
async def state(world: str = 'main'):
    ledger, source, broker, worker, branch_id = components(world)
    requested_clock = source_clock(source)
    agents_result, overview_result = await asyncio.gather(broker.portfolios(), source.overview(), return_exceptions=True)
    verify_branch(world, branch_id)
    agents = ledger.list_agents() if isinstance(agents_result, Exception) else agents_result
    overview = {'markets': [], 'coverage': {}, 'featured': [], 'error': str(overview_result)} if isinstance(overview_result, Exception) else overview_result
    for agent in agents:
        cache_for(world, branch_id)[agent['id']] = portfolio_cache_entry(agent, source, requested_clock)
    provider = await check_provider()
    verify_branch(world, branch_id)
    update = live_update(world)
    return {**update, **overview, 'equityHistory': ledger.equity_history(),
            'provider': provider, **world_fields(world, branch_id)}

@app.get('/api/stream')
async def stream(request: Request, world: str = 'main'):
    if world not in {'main', 'historical'}:
        raise HTTPException(422, 'Choose main or historical')
    return StreamingResponse(stream_updates(request, world), media_type='text/event-stream',
                             headers={'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no'})

@app.get('/api/chat')
async def chat_state(world: str = 'main'):
    ledger, _, _, _, branch_id = components(world)
    return {**ledger.chat_state(), **world_fields(world, branch_id)}

@app.get('/api/instruments')
async def instruments(q: str = Query('', max_length=100), market: str = Query('', max_length=20),
                      limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0, le=100000), world: str = 'main'):
    _, source, _, _, branch_id = components(world)
    result = await source.search(query=q, market=market, limit=limit, offset=offset)
    verify_branch(world, branch_id)
    return {**result, **world_fields(world, branch_id)}

@app.get('/api/quote/{instrument_id}')
async def quote(instrument_id: str, world: str = 'main'):
    _, source, _, _, branch_id = components(world)
    try:
        result = await source.get_quote(instrument_id)
        verify_branch(world, branch_id)
        info = source.catalog.get(instrument_id, {})
        return {**info, **result, **world_fields(world, branch_id)}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(503 if instrument_id in source.catalog else 404, str(exc))

@app.get('/api/history/{instrument_id}')
async def history(instrument_id: str, world: str = 'main'):
    _, source, _, _, branch_id = components(world)
    try:
        result = await source.history(instrument_id)
        verify_branch(world, branch_id)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(503, str(exc))

@app.get('/api/agents/{agent_id}')
async def agent_detail(agent_id: str, world: str = 'main'):
    ledger, _, broker, worker, branch_id = components(world)
    rows = await broker.portfolios()
    verify_branch(world, branch_id)
    agent = next((a for a in rows if a['id'] == agent_id), None)
    if agent is None:
        raise HTTPException(404, 'Unknown investor')
    agent = enrich_agents([agent], worker.status(), ledger=ledger)[0]
    return {**world_fields(world, branch_id), 'agent': agent, 'positions': agent.get('positions', ledger.positions(agent_id)),
            'trades': ledger.trades(limit=100, agent_id=agent_id),
            'messages': ledger.messages(limit=100, agent_id=agent_id), 'equityHistory': ledger.equity_history(agent_id)}

class IntervalSetting(BaseModel):
    intervalSeconds: int = Field(ge=60, le=3600)

class MessageInput(BaseModel):
    sender: str = 'human'
    recipient: str = 'all'
    kind: str = 'tip'
    content: str = Field(min_length=1, max_length=2000)
    roomId: str | None = Field(default=None, max_length=150)

class GroupInput(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    members: list[str] = Field(min_length=1, max_length=10)

@app.post('/api/chat/groups')
async def create_group(body: GroupInput, world: str = 'main'):
    ledger, _, _, _, _ = components(world)
    try:
        return ledger.create_group('human', body.name, body.members)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

@app.post('/api/simulation/start')
async def start(world: str = 'main'):
    require_main_control(world)
    current = runtime.status()
    if current.get('inProgress') and not current.get('running'):
        raise HTTPException(409, 'Wait for the current evaluation to finish before starting continuous investors')
    p = await check_provider(True)
    if not p['connected']:
        raise HTTPException(503, p['error'])
    try:
        await runtime.start()
    except RemoteRuntimeError as exc:
        raise HTTPException(exc.status_code, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    return runtime.status()

@app.post('/api/simulation/pause')
async def pause(world: str = 'main'):
    require_main_control(world)
    try:
        await runtime.pause()
    except RemoteRuntimeError as exc:
        raise HTTPException(exc.status_code, str(exc)) from exc
    await cancel_manual_rounds()
    return runtime.status()

@app.post('/api/simulation/round')
async def run_round(world: str = 'main'):
    require_main_control(world)
    status = runtime.status()
    if status.get('running') or status.get('inProgress') or any(not task.done() for task in manual_round_tasks):
        raise HTTPException(409, 'Investors are already evaluating; pause before requesting one evaluation')
    if isinstance(runtime, RemoteRuntime):
        try:
            return await runtime.run_round()
        except RemoteRuntimeError as exc:
            raise HTTPException(exc.status_code, str(exc)) from exc
    task = asyncio.create_task(runtime.run_round())
    manual_round_tasks.add(task)
    task.add_done_callback(finish_manual_round)
    await asyncio.sleep(0)
    return {'accepted': True, **runtime.status()}

@app.post('/api/simulation/reset')
async def reset(world: str = 'main'):
    require_main_control(world)
    metadata = worlds_metadata()
    historical = metadata.get('historical') or {}
    phase = historical.get('phase', 'inactive')
    merge = metadata.get('merge') or {}
    deadline = merge.get('deadlineAt')
    try:
        merge_open = bool(deadline) and datetime.fromisoformat(deadline.replace('Z', '+00:00')) > datetime.now(timezone.utc)
    except (ValueError, TypeError):
        merge_open = bool(merge.get('openedAt'))
    if phase not in {'inactive', 'closed', 'discarded', 'merged', 'completed', 'unavailable'} or merge_open:
        raise HTTPException(409, 'Finish or close the historical world and its merge window before resetting main balances')
    status = runtime.status()
    if status.get('running') or status.get('inProgress') or status.get('pausing') or status.get('canReset') is False or any(not task.done() for task in manual_round_tasks):
        raise HTTPException(409, 'Pause the simulation and wait for the round to stop before resetting')
    if isinstance(runtime, RemoteRuntime):
        try:
            result = await runtime.reset()
            portfolio_cache.clear()
            return result
        except RemoteRuntimeError as exc:
            raise HTTPException(exc.status_code, str(exc)) from exc
    store.reset()
    # Runtime counters must also restart after an explicit reset.
    reset_fn = getattr(runtime, 'reset_status', None)
    if reset_fn:
        reset_fn()
    return {'ok': True}

@app.post('/api/settings')
async def settings(body: IntervalSetting, world: str = 'main'):
    require_main_control(world)
    if isinstance(runtime, RemoteRuntime):
        try:
            await runtime.set_interval(body.intervalSeconds)
            return runtime.status()
        except RemoteRuntimeError as exc:
            raise HTTPException(exc.status_code, str(exc)) from exc
    store.set_setting('intervalSeconds', body.intervalSeconds)
    setter = getattr(runtime, 'set_interval', None)
    if setter:
        setter(body.intervalSeconds)
    return runtime.status()

@app.post('/api/messages')
async def message(body: MessageInput, world: str = 'main'):
    ledger, _, _, _, _ = components(world)
    if body.sender != 'human' or body.kind not in {'tip', 'distraction', 'message', 'public_speech'}:
        raise HTTPException(400, 'Use a human sender and a supported message kind')
    groups = {g['id'] for g in ledger.chat_state()['groups']}
    if body.recipient != 'all' and body.recipient not in {a['id'] for a in ledger.list_agents()} | groups:
        raise HTTPException(404, 'Unknown investor')
    content = body.content.strip()
    if not content:
        raise HTTPException(400, 'Message cannot be blank')
    try:
        return ledger.add_message('human', body.recipient, body.kind, content, room_id=body.roomId)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

@app.get('/api/worlds')
async def worlds_state():
    return {'worlds': worlds_metadata()}

@app.get('/api/worlds/historical/state')
async def historical_state():
    return await state('historical')

@app.get('/api/worlds/historical/chat')
async def historical_chat():
    return await chat_state('historical')

@app.get('/api/worlds/historical/instruments')
async def historical_instruments(q: str = Query('', max_length=100), market: str = Query('', max_length=20),
        limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0, le=100000)):
    return await instruments(q, market, limit, offset, 'historical')

@app.get('/api/worlds/historical/quote/{instrument_id}')
async def historical_quote(instrument_id: str):
    return await quote(instrument_id, 'historical')

@app.get('/api/worlds/historical/history/{instrument_id}')
async def historical_history(instrument_id: str):
    return await history(instrument_id, 'historical')

@app.get('/api/worlds/historical/agents/{agent_id}')
async def historical_agent_detail(agent_id: str):
    return await agent_detail(agent_id, 'historical')

@app.post('/api/worlds/historical/messages')
async def historical_message(body: MessageInput):
    return await message(body, 'historical')

@app.post('/api/worlds/historical/chat/groups')
async def historical_create_group(body: GroupInput):
    return await create_group(body, 'historical')

@app.post('/api/worlds/historical/simulation/{action}')
@app.post('/api/worlds/historical/settings')
async def reject_historical_control(action: str = ''):
    raise HTTPException(403, 'Historical activation, clock and merge are managed by the world controller')

app.mount('/static', StaticFiles(directory=ROOT / 'static'), name='static')
