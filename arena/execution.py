# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Cash-only virtual order execution and bounded, honest portfolio marking."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from .store import MAX_QUANTITY, MICROS, decimal_value, micros, money

CRYPTO_MAX_AGE = 120
STOCK_MAX_AGE = 20 * 60
# The feed is a daily reference rate, including weekends and market holidays.
FX_MAX_AGE = 4 * 24 * 60 * 60
HISTORICAL_FX_MAX_AGE = 14 * 24 * 60 * 60
HISTORICAL_CRYPTO_MAX_AGE = 2 * 60 * 60
ASHARE_MARKETS = {'SSE', 'SZSE', 'BSE'}


def _now():
    return datetime.now(timezone.utc)


def _timestamp(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('Market data is missing its timestamp')
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise ValueError('Invalid market-data timestamp') from None
    if result.tzinfo is None:
        raise ValueError('Market-data timestamp must specify a timezone')
    return result.astimezone(timezone.utc)


def _age(value, max_age, label, now=None):
    age = ((now or _now()) - _timestamp(value)).total_seconds()
    if age < -120:
        raise ValueError(f'{label} timestamp is in the future')
    if age > max_age:
        raise ValueError(f'{label} is stale ({int(age)} seconds old)')
    return max(0, age)


class TradeExecutor:
    def __init__(self, store, markets):
        self.store, self.markets = store, markets
        self._mark_cache = {}
        self._fx_cache = None
        self._failed_marks = {}
        self._portfolio_lock = asyncio.Lock()
        self._mark_clock = None

    def invalidate_marks(self):
        self._mark_cache.clear()
        self._fx_cache = None
        self._failed_marks.clear()

    def _historical(self):
        return callable(getattr(self.markets, 'clock_now', None))

    def _clock_now(self):
        clock = self.markets.clock_now() if self._historical() else _now()
        if not isinstance(clock, datetime) or clock.tzinfo is None:
            raise ValueError('Market clock must specify an aware date and time')
        return clock.astimezone(timezone.utc)

    def _historical_gate(self):
        """Last synchronous check before committing a replay order."""
        if not self._historical():
            return
        if getattr(self.markets, 'active', False) is not True:
            raise ValueError('Historical world is closed; replay orders are disabled')
        main = getattr(self.markets, 'main', None)
        if main is None:
            raise ValueError('Historical provider has no live-market reopening guard')
        for market in ('SSE', 'SZSE', 'BSE', 'NASDAQ', 'NYSE'):
            status = main.market_status(market)
            if status.get('calendarError'):
                raise ValueError('Main-world trading calendar is unavailable; historical reopening cannot be verified')
            if status.get('isOpen'):
                raise ValueError('A main-world stock market has reopened; historical execution is closed')

    def _instrument(self, instrument_id):
        if not isinstance(instrument_id, str):
            raise ValueError('Instrument ID must be a string')
        catalog = self.markets.catalog
        item = catalog.get(instrument_id)
        if item is None:
            folded = instrument_id.upper()
            item = catalog.get(folded)
        if item is None:
            raise ValueError('Unknown instrument; use market_search to obtain its catalog ID')
        if item.get('currency') not in {'USD', 'CNY'}:
            raise ValueError('Unsupported instrument currency')
        return item

    def _validate_quote(self, instrument, quote, strict=True):
        price = decimal_value(quote.get('price'), 'quote price')
        if price <= 0:
            raise ValueError('Quote price must be positive')
        if quote.get('instrumentId') != instrument['id']:
            raise ValueError('Quote instrument does not match the order')
        if quote.get('currency') != instrument['currency']:
            raise ValueError('Quote currency does not match the instrument')
        if not quote.get('source'):
            raise ValueError('Quote source is missing')
        max_age = CRYPTO_MAX_AGE if instrument.get('kind') == 'crypto' else STOCK_MAX_AGE
        timestamp = _timestamp(quote.get('timestamp'))
        clock = self._clock_now()
        age = (clock - timestamp).total_seconds()
        historical = self._historical()
        if age < (0 if historical else -120):
            raise ValueError('Quote timestamp is in the future')
        if historical:
            if quote.get('replayTime') and _timestamp(quote['replayTime']) != clock:
                raise ValueError('Historical quote belongs to an earlier replay clock; refresh the dated observation')
            policy = quote.get('executionPolicy')
            if instrument.get('kind') == 'crypto':
                max_age = HISTORICAL_CRYPTO_MAX_AGE
                if strict and policy != 'historical_completed_hour':
                    raise ValueError('Historical crypto execution requires a completed hourly source candle')
            else:
                expiry = _timestamp(quote.get('validUntil'))
                duration = (expiry - timestamp).total_seconds()
                max_age = max(STOCK_MAX_AGE, duration)
                if strict:
                    if policy != 'historical_daily_open' or quote.get('barType') != 'daily_open':
                        raise ValueError('Historical stock execution requires its dated daily opening price')
                    if not 0 < duration <= 8 * 3600 or clock >= expiry:
                        raise ValueError('Historical daily-open quote has expired')
                    tz = ZoneInfo('Asia/Shanghai' if instrument['market'] in ASHARE_MARKETS else 'America/New_York')
                    if timestamp.astimezone(tz).date() != clock.astimezone(tz).date():
                        raise ValueError('Historical opening quote is from a different replay session')
                    session_lookup = getattr(getattr(self.markets, 'main', None), '_session', None)
                    if callable(session_lookup):
                        session = session_lookup(instrument['market'], clock.astimezone(tz).date())
                        if not session or timestamp != session[0] or expiry != session[1]:
                            raise ValueError('Historical daily-open timestamp/expiry does not match its exchange session')
            if strict:
                if quote.get('validForExecution') is not True:
                    raise ValueError('Historical source observation is not eligible for execution')
                if age > max_age:
                    raise ValueError('Historical source quote is stale at the replay clock')
                if quote.get('validUntil') and clock >= _timestamp(quote['validUntil']):
                    raise ValueError('Historical source quote has expired at the replay clock')
            return price, max(0, age), max_age
        if strict:
            _age(quote.get('timestamp'), max_age, 'Quote', clock)
            declared_delay = decimal_value(quote.get('delaySeconds', max(0, age)), 'quote delay')
            if declared_delay < 0 or declared_delay > max_age:
                raise ValueError('Quote feed is too delayed to execute this order')
        return price, max(0, age), max_age

    def _validate_fx(self, fx, strict=True):
        rate = decimal_value(fx.get('rate'), 'FX rate')
        if rate <= 0:
            raise ValueError('FX rate must be positive')
        if not fx.get('source'):
            raise ValueError('FX source is missing')
        timestamp = _timestamp(fx.get('timestamp'))
        clock = self._clock_now()
        historical = self._historical()
        age = (clock - timestamp).total_seconds()
        if age < (0 if historical else -120):
            raise ValueError('FX timestamp is in the future')
        if strict:
            if historical:
                available = _timestamp(fx.get('conservativeAvailableAt'))
                if available < timestamp:
                    raise ValueError('Historical FX availability precedes its source observation')
                if available > clock:
                    raise ValueError('Historical USD/CNY reference was not available at the replay clock')
                _age(fx.get('timestamp'), HISTORICAL_FX_MAX_AGE, 'Historical USD/CNY reference rate', clock)
            else:
                _age(fx.get('timestamp'), FX_MAX_AGE, 'USD/CNY reference rate', clock)
        return rate, max(0, age)

    async def execute(self, agent_id, action, instrument_id, quantity, reason='', round_id=None):
        """Reject invalid/unavailable orders without changing any account state."""
        try:
            if self.store.get_agent(agent_id) is None:
                raise ValueError('Unknown investor')
            if action not in {'buy', 'sell'}:
                raise ValueError('Action must be buy or sell')
            quantity = decimal_value(quantity, 'quantity').normalize()
            if quantity <= 0 or quantity > MAX_QUANTITY:
                raise ValueError('Quantity must be positive and no greater than 1 billion units')
            if quantity.as_tuple().exponent < -8:
                raise ValueError('Crypto quantities support at most 8 decimal places')
            instrument = self._instrument(instrument_id)
            instrument_id = instrument['id']
            market = instrument['market'].upper()
            crypto = instrument.get('kind') == 'crypto'
            if not crypto and quantity != quantity.to_integral_value():
                raise ValueError('Stock orders require whole shares')
            if not crypto and market in ASHARE_MARKETS:
                symbol = str(instrument.get('symbol', '')).removeprefix('sh').removeprefix('sz').removeprefix('bj')
                star = market == 'SSE' and symbol.startswith(('688', '689'))
                if action == 'buy':
                    if star and quantity < 200:
                        raise ValueError('STAR Market buys require at least 200 shares')
                    if market == 'BSE' and quantity < 100:
                        raise ValueError('Beijing A-share buys require at least 100 shares')
                    if not star and market != 'BSE' and quantity % 100:
                        raise ValueError('A-share buys require lots of 100 shares')
                else:
                    held = next((p['quantity'] for p in self.store.positions(agent_id) if p['instrumentId'] == instrument_id), 0)
                    # Residual holdings below the board minimum are sold in one order.
                    minimum = 200 if star else 100
                    if quantity < minimum and quantity != Decimal(str(held)):
                        raise ValueError('A-share odd-lot residual must be sold in full')
                    if not star and market != 'BSE' and quantity % 100 and quantity != Decimal(str(held)):
                        raise ValueError('A-share sells require 100-share lots or the full residual')
            status = self.markets.market_status(market)
            if not status.get('isOpen'):
                raise ValueError(f"{market} market is closed: {status.get('reason', 'outside trading session')}")
            self._historical_gate()
            try:
                quote = await asyncio.wait_for(self.markets.get_quote(instrument_id), timeout=12)
            except asyncio.TimeoutError:
                raise ValueError('Quote source timed out; order rejected') from None
            price, _, _ = self._validate_quote(instrument, quote)
            self._mark_cache[instrument_id] = (time.monotonic(), quote)
            rate = Decimal(1)
            fx = None
            if instrument['currency'] == 'USD':
                try:
                    fx = await asyncio.wait_for(self.markets.get_fx(), timeout=10)
                except asyncio.TimeoutError:
                    raise ValueError('USD/CNY source timed out; order rejected') from None
                rate, _ = self._validate_fx(fx)
                self._fx_cache = (time.monotonic(), fx)
            # Check again after network I/O: the regular session may have closed.
            status = self.markets.market_status(market)
            if not status.get('isOpen'):
                raise ValueError(f'{market} market closed before execution')
            # Source timestamps, FX availability and world gates may have changed
            # while awaiting I/O. Revalidate them all against the current clock.
            price, _, _ = self._validate_quote(instrument, quote)
            if fx is not None:
                rate, _ = self._validate_fx(fx)
            clock = self._clock_now()
            date = clock.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()
            self._historical_gate()
            trade = self.store.record_trade(agent_id=agent_id, action=action,
                instrument_id=instrument_id, quantity=quantity, price=price,
                currency=instrument['currency'], fx=rate, reason=str(reason),
                quote_timestamp=quote['timestamp'], source=quote['source'],
                acquired_date=date, enforce_t1=market in ASHARE_MARKETS,
                round_id=round_id,
                **({'timestamp': clock.isoformat()} if self._historical() else {}))
            if self._historical():
                trade.update(world='historical', worldId=self.store.get_setting('branchWorldId', 'historical'),
                    replay=True, asOf=clock.isoformat(), recordedAt=_now().isoformat(),
                    quoteTimestamp=quote['timestamp'], quoteSource=quote['source'],
                    quoteSourceUrl=quote.get('sourceUrl'), fxTimestamp=fx.get('timestamp') if fx else None,
                    fxSource=fx.get('source') if fx else None,
                    fxConservativeAvailableAt=fx.get('conservativeAvailableAt') if fx else None)
            self.store.add_event('trade', agent_id, f"{action.title()} {quantity} {instrument.get('symbol', instrument_id)}", trade)
            return trade
        except Exception as exc:
            details = {'action': str(action), 'instrumentId': str(instrument_id)}
            if self._historical():
                details.update(world='historical', worldId=self.store.get_setting('branchWorldId', 'historical'),
                    replay=True, asOf=self._clock_now().isoformat())
            self.store.add_event('order_rejected', agent_id, str(exc)[:2000], details)
            raise

    async def portfolios(self):
        """Read portfolios without adding history snapshots.

        At most eight simultaneous quotes run, for at most six seconds overall.
        Quotes are cached for 15 seconds. Cached historic marks carry explicit
        warnings; missing marks make total equity/PnL unknown (None), while
        knownEquity retains the value of cash and the actually priced holdings.
        """
        async with self._portfolio_lock:
            if self._historical():
                replay_clock = self._clock_now()
                if self._mark_clock != replay_clock:
                    self.invalidate_marks()
                    self._mark_clock = replay_clock
            holdings = self.store.positions()
            ids = list(dict.fromkeys(p['instrumentId'] for p in holdings))
            semaphore = asyncio.Semaphore(8)
            failures = {}

            async def fetch(instrument_id):
                cached = self._mark_cache.get(instrument_id)
                if cached and time.monotonic() - cached[0] < 15:
                    return
                failed = self._failed_marks.get(instrument_id)
                if failed and time.monotonic() - failed[0] < 15:
                    failures[instrument_id] = failed[1]
                    return
                async with semaphore:
                    try:
                        quote = await asyncio.wait_for(self.markets.get_quote(instrument_id), timeout=4)
                        self._validate_quote(self._instrument(instrument_id), quote, strict=False)
                        self._mark_cache[instrument_id] = (time.monotonic(), quote)
                        self._failed_marks.pop(instrument_id, None)
                    except Exception as exc:
                        failures[instrument_id] = str(exc)
                        self._failed_marks[instrument_id] = (time.monotonic(), str(exc))

            async def fetch_fx():
                if self._fx_cache and time.monotonic() - self._fx_cache[0] < 30:
                    return
                failed = self._failed_marks.get('FX')
                if failed and time.monotonic() - failed[0] < 15:
                    failures['FX'] = failed[1]
                    return
                try:
                    fx = await asyncio.wait_for(self.markets.get_fx(), timeout=4)
                    self._validate_fx(fx, strict=False)
                    self._fx_cache = (time.monotonic(), fx)
                    self._failed_marks.pop('FX', None)
                except Exception as exc:
                    failures['FX'] = str(exc)
                    self._failed_marks['FX'] = (time.monotonic(), str(exc))

            tasks = [asyncio.create_task(fetch(key)) for key in ids]
            if any(p['currency'] == 'USD' for p in holdings):
                tasks.append(asyncio.create_task(fetch_fx()))
            if tasks:
                _, pending = await asyncio.wait(tasks, timeout=6)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            # Fetch may overlap executed trades: read consistent current holdings
            # again; new symbols not fetched here remain explicitly unpriced.
            current = self.store.positions()
            agents = self.store.list_agents()
            for agent in agents:
                warnings, positions = [], []
                value = Decimal(str(agent['cash']))
                missing, estimated = False, False
                for row in (p for p in current if p['agentId'] == agent['id']):
                    position = dict(row)
                    instrument_id = row['instrumentId']
                    info = self.markets.catalog.get(instrument_id, {})
                    position.update(symbol=info.get('symbol', instrument_id), name=info.get('name', instrument_id), market=info.get('market'))
                    position_warnings = []
                    try:
                        cache = self._mark_cache.get(instrument_id)
                        if cache is None:
                            raise ValueError(failures.get(instrument_id, 'No genuine observed quote is available'))
                        quote = cache[1]
                        instrument = self._instrument(instrument_id)
                        price, age, max_age = self._validate_quote(instrument, quote, strict=False)
                        if age > max_age:
                            position_warnings.append(f'Historical quote: {int(age)} seconds old')
                        if instrument_id in failures:
                            position_warnings.append('Using last observed quote after quote refresh failed')
                        rate = Decimal(1)
                        if row['currency'] == 'USD':
                            if self._fx_cache is None:
                                raise ValueError(failures.get('FX', 'No USD/CNY conversion is available'))
                            rate, fx_age = self._validate_fx(self._fx_cache[1], strict=False)
                            if fx_age > (HISTORICAL_FX_MAX_AGE if self._historical() else FX_MAX_AGE):
                                position_warnings.append(f'Historical USD/CNY reference: {int(fx_age)} seconds old')
                            if 'FX' in failures:
                                position_warnings.append('Using last observed USD/CNY reference after refresh failed')
                        marked = money(micros(Decimal(str(row['quantity'])) * price * rate))
                        value += Decimal(str(marked))
                        position.update(price=float(price), priceCny=float(price * rate), valueCny=marked,
                                        pnl=marked - row['costBasisCny'], quoteTimestamp=quote['timestamp'],
                                        quoteSource=quote['source'], isEstimated=bool(position_warnings))
                        estimated |= bool(position_warnings)
                    except Exception as exc:
                        missing = True
                        position.update(price=None, priceCny=None, valueCny=None, pnl=None, isEstimated=True)
                        position_warnings.append(str(exc))
                    position['valuationWarnings'] = position_warnings
                    warnings.extend(f'{instrument_id}: {warning}' for warning in position_warnings)
                    positions.append(position)
                equity = float(value) if not missing else None
                initial = float(decimal_value(agent.get('initialCash', 800_000), 'initial capital'))
                agent.update(positions=positions, equity=equity, knownEquity=float(value),
                             pnl=equity - initial if equity is not None else None,
                             pnlPct=(equity / initial - 1) * 100 if equity is not None and initial > 0 else None,
                             valuationComplete=not missing, isEstimated=estimated or missing,
                             valuationWarnings=warnings)
                if self._historical():
                    agent.update(world='historical', worldId=self.store.get_setting('branchWorldId', 'historical'),
                        replay=True, asOf=self._clock_now().isoformat())
            return agents
