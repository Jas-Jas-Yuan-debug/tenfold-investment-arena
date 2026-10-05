# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Dated 2023 market replay, isolated from the live quote service.

Stocks use unadjusted daily opens during a session and completed daily closes
after it.  An open is held fixed until the next dated session: these data do not
claim to be historical intraday ticks.  The current symbol directory is only a
discovery list; a dated bar must exist before an instrument can be traded.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import hashlib
import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
import random
import re
from typing import Any
from urllib.parse import quote

import httpx

from .markets import MARKET_NAMES, NEW_YORK, SHANGHAI, UTC, MarketService, _iso, _parse_ts, _positive


CHINA_HISTORY_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
US_HISTORY_URL = "https://stock.finance.sina.com.cn/usstock/api/jsonp.php/IO.XSRV2.CallbackList/US_MinKService.getDailyK"
FX_HISTORY_URL = "https://www.federalreserve.gov/releases/h10/hist/dat00_ch.htm"
REPLAY_YEAR = 2023


class HistoricalMarketService:
    """MarketService-compatible read-only provider with an explicit replay clock.

    Fetching data is lazy, coalesced, and cached to disk.  No live-price fallback
    is used, including when a current-directory ticker did not exist in 2023.
    """

    def __init__(self, main: MarketService, data_dir: Path | None = None,
                 replay_time: datetime | str | None = None, seed: Any = None):
        self.main = main
        self.data_dir = Path(data_dir or main.data_dir / "historical")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(15, connect=5), trust_env=False,
            follow_redirects=True, limits=httpx.Limits(max_connections=6, max_keepalive_connections=4),
            headers={"User-Agent": "InvestmentArena/1.0 dated historical-data replay"})
        self._network_limit = asyncio.Semaphore(3)
        self._bars: OrderedDict[str, list[dict]] = OrderedDict()
        self._loads: dict[str, asyncio.Task] = {}
        self._fx_rows: list[dict] | None = None
        self._fx_lock = asyncio.Lock()
        self.active = True
        self._clock = self._coerce_clock(replay_time) if replay_time else self.choose_random_session(seed)

    @property
    def catalog(self):
        return self.main.catalog

    @property
    def coverage(self):
        return {market: {**row, "complete": False, "historicalUniverseComplete": False,
            "status": "historical_availability_checked_on_demand", "replay": True,
            "catalogAsOf": row.get("asOf"), "note": (
                "Current directory used for discovery; listing names and surviving symbols may reflect later dates. "
                "This is not a certified 2023 universe. A genuine replay-date bar is required for trading.")}
            for market, row in self.main.coverage.items()}

    async def initialize(self):
        return self.metadata()

    async def close(self):
        tasks = list(self._loads.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.client.aclose()

    @staticmethod
    def _coerce_clock(value):
        if isinstance(value, str):
            value = _parse_ts(value)
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ValueError("Replay clock must be a timezone-aware date and time")
        value = value.astimezone(UTC)
        if value.year != REPLAY_YEAR:
            raise ValueError("Historical replay is restricted to dated 2023 observations")
        return value

    def clock_now(self):
        return self._clock

    def set_clock(self, value):
        self._clock = self._coerce_clock(value)
        return self._clock

    def choose_random_session(self, seed=None, market=None, start_date=None, end_date=None):
        """Choose an actual 2023 session open; China and US hours do not overlap."""
        if market is not None and market not in ("SSE", "SZSE", "BSE", "NASDAQ", "NYSE"):
            raise ValueError("Choose a stock exchange for a historical open session")
        venues = [market] if market else ["SSE", "NASDAQ"]
        candidates = []
        start = date.fromisoformat(start_date) if isinstance(start_date, str) else (start_date or date(REPLAY_YEAR, 1, 1))
        end = date.fromisoformat(end_date) if isinstance(end_date, str) else (end_date or date(REPLAY_YEAR, 4, 30))
        if start.year != REPLAY_YEAR or end.year != REPLAY_YEAR or start > end:
            raise ValueError("Random historical session date bounds must form a valid interval within 2023")
        day = start
        while day <= end:
            for venue in venues:
                session = self.main._session(venue, day)
                if session:
                    candidates.append(session[0])
            day += timedelta(days=1)
        if not candidates:
            raise ValueError("No historical stock sessions are available from the exchange calendar")
        return random.Random(seed).choice(candidates).astimezone(UTC)

    def advance_session(self):
        """Move to the next chronological stock session, without wrapping backward."""
        starts = []
        day = self._clock.date() - timedelta(days=1)
        for _ in range(15):
            if day.year == REPLAY_YEAR:
                for venue in ("SSE", "NASDAQ"):
                    session = self.main._session(venue, day)
                    if session and session[0] > self._clock:
                        starts.append(session[0])
            day += timedelta(days=1)
        if not starts:
            raise ValueError("The dated 2023 replay has reached its final stock session")
        return self.set_clock(min(starts))

    def metadata(self):
        return {"world": "historical", "replay": True, "replayTime": _iso(self._clock),
            "year": REPLAY_YEAR, "active": self.active, "stockResolution": "daily_open_session_replay",
            "cryptoResolution": "completed_hourly_candles",
            "pricePolicy": "Current stock daily open, previous completed stock closes, completed crypto hours only",
            "timestampPolicy": "Stock timestamps are exchange session boundaries assigned to original dated daily bars; crypto timestamps are completed candle ends",
            "clockPolicy": "Shared replay moves to the next dated stock session open every five real minutes; investor decisions continue independently",
            "clockAdvanceIntervalSeconds": 300,
            "catalogPolicy": "Current directory; historical existence checked from dated bars on demand",
            "survivorshipBias": True, "pointInTimeUniverseCertified": False,
            "corporateActionsIncluded": False,
            "limitations": ["Historical listing universe and company names are not point-in-time certified",
                "Pretrained model knowledge can include events after 2023; the replay is not a certified point-in-time backtest",
                "Stock bars are unadjusted; splits, dividends and other corporate actions are not booked automatically",
                "Daily stock opens are held fixed within a replay session; no historical bid/ask or depth is available",
                "FX uses a conservative dated Federal Reserve weekly reference, not an executable historical spread"]}

    def _resolve(self, instrument_id):
        return self.main._resolve(instrument_id)

    def market_status(self, market, now=None):
        clock = self._coerce_clock(now) if now is not None else self._clock
        status = self.main.market_status(market, clock)
        if not self.active:
            status = {**status, "isOpen": False, "reason": "Historical world is closed; dated marks are available for valuation only"}
        return {**status, "world": "historical", "replay": True, "replayTime": _iso(clock),
            "calendarSource": "exchange_calendars XSHG 2023" if market in ("SSE", "SZSE", "BSE")
                              else ("Spot market 24/7" if market == "CRYPTO" else "exchange_calendars XNYS 2023")}

    async def search(self, query="", market="", limit=100, offset=0):
        result = await self.main.search(query, market, limit, offset)
        return {**result, "items": [{**row, "historicalAvailability": "unverified_until_dated_bar_loaded",
            "replay": True} for row in result["items"]], "coverage": self.coverage, **self.metadata()}

    async def _get(self, url, **kwargs):
        async with self._network_limit:
            response = await self.client.get(url, **kwargs)
            response.raise_for_status()
            if len(response.content) > 4 * 1024 * 1024:
                raise ValueError("Historical source response exceeds the bounded data limit")
            return response

    def _cache_path(self, key):
        return self.data_dir / (hashlib.sha256(key.encode()).hexdigest() + ".json")

    async def _load_stock(self, instrument):
        if instrument["market"] in ("SSE", "SZSE", "BSE"):
            vendor = instrument.get("quoteSymbol")
            if not vendor:
                raise ValueError("No historical China symbol mapping")
            response = await self._get(CHINA_HISTORY_URL, params={
                "param": f"{vendor},day,2022-12-01,2023-12-31,640,"})
            data = response.json().get("data", {}).get(vendor, {})
            # Only original (unadjusted) bars: qfq/hfq may apply corporate actions
            # that happened after the replay date.
            rows = data.get("day") or []
            source, source_url = "Tencent Finance unadjusted dated daily OHLC", str(response.url)
            normalized = [{"date": row[0], "open": row[1], "close": row[2], "high": row[3],
                           "low": row[4]} for row in rows if isinstance(row, list) and len(row) >= 5]
        else:
            response = await self._get(US_HISTORY_URL, params={"symbol": instrument["symbol"].lower()})
            match = re.search(r"IO\.XSRV2\.CallbackList\((.*)\);?\s*$", response.text, re.S)
            if not match:
                raise ValueError("Sina historical response is not the expected JSON callback")
            rows = json.loads(match.group(1))
            if not isinstance(rows, list):
                raise ValueError("Sina historical response contains no dated daily bars")
            normalized = [{"date": row.get("d"), "open": row.get("o"), "close": row.get("c"),
                           "high": row.get("h"), "low": row.get("l")} for row in rows if isinstance(row, dict)]
            source, source_url = "Sina Finance unadjusted dated US daily OHLC", str(response.url)
        bars = []
        for row in normalized:
            day = date.fromisoformat(str(row["date"]))
            if not date(2022, 12, 1) <= day <= date(2023, 12, 31):
                continue
            # Store the vendor's original values, and validate observations only
            # when they become visible at the replay clock. A malformed future
            # close/high/low must not suppress an earlier genuine open.
            bars.append({"date": str(day), **{key: row[key] for key in ("open", "close", "high", "low")},
                         "source": source, "sourceUrl": source_url})
        if not any(date.fromisoformat(row["date"]).year == REPLAY_YEAR for row in bars):
            raise ValueError(f"Genuine 2023 daily bars are unavailable for {instrument['id']}; no live-price substitution")
        return sorted(bars, key=lambda row: row["date"])

    async def _stock_bars(self, instrument):
        key = instrument["id"]
        if key in self._bars:
            self._bars.move_to_end(key)
            return self._bars[key]
        cache = self._cache_path("stock-v1:" + key)
        if cache.exists():
            try:
                saved = json.loads(cache.read_text("utf-8"))
                if saved.get("version") == 1 and saved.get("instrumentId") == key and saved.get("bars"):
                    self._bars[key] = saved["bars"]
                    return saved["bars"]
            except (OSError, ValueError, TypeError):
                pass
        if key not in self._loads:
            self._loads[key] = asyncio.create_task(self._load_stock(instrument))
        task = self._loads[key]
        try:
            bars = await asyncio.shield(task)
            self._bars[key] = bars
            while len(self._bars) > 128:
                self._bars.popitem(last=False)
            target = cache.with_suffix(".tmp")
            target.write_text(json.dumps({"version": 1, "instrumentId": key, "bars": bars}), "utf-8")
            target.replace(cache)
            return bars
        finally:
            if task.done() and self._loads.get(key) is task:
                self._loads.pop(key, None)

    def _stock_observations(self, instrument, bars, clock):
        result = []
        counts = {}
        for row in bars:
            counts[row["date"]] = counts.get(row["date"], 0) + 1
        seen = set()
        for row in bars:
            day = date.fromisoformat(row["date"])
            session = self.main._session(instrument["market"], day)
            if not session or session[0] > clock:
                continue
            if row["date"] in seen:
                continue
            seen.add(row["date"])
            try:
                if counts[row["date"]] != 1:
                    raise ValueError("Historical source returned duplicate dated bars")
                if session[1] <= clock:
                    prices = {key: _positive(row[key]) for key in ("open", "close", "high", "low")}
                    if not prices["low"] <= min(prices["open"], prices["close"]) <= max(prices["open"], prices["close"]) <= prices["high"]:
                        raise ValueError("Historical source returned inconsistent completed OHLC prices")
                    result.append({**row, "timestamp": session[1], "price": prices["close"], "barType": "daily_close",
                                   "validForExecution": False, "validUntil": session[1]})
                else:
                    result.append({**row, "timestamp": session[0], "price": _positive(row["open"]), "barType": "daily_open",
                                   "validForExecution": self.market_status(instrument["market"], clock)["isOpen"],
                                   "validUntil": session[1]})
            except (ValueError, TypeError, KeyError) as exc:
                result.append({"date": row["date"], "validationError": str(exc),
                               "timestamp": session[1] if session[1] <= clock else session[0]})
        return result

    async def _crypto_points(self, instrument, clock):
        end = clock.replace(minute=0, second=0, microsecond=0)
        key = "crypto-hour-v1:" + instrument["id"] + ":" + _iso(end)
        if key not in self._loads:
            self._loads[key] = asyncio.create_task(self._fetch_crypto_points(instrument, clock))
        task = self._loads[key]
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._loads.get(key) is task:
                self._loads.pop(key, None)

    async def _fetch_crypto_points(self, instrument, clock):
        end = clock.replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(hours=72)
        key = "crypto-hour-v1:" + instrument["id"] + ":" + _iso(end)
        cache = self._cache_path(key)
        if cache.exists():
            try:
                saved = json.loads(cache.read_text("utf-8"))
                if saved.get("version") == 1 and saved.get("points"):
                    return saved["points"]
            except (OSError, ValueError, TypeError):
                pass
        url = f"https://api.exchange.coinbase.com/products/{quote(instrument['symbol'], safe='')}-USD/candles"
        response = await self._get(url, params={"granularity": 3600, "start": _iso(start), "end": _iso(end)})
        rows = response.json()
        if not isinstance(rows, list):
            raise ValueError("Coinbase historical candles are unavailable")
        points = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 5:
                continue
            # Coinbase timestamps the BUCKET START; its close exists only after
            # the full hour. This filter also removes extra returned future bars.
            opening = datetime.fromtimestamp(float(row[0]), UTC)
            closing = opening + timedelta(hours=1)
            if start <= opening and closing <= clock:
                points.append({"timestamp": _iso(closing), "price": _positive(row[4]),
                    "source": "Coinbase Exchange completed historical hourly candle", "sourceUrl": str(response.url),
                    "barType": "hourly_close", "candleStart": _iso(opening)})
        points.sort(key=lambda row: row["timestamp"])
        if not points:
            raise ValueError(f"Completed historical candles are unavailable for {instrument['id']}")
        target = cache.with_suffix(".tmp")
        target.write_text(json.dumps({"version": 1, "points": points}), "utf-8")
        target.replace(cache)
        return points

    async def get_quote(self, instrument_id):
        instrument = self._resolve(instrument_id)
        clock = self._clock
        if instrument["kind"] == "crypto":
            points = await self._crypto_points(instrument, clock)
            point = points[-1]
            timestamp = _parse_ts(point["timestamp"])
            data = {**point, "timestamp": timestamp, "validForExecution": (clock - timestamp).total_seconds() < 7200,
                "validUntil": timestamp + timedelta(hours=2), "executionPolicy": "historical_completed_hour"}
            previous = points[-2]["price"] if len(points) > 1 else None
            previous_timestamp = points[-2]["timestamp"] if len(points) > 1 else None
            warnings = []
        else:
            bars = await self._stock_bars(instrument)
            points = self._stock_observations(instrument, bars, clock)
            if not points:
                raise ValueError(f"No actual dated observation existed for {instrument['id']} by {_iso(clock)}")
            if points[-1].get("validationError"):
                raise ValueError(points[-1]["validationError"])
            data = {**points[-1], "executionPolicy": "historical_daily_open"}
            previous_points = [point for point in points[:-1] if not point.get("validationError")]
            previous = previous_points[-1]["price"] if previous_points else None
            previous_timestamp = _iso(previous_points[-1]["timestamp"]) if previous_points else None
            warnings = [{"date": point["date"], "message": point["validationError"]}
                        for point in points if point.get("validationError")][-5:]
        # Deliberately omit future daily OHLC fields from every investor/API quote.
        return {"instrumentId": instrument["id"], "symbol": instrument["symbol"], "name": instrument["name"],
            "market": instrument["market"], "currency": instrument["currency"], "price": _positive(data["price"]),
            "timestamp": _iso(data["timestamp"]), "source": data["source"], "sourceUrl": data["sourceUrl"],
            "changePct": (data["price"] / previous - 1) * 100 if previous else None,
            "changeReferenceTimestamp": previous_timestamp, "changeReference": "latest available prior observation",
            "delaySeconds": max(0, int((clock - data["timestamp"]).total_seconds())),
            "world": "historical", "replay": True, "replayTime": _iso(clock), "barType": data["barType"],
            "timestampBasis": "completed_hourly_candle_end" if instrument["kind"] == "crypto" else "exchange_session_boundary_for_dated_daily_bar",
            "validForExecution": bool(self.active and data["validForExecution"]), "validUntil": _iso(data["validUntil"]),
            "executionPolicy": data["executionPolicy"], "datedSymbolObservationVerified": True,
            "pointInTimeIssuerIdentityCertified": False,
            "corporateActionsIncluded": False, "dataWarnings": warnings}

    async def history(self, instrument_id):
        instrument = self._resolve(instrument_id)
        clock = self._clock
        if instrument["kind"] == "crypto":
            points = await self._crypto_points(instrument, clock)
        else:
            points = self._stock_observations(instrument, await self._stock_bars(instrument), clock)
            points = [point for point in points if not point.get("validationError")]
        if not points:
            raise ValueError("No historical price observations existed before this replay clock")
        return [{"timestamp": _iso(point["timestamp"]) if isinstance(point["timestamp"], datetime) else point["timestamp"],
                 "price": point["price"], "barType": point["barType"], "source": point["source"],
                 "sourceUrl": point["sourceUrl"], "world": "historical", "replay": True} for point in points[-120:]]

    async def get_fx(self):
        async with self._fx_lock:
            if self._fx_rows is None:
                cache = self._cache_path("federal-reserve-cny-v1")
                if cache.exists():
                    try:
                        saved = json.loads(cache.read_text("utf-8"))
                        if saved.get("version") == 1 and saved.get("rows"):
                            self._fx_rows = saved["rows"]
                    except (OSError, ValueError, TypeError):
                        pass
                if self._fx_rows is None:
                    response = await self._get(FX_HISTORY_URL)
                    rows = re.findall(r"<th\b[^>]*>\s*(\d{1,2}-[A-Z]{3}-\d{2})\s*</th>\s*<td\b[^>]*>\s*([\d.]+)\s*</td>",
                                      response.text, re.I)
                    self._fx_rows = []
                    for day, value in rows:
                        parsed = datetime.strptime(day.upper(), "%d-%b-%y").date()
                        if date(2022, 12, 1) <= parsed <= date(2023, 12, 31):
                            self._fx_rows.append({"date": str(parsed), "rate": _positive(value)})
                    self._fx_rows.sort(key=lambda row: row["date"])
                    if not self._fx_rows:
                        self._fx_rows = None
                        raise ValueError("Federal Reserve dated 2023 USD/CNY observations are unavailable")
                    target = cache.with_suffix(".tmp")
                    target.write_text(json.dumps({"version": 1, "rows": self._fx_rows}), "utf-8")
                    target.replace(cache)
        # H.10 is published weekly. A ten-calendar-day lag avoids using the
        # replay day's noon observation before the following weekly release.
        # This is a conservative reference-rate reconstruction, not a certified
        # original vintage; current Fed history can contain later corrections.
        cutoff = self._clock - timedelta(days=10)
        eligible = [row for row in self._fx_rows
                    if datetime.combine(date.fromisoformat(row["date"]), time(12), NEW_YORK) <= cutoff]
        if not eligible:
            raise ValueError("No conservatively available historical USD/CNY reference rate")
        selected = eligible[-1]
        observation = datetime.combine(date.fromisoformat(selected["date"]), time(12), NEW_YORK)
        return {"rate": selected["rate"], "timestamp": _iso(observation), "source": "Federal Reserve H.10 dated USD/CNY reference",
            "sourceUrl": FX_HISTORY_URL, "base": "USD", "quote": "CNY", "updateFrequency": "weekly_publication",
            "delaySeconds": max(0, int((self._clock - observation).total_seconds())), "world": "historical", "replay": True,
            "replayTime": _iso(self._clock), "referenceLagDays": 10, "pointInTimeVintageCertified": False,
            "conservativeAvailableAt": _iso(observation + timedelta(days=10)),
            "note": "Conservative ten-day lag; reference conversion only, not a historical executable FX quote"}

    async def overview(self):
        ids = [key for key in ("CRYPTO:BTC", "CRYPTO:ETH", "NASDAQ:AAPL", "NYSE:BRK.B", "SSE:600519", "SZSE:000001")
               if key in self.catalog]
        results = await asyncio.gather(self.get_fx(), *(self.get_quote(key) for key in ids), return_exceptions=True)
        result = {"markets": [self.market_status(market) for market in MARKET_NAMES], "coverage": self.coverage,
            "featured": [], "errors": [], **self.metadata()}
        if isinstance(results[0], Exception):
            result["fxError"] = str(results[0])[:300]
        else:
            result["fx"] = results[0]
        for key, value in zip(ids, results[1:]):
            if isinstance(value, Exception):
                result["errors"].append({"instrumentId": key, "error": str(value)[:300]})
            else:
                result["featured"].append(value)
        return result
