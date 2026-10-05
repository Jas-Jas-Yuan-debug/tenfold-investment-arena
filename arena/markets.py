# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Public instrument catalogs and real market data for the virtual arena.

Catalog completeness is checked against each upstream's declared row count. The
US directory is the Nasdaq Trader directory, filtered to non-test, non-ETF
securities on the requested exchange; preferred shares/warrants remain visible.
China's live Sina directory is paged fully. Availability of a catalog does not
imply every vendor supports a quote for every unusual security.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import csv
from datetime import date, datetime, time, timedelta, timezone
import io
import json
import math
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import httpx

UTC = timezone.utc
SHANGHAI = ZoneInfo("Asia/Shanghai")
NEW_YORK = ZoneInfo("America/New_York")
MARKET_NAMES = {"SSE": "Shanghai A-shares", "SZSE": "Shenzhen A-shares",
                "BSE": "Beijing A-shares", "NASDAQ": "NASDAQ", "NYSE": "New York Stock Exchange",
                "CRYPTO": "BTC / ETH"}
NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
NYSE_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
SINA_BASE = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
HOLIDAY_SOURCE = "https://www.sse.com.cn/disclosure/dealinstruc/closed/c/c_20251222_10802510.shtml"
# The exchange's published 2026 notice overrides any calendar prediction.
CN_HOLIDAY_RANGES = [("2026-01-01", "2026-01-03"), ("2026-02-15", "2026-02-23"),
                     ("2026-04-04", "2026-04-06"), ("2026-05-01", "2026-05-05"),
                     ("2026-06-19", "2026-06-21"), ("2026-09-25", "2026-09-27"),
                     ("2026-10-01", "2026-10-07")]


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _positive(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("The market data source returned an invalid price")
    return number


class MarketService:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_path = self.data_dir / "market_catalog.json"
        self.catalog: dict[str, dict] = {}
        self.coverage: dict[str, dict] = {}
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(8, connect=4), follow_redirects=True,
                                       limits=httpx.Limits(max_connections=12, max_keepalive_connections=8),
                                       headers={"User-Agent": "InvestmentArena/1.0 public-data research"})
        self._network_limit = asyncio.Semaphore(6)
        self._quotes: OrderedDict[str, tuple[datetime, dict]] = OrderedDict()
        self._quote_tasks: dict[str, asyncio.Task] = {}
        self._history: OrderedDict[str, tuple[datetime, list]] = OrderedDict()
        self._fx: tuple[datetime, dict] | None = None
        self._calendars: dict[str, Any] = {}
        self._fx_lock = asyncio.Lock()

    async def close(self):
        await self.client.aclose()

    async def _get(self, url: str, **kwargs) -> httpx.Response:
        async with self._network_limit:
            response = await self.client.get(url, **kwargs)
            response.raise_for_status()
            return response

    async def initialize(self):
        """Load a durable catalog, then fill/refresh each source independently."""
        if self.cache_path.exists():
            try:
                saved = json.loads(self.cache_path.read_text("utf-8"))
                if saved.get("version") == 1:
                    self.catalog = {r["id"]: r for r in saved["instruments"]}
                    self.coverage = saved["coverage"]
                    for market, metadata in self.coverage.items():
                        actual = sum(r["market"] == market for r in self.catalog.values())
                        if actual != metadata.get("count"):
                            metadata.update(complete=False, status="invalid_cache", count=actual,
                                            error="Cached rows do not match the recorded count")
            except (OSError, ValueError, KeyError, TypeError):
                self.catalog = {}
                self.coverage = {}
        for symbol, name in [("BTC", "Bitcoin"), ("ETH", "Ethereum")]:
            instrument = self._instrument(symbol, name, "CRYPTO", "USD", "crypto")
            self.catalog[instrument["id"]] = instrument
        self.coverage["CRYPTO"] = {"market": "CRYPTO", "count": 2, "expectedCount": 2,
                                    "complete": True, "status": "complete", "source": "Requested BTC / ETH universe",
                                    "asOf": _iso(_now()), "note": "Spot markets; quotes fetched from public exchanges."}
        loaders = {"SSE": lambda: self._china_catalog("SSE", "sh_a"),
                   "SZSE": lambda: self._china_catalog("SZSE", "sz_a"),
                   "BSE": lambda: self._china_catalog("BSE", "hs_bjs"),
                   "NASDAQ": lambda: self._us_catalog("NASDAQ", NASDAQ_URL),
                   "NYSE": lambda: self._us_catalog("NYSE", NYSE_URL)}

        async def update(market: str, loader):
            previous = self.coverage.get(market, {})
            try:
                # Avoid hitting upstreams on every restart; the source date remains visible.
                age = (_now() - _parse_ts(previous["asOf"])).total_seconds() if previous.get("asOf") else float("inf")
                if previous.get("complete") and age < 21600 and any(r["market"] == market for r in self.catalog.values()):
                    previous["status"] = "complete_cached"
                    return
                rows, metadata = await loader()
                self.catalog = {key: row for key, row in self.catalog.items() if row["market"] != market}
                self.catalog.update({row["id"]: row for row in rows})
                self.coverage[market] = metadata
            except Exception as exc:
                count = sum(row["market"] == market for row in self.catalog.values())
                self.coverage[market] = {**previous, "market": market, "count": count,
                                         "complete": bool(previous.get("complete") and count),
                                         "status": "stale_cache" if count else "unavailable",
                                         "error": f"{type(exc).__name__}: {str(exc)[:240]}",
                                         "lastAttempt": _iso(_now())}
        await asyncio.gather(*(update(market, loader) for market, loader in loaders.items()))
        self._save_catalog()

    def _save_catalog(self):
        target = self.cache_path.with_suffix(".tmp")
        target.write_text(json.dumps({"version": 1, "instruments": list(self.catalog.values()),
                                      "coverage": self.coverage}, ensure_ascii=False), "utf-8")
        target.replace(self.cache_path)

    @staticmethod
    def _instrument(symbol, name, market, currency="CNY", kind="stock", **extra):
        return {"id": f"{market}:{symbol}", "symbol": symbol, "name": name, "market": market,
                "currency": currency, "kind": kind, **extra}

    async def _china_catalog(self, market, node):
        source = SINA_BASE + "Market_Center.getHQNodeData"

        async def page(number):
            response = await self._get(source, params={"page": number, "num": 100,
                "sort": "symbol", "asc": 1, "node": node, "symbol": "", "_s_r_a": "page"})
            rows = response.json()
            if not isinstance(rows, list):
                raise ValueError("Catalog page is not a list")
            return rows

        async def count():
            response = await self._get(SINA_BASE + "Market_Center.getHQNodeStockCount", params={"node": node})
            total = int(response.json())
            minimum = 50 if market == "BSE" else 500
            if not minimum <= total <= 10000:
                raise ValueError(f"Implausible upstream stock count: {total}")
            return total

        total, first = await asyncio.gather(count(), page(1))
        rest = await asyncio.gather(*(page(n) for n in range(2, math.ceil(total / 100) + 1)))
        all_rows = first + [row for chunk in rest for row in chunk]
        prefix = {"SSE": "sh", "SZSE": "sz", "BSE": "bj"}[market]
        rows = []
        for row in all_rows:
            symbol = str(row.get("code", ""))
            provider_symbol = str(row.get("symbol", ""))
            if not re.fullmatch(r"\d{6}", symbol) or provider_symbol != prefix + symbol or not row.get("name"):
                raise ValueError(f"Invalid upstream instrument in {market} directory")
            rows.append(self._instrument(symbol, str(row["name"]), market, quoteSymbol=provider_symbol))
        if len(rows) != total or len({row["id"] for row in rows}) != total:
            raise ValueError(f"Incomplete/duplicate catalog: {len(rows)} rows, upstream count {total}")
        # Re-read the count to detect changes while walking pages.
        if await count() != total:
            raise ValueError("Directory changed during pagination; a later refresh is required")
        return rows, {"market": market, "count": len(rows), "expectedCount": total,
                       "complete": True, "status": "complete", "source": "Sina Finance full A-share directory",
                       "sourceUrl": source + "?node=" + node, "asOf": _iso(_now()),
                       "pages": math.ceil(total / 100),
                       "note": "All pages fetched; unique rows match the provider's declared count. Exchange-independent certification unavailable."}

    async def _us_catalog(self, market, url):
        response = await self._get(url)
        raw = response.text
        if "File Creation Time:" not in raw:
            raise ValueError("The Nasdaq directory has no completion trailer")
        rows = []
        upstream_count = 0
        for row in csv.DictReader(io.StringIO(raw), delimiter="|"):
            symbol = row.get("Symbol" if market == "NASDAQ" else "ACT Symbol", "")
            if not symbol or symbol.startswith("File Creation Time:"):
                continue
            upstream_count += 1
            if row.get("Test Issue") != "N" or row.get("ETF") == "Y":
                continue
            if market == "NYSE" and row.get("Exchange") != "N":
                continue
            if not re.fullmatch(r"[A-Za-z0-9.$^/\-]+", symbol):
                raise ValueError(f"Unexpected directory symbol: {symbol}")
            name = row.get("Security Name", symbol)
            cqs = row.get("CQS Symbol") or symbol
            yahoo = symbol.replace("$", "-P").replace(".", "-")
            rows.append(self._instrument(symbol, name, market, "USD", quoteSymbol="us" + cqs,
                                         yahooSymbol=yahoo))
        if len(rows) < 1000 or len({row["id"] for row in rows}) != len(rows):
            raise ValueError(f"Incomplete Nasdaq Trader directory: {len(rows)} securities")
        trailer = next(line for line in raw.splitlines() if line.startswith("File Creation Time:"))
        return rows, {"market": market, "count": len(rows), "expectedCount": len(rows), "complete": True,
                       "status": "complete", "source": "Nasdaq Trader official symbol directory", "sourceUrl": url,
                       "asOf": _iso(_now()), "sourceDate": trailer.split("|")[0].replace("File Creation Time:", "").strip(),
                       "upstreamFileRows": upstream_count,
                       "note": "All non-test, non-ETF securities listed on this exchange, including common and preferred shares, units and warrants. NYSE American and NYSE Arca are separate exchanges."}

    async def search(self, query="", market="", limit=100, offset=0):
        query = str(query).strip().casefold()[:100]
        market = str(market).strip().upper()
        limit = max(1, min(int(limit), 250))
        offset = max(0, int(offset))
        china = market in ("A", "A-SHARES", "A股", "CN", "CHINA")
        rows = [row for row in self.catalog.values()
                if (not market or row["market"] == market or (china and row["market"] in ("SSE", "SZSE", "BSE")))
                and (not query or any(query in str(row[k]).casefold() for k in ("id", "symbol", "name")))]
        rows.sort(key=lambda row: (list(MARKET_NAMES).index(row["market"]), row["symbol"]))
        return {"items": rows[offset:offset + limit], "total": len(rows), "coverage": self.coverage}

    def _resolve(self, instrument_id):
        if instrument_id in self.catalog:
            return self.catalog[instrument_id]
        normalized = str(instrument_id).upper()
        if normalized in self.catalog:
            return self.catalog[normalized]
        matches = [r for r in self.catalog.values() if r["symbol"].upper() == normalized]
        if len(matches) == 1:
            return matches[0]
        raise ValueError("Unknown or ambiguous instrument ID; select an instrument from the catalog")

    def _calendar(self, market):
        code = "XSHG" if market in ("SSE", "SZSE", "BSE") else "XNYS"
        if code not in self._calendars:
            self._calendars[code] = xcals.get_calendar(code)
        return self._calendars[code]

    def _session(self, market, day):
        if market in ("SSE", "SZSE", "BSE"):
            if day.weekday() >= 5 or any(date.fromisoformat(start) <= day <= date.fromisoformat(end) for start, end in CN_HOLIDAY_RANGES):
                return None
        calendar = self._calendar(market)
        key = str(day)
        if not calendar.is_session(key):
            return None
        opening = calendar.session_open(key).to_pydatetime()
        closing = calendar.session_close(key).to_pydatetime()
        return opening, closing

    def market_status(self, market, now=None):
        market = market.upper()
        if market not in MARKET_NAMES:
            raise ValueError("Unknown market")
        now = now or _now()
        if isinstance(now, str):
            now = _parse_ts(now)
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        result = {"id": market, "name": MARKET_NAMES[market], "isOpen": False}
        if market == "CRYPTO":
            return {**result, "isOpen": True, "reason": "Spot trading is open 24/7"}
        china = market in ("SSE", "SZSE", "BSE")
        local = now.astimezone(SHANGHAI if china else NEW_YORK)
        try:
            session = self._session(market, local.date())
            if session and session[0] <= now < session[1]:
                if china and time(11, 30) <= local.time().replace(tzinfo=None) < time(13):
                    result.update(reason="Lunch break; afternoon session starts at 13:00 China time",
                                  nextOpen=_iso(datetime.combine(local.date(), time(13), SHANGHAI)))
                else:
                    return {**result, "isOpen": True, "reason": "Regular trading session"}
            else:
                holiday = china and any(date.fromisoformat(start) <= local.date() <= date.fromisoformat(end) for start, end in CN_HOLIDAY_RANGES)
                result["reason"] = "Exchange holiday" if holiday else ("Weekend" if local.weekday() >= 5 else "Outside regular trading hours / exchange holiday")
            if not result.get("nextOpen"):
                for offset in range(46):
                    next_session = self._session(market, local.date() + timedelta(days=offset))
                    if next_session and next_session[0] > now:
                        result["nextOpen"] = _iso(next_session[0])
                        break
            result["calendarSource"] = HOLIDAY_SOURCE if china else "exchange_calendars XNYS"
        except Exception as exc:
            result["reason"] = "Trading calendar unavailable; trading is closed for safety"
            result["calendarError"] = str(exc)[:180]
        return result

    async def get_quote(self, instrument_id):
        instrument = self._resolve(instrument_id)
        key = instrument["id"]
        cached = self._quotes.get(key)
        cache_seconds = 5 if instrument['kind'] == 'crypto' else 30
        if cached and (_now() - cached[0]).total_seconds() < cache_seconds:
            self._quotes.move_to_end(key)
            return {**cached[1], "delaySeconds": max(0, int((_now() - _parse_ts(cached[1]["timestamp"])).total_seconds()))}
        if key not in self._quote_tasks:
            self._quote_tasks[key] = asyncio.create_task(self._fetch_quote(instrument))
        task = self._quote_tasks[key]
        try:
            result = await asyncio.shield(task)
            self._quotes[key] = (_now(), result)
            self._quotes.move_to_end(key)
            while len(self._quotes) > 512:
                self._quotes.popitem(last=False)
            return result
        finally:
            if task.done() and self._quote_tasks.get(key) is task:
                self._quote_tasks.pop(key, None)

    def _quote_result(self, instrument, price, timestamp, source, url, change=None, **extra):
        timestamp = timestamp.astimezone(UTC)
        if timestamp > _now() + timedelta(minutes=2):
            raise ValueError("The market data source returned a future timestamp")
        return {"instrumentId": instrument["id"], "symbol": instrument["symbol"], "name": instrument["name"],
                "market": instrument["market"], "price": _positive(price), "currency": instrument["currency"],
                "timestamp": _iso(timestamp), "source": source, "sourceUrl": url,
                "changePct": change, "delaySeconds": max(0, int((_now() - timestamp).total_seconds())), **extra}

    async def _fetch_quote(self, instrument):
        errors = []
        if instrument["kind"] == "crypto":
            symbol = instrument["symbol"]
            url = f"https://api.exchange.coinbase.com/products/{symbol}-USD/ticker"
            try:
                row = (await self._get(url)).json()
                # This is the exchange's last trade time, rather than a fabricated current time.
                return self._quote_result(instrument, row["price"], _parse_ts(row["time"]), "Coinbase Exchange last trade", url)
            except Exception as exc:
                errors.append(str(exc))
            url = "https://api.coingecko.com/api/v3/simple/price"
            try:
                name = "bitcoin" if symbol == "BTC" else "ethereum"
                row = (await self._get(url, params={"ids": name, "vs_currencies": "usd",
                    "include_last_updated_at": "true", "include_24hr_change": "true"})).json()[name]
                change = row.get("usd_24h_change")
                return self._quote_result(instrument, row["usd"], datetime.fromtimestamp(row["last_updated_at"], UTC),
                                          "CoinGecko USD aggregate spot quote", url, change)
            except Exception as exc:
                errors.append(str(exc))
            try:
                return await self._yahoo_quote(instrument)
            except Exception as exc:
                errors.append(str(exc))
        else:
            # Tencent gives broad A-share and US coverage, including class-share symbols.
            try:
                return await self._tencent_quote(instrument)
            except Exception as exc:
                errors.append(str(exc))
            try:
                return await self._yahoo_quote(instrument)
            except Exception as exc:
                errors.append(str(exc))
        raise ValueError(f"Quote unavailable for {instrument['id']}: " + "; ".join(errors)[-400:])

    async def _tencent_quote(self, instrument):
        vendor = instrument.get("quoteSymbol")
        if not vendor:
            raise ValueError("No Tencent symbol mapping")
        url = "https://qt.gtimg.cn/q=" + quote(vendor, safe=".$-")
        response = await self._get(url)
        response.encoding = "gb18030"
        match = re.search(r'=\s*"([^"]+)"', response.text)
        fields = match.group(1).split("~") if match else []
        if len(fields) < 33 or fields[0] in ("1", "51", "62") and instrument["market"] not in ("SSE", "SZSE", "BSE"):
            raise ValueError("Vendor returned no supported instrument")
        if instrument["market"] in ("SSE", "SZSE", "BSE"):
            if fields[2] != instrument["symbol"]:
                raise ValueError("Quote symbol mismatch")
            stamp = datetime.strptime(fields[30], "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI)
        else:
            expected = vendor[2:]
            if fields[0] != "200" or not (fields[2] == expected or fields[2].startswith(expected + ".")):
                raise ValueError("US quote symbol mismatch")
            if fields[35] != "USD":
                raise ValueError("US quote currency mismatch")
            stamp = datetime.strptime(fields[30], "%Y-%m-%d %H:%M:%S").replace(tzinfo=NEW_YORK)
        change = float(fields[32]) if fields[32] else None
        if change is not None and not math.isfinite(change):
            change = None
        return self._quote_result(instrument, fields[3], stamp, "Tencent Finance last trade (may be delayed)", url, change)

    def _yahoo_symbol(self, instrument):
        if instrument["kind"] == "crypto":
            return instrument["symbol"] + "-USD"
        if instrument["market"] in ("SSE", "SZSE", "BSE"):
            return instrument["symbol"] + {"SSE": ".SS", "SZSE": ".SZ", "BSE": ".BJ"}[instrument["market"]]
        return instrument.get("yahooSymbol", instrument["symbol"])

    async def _yahoo_chart(self, instrument, interval="5m", period="1d"):
        symbol = self._yahoo_symbol(instrument)
        url = "https://query1.finance.yahoo.com/v8/finance/chart/" + quote(symbol, safe=".-^")
        response = await self._get(url, params={"interval": interval, "range": period})
        data = response.json().get("chart", {})
        rows = data.get("result") or []
        if not rows:
            raise ValueError("Yahoo returned no chart")
        return rows[0], url

    async def _yahoo_quote(self, instrument):
        row, url = await self._yahoo_chart(instrument)
        meta = row["meta"]
        if meta.get("currency") != instrument["currency"]:
            raise ValueError("Yahoo currency mismatch")
        price = _positive(meta["regularMarketPrice"])
        previous = meta.get("chartPreviousClose") or meta.get("previousClose")
        change = (price / _positive(previous) - 1) * 100 if previous else None
        return self._quote_result(instrument, price, datetime.fromtimestamp(meta["regularMarketTime"], UTC),
                                  "Yahoo Finance regular market quote (may be delayed)", url, change)

    async def get_fx(self):
        async with self._fx_lock:
            if self._fx and (_now() - self._fx[0]).total_seconds() < 1800:
                return self._fx[1]
            url = "https://open.er-api.com/v6/latest/USD"
            row = (await self._get(url)).json()
            if row.get("result") != "success" or row.get("base_code") != "USD":
                raise ValueError("USD/CNY exchange-rate feed unavailable")
            timestamp = datetime.fromtimestamp(int(row["time_last_update_unix"]), UTC)
            if timestamp > _now() + timedelta(minutes=2):
                raise ValueError("FX provider returned a future timestamp")
            result = {"rate": _positive(row["rates"]["CNY"]), "timestamp": _iso(timestamp),
                      "source": "ExchangeRate-API public daily reference rate", "sourceUrl": url,
                      "base": "USD", "quote": "CNY", "updateFrequency": "daily",
                      "delaySeconds": max(0, int((_now() - timestamp).total_seconds()))}
            self._fx = (_now(), result)
            return result

    async def overview(self):
        featured_ids = [key for key in ("CRYPTO:BTC", "CRYPTO:ETH", "NASDAQ:AAPL", "NYSE:BRK.B", "SSE:600519", "SZSE:000001") if key in self.catalog]
        responses = await asyncio.gather(self.get_fx(), *(self.get_quote(key) for key in featured_ids), return_exceptions=True)
        result = {"markets": [self.market_status(market) for market in MARKET_NAMES],
                  "coverage": self.coverage, "featured": [], "errors": []}
        if isinstance(responses[0], Exception):
            result["fxError"] = str(responses[0])[:300]
        else:
            result["fx"] = responses[0]
        for key, response in zip(featured_ids, responses[1:]):
            if isinstance(response, Exception):
                result["errors"].append({"instrumentId": key, "error": str(response)[:300]})
            else:
                result["featured"].append(response)
        return result

    async def history(self, instrument_id):
        instrument = self._resolve(instrument_id)
        key = instrument["id"]
        cached = self._history.get(key)
        if cached and (_now() - cached[0]).total_seconds() < 300:
            return cached[1]
        points = []
        if instrument["kind"] == "crypto":
            url = f"https://api.exchange.coinbase.com/products/{instrument['symbol']}-USD/candles"
            rows = (await self._get(url, params={"granularity": 3600})).json()
            if not isinstance(rows, list):
                raise ValueError("Crypto price history unavailable")
            points = [{"timestamp": _iso(datetime.fromtimestamp(float(row[0]), UTC)), "price": _positive(row[4])}
                      for row in rows if isinstance(row, list) and len(row) >= 5]
        else:
            vendor = instrument["quoteSymbol"]
            url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
            try:
                row = (await self._get(url, params={"param": f"{vendor},day,,,120,qfq"})).json()
                dataset = row.get("data", {}).get(vendor, {})
                rows = dataset.get("qfqday") or dataset.get("day") or []
                tz = SHANGHAI if instrument["currency"] == "CNY" else NEW_YORK
                close_time = time(15) if instrument["currency"] == "CNY" else time(16)
                points = [{"timestamp": _iso(datetime.combine(date.fromisoformat(row[0]), close_time, tz)),
                           "price": _positive(row[2])} for row in rows]
            except Exception:
                points = []
            if not points:
                chart, _ = await self._yahoo_chart(instrument, "1d", "6mo")
                if chart.get("meta", {}).get("currency") != instrument["currency"]:
                    raise ValueError("Price history currency mismatch")
                closes = chart["indicators"]["quote"][0]["close"]
                points = [{"timestamp": _iso(datetime.fromtimestamp(stamp, UTC)), "price": _positive(price)}
                          for stamp, price in zip(chart.get("timestamp", []), closes) if price is not None]
        points = sorted((p for p in points if _parse_ts(p["timestamp"]) <= _now()), key=lambda p: p["timestamp"])
        if not points:
            raise ValueError(f"History unavailable for {key}")
        self._history[key] = (_now(), points)
        while len(self._history) > 128:
            self._history.popitem(last=False)
        return points
