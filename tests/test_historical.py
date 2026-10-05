# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

import asyncio
from datetime import datetime, timedelta, timezone
import json

import httpx
import pytest

from arena.historical import HistoricalMarketService, US_HISTORY_URL, CHINA_HISTORY_URL, FX_HISTORY_URL
from arena.markets import MarketService


UTC = timezone.utc
US_OPEN = datetime(2023, 6, 1, 13, 30, tzinfo=UTC)
CN_OPEN = datetime(2023, 6, 1, 1, 30, tzinfo=UTC)


@pytest.fixture
def service(tmp_path):
    main = MarketService(tmp_path)
    rows = [main._instrument("AAPL", "Apple", "NASDAQ", "USD", quoteSymbol="usAAPL"),
            main._instrument("600519", "Moutai", "SSE", quoteSymbol="sh600519"),
            main._instrument("BTC", "Bitcoin", "CRYPTO", "USD", "crypto"),
            main._instrument("NEW", "Later IPO", "NASDAQ", "USD", quoteSymbol="usNEW")]
    main.catalog = {row["id"]: row for row in rows}
    main.coverage = {"NASDAQ": {"count": 2, "complete": True, "asOf": "2026-10-04T00:00:00Z"}}
    historical = HistoricalMarketService(main, replay_time=US_OPEN)
    yield historical
    asyncio.run(historical.close())
    asyncio.run(main.close())


def response(url, payload=None, text=None):
    request = httpx.Request("GET", url)
    return httpx.Response(200, request=request, json=payload) if text is None else httpx.Response(200, request=request, text=text)


def us_rows():
    return [{"d": "2023-05-31", "o": "100", "c": "101", "h": "102", "l": "99"},
            {"d": "2023-06-01", "o": "103", "c": "199", "h": "205", "l": "90"},
            {"d": "2023-06-02", "o": "250", "c": "299", "h": "305", "l": "240"}]


def mock_us(service, monkeypatch, rows=None):
    async def fake_get(url, **kwargs):
        assert url == US_HISTORY_URL
        return response(url, text="/* JSON callback */\nIO.XSRV2.CallbackList(" + json.dumps(rows if rows is not None else us_rows()) + ");")
    monkeypatch.setattr(service, "_get", fake_get)


def test_random_session_is_real_deterministic_and_not_always_same_venue(service):
    picked = service.choose_random_session("repeatable", "NASDAQ")
    assert picked == service.choose_random_session("repeatable", "NASDAQ")
    assert picked.year == 2023 and service.market_status("NASDAQ", picked)["isOpen"]
    assert picked.month <= 4
    bounded = service.choose_random_session(21, "NASDAQ", "2023-11-01", "2023-11-30")
    assert bounded.month == 11 and service.market_status("NASDAQ", bounded)["isOpen"]
    china = service.choose_random_session(21, "SSE")
    assert service.market_status("SSE", china)["isOpen"]
    assert not service.market_status("NASDAQ", china)["isOpen"]
    assert not service.market_status("SSE", picked)["isOpen"]


def test_closed_historical_world_keeps_valuation_marks_but_rejects_trading(service, monkeypatch):
    mock_us(service, monkeypatch)
    before = asyncio.run(service.get_quote("NASDAQ:AAPL"))
    service.active = False
    after = asyncio.run(service.get_quote("NASDAQ:AAPL"))
    assert after["price"] == before["price"] == 103
    assert not after["validForExecution"]
    assert not service.market_status("NASDAQ")["isOpen"]
    assert not service.market_status("CRYPTO")["isOpen"]
    assert not service.metadata()["active"]


def test_replay_calendar_uses_2023_holiday_lunch_dst(service):
    assert not service.market_status("SSE", "2023-10-02T02:00:00Z")["isOpen"]
    assert not service.market_status("SSE", "2023-06-01T04:00:00Z")["isOpen"]
    assert service.market_status("NASDAQ", "2023-03-10T14:30:00Z")["isOpen"]
    assert service.market_status("NASDAQ", "2023-03-13T13:30:00Z")["isOpen"]
    assert "2023" in service.market_status("SSE", CN_OPEN)["calendarSource"]


def test_clock_must_be_aware_and_2023_and_advance_never_wraps(service):
    with pytest.raises(ValueError, match="timezone"):
        service.set_clock(datetime(2023, 6, 1))
    with pytest.raises(ValueError, match="2023"):
        service.set_clock("2026-10-04T00:00:00Z")
    service.set_clock(CN_OPEN)
    assert service.advance_session() == US_OPEN
    service.set_clock("2023-12-29T20:00:00Z")
    with pytest.raises(ValueError, match="final"):
        service.advance_session()


def test_stock_open_hides_same_day_close_high_low_and_future_prices(service, monkeypatch):
    mock_us(service, monkeypatch)
    value = asyncio.run(service.get_quote("NASDAQ:AAPL"))
    assert value["price"] == 103
    assert value["timestamp"] == "2023-06-01T13:30:00Z"
    assert value["validForExecution"] and value["validUntil"] == "2023-06-01T20:00:00Z"
    assert not {"open", "close", "high", "low", "futurePrice"} & value.keys()
    assert value["changePct"] == pytest.approx((103 / 101 - 1) * 100)
    points = asyncio.run(service.history("NASDAQ:AAPL"))
    assert [point["price"] for point in points] == [101, 103]
    assert [point["barType"] for point in points] == ["daily_close", "daily_open"]
    assert all(point["timestamp"] <= value["replayTime"] for point in points)


def test_stock_close_revealed_only_after_session_and_is_not_executable(service, monkeypatch):
    mock_us(service, monkeypatch)
    service.set_clock("2023-06-01T19:59:59Z")
    assert asyncio.run(service.get_quote("NASDAQ:AAPL"))["price"] == 103
    service.set_clock("2023-06-01T20:00:00Z")
    value = asyncio.run(service.get_quote("NASDAQ:AAPL"))
    assert value["price"] == 199 and value["barType"] == "daily_close"
    assert not value["validForExecution"]


def test_suspended_date_and_post_2023_listing_never_get_live_substitution(service, monkeypatch):
    mock_us(service, monkeypatch, [us_rows()[0], us_rows()[2]])
    value = asyncio.run(service.get_quote("NASDAQ:AAPL"))
    assert value["price"] == 101 and not value["validForExecution"]
    mock_us(service, monkeypatch, [{"d": "2024-01-02", "o": 50, "c": 52, "h": 53, "l": 49}])
    with pytest.raises(ValueError, match="2023 daily bars"):
        asyncio.run(service.get_quote("NASDAQ:NEW"))


def test_china_uses_unadjusted_bar_and_actual_cn_session_open(service, monkeypatch):
    calls = []
    async def fake_get(url, **kwargs):
        calls.append(kwargs)
        assert url == CHINA_HISTORY_URL
        return response(url, {"data": {"sh600519": {"day": [
            ["2023-05-31", "100", "101", "102", "99"],
            ["2023-06-01", "103", "199", "205", "90"]], "qfqday": [["2023-06-01", "1", "2", "3", "1"]]}}})
    monkeypatch.setattr(service, "_get", fake_get)
    service.set_clock(CN_OPEN)
    value = asyncio.run(service.get_quote("SSE:600519"))
    assert value["price"] == 103 and value["currency"] == "CNY"
    assert value["timestamp"] == "2023-06-01T01:30:00Z"
    assert value["validForExecution"]
    assert calls[0]["params"]["param"].endswith(",")
    assert "qfq" not in calls[0]["params"]["param"]


def test_crypto_uses_only_fully_completed_hour_candles(service, monkeypatch):
    service.set_clock("2023-06-01T13:30:00Z")
    async def fake_get(url, **kwargs):
        assert kwargs["params"]["granularity"] == 3600
        base = datetime(2023, 6, 1, 12, tzinfo=UTC)
        return response(url, [[base.timestamp(), 90, 120, 100, 110],
            [(base + timedelta(hours=1)).timestamp(), 80, 1000, 110, 999],
            [(base + timedelta(hours=2)).timestamp(), 80, 2000, 999, 1999]])
    monkeypatch.setattr(service, "_get", fake_get)
    value = asyncio.run(service.get_quote("CRYPTO:BTC"))
    assert value["price"] == 110 and value["timestamp"] == "2023-06-01T13:00:00Z"
    assert value["validForExecution"] and value["delaySeconds"] == 1800
    points = asyncio.run(service.history("CRYPTO:BTC"))
    assert len(points) == 1 and points[0]["price"] == 110


def test_fx_reference_excludes_later_observation_and_discloses_vintage_limit(service, monkeypatch):
    async def fake_get(url, **kwargs):
        assert url == FX_HISTORY_URL
        return response(url, text="""
          <tr><th id="a">19-MAY-23</th><td>7.01</td></tr>
          <tr><th id="b">22-MAY-23</th><td>7.02</td></tr>
          <tr><th id="c">1-JUN-23</th><td>9.99</td></tr>
        """)
    monkeypatch.setattr(service, "_get", fake_get)
    value = asyncio.run(service.get_fx())
    assert value["rate"] == 7.01
    assert value["timestamp"] == "2023-05-19T16:00:00Z"
    assert value["conservativeAvailableAt"] <= value["replayTime"]
    assert value["referenceLagDays"] == 10 and not value["pointInTimeVintageCertified"]
    assert value["replay"] and value["world"] == "historical"


def test_disk_cache_does_not_reveal_future_rows_when_clock_changes(service, monkeypatch):
    mock_us(service, monkeypatch)
    first = asyncio.run(service.get_quote("NASDAQ:AAPL"))
    service.set_clock("2023-06-02T13:30:00Z")
    assert asyncio.run(service.get_quote("NASDAQ:AAPL"))["price"] == 250
    service._bars.clear()
    service.set_clock(US_OPEN)
    async def unavailable(*args, **kwargs):
        raise AssertionError("Cached dated bars should be reused")
    monkeypatch.setattr(service, "_get", unavailable)
    assert asyncio.run(service.get_quote("NASDAQ:AAPL"))["price"] == first["price"]


def test_history_availability_and_current_catalog_limitations_are_explicit(service):
    result = asyncio.run(service.search("Apple"))
    assert result["items"][0]["historicalAvailability"] == "unverified_until_dated_bar_loaded"
    assert not result["coverage"]["NASDAQ"]["complete"]
    assert result["survivorshipBias"] and not result["pointInTimeUniverseCertified"]
    assert not result["corporateActionsIncluded"]


@pytest.mark.parametrize("defect", ["duplicate", "negative", "inconsistent"])
def test_malformed_historical_bars_are_rejected(service, monkeypatch, defect):
    rows = us_rows()
    if defect == "duplicate":
        rows.append(rows[1])
    elif defect == "negative":
        rows[1]["o"] = -1
    else:
        rows[1]["h"] = 10
        service.set_clock("2023-06-01T20:00:00Z")
    mock_us(service, monkeypatch, rows)
    with pytest.raises(ValueError):
        asyncio.run(service.get_quote("NASDAQ:AAPL"))


def test_bad_future_ohlc_cannot_block_an_earlier_genuine_open(service, monkeypatch):
    rows = us_rows()
    rows[2]["h"] = 1
    mock_us(service, monkeypatch, rows)
    assert asyncio.run(service.get_quote("NASDAQ:AAPL"))["price"] == 103
    service.set_clock("2023-06-02T13:30:00Z")
    assert asyncio.run(service.get_quote("NASDAQ:AAPL"))["price"] == 250
    service.set_clock("2023-06-02T20:00:00Z")
    with pytest.raises(ValueError, match="inconsistent"):
        asyncio.run(service.get_quote("NASDAQ:AAPL"))
