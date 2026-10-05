# Market data and coverage

This arena uses real public market data and virtual money. `1 VC = 1 CNY` is the
ledger unit. USD securities and cryptocurrency are valued using the public
USD/CNY reference rate. Catalog inclusion and availability of a usable quote are
reported separately: a complete catalog does not guarantee that every provider
can price every security.

## Catalog snapshot

The first complete local snapshot, fetched **2026-10-04 09:19 UTC / 17:19 China
time**, contains **12,743 unique instruments**:

| Market | Instruments | Provider fetch time (UTC) | Verification |
| --- | ---: | --- | --- |
| Shanghai A-shares | 2,319 | 2026-10-04 09:19:27 | 24 pages; matches Sina count |
| Shenzhen A-shares | 2,904 | 2026-10-04 09:19:27 | 30 pages; matches Sina count |
| Beijing A-shares | 348 | 2026-10-04 09:19:27 | 4 pages; matches Sina count |
| NASDAQ | 4,333 | 2026-10-04 09:19:20 | Complete official directory, filtered |
| NYSE | 2,837 | 2026-10-04 09:19:20 | Complete official directory, filtered |
| BTC / ETH | 2 | Initialized locally | Exactly the two requested spot assets |

These are dated observations, not permanent expected counts. Listings change.
`data/market_catalog.json` stores the instruments and each market's source URL,
fetch timestamp, expected count, completeness and refresh status. Offline tests
check internal consistency rather than freezing this snapshot's counts.

### China

Sina Finance supplies A-share directories via these nodes: `sh_a` (Shanghai),
`sz_a` (Shenzhen), and `hs_bjs` (Beijing). The application reads the declared
stock count, fetches every page at 100 rows per page, checks each code and
exchange prefix, rejects duplicate/missing rows, then checks the declared count
again to catch a change during pagination.

- [Count endpoint, Shanghai example](https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount?node=sh_a)
- [Paged directory, Shanghai example](https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?node=sh_a&page=1&num=100&sort=symbol&asc=1)

Completeness here means all unique rows supplied by Sina match Sina's declared
count. It is not independently certified against all three exchange registries.
The A-share nodes exclude the separate B-share and fund directories.

### United States

The sources are the Nasdaq Trader official text directories:

- [NASDAQ-listed directory](https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt)
- [Other-listed directory](https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt)
- [Official field definitions](https://www.nasdaqtrader.com/Trader.aspx?id=SymbolDirDefs)

The initial files both carry the creation trailer `1002202621:31`. The complete
files contain 5,634 and 7,661 security rows before filtering. The arena includes
only non-test, non-ETF rows. The NYSE selection additionally requires exchange
code `N`; NYSE American and NYSE Arca are separate exchanges and are excluded.
Preferred shares, depositary shares, units, warrants and class shares remain in
the selected directories. This is an exchange security universe, rather than a
common-share-only universe. IDs retain official symbols such as `NYSE:BRK.B`
and `NYSE:BAC$B`; provider-specific quote symbols are stored separately.

Refreshes require a completion trailer, a plausible minimum row count and unique
IDs. Nasdaq's files do not declare a separate filtered stock count, so the US
expected count is the number of eligible rows in the fully parsed file.

## Quotes and history

| Data | Primary source | Fallbacks / limitations |
| --- | --- | --- |
| BTC and ETH USD last trade | [Coinbase BTC ticker](https://api.exchange.coinbase.com/products/BTC-USD/ticker), [ETH ticker](https://api.exchange.coinbase.com/products/ETH-USD/ticker) | CoinGecko aggregate USD price, then Yahoo Finance |
| A-share and US equity last trade | [Tencent Finance, AAPL example](https://qt.gtimg.cn/q=usAAPL) | Yahoo chart metadata; public equity data may be delayed |
| USD/CNY reference rate | [ExchangeRate-API daily USD feed](https://open.er-api.com/v6/latest/USD) | No invented rate if the feed is unavailable |
| BTC/ETH hourly candles | [Coinbase BTC candles](https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=3600) | Up to the public endpoint's candle window |
| Equity daily history | [Tencent daily candles, AAPL example](https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=usAAPL,day,,,120,qfq) | Yahoo daily chart over six months |

The cryptocurrency fallback is
[CoinGecko's public simple-price endpoint](https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_last_updated_at=true&include_24hr_change=true).
The final quote fallback uses
[Yahoo's chart endpoint, AAPL example](https://query1.finance.yahoo.com/v8/finance/chart/AAPL?interval=5m&range=1d).
These public services can throttle, change formats or omit unusual securities.
An unsupported preferred share, warrant or other instrument receives an explicit
quote-unavailable error; no substitute asset price or fabricated quote is used.

Provider timestamps are retained as UTC `timestamp` values. Cache fetch time is
not substituted for the provider's last trade time. Equity quotes verify symbol
and currency when the source supplies those fields. All prices and FX rates
must be positive and finite; implausibly future timestamps are rejected.
Quotes are cached for 30 seconds with concurrent requests shared. The FX feed
is a **daily reference rate**, cached for 30 minutes, not an intraday executable
currency quote.

Read-only live verification at **2026-10-04 09:25:48 UTC / 17:25:48 China time**
returned these provider timestamps:

| Instrument / feed | Source | Original timestamp (UTC) |
| --- | --- | --- |
| BTC | Coinbase last trade | 2026-10-04 09:25:43.345716 |
| ETH | Coinbase last trade | 2026-10-04 09:25:43.608761 |
| AAPL | Tencent last trade | 2026-10-02 20:00:01 |
| BRK.B | Tencent last trade | 2026-10-02 20:04:35 |
| 600519 | Tencent last trade | 2026-09-30 08:14:58 |
| USD/CNY | ExchangeRate-API daily update | 2026-10-04 00:02:32 |

The older equity timestamps reflect closed stock markets at verification time.
The dashboard displays source and quote age. Order execution separately rejects
closed markets and stale/unusable quotes or FX. A cached portfolio estimate
does not grant permission to execute a trade from that estimate.

## Trading calendars

China uses Asia/Shanghai time, weekdays 09:30–11:30 and 13:00–15:00, with exchange
holidays. The official
[SSE 2026 closure notice](https://www.sse.com.cn/disclosure/dealinstruc/closed/c/c_20251222_10802510.shtml),
published 2025-12-22 and checked 2026-10-04, overrides calendar predictions for
the listed 2026 Chinese holidays. National Day closes trading October 1–7;
the next China session is **October 8 at 09:30 China time**. The same published
holiday ranges are applied to the Shanghai, Shenzhen and Beijing simulation
venues. A future year's official notice must be reviewed when extending this
override beyond 2026.

US sessions use `exchange_calendars`' `XNYS` calendar and America/New_York time
for NASDAQ and NYSE. UTC opening times follow New York daylight saving time;
holiday closures and early closes are respected. For example, November 27,
2026 closes at 13:00 New York time. Crypto spot trading is open 24/7. If calendar
lookup fails, the stock market is marked closed with a visible error.

On **Sunday, October 4, 2026**, only BTC and ETH are open for trading in this
arena. Stock catalogs remain searchable while their markets are closed.

## Outages and refresh behavior

A completed catalog is reused for six hours after its fetch time. Refreshes
update each market independently. A failed refresh preserves available cached
rows, labels them `stale_cache` and includes the error; a market with no saved
rows is `unavailable`. A saved count mismatch downgrades completeness. The
dashboard should therefore assess `complete`, `status`, `asOf` and quote age
together rather than assuming that a searchable instrument is immediately
tradeable.

## Dated 2023 replay sources

Historical stock prices never fall back to live prices. China stocks use [Tencent unadjusted daily bars](https://web.ifzq.gtimg.cn/appstock/app/fqkline/get); US securities use Sina Finance's public `US_MinKService.getDailyK` dated daily-bar endpoint. A real dated bar must exist for the selected catalog ID. Before a day's close, its close/high/low are excluded. The daily open timestamp is the exchange-calendar session boundary corresponding to that source bar date; it is not a historical tick timestamp.

BTC/ETH use [Coinbase Exchange completed historical candles](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles). USD/CNY uses the [Federal Reserve H.10 historical CNY series](https://www.federalreserve.gov/releases/h10/hist/dat00_ch.htm). The replay imposes a conservative ten-day availability lag and rejects FX observations more than fourteen days old at the source clock. The current published series is not a certified original historical vintage or executable FX spread.

The historical provider records actual source dates, URLs, bar types and the separate replay clock. Its discovery catalog is the current directory, with explicit survivorship/issuer-identity limits. No future article, source observation or completed candle enters the replay through the dated provider. Pretrained knowledge and unmodeled corporate actions prevent claiming a certified point-in-time strategy test.
