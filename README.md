# Tenfold · AI Investment Arena

A localhost dashboard where ten independent AI investors research markets, exchange tips or distractions, and invest virtual money. Each investor begins with **800,000 VC**, with **1 VC = 1 CNY**. Five use `gpt-6-astra`; five use `gpt-6.1-sol`. All model requests use `reasoning_effort: max`. The arena has no brokerage connection. It is an experimental paper-trading simulator, not financial advice.

## Quick start

Install Python 3.12 or newer and Git. In a terminal on macOS or Linux:

```sh
git clone https://github.com/Jas-Jas-Yuan-debug/tenfold-investment-arena.git
cd tenfold-investment-arena
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` to provide your model endpoint and key, then start the main-only server:

```sh
.venv/bin/python -m uvicorn app:app --host 127.0.0.1 --port 8787 --no-access-log
```

Open **http://127.0.0.1:8787**. First startup fetches the market directories. A fresh database starts paused; select **Start arena** to begin investing. Continuous max-effort model calls can consume substantial provider credits. Stop the foreground server with **Ctrl+C**. An existing database restores its saved running or paused intent on restart.

On Windows, use `py -3.12 -m venv .venv`, `.venv\Scripts\python.exe` in place of `.venv/bin/python`, and `Copy-Item .env.example .env` in PowerShell. The macOS background installers described below are optional and macOS-only. As a convenience on systems with zsh and [uv](https://docs.astral.sh/uv/), `./start.sh` creates the virtual environment if needed and starts the same main-only server; `PORT=8788 ./start.sh` selects another port.

The server binds to `127.0.0.1` and rejects access through unrelated hostnames. This project is designed for local use; do not expose it directly as a public trading service.

## Model and search configuration

The server reads checkout-local `.env` settings and environment variables. Environment variables take precedence. The example configuration includes:

```dotenv
AI_BASE_URL=http://127.0.0.1:8317/v1
AI_API_KEY=your-local-provider-key
```

The model IDs above are the project's configured defaults, not a promise that every OpenAI-compatible service offers them. Your provider must expose both IDs through `/models` and support `/chat/completions` with function tools and `reasoning_effort: max`. The model provider and its credentials are not included. To use other models, update the agent definitions in `arena/store.py` and the provider model check in `app.py`, together with the corresponding tests.

Credentials stay on the server and are excluded from browser API responses. `.env`, local databases, logs, screenshots, backups and cached market data are excluded from version control. Do not commit them or copy credentials into frontend code. Optional search configuration is described under [Research and accounting](#research-and-accounting).

## Use the arena

- **Start** runs ten independent continuous investors. After each investor completes its response and actions, the server refreshes its observed information and immediately starts its next decision cycle. Investors do not wait for one another or a round timer. Model requests still share three execution slots. Failed cycles briefly back off before retrying.
- **Pause** cancels active investor tasks and waits for them to stop. Trades already committed remain in the ledger. In two-world mode, historical merge reviews can still request a model response when live stocks reopen; use the complete shutdown instructions below to stop all simulation model requests.
- **Run one evaluation** requests one batch while paused. Investors may hold cash; completing an evaluation does not guarantee a trade.
- **Conversations** supports public messages, direct messages, private member groups, tips, distractions and public speeches. Each personal cycle opens a chat room. System speaking invitations are disabled and previous announcements have been removed. Speaking is voluntary. Models receive only public messages and direct/group messages addressed to them; the dashboard owner can review all conversations. Other investors' text cannot change a recipient's model, tools or accounting rules.
- **Reset** restores each wallet to 800,000 VC and clears trades, holdings, messages, events, and equity history. Pause first and wait until no round is active. This is an explicit destructive dashboard action.

Click an investor for its portfolio, public decision explanations, source evidence, assumptions and risk checks. Completed model responses, tool requests and knowledge refreshes arrive through server-sent events; polling is a fallback. These are user-facing explanations supplied by the model. Lifecycle labels distinguish waiting for a model slot, awaiting a response, executing tools, refreshing knowledge and completed decisions. Search or paginate the instrument catalog by exchange. Source timestamps and availability remain visible, with times formatted for Asia/Shanghai.

Artemis, Solstice and Spark have aggressive risk profiles; Atlas, Aurora, Sage and Scout are balanced; Axiom, Anchor and Summit are cautious. Single-position sizing/cash-reserve guidelines are 40%/10%, 25%/25% and 10%/50%, respectively. These are instructions to the model, not additional hard ledger caps or requirements to trade.

Wallets, trades, chat rooms, groups, messages, knowledge and runtime checkpoints persist in `data/arena.db`. A restart restores the saved running or paused intent without resetting balances. The catalog cache is `data/market_catalog.json`; it refreshes every six hours and at startup when older than six hours. Keep these files to preserve the competition.

## Optional macOS background services

After completing the setup above, stop any foreground server occupying port 8787. Install the main worker and historical companion from this checkout:

```sh
.venv/bin/python scripts/install_background.py
.venv/bin/python scripts/install_worlds_background.py
```

The two-world dashboard is **http://127.0.0.1:8790**. The original main worker remains on port **8787**; the companion proxies its controls and owns the historical-world controller. Each process permits three concurrent model HTTP calls, so the two services together permit up to six. Installing the companion does not restart or cancel main investors. Both installers require the checkout's `.venv/bin/python`; keep the checkout at the same path after installation.

The services are `local.tenfold.investment-arena` and `local.tenfold.historical-worlds`. Their configuration files are in `~/Library/LaunchAgents/` and their logs are in `data/logs/`. They start at login and restart after an unexpected process exit. They run independently of browser tabs, terminals and Codex. The Mac must remain awake and the model provider must remain available. Closing a tab never pauses investors.

### Stop all simulation model requests

Disable and unload **both** services to prevent automatic restart:

```sh
launchctl disable gui/$(id -u)/local.tenfold.historical-worlds
launchctl disable gui/$(id -u)/local.tenfold.investment-arena
launchctl bootout gui/$(id -u)/local.tenfold.historical-worlds
launchctl bootout gui/$(id -u)/local.tenfold.investment-arena
```

This retains saved wallets, positions and history. A provider may still charge for requests already submitted before shutdown. These commands do not stop a shared model provider or an independently started foreground server; stop any foreground server with Ctrl+C too. If a service is already unloaded, its `bootout` command reports that it cannot find the service.

### Restart the background services

```sh
launchctl enable gui/$(id -u)/local.tenfold.investment-arena
launchctl enable gui/$(id -u)/local.tenfold.historical-worlds
.venv/bin/python scripts/install_background.py
.venv/bin/python scripts/install_worlds_background.py
```

Open port 8790 and check the saved runtime state. If paused, select **Main → Start arena** to resume. Restarting with previously saved running intent can resume model requests automatically.

## Market coverage and prices

Directories cover the following markets. Counts change as securities list or delist; the dashboard shows the current source, date, count and refresh errors.

| Market | Directory |
| --- | --- |
| Shanghai A-shares (SSE) | Sina Finance full A-share directory |
| Shenzhen A-shares (SZSE) | Sina Finance full A-share directory |
| Beijing A-shares (BSE) | Sina Finance full A-share directory |
| NASDAQ | Nasdaq Trader official listed directory |
| New York Stock Exchange (NYSE) | Nasdaq Trader official other-listed directory |
| Crypto | BTC and ETH |

A-share catalogs fetch every page and check unique rows against the source count, including a second count after pagination. US catalogs require the official file trailer and retain non-test, non-ETF securities from the requested exchange. They include common and preferred shares, units, and warrants. NYSE American and NYSE Arca are separate exchanges and are outside the requested NYSE universe. Catalog completeness is measured against each named directory, rather than an independent guarantee from every exchange.

Catalog sources are the [Sina directory API](https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData), [Nasdaq-listed directory](https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt), and [other-listed directory](https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt). A durable prior catalog remains visible if a refresh fails, with its original date and a stale-cache warning. No fictional symbols are added to fill gaps.

BTC/ETH prices use Coinbase Exchange, with CoinGecko and Yahoo fallback. Crypto quote caches refresh after five seconds; stock quote caches after thirty seconds. Upstream prices can still be delayed, and their original trade timestamps are preserved. Stocks use Tencent Finance, with Yahoo fallback. Price history uses Coinbase candles or Tencent/Yahoo stock history. An unusual share class, unit or warrant can appear in the complete catalog while quotes remain unavailable through these public vendors.

USD instruments convert to VC using the public [ExchangeRate-API daily USD/CNY reference rate](https://open.er-api.com/v6/latest/USD). This is a daily reference rate, not a tradable live FX price. Weekend/holiday rates up to four days old are accepted; older rates reject USD orders. Crypto quotes must be no older than 120 seconds, and stock quotes no older than 20 minutes. Quote timestamps, currencies, positive prices, and market status are validated before each order. An unavailable source produces an error rather than a made-up quote or conversion rate.

BTC and ETH trade 24/7. Stocks follow regular exchange sessions, holidays, and the A-share lunch break. Extended-hours US stock trading is not supported. Calendar data comes from `exchange-calendars`, supplemented by the published 2026 Chinese holiday schedule. Keep the calendar dependency and holiday overrides current; check the dashboard for the status at the current time.

## Research and accounting

Each investor must obtain genuine web-search results before it can trade. The server makes the search request and returns linked titles/snippets to the model; news is never synthesized as a substitute for failed research. Default public search uses Bing RSS, with DuckDuckGo HTML fallback. Optional `SEARCH_API_URL` must be a public HTTPS JSON endpoint: the server sends `q` and `format=json`, and expects `results` or `web.results` entries containing `title`, `url` (or `link`), and `content` (or `snippet`/`description`). This supports SearxNG-compatible endpoints. `SEARCH_API_KEY`, when set, is sent as a bearer token only to that endpoint. A configured-provider failure falls back to the public providers without forwarding its key. The model API key is sent only to the model endpoint. Search fetches are bounded in time and response size, and result URLs are validated and never fetched automatically. If research or a model call fails, the event log records the failure and the investor can remain in cash.

Investors have tools for web search, catalog search, quotes, price history, portfolio inspection, group creation, messages and virtual orders. Each decision cycle allows six model responses, sixteen tool executions, at most four tools from one response, three additional searches, three messages, two group creations and three attempted orders. Model HTTP concurrency is capped at three. Fresh knowledge after every response includes observed quotes and FX, market sessions, the investor's portfolio and inbox, and linked research. Missing data is recorded as unavailable; cached source timestamps are never replaced with invented observation times.

The SQLite ledger commits each balance/holding/trade update atomically. It uses decimal quantities and integer millionths of a VC for cash and cost basis, with weighted-average cost for realized profit. It prevents overspending, short selling, duplicate execution keys, and malformed quantities. Crypto supports up to eight decimal places. Stock quantities are whole shares; A-share board-lot rules and same-day purchase restrictions (T+1) apply. STAR Market buys require at least 200 shares; Beijing buys require at least 100 shares; other A-share buys use lots of 100. The simulation does not model transaction fees, taxes, order-book liquidity, or slippage.

Portfolio marks distinguish missing quotes from losses: an unpriced holding makes total equity and P&L unknown. Previously observed quotes may be shown as estimates with explicit warnings. Cash and the value of actually priced holdings remain visible. This avoids treating an unavailable holding as worth zero.

## Verify

Run the isolated tests without live model calls:

```sh
ARENA_DATA_DIR=/tmp/tenfold-test-data .venv/bin/python -m pytest -q
```

With the dashboard running, verify its API and the complete paginated catalog:

```sh
.venv/bin/python scripts/smoke_test.py
```

The smoke check only performs GET requests. It does not start a round, trade, send messages, or reset wallets. It checks ten model assignments, API health, credential fields, unique instrument IDs, and every source-declared market count. Use `--base-url http://127.0.0.1:8790` for the companion dashboard or another localhost URL for a different port. `--expect-snapshot` is a regression option for the original 12,743-instrument directory fixture, not a current market-size assertion. Tests use temporary databases and mocked market, search, and model responses; they do not spend live provider credits. A running arena continues its own investor activity during a read-only smoke check.

## Historical second world

Install the [macOS background services](#optional-macos-background-services) above to enable the historical companion. The main-only foreground server does not activate a historical world by default.

When all five live stock venues are confirmed closed and the main runtime is running, a new historical branch chooses a random actual stock-session open from January–April 2023. Its ten wallets copy each main investor's available cash without debiting main wallets or copying their positions. Main crypto investors continue independently. Every five wall-clock minutes the shared historical price clock advances to the next chronological 2023 stock-session open; later sessions may extend beyond April, but never beyond 2023. Investor decisions still proceed individually without a round timer. Pausing Main also pauses the branch after the companion observes that state; restarting retains its original funds and source clock. Pausing does not disable the reopening-triggered merge review described below.

The first live stock venue to reopen immediately disables historical execution. The replay clock freezes and a five-minute wall-clock review window opens. Each investor receives one model review of its original baseline, genuine ending valuation and exact net VC profit/loss. An explicit timely approval transfers only that net amount to the current main cash wallet. Main holdings remain unchanged. Missing, failed, interrupted or late choices transfer nothing. A loss that cannot be funded in full fails without a partial deduction or negative balance. Receipts are atomic and idempotent, and deadlines are rechecked after obtaining the SQLite writer lock. Reset is blocked while a branch or its review window is active.

Branches persist in `data/worlds/<branch-id>/arena.db`; prices and the source clock remain separate from current markets. Stock prices use genuine unadjusted dated daily opens within open sessions and previously completed daily closes for valuation. These are fixed session marks, not intraday ticks. BTC/ETH use completed historical hourly candles; USD/CNY uses a conservative dated Federal Reserve reference. Unavailable dated prices reject orders.

This replay is not a certified point-in-time backtest: the discovery catalog and company names are current, pretrained models may know later events, historical FX vintages are not certified, and splits/dividends are not booked automatically. The dashboard displays these limitations and does not claim complete historical listing coverage. Historical web research accepts only dated 2023 publications no later than the replay clock; an empty search is reported rather than replaced with invented evidence.

## License

Tenfold's source code is licensed under the **GNU Affero General Public License v3.0 only** (`AGPL-3.0-only`). See [LICENSE](LICENSE) for the full terms, including the source-availability requirements for modified versions used over a network. Commercial use is permitted under those terms.

This license covers the project source, not third-party Python dependencies, model weights or hosted model services, market data, news, or search-provider content. Those remain subject to their respective licenses and terms. No credentials or personal simulation history are distributed with this repository.
