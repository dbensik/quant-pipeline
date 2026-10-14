# CLAUDE.md — Quant Pipeline

End-to-end modular framework for systematic trading: data ingestion, backtesting, ML models, a FastAPI backend and a React dashboard.

Streamlit (`dashboard_app/`) was deleted on 2026-08-09 after every one of its
features was ported to a router and a React page. Do not reintroduce it.

## Run

```bash
./run_pipeline.sh all          # gRPC + GraphQL + FastAPI + React + verification
./run_pipeline.sh rest         # FastAPI only (REST + websockets, port 8001)
./run_pipeline.sh dashboard    # React dev server only (port 5174)
./run_pipeline.sh api          # GraphQL gateway only (port 8002) — NOT FastAPI
./run_pipeline.sh grpc         # gRPC service only
python -m cli.run_pipeline     # ingest bars into TimescaleDB (same path as the API)
```

`api`/`gateway` means the GraphQL gateway, not the REST API — a naming wart
that predates FastAPI. Use `rest` for FastAPI.

## Ports

**This project deliberately avoids the framework defaults.** uvicorn wants
8000, Vite wants 5173, Postgres wants 5432 — and so does every other
Python/React project on this machine, which makes collisions certain rather
than unlucky. On 2026-08-11 `siting-platform`'s uvicorn held 8000, so the
GraphQL gateway never started and `verify` failed with a 404 that read as a
code fault.

| port | service | default it avoids | override |
|---|---|---|---|
| 8002 | GraphQL gateway | 8000 (uvicorn) | `QUANT_GRAPHQL_PORT` |
| 8001 | FastAPI REST + WS | — | `QUANT_REST_PORT` |
| 5174 | Vite dev server | 5173 (Vite) | `QUANT_VITE_PORT` |
| 15432 | TimescaleDB, **host side only** | 5432 (Postgres) | edit `docker-compose.yml` |
| 50051 | gRPC signal service | — | `QUANT_GRPC_PORT` |

`run_pipeline.sh` checks each port is free before starting anything and names
the process holding it. Without that, a bound port surfaced minutes later as a
connection error against a service that had silently never started.

Three places must agree on the Vite port or the browser gets a CORS failure
that reads like an API bug: `frontend/vite.config.ts`, `QUANT_VITE_PORT`, and
the allow-list in `app/core/config.py`.

Only the DB's **host** port moved. Inside the compose network services still
address `timescaledb:5432`, so container-to-container config is unchanged —
but anything connecting from the host needs `localhost:15432`.

Ingestion goes through `core/ingest.py`, reached by both `POST /api/v1/ingest`
and `cli.run_pipeline`. The SQLite `PipelineOrchestrator` was deleted on
2026-08-09 — it wrote a database nothing read while reporting success.

**Nothing writes over a stored bar.** `--full-backfill` (which overwrote
history to cure split drift) was removed on 2026-09-28: read-time adjustment
made split drift impossible for every served asset, and the overwrite had
destroyed 1390 genuine PARA bars. The API refuses a request that still sends
`full_backfill` (422) rather than quietly resuming. Do not reintroduce a
restating write — see `research/dividend-drift-plan-2026-09-27.md`.

## Scheduled

```bash
launchctl print gui/$(id -u)/com.dbensik.quant-pipeline.daily-maintenance
scripts/launchd/install.sh                  # (re)install after editing the plist
scripts/cron/daily_maintenance.sh           # ingest, risk-free rate, SEC facts, reassigned + missing-day + fresh-return checks, snapshot, DCF line check, option surfaces, backup
scripts/ingest_rates.py                     # risk-free rate (^IRX) only; --as-of DATE prints the rate a reader gets
scripts/check_reassigned.py                 # the check alone; read-only, exit 1 = flagged
scripts/check_missing_days.py               # holes inside series; read-only, exit 1 = lost bars
scripts/check_fresh_returns.py              # returns readers see vs fresh Yahoo, 30 sessions; exit 1 = mismatch
scripts/check_line_concepts.py              # DCF lines another concept reports differently, or a stale per-filer map; exit 1 = flagged
scripts/precompute_surfaces.py --days 7     # option surfaces for the last week + missed capture days; 1 = missed, 2 = failed
scripts/load_fundamentals.py --recent       # SEC facts for registry filers of the last 7 days (--all: everyone, ~10 min)
scripts/backup.py                           # LAST step: DB dump + option archive to QUANT_BACKUP_DIR, proven by a restore
tail -f logs/daily_maintenance.log
```

**launchd, not cron, since 2026-08-25.** cron only fires if the machine is
awake at the scheduled minute and never catches up. Measured: the job did not
run at all from 2026-08-18 to 2026-08-24 — seven consecutive days, no log lines
at all, because the Mac was asleep at 06:00. launchd's `StartCalendarInterval`
runs a missed job on next wake. `install.sh` removes the crontab entry, because
two schedules would race for the lock file and the loser logs "SKIPPED: a run is
already in progress", which reads like a bug.

Ingest runs before the snapshot so a name that joined an index today already
has bars. The job is single-instance (lock file) and aborts with one clear
line if TimescaleDB is unreachable, rather than two stack traces.

**A partial snapshot exits non-zero and posts a notification.** Until
2026-08-25 `scripts/snapshot_universes.py` returned 0 whenever *any* index
succeeded, on the reasoning that one moved page should not page anyone. That is
wrong for un-backdatable data: Wikipedia dropped the constituents table from the
DJIA page on 2026-08-16, `dow_jones` returned empty every morning, the job
logged "Snapshotted 2 of 3" and exited 0, and ten index-days were lost before
anyone looked. Any failure is now non-zero, and the wrapper raises a macOS
notification when it is not attached to a terminal.

**`dow_jones` does not come from Wikipedia** (fixed 2026-08-25). That page
carried a components table until roughly 2026-08-16 and then became a navbox
with no table at all — the source moved, so no amount of parsing would have
helped. Constituents now come from `URL_DOWJONES_CONSTITUENTS` in
`config/settings.py`.

**Every scrape is checked against a plausible count** (`_implausible()` in
`data_pipeline/dynamic_universe.py`) — `DOWJONES_EXPECTED_COUNT = 30` exactly,
`SP500_EXPECTED_RANGE = (480, 520)`. The Dow is thirty stocks by definition; the
S&P needs a range because it targets 500 *companies* but lists more
*securities* (GOOG/GOOGL, FOX/FOXA, NWS/NWSA), so 503 today.

A SHORT read is the case worth guarding. An empty one is already safe — the
caller refuses to record it, since claiming an index has no members is worse
than recording nothing. But 28 Dow tickers or 250 S&P names looks exactly like a
healthy snapshot, gets written, and quietly corrupts point-in-time membership.
Discarding costs one visible index-day; recording a wrong list corrupts history
that cannot be backdated. DIA ETF holdings pages were rejected as a Dow source
for exactly this reason: they paginate at 25 rows.

Bounds are loose on purpose — they catch a changed source shape, not index
turnover. A check that fires on legitimate reconstitution gets ignored, then
deleted.

**After ingest, `scripts/check_reassigned.py` flags any equity or ETF whose
dollar volume collapses 20x or more** (added 2026-09-24). This is the guard for
PARA's failure: a delisted ticker's key re-issued to another company, whose bars
the daily job then appended. A flag exits 1 and posts its own notification; it
writes nothing. The signal is dollar volume, not a hole in the dates. A split
leaves dollar volume unchanged, and a reassignment can arrive with no hole.
Clear a reviewed break by adding its date to `metadata.reassignment_cleared` (a
list) — details in the script's docstring. Crypto is excluded: it has its own
guard, and stETH/DAI-style volume shifts would fire daily.

**Ingest re-requests the last 14 days (`INGEST_OVERLAP_DAYS`), inserting only
missing bars** (since 2026-09-25). Before that it resumed the day after the
newest bar, so a day lost while a later one landed was never requested again:
463 of 527 equities silently lost 2026-08-28 for a month. Existing bars are
never rewritten by the overlap. `scripts/check_missing_days.py` then reports any
hole older than the overlap; fill one with
`python -m cli.run_pipeline --symbols X --start YYYY-MM-DD` (insert-only), or
accept a day Yahoo no longer serves via
`metadata.missing_days_accepted`.

**Prices are adjusted at READ time** (cutover 2026-09-28, the plan in
`research/dividend-drift-plan-2026-09-27.md`). Ingest stores prices as Yahoo
SERVED them. One `auto_adjust=False,
actions=True` download fills both the adjusted columns every reader uses —
bit-identical to the old `auto_adjust=True` — and `served_*` + `fetched_at`,
plus splits and dividends into `corporate_actions`. Served values are written
once — filled only where NULL, never overwritten — and an action keeps its
first row. `fetch_range(..., adjust="total"|"split"|"none")` derives every
read from them for assets with `price_basis = 'served'` (518 equities/ETFs; TQQQ joined 2026-10-08),
so all bars are adjusted to one as-of date and the dividend/split drift is
gone. `legacy` assets (11: PARA and names Yahoo no longer serves) and crypto
(NULL) read the stored columns. `config.settings.SERVED_PRICES_ENABLED` is
the switch: False returns every read to the stored columns. The stored columns
are still written, because legacy, crypto and several scripts read them.
`scripts/check_fresh_returns.py` (06:00 job) compares 30 sessions against a
fresh Yahoo fetch — it went from 250 mismatched days on stored prices to 0 at
cutover, and it is what catches an unlisted spinoff (HWM-type), a dividend
Yahoo later corrects, or a wrong morning-split rule.

**`top_100_crypto` needs a CoinGecko key** (since 2026-09-29). Keyless
requests to `/coins/markets` get a 403 from this machine, and the snapshot then
fails — correctly refusing to record an empty index. Put `COINGECKO_API_KEY`
(a free Demo key) in `.env`; `config.settings.coingecko_headers()` sends it as
a header on CoinGecko calls only, never in a URL and never on the shared
session that also scrapes Wikipedia. Re-take a missed day's crypto snapshot the
same day with `python scripts/snapshot_universes.py --indexes top_100_crypto`.

**A crypto asset may be fetched under a different provider ticker**
(`CRYPTO_PROVIDER_SYMBOLS` in `config/settings.py`, since 2026-10-04). Yahoo's
bare `UNI-USD` is "UNICORN Token"; Uniswap is `UNI7083-USD`. The asset keeps
its symbol everywhere; only the fetch and `scripts/audit_crypto_identity.py`
use the provider ticker, and bars are stored under the asset. An entry is a
claim, not a verdict: ingest refuses a mapped symbol until the audit has been
run with `--write` against that exact ticker (`identity_provider_symbol` on
the row), so editing a mapping blocks the asset until it is re-verified. 21
assets are mapped and loaded; POL-USD is mapped but blocked as `suspect`
(names differ, a human call); PEPE, TON, BSC and BUIDL have no usable ticker
and stay empty — the reasons are beside the mapping.

**The risk-free rate is `^IRX`, in `rate_observations`, not `assets`**
(since 2026-10-08, `core/rates.py`, step in the 06:00 job via
`scripts/ingest_rates.py`). It is a 13-week T-bill rate quoted on a
BANK-DISCOUNT basis in percent (checked against Treasury, not assumed), stored
as served and converted on read: `risk_free_rate(repo, as_of)` returns the
continuous and bond-equivalent rates, the observation date used (on or
before `as_of`, never an exact-date join: bond holidays are stock sessions)
and whether it is stale. Insert-only; today's New York date is never stored.
A yield is not a price: as an asset it would have reached backtests, the
symbol picker and every price check. The DCF reads it (`modeling/valuation.py`,
continuous). Since 2026-10-12 every backtest (backtest, compare, strategy
optimize, portfolio backtest, simulate, and their websockets) CREDITS idle
cash with the bond-equivalent rate bar by bar, and its Sharpe is measured
against that cash account: geometric excess growth over the volatility of the
excess return, so a strategy that never trades scores exactly 0.
`api/risk_free.py` resolves the rate and the per-bar accrual once per request
(the rate known at each period's START; calendar days, so Monday earns three);
a window the series does not cover is 422, and `risk_free_rate` in the request
overrides. `Backtester.run`, `PortfolioBacktester.run`, `ParameterOptimizer`
and `PerformanceAnalyzer` take the accrual with NO default; zeros reproduce the
old runs bit for bit. The portfolio weights optimizer scores each weighting
as a daily-rebalanced, cost-free backtest of it (since 2026-10-12): CAGR,
volatility and the same Sharpe, through the one function both use
(`cash_account_sharpe` in `analysis/performance_analyzer.py`), so the
Optimize page and a backtest agree. It had used mean daily return x 252 minus
the window mean rate: SPY/AGG/GLD 2023-2025 showed 1.82 (now 1.94) and a
26.3% "return" for a basket that compounded 29.0% a year. Scoring is one
matrix product per 250 weightings, 14-26 ms for 5,000 (was ~140 ms).

**Fills cost something** (since 2026-10-12). `SimulatedExecutionHandler`
slippage is ADVERSE (buys fill above the close, sells below, uniform in
[0, 0.05%]; it was +/-0.05% whatever the side, averaging to zero), and the
API's `transaction_cost` (default 0.001 = 10 bps, which the UI always gets) is
charged on each fill's value on top of the $1 fee. It had been accepted by
every endpoint and read by nothing. Portfolio backtests take it too. High
turnover pays: SPY mean_reversion 2016-2025 (174 trades) ends at $94.9k, not
$117.5k, and the SPY ma_crossover grid winner moved from 10/50 (58 trades) to
10/200 (16) — both measured before the signal fix below (now $150.8k, and
10/100 with 27 trades). The handler owns sizing a BUY to the cash
(`affordable_quantity`, `resize`), since the cost now scales with quantity.

**An unaffordable entry is resized, not dropped** (fixed 2026-10-12).
`Backtester` sizes a buy as `floor(cash / close)`; adverse slippage plus the
commission can make that fill cost more than the cash. It used to skip the
order and retry next bar; it now buys `floor((cash - commission) / fill)` at
the SAME fill (one slippage draw per order, so runs that never hit it are
bit-identical). Measured on 15 real runs (5 symbols x 3 default strategies,
2016-2025): 80 buy bars went unfilled before, 3 after — those 3 were BTC-USD
bars where one coin cost more than the cash, since fixed by lot sizes (below).
Buy and hold on AAPL and TQQQ had started a day late.
`PortfolioBacktester` has the same cash check (2026-10-12) and executes each
bar's SELLS BEFORE its BUYS, so a rebalance's sales fund its purchases. It had
no check: real AAPL+TQQQ baskets ended 60 bars overdrawn (to -$70), and in
ticker order a buy often preceded the sale funding it (27-75 bars a run went
negative mid-bar). A cash check without sells-first starves those purchases;
with both, 6 real baskets stay >= $0 and land within 0.05% of the old values.

**A signal is an EVENT: 1 enters, -1 exits, 0 holds** (fixed 2026-10-14,
`tests/unit/test_signal_semantics.py`). That is the contract in
`alpha_models/base_model.py` and how `PortfolioBacktester` always read it;
`Backtester` (backtest, compare, optimize, simulate) instead liquidated on a
0, since its first version. Every strategy that emits `position.diff()`
sold on the bar after it bought: SPY 2016-2025 ma_crossover held 10 times,
one day each (0.4% of the time); mean_reversion, trend_following, rsi and
atr_breakout 1.3-8.2%. Now they hold 21-79% of the time (SPY ma_crossover
$126.8k -> $271.6k), and push_response, whose -1 had never exited, trades
its own flips (SPY $285.2k -> $58.0k). mean_reversion, rsi and atr_breakout
also wrote 0.0 where they meant "no change", so their forward fill held
nothing: each is now NaN between its entry and exit, held until the exit
rule. Every single-asset figure measured before 2026-10-14 (including the
ones above) was under the old reading.

**`ml_random_forest` is walk-forward** (since 2026-10-14). It had trained
once on the entire history and predicted that same history; with positions
held it reached a Sharpe of 11.3 on SPY and $3.7e17 on TQQQ. It now refits
every `retrain_every` bars (63) on the rows whose outcome is already known
and is flat for its first `train_window` (252); SPY 2016-2025 ends at $78.4k,
Sharpe -0.33, and a run takes ~3 s. Its look-ahead xfails are gone;
`LOOK_AHEAD_XFAILS` is empty. Its features still include raw OHLCV levels
beside the returns (a modelling choice, not a leak).

**Backtests trade in lots: whole shares, 1e-8 of a coin** (since
2026-10-12). `config.settings.BACKTEST_LOT_SIZES` maps asset class to the
smallest tradable quantity (`lot_size_for`; an unconfigured class is 422,
never defaulted), and `api/lots.py` resolves it per symbol in every backtest
router. `Backtester.run`, `ParameterOptimizer`, `PortfolioBacktester.run`
(`lot_sizes`, per ticker, so one basket mixes whole AAPL with a fraction of
BTC) and the risk manager's sizing take it with NO default, like
`cash_accrual`. Whole units had made a BTC backtest with less cash than one
coin buy nothing, and the risk manager's 2% cap round a basket's BTC order to
0. `to_lot` snaps to the nearest lot within 1e-6 of one: 32,659,001 satoshi
divides to 32659000.999999996, and a plain floor dropped a satoshi. Lot 1.0
is bit-identical: 12 equity/ETF runs and 6 baskets matched `7108a63`. At
`7108a63`, with costs, BTC-USD 2016-2025 had 14 unfilled buy bars
(ma_crossover 1, mean_reversion 13; the 3 above was measured before costs),
now 0; buy and hold
ends at $1.21M, not $1.14M (whole coins had left the remainder in cash).

**SEC facts are refreshed from EDGAR's daily form index** (since
2026-10-11, step in the 06:00 job). The quarter's `index.json` names the
business days that exist — a weekend's index URL answers 403 AccessDenied,
which must never be read as a block — and only registry companies (own CIK
or a predecessor) that filed a 10-K, 10-Q, 20-F, 40-F or an amendment in
`SEC_REFRESH_LOOKBACK_DAYS` are reloaded: a few requests, not the ~500 and
2 GB of `--all`. Needs `SEC_USER_AGENT` in `.env` (Danny's identity, his
choice; never supplied by code — `core/sec.py` refuses to construct without
it). A block (HTML 403) or a 429 stops the run.

**DCF lines are re-checked against new filings daily** (since 2026-10-13,
`scripts/check_line_concepts.py`, after the snapshot). The DCF reads each line
from the first concept of an ordered list, or from
`LINE_CONCEPTS_BY_SYMBOL` where a 10-K showed the list picks wrong; both are
claims new filings can break. A CONCEPT flag: another concept reports a
valued company's newest fiscal year differently and valuing on it moves
value/price by 0.05 and 5%. A STALE flag: a map names concepts the filer
stopped using. Flags only, ~45 s, no SEC requests. Clear one by reading the
statement, then mapping the right concept or adding the reviewed one, with
what the statement shows, to `LINE_CONCEPTS_REVIEWED`.

**Everything is backed up daily, and each backup is proven by a restore**
(since 2026-10-12; before that nothing was, not even Time Machine).
`scripts/backup.py` is the 06:00 job's last step: the container's own
`pg_dump -Fc` (90 MB) and a tar of `data/option_chains*` and the JSON files
go to `QUANT_BACKUP_DIR` in `.env` (Google Drive, dbensik; in `.env` because
this repo is public and the path names the account), 14 daily and 12 monthly
kept. Each dump is first restored into a scratch database `quant_restore_check`
and every table's row count compared with the live one; a dump that fails is
not copied, so it never prunes a good one. A failed backup fails the job.
**Restore**, into the compose container (same TimescaleDB version):
`createdb` a database, `CREATE EXTENSION timescaledb; SELECT
timescaledb_pre_restore();`, `pg_restore --no-owner -d <db> < <dump>`, then
`SELECT timescaledb_post_restore();` — exactly what `verify()` in the script
does. Untar the files archive at the project root.

**Universe snapshots cannot be backdated.** A missed day is a permanent gap in
point-in-time membership, and membership is what makes survivorship-free
screening possible — so this job matters more than its size suggests.

**Membership before 2026-08-09 is RECONSTRUCTED, in its own table** (built
2026-10-04). `universe_membership_reconstructed` holds month-end S&P 500 lists
for 2014-12 to 2026-07, read from past revisions of the Wikipedia page by
`scripts/reconstruct_sp500_membership.py` (insert-only; a re-run adds only
missing months). It is never mixed into `universe_membership`, which stays
observations only, and nothing reads it yet. Symbols are the tickers of the
day (FB, not META). It does NOT make a cross-sectional backtest honest: Yahoo
has no prices for 151 of the 268 tickers that left the index since 2015, and
every departed name it can price is a survivor. See
`research/sp500-membership-reconstruction-2026-10-04.md`.

It needs TimescaleDB up at 06:00, which takes two settings, both applied
2026-08-09:

- Docker Desktop starts at login (`AutoStart: true` in
  `~/Library/Group Containers/group.com.docker/settings-store.json`).
- `docker-compose.yml` uses **`restart: always`**, not `unless-stopped`.
  Measured on macOS 26.6: Docker Desktop's shutdown stops containers in a way
  the daemon records as an explicit stop, so `unless-stopped` left the
  container Exited across a restart. Verified both ways. Consequence:
  `docker compose stop` is undone by a Docker restart — use
  `docker compose down` for maintenance.

## Test

```bash
python -m pytest tests/        # needs NO Docker — keep it that way
python -m pytest -m integration  # +19 tests, needs TimescaleDB running
./run_pipeline.sh verify       # verify 3-layer architecture integrity
```

Integration tests that drive the async engine directly must mark
`loop_scope="session"`. `db/session.py` builds one module-level engine and its
pooled asyncpg connections bind to the first event loop, so pytest-asyncio's
default per-test loop fails in teardown with "Event loop is closed".

`tests/api/` covers every router without a database, via the repository
Protocol (`db/repositories/market_data.py` documents this as its purpose).
Integration tests are deselected by default and skip if the DB is down.

```bash
cd frontend && npm test        # 256 tests — also needs NO API/Docker
```

When adding tests: assert on the *output*, not on values the response merely
echoes back; check the fixture actually exercises the behaviour; and verify the
test FAILS against the bug it covers. All three mistakes have produced tests
here that passed against the very bug they were written to catch.

Before asserting on relationships between symbols, read `tests/api/CLAUDE.md` (AAPL and BTC-USD share one price formula).

Determinism tests need the same scrutiny. Slippage is seeded, and asserting
"the same winner" is often weaker than it looks — a dominant candidate wins
whether or not the seed is threaded. Assert on the metrics, and confirm the
unseeded version actually differs before committing the test.

Frontend testing specifics (charts under jsdom, lazy Plotly): `frontend/CLAUDE.md`.

## Environment

Poetry is authoritative (decided 2026-07-31; `package-mode = false` is set): `poetry install`.
`run_pipeline.sh` activates the Poetry venv (Poetry-only; the conda fallback and `environment.yml` were removed 2026-07-31 — do not recreate them or add conda-based setup instructions).
For the TimescaleDB layer, copy `.env.example` → `.env`; without it, `db/session.py` defaults to the local docker-compose database.

## Architecture

- `data_pipeline/` — `FundamentalPipeline`, `DynamicUniverse`; legacy fetchers still used for universe listings. The SQLite-era `EquityPipeline`, `CryptoPipeline`, `pipeline.py`, `new.py` and `DataEnricher` (the last hardcoded 0.02 risk-free rate) were deleted 2026-10-12: they only imported each other.
- `core/ingest.py` — THE write path: fetch via adapters, persist via the repository into TimescaleDB
- `core/corporate_actions.py` — split-adjustment drift, delisting detection,
  and rename/successor detection (`scripts/find_successors.py` proposes, never
  writes). A rename is ONE instrument re-keyed and shows a constant old/new
  ratio; a merger or acquisition genuinely ends the target and correctly finds
  nothing. Threshold is `1e-4`, not `1e-2` — merger arbitrage pins a target to
  its acquirer at ~4.6e-03 before a deal closes, which a loose threshold calls
  a rename. See `research/corporate-actions-findings-2026-09-15.md`.
- `backtesting/backtester.py` — Simulates strategy on historical data; produces equity curves and KPIs. Per-bar state goes into numpy arrays, not pandas cell writes (2026-10-07: 291 ms → 5 ms a run, bit-identical). A change to the loop must stay bit-identical on real runs — the equity frame, trade log and metrics are the regression test (with a zero `cash_accrual`; 15 real runs checked when interest on cash went in, 2026-10-12). A REFACTOR must; a deliberate change of meaning is measured before and after instead (the 2026-10-14 signal fix changed every event-coded run).
- `simulation/` — Monte Carlo kernels; read `simulation/CLAUDE.md` before changing them.
- `pricing/` — option pricing; read `pricing/CLAUDE.md` before changing it (no recorded spot; discrete dividends).
- `core/options_surface.py` + `api/routers/options.py` — `/api/v1/options` archive|surface|price|pricers|realised-vol. A surface costs 10–35 s, so it is cached in `data/surfaces/<TICKER>/<day>.json` (gitignored, derivable, safe to delete). The key hashes the capture bytes, the rate observation, the dividend schedule and a fingerprint DERIVED from the pricing source and settings — never hand-bump a version. The realised-vol cone is NOT cached (bars are not in the key). A surface for today's capture uses yesterday's rate and is recomputed tomorrow: correct. `scripts/precompute_surfaces.py` fills the cache (108 captures ~37 min cold); the 06:00 job runs it with `--days 7` (~2-3 min a night, a pricing change recomputes a week, not the archive), which also names MISSED CAPTURE days: 2026-10-09 was lost for every ticker (a Yahoo dtype crash) and nobody knew for four days. Known losses: `OPTION_CAPTURE_DAYS_LOST`.
- `core/sec.py` + `core/fundamentals.py` + `modeling/` + `api/routers/valuation.py` — the DCF; read `modeling/CLAUDE.md` before changing any of them. Missing is never zero: a missing input refuses, it is never defaulted.
- `api/routers/` — the REST/WS surface: ohlcv, assets, strategies, backtest,
  compare, optimize, simulate, options, valuation, screeners, statistics, portfolios,
  watchlists, research, ingest, results, signals, ws
- `frontend/` — React dashboard; `routes.tsx` declares pages once for both the
  router and the nav bar; TanStack Query owns server state, Zustand owns UI
  selections only
- `services/` — 3-layer architecture: gRPC signal service → GraphQL gateway → Ed25519/SHA256 crypto audit log (`audit_log.json`)
- `core/portfolio.py` — portfolio accounting; the trade log is the only stored
  state and cash/positions/P&L are derived from it (`db/models.py` has no
  `cash` or `positions` column, deliberately)

## Invariants

- New strategies inherit from `alpha_models/base_model.py`.
- Pages read server state through TanStack Query hooks in `api/queries.ts`; Zustand holds UI selections only, never fetched data.
- A mutation must invalidate every query its write affects — for portfolios that includes the DERIVED state, not just the trade list.
- Two Pydantic models must never share a class name across routers: FastAPI qualifies the collision by module, silently renaming the generated TypeScript type. `tests/api/test_openapi_contract.py` enforces this.
- Run `./run_pipeline.sh verify` after any change touching `services/`.
- Settings changes go through `config/settings.py` — no hardcoded parameters in pipelines or strategies.
