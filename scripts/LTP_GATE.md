# LTP exit-method gate

`scripts/gate_ltp.py` runs topic backtests one market leg at a time and checks the results JSON for
the `--exit-method drop|ltp|both` wiring (B005, last-traded-price exits). The checks are the ones that
passed all 75 wired topics on 2026-10-10.

The default exit method lives in one place, `data_utils.DEFAULT_EXIT_METHOD` (`ltp` since stage B
wave 1; it was `drop` before). The gate imports it, so nothing here changes when it flips.

## Checks

`--check` takes a comma list (default `stage-a`). Each check needs certain runs per leg; runs are
shared between checks, so with the default `ltp`, `stage-a,det,flip` costs 5 runs per leg (`both`,
`drop`, `ltp`, `default`, `ltp.rep`), not 7.

| check | runs | passes when |
|---|---|---|
| `stage-a` | `both`, `drop` (+ `ltp` with `--with-ltp`) | In the both run, every published CAGR equals its LTP block's `drop_portfolio.cagr`. A run-level block (one with `fills`) has `missing_exits == ltp_filled + still_missing` and `ltp_filled == len(fills)`; a per-track block without `fills` only needs `ltp_filled <= missing_exits`. The drop run has no `exit_method` key and no key containing `ltp` anywhere (`ltp`, `ltp_filled`, `*_ltp`, `ltp_audit`, `period_ltp_audit`, ...), and equals the both run with those keys removed. Whenever the runs include `ltp` (`--with-ltp`, `--det-mode ltp`, or `flip` once the default is `ltp`), the ltp run's published CAGR equals the both run's `ltp_portfolio.cagr`. |
| `det` | `<det-mode>`, `<det-mode>.rep` | The same leg run twice in `--det-mode` (default `DEFAULT_EXIT_METHOD`) writes identical JSON, LTP fills and their order included when the mode is `ltp` or `both`. |
| `flip` | `default`, `<DEFAULT_EXIT_METHOD>` | A run with no `--exit-method` equals an explicit `--exit-method DEFAULT_EXIT_METHOD` run, and is in that mode: no LTP key when the default is `drop`, otherwise every `exit_method` value equals the default. This checks labels and run-to-run equality only. A run labelled `ltp` that still publishes the drop book passes `flip` and `det`; only stage-a's ltp-vs-both comparison proves the published book is the LTP book. |

Every run name except `default` passes `--exit-method <mode>` explicitly, so the `drop` run stays a
drop run after the default flips.

Reading a `det` failure: both runs fetch live warehouse data, so a difference can be a data refresh
between them rather than code. The gate runs a leg's runs back to back; on a failure, check the two
logs' fetch counts (rows, symbols, periods) match before blaming an ordering, and rerun the pair.

## Running it

From the repo root, with the project venv (`CR_API_KEY` or `TS_API_KEY` in the environment):

```bash
python3 scripts/test_gate_ltp.py                                   # offline tests, no network
python3 scripts/gate_ltp.py plan   scripts/gate_legs/wave1.json --check stage-a,det,flip   # commands only
python3 scripts/gate_ltp.py run    scripts/gate_legs/wave1.json --topic asset-growth --leg uk --jobs 1
python3 scripts/gate_ltp.py report scripts/gate_legs/wave1.json --topic asset-growth --leg uk
```

`run` prints one line per attempt (`topic leg run attempt N rc=0 49s ok`), then the report. `report`
re-reads what is on disk and exits 1 if any leg has a problem. Filters: `--topic a,b`, `--leg uk,us`.
A long gate in the background on macOS: `nohup caffeinate -i python3 -u scripts/gate_ltp.py run ... > gate.log 2>&1 & disown`.

- **Concurrency.** At most two backtests on the machine at once, counting anyone else's (a `pgrep`
  on `python ... backtest.py`). `--jobs 1|2` sets how many this gate runs itself.
- **Retries.** Up to `--attempts` (4) per run, `--timeout` (7200 s) each. A run fails if it exits
  non-zero, writes no output, or logs a parquet `magic bytes` error (even with rc 0: data went
  missing). Known transients (rate limit, 429/5xx, timeouts, connection resets, corrupt parquet) keep
  retrying; "No data available" (an empty market) stops at once; any other traceback gets one retry
  and then stops.
- **Outputs.** `--out-dir` (default `results/ltp_gate/`, gitignored) holds
  `<topic>/<leg>.<run>.json`, `.log`, `.meta.json` (command, rc, seconds, attempts), every failed
  attempt as `.failN`, and `gate_report.json`. Existing outputs are reused, so an interrupted gate
  resumes. They are reused whatever the code version, so use a fresh `--out-dir` after any code change.

## Stage B

`DEFAULT_EXIT_METHOD` became `ltp` with the wave 1 code. One gate on wave1 covers it: the `ltp` run
gives the ltp-vs-both comparison, `default` proves a flagless run is the ltp run, and `ltp.rep`
proves the ltp run repeats exactly.

```bash
# both, drop, ltp, default, ltp.rep = 5 runs/leg, 355 backtests for 71 legs
python3 scripts/gate_ltp.py run scripts/gate_legs/wave1.json --check stage-a,flip,det --out-dir results/ltp_gate_w1
```

The first proves, per leg: the both run publishes the drop book, the drop run equals the both run
minus LTP keys, the ltp run publishes the both run's LTP book, and the ltp run (fills and their order
included) is deterministic. The second repeats that on the flipped code (`det` now repeats the new
default, `ltp`) and adds: no flag == `--exit-method ltp`. At two backtests at a time these are long;
scope with `--topic`/`--leg` if needed.

The gate runs `<topic>/backtest.py <leg_args>` one market at a time. The wave 1 rerun goes through
the `--global` loops and `run_all_exchanges.py`, which the gate never runs: check those take
`--exit-method` from `add_exit_method_arg` and pass it to every market before trusting the rerun.
Exception: cash-conversion's `--global` writes only the combined file (keyed `NSE`, not `India`) and no
`ccc_metrics_*.json`, which are what its charts and posts read. Rerun it one market at a time:
`--preset <name> --output results/ccc_metrics_<published name>.json` (US_MAJOR, India, XETRA, China,
HKSE, JPX, Canada, LSE, SIX, STO, KSC, SAO, Taiwan, SES, JSE).
Wave 2 uses no topic runner at all: it is gated on the per-leg file `scripts/gate_legs/wave2.json`
(same checks, its own `--out-dir`) and its rerun installs that gate's `ltp` outputs (see Legs files).

After a rerun, before anything is published: every per-market file the posts read, and every node of
a comparison JSON the charts read, must carry `exit_method` equal to the intended mode, and the
comparison JSON must hold no `error` entry. A `--global` loop catches a market's exception, records it
there, and leaves that market's old file on disk. A one-market rerun leaves the old comparison JSON on
disk: no `error`, and no `exit_method` either.

## Legs files

`scripts/gate_legs/wave1.json` is `{topic: {pairs, legs: {name: leg_args}}}`; keys starting with `_`
are notes. The stage A flat list `[{topic, leg_args, pairs, leg?}]` also loads (leg name defaults to
the last leg arg). `pairs` lists each published book as `{published_cagr_path, ltp_block_path}`,
dotted paths into the results JSON.

wave1 holds every market leg of cyclical-timing (16), asset-growth (16), fcf-conversion (16),
cash-conversion (15) and graham-net-net (8), 71 legs. Each mirrors what the topic's own multi-market
runner does, so a leg's JSON is the per-market file the global run writes:

- cyclical-timing, asset-growth, fcf-conversion: the `--global` `presets_to_run` list, as
  `--exchange <comma list>` (taiwan is `TAI,TWO` in cyclical-timing, `TAI` in the other two).
  asset-growth and fcf-conversion leave out singapore: their `"SGX"` matches no rows, the run writes
  no file, and nothing is published for it (`EXCHANGE_PRESETS` singapore is `SES`).
- cash-conversion: `GLOBAL_PRESETS`, as `--preset <name>` (its global loop reads `EXCHANGE_PRESETS`,
  so canada is TSX+TSXV and singapore is SES), minus france: France has almost no CCC data, the run
  writes an error file and nothing is published.
- graham-net-net: `run_all_exchanges.py` `EXCHANGES` (backtest.py has no `--global`).

`scripts/gate_legs/wave2.json` holds the legs of dogs-of-dow (17), low-debt (15), fcf-compounders (18),
fcf-yield (16), price-to-sales (16), revenue-accel (19, four of them `--exclude-funds` diagnostics),
dividend-growth (14), deleveraging (16), pe-mean-revert (13), dividend-sustainability (15),
rising-yield (17) and graham-number (17), 193 legs. A leg runs the exchange set of the live per-market
file (fcf-compounders singapore is new: SES, where its list said SGX). These must be run one market at
a time, not through the topic's own runner:

- dogs-of-dow: `run_all_exchanges.py` writes only the combined file, none of the per-market files.
- graham-number: `run_all_exchanges.py` overwrites the curated 12-market `exchange_comparison.json`
  with all 17 markets.
- fcf-yield: `--global` writes only the combined file, no per-market files.
- Canada and Taiwan in fcf-compounders, price-to-sales and dividend-sustainability, and Canada in
  deleveraging: the live `returns_TSX.json` / `returns_TAI.json` hold the preset universes (TSX+TSXV,
  TAI+TWO), and `--global` writes TSX and TAI alone to those names. Their legs are `--preset canada`
  and `--preset taiwan`.

So the wave 2 rerun installs gate outputs and runs nothing again. Per leg, copy the `ltp` run,
`<out-dir>/<topic>/<leg>.ltp.json`, to `<topic>/results/` under the name the topic's `_outputs` note
gives. `flip` and `det` prove `default` and `ltp.rep` equal it. Never install `both`: its published
fields are the drop book. Around the copy:

- Rename first (`git mv`) where the live figure sits under another name: dividend-growth
  `returns_us.json` to `returns_NYSE_NASDAQ_AMEX.json` (`-f`, over a stale file) and `returns_nse.json`
  to `returns_NSE.json`; deleveraging `returns_US.json` to `returns_NYSE_NASDAQ_AMEX.json` and
  `returns_nse.json` to `returns_NSE.json`. The `nse` renames are case-only: on a case-insensitive disk
  (macOS) go through a temporary name, or the copy keeps the lowercase name and the rebuild stops on
  a key `nse` that differs from `NSE` only by case.
- `*/results/` is gitignored: `git add -f` the five files with no earlier version (dogs-of-dow
  `returns_XETRA.json`, `returns_SIX.json`, `returns_JNB.json`, `returns_SAU.json`; fcf-compounders
  `returns_SES.json`).

Then rebuild `<topic>/results/exchange_comparison.json` before `generate_charts.py`. No leg writes it,
and it is the only results file the 12 chart scripts read, so charts drawn without the rebuild are the
pre-LTP charts. `scripts/rebuild_comparison.py` does every topic: each node is its per-market file,
nothing added, in the comparison file's key order with new keys after it. Run each rebuild again with
`--check` added (same `--keep` or `--drop`): it writes nothing and exits 1 on any disagreement, a
node that differs from its file, an `error` entry, a node without a file or a file without a node.
A per-market file that does not parse stops the rebuild with nothing written.

- low-debt, fcf-compounders, price-to-sales, revenue-accel, deleveraging, pe-mean-revert,
  dividend-sustainability, rising-yield: `python3 scripts/rebuild_comparison.py <topic>`.
  It folds in every `results/returns_*.json`, keyed by file name, and the comparison charts draw the
  keys they find. So first remove (`git rm`) the pre-LTP files no `_outputs` entry names, or pass
  their keys to `--drop` on every rebuild: low-debt `BSE_NSE`, `SHH_SHZ`, `US_MAJOR`; price-to-sales
  `BSE_NSE`, `TAI_TWO`, `OSL`, `SAO`; dividend-sustainability `BSE_NSE`, `US_MAJOR`. Two legs add a
  key the live comparison does not hold: `SES` in fcf-compounders (an 18th key) and price-to-sales
  (a 16th). `--drop` each unless its post adds that market.
- dividend-growth, fcf-yield, graham-number: `--keep`, the curated keys in the live file's order.
  dogs-of-dow: a plain rebuild.

  ```bash
  python3 scripts/rebuild_comparison.py dividend-growth --keep NYSE_NASDAQ_AMEX NSE TSX LSE XETRA SIX STO HKSE JPX TAI SHZ_SHH KSC JNB
  python3 scripts/rebuild_comparison.py dividend-growth --keep NYSE_NASDAQ_AMEX NSE TSX LSE XETRA SIX STO HKSE JPX TAI SHZ_SHH KSC JNB --check
  python3 scripts/rebuild_comparison.py fcf-yield --keep US_MAJOR LSE JPX Canada HKSE China SIX Taiwan SET NSE
  python3 scripts/rebuild_comparison.py fcf-yield --keep US_MAJOR LSE JPX Canada HKSE China SIX Taiwan SET NSE --check
  python3 scripts/rebuild_comparison.py graham-number --keep US_MAJOR India Germany Canada Sweden Brazil Korea HKSE Taiwan Switzerland Japan UK
  python3 scripts/rebuild_comparison.py graham-number --keep US_MAJOR India Germany Canada Sweden Brazil Korea HKSE Taiwan Switzerland Japan UK --check
  python3 scripts/rebuild_comparison.py dogs-of-dow
  python3 scripts/rebuild_comparison.py dogs-of-dow --check
  ```

  - dividend-growth: the live 13, with `US` under its new name `NYSE_NASDAQ_AMEX` (its charts label
    both). Not kept: the pre-LTP `BSE_NSE`, `US_MAJOR` and `PAR` files, and `ASX`, a leg the live
    comparison does not hold (a 14th key): add it to the list if its post adds that market.
  - fcf-yield: files are `value-04-fcf-yield_<leg>.json`, key = the file's `universe` field. The
    curated 10 leave out germany, korea, sweden, norway, saudi, israel.
  - graham-number: files are `<leg name, lowercased>_results.json`, key = the leg name. The curated
    12 leave out Australia, Norway, France, SouthAfrica, Singapore.
  - dogs-of-dow: all 17, key = the `returns_` suffix, `us.json` as `US_MAJOR`, `sweden.json` as `STO`
    (`india.json` is not read). Its chart script reads only `exchange_comparison.json` now. A node
    must NOT carry `status`: a single run writes none, no chart reads it, and `--check` fails a node
    that has one (the live file's `run_all_exchanges.py` nodes do, until this rebuild).

When a topic's market list changes, change its legs to match; `plan` validates the file.

Wave 3 has two legs files, and no single `--check` list fits either whole: each topic's `_checks` note
says what applies. Results are installed from the gate's outputs, with a per-topic comparison rebuild
(Installing `wave3.json`, below).

```bash
# both, drop, ltp, default, ltp.rep = 5 runs/leg, 330 backtests for 66 legs
python3 scripts/gate_ltp.py run scripts/gate_legs/wave3.json --check stage-a,det,flip \
    --topic 52-week-high,dcf-discount,defensive-quality,pairs-fundamentals --out-dir results/ltp_gate_w3
# ltp, default, ltp.rep = 3 runs/leg, 72 backtests for 24 legs
python3 scripts/gate_ltp.py run scripts/gate_legs/wave3.json --check det,flip \
    --topic pairs-zscore,pairs-multi-pair --out-dir results/ltp_gate_w3
# drop, drop.rep = 2 runs/leg, 26 backtests for 13 legs
python3 scripts/gate_ltp.py run scripts/gate_legs/wave3_osl.json --check det --det-mode drop \
    --out-dir results/ltp_gate_w3_osl
```

- `wave3.json` holds 52-week-high (18 legs), dcf-discount (16), defensive-quality (17),
  pairs-fundamentals (15), pairs-zscore (12) and pairs-multi-pair (12), 90 legs. The first four take
  all three checks; pairs-fundamentals' pair-level block has the `LtpExits` shape, so stage-a reads it
  unchanged.
- pairs-zscore and pairs-multi-pair take `det,flip` only. A pair is one position and closes at the
  last bar both legs traded, so there is no drop book: `--exit-method drop` and `both` stop at the
  parser (exit 2). `flip` proves a flagless run is the `ltp` run and `det` that it repeats; nothing in
  the gate compares their book with another one (`scripts/test_pairs_exits.py` covers the exits
  offline). The gate does not read `_checks`: run on the whole file with `stage-a`, these 24 legs get
  48 runs that fail all four attempts and report "both run missing". Always pass `--topic`.
- dcf-discount is run per region, `--preset <region>` to `<region>.json`, the 16 files its charts
  read. Its `--global` runs canada as TSX alone and singapore as SGX (no rows) and writes
  `returns_<KEY>.json`, which the charts read only when no region file exists.
- `wave3_osl.json` is the Norway leg of 13 topics whose other markets are not rerun. Each is a
  drop-mode repair: `--exit-method drop`, so the file matches its sibling markets, on today's period
  accounting. `--det-mode drop` is required: without it the gate repeats the `ltp` run and passes.
  Install `<leg>.drop.json` under the `_outputs` name (it must hold no `exit_method`), then
  `python3 scripts/rebuild_comparison.py <topic> --only OSL` and the same with `--check`. `--only`
  replaces the OSL node from `results/returns_OSL.json` and leaves every other node byte-identical;
  a plain rebuild would not (etf-underowned's and oversold-quality's other per-market files are older
  than their nodes). oversold-quality's run goes under two names, `returns_OSL.json` and
  `oversold_quality_osl.json`: its `returns_OSL.json` is an older run today, and with only the second
  name installed `--only OSL` folds that older run into the node and `--check` still passes. Eight of
  the 13 (the ones with a negative `invested_periods`) were on the known-bad list of
  `scripts/scan_results_invariant.py` (`EXPECT_FLAGGED`, `EXPECT_PAIRS`) until the wave 3 install
  repaired them and took them off it; the self-test checks that list node by node, so any later
  repair updates it in the same change.

Installing `wave3.json`: per leg, copy `<out-dir>/<topic>/<leg>.ltp.json` to `<topic>/results/` under
its `_outputs` name, as in wave 2 (never `both`). `git add -f` the two files with no earlier version,
`pairs-fundamentals/results/returns_NSE.json` and `pairs-multi-pair/results/returns_NSE.json`. Then
remove the pre-LTP per-market files no leg replaces and rebuild each comparison, each rebuild followed
by the same command with `--check`:

```bash
git rm defensive-quality/results/returns_BSE_NSE.json defensive-quality/results/returns_SHZ.json defensive-quality/results/returns_US_MAJOR.json
git rm pairs-fundamentals/results/returns_BSE_NSE.json pairs-fundamentals/results/returns_US.json
git rm pairs-zscore/results/returns_BSE_NSE.json
git rm pairs-multi-pair/results/returns_BSE_NSE.json
python3 scripts/rebuild_comparison.py 52-week-high
python3 scripts/rebuild_comparison.py 52-week-high --check
python3 scripts/rebuild_comparison.py defensive-quality --keep NYSE_NASDAQ_AMEX NSE SHZ_SHH HKSE SET JPX LSE XETRA TAI_TWO KSC TSX SIX STO JNB KLS JKT
python3 scripts/rebuild_comparison.py defensive-quality --keep NYSE_NASDAQ_AMEX NSE SHZ_SHH HKSE SET JPX LSE XETRA TAI_TWO KSC TSX SIX STO JNB KLS JKT --check
python3 scripts/rebuild_comparison.py pairs-fundamentals --keep NYSE_NASDAQ_AMEX JPX LSE HKSE KSC SHZ_SHH TAI_TWO TSX STO XETRA JNB NSE
python3 scripts/rebuild_comparison.py pairs-fundamentals --keep NYSE_NASDAQ_AMEX JPX LSE HKSE KSC SHZ_SHH TAI_TWO TSX STO XETRA JNB NSE --check
python3 scripts/rebuild_comparison.py pairs-zscore --keep NYSE_NASDAQ_AMEX JPX TSX HKSE SHZ_SHH KSC TAI_TWO STO JNB NSE LSE XETRA
python3 scripts/rebuild_comparison.py pairs-zscore --keep NYSE_NASDAQ_AMEX JPX TSX HKSE SHZ_SHH KSC TAI_TWO STO JNB NSE LSE XETRA --check
python3 scripts/rebuild_comparison.py pairs-multi-pair
python3 scripts/rebuild_comparison.py pairs-multi-pair --check
```

- 52-week-high and pairs-multi-pair: a plain rebuild. Their 18 and 12 installed files are the live
  comparison's keys one to one (pairs-multi-pair's `NSE` node has had no file until now).
- defensive-quality: `--keep`, the live 16 with `NSE` in `BSE_NSE`'s place (its charts label `NSE`, and
  the India charts read `returns_NSE.json`). `SHH` is installed but not kept: `returns_SHH.json` is the
  comparison post's China row, and the charts' China is `SHZ_SHH`. A plain rebuild would add it, and
  the two comparison charts draw every node with `invested_periods > 0` and no truncated window,
  labelled by key where they set no label. Nothing else catches that: `check_pairs_results.py` is pairs-only, and the `SHH` node is an
  `ltp` run. The removed `BSE_NSE`, `SHZ` and `US_MAJOR` files are pre-LTP runs no leg repeats.
- pairs-fundamentals: `--keep`, the live 12, which are its charts' `VALID_EXCHANGES` (read with no
  fallback). `SIX`, `SET` and `SES` are installed but not kept: the posts name them as excluded.
- pairs-zscore: `--keep`, the live 12 with `NSE` in `BSE_NSE`'s place. No chart draws India
  (`CONTENT_EXCHANGES` leaves it out).
- The pairs `git rm`s are required, not tidying: `check_pairs_results.py <topic>` reads every
  `results/returns_*.json` and fails on a pre-LTP one, kept or not.
- dcf-discount: no rebuild and no comparison file. While one region file exists its charts read the
  16 region files and nothing else. Check instead that each region file carries `exit_method` `ltp`.
  The install also removed its 17 March 2026 files (`returns_*.json`, `returns_*_v2.json`,
  `exchange_comparison_v2.json`): no chart or post read them, and a rebuild would have built a
  comparison from them.

`scripts/test_wave3_install.py` runs this block as written on copies of the six results directories,
with a stand-in `ltp` run under every `_outputs` name.

After installing a pairs topic: `python3 scripts/check_pairs_results.py <topic>` (every per-market file
and comparison node is an `ltp` run of the per-year universe with the current `universe_rule` text, no
same-company pair, no non-finite correlation and no `error` node). It fails on any file left from
before the rerun.

## Per-topic adapters (kept from stage A)

- **capex-efficiency** ignores the `--output` file name and writes `save_results` files (metrics JSON
  with CAGR as a fraction, plus a CSV). `ADAPT` synthesizes the gate file from them: percent CAGR,
  the LTP keys, the CSV header (so an `_ltp` column in a drop run is caught) and the whole run for
  det/flip. Its legs must use `--exchange`. The full drop-vs-both compare is skipped for it.
- **Fraction CAGRs** (graham-timing publishes the raw `compute_metrics` fraction): published and
  `drop_portfolio.cagr` match if equal as percent or after `round(x * 100, 2)`.
- **Keyed outputs**: graham-timing writes `{universe: result}`, so its pairs are `LSE.portfolio.cagr`
  and `LSE.ltp`. sp500-survivorship takes no market args (`leg_args: []`).
- **Per-track LTP blocks without fills** (piotroski `ltp.low_track`/`ltp.all_track`, altman-z's
  sub-tracks) get the counts-only check, never a false "counts inconsistent".

## Stage A record

2026-10-10, LSE leg of every wired topic, both + drop: 75 topics, 0 problems. Re-gate with an ltp
run on six (piotroski, 52-week-high, graham-net-net, sector-momentum, altman-z, trending-value): 0
problems. The 75-topic legs file was never saved; rebuild it in the flat format if all topics need
re-gating. Every other topic has only had the both-mode drop==published check, not an ltp run.

## Wiring invariants (I1-I8)

What a topic's `--exit-method` wiring must satisfy. Reference implementations: `low-pe/backtest.py`
(inline loop), `graham-net-net/backtest.py` (filter_returns), `piotroski/backtest.py` (several tracks
per period, per-track audit), `52-week-high/backtest.py` (local full price cache:
`LtpExits(cr, con, local=True)`). `git show 419594e -- <file>` shows what the wiring added.

- `drop`: no LtpExits object, no extra query, no new key in the results JSON, the same code path as
  before the wiring.
- `ltp`: the published-path fields (`portfolio`, `annual_returns`, `period_data[].portfolio_return`,
  `stocks_held`, `holdings`, every metric) come from the LTP book. JSON gains `exit_method` and `ltp`.
- `both`: the published-path fields are the DROP book (identical to a `drop` run on the same data);
  the LTP book is computed from the same screen, prices and connection and reported next to it
  (`portfolio_return_ltp`, `stocks_held_ltp`, `ltp_filled` on each invested period record, and the
  `ltp` block with `drop_portfolio` / `ltp_portfolio`). `ltp.drop_portfolio.cagr` must equal
  `portfolio.cagr`.

The recipe: `add_exit_method_arg(parser)`; thread `ltp=None, exit_method=...` through every mode the
topic has (`--global`, `run_single` wrappers, runner scripts); build
`LtpExits(cr, con, verbose=...) if exit_method != "drop" else None` once per exchange run (`local=True`
only when `prices_cache` holds continuous, oscillation-cleaned history over the whole holding period);
in the period loop request `ltp.fill(...)` only when the topic's own entry-side cash guard passes,
compute the book through one closure `book(xmap)` holding the unchanged return code, and in `both`
mode call it a second time with the LTP exit map; attach `ltp.results_block(...)` over the same
`valid` periods the metrics use.

- **I1 default unchanged.** With `drop`: no LtpExits construction, no query, no new JSON key, and the
  arithmetic is the same as before (moving code into a closure must not change it: same order of
  symbols, same rounding, same variables captured).
- **I2 cash decisions never read the LTP book.** Whether a period is cash is decided by the topic's
  existing entry-side guard (`entry_buyable` / `entry_buyable_prices` against MIN_STOCKS, the screen
  count, a timing signal), exactly as before. Fills are requested only for periods that pass that guard,
  and no funnel/guard field (`screened`, `entry_buyable`, cash counters, signal fields) changes with
  the exit method. A topic that still decides cash from the exit-side book (`len(clean) < MIN_STOCKS`
  with no entry-side guard) is unresolved: report it, don't wire around it.
- **I3 fills go through the same filters.** A filled name passes through the same `filter_returns` /
  return cap / entry floor / costs as a priced one. Pass the topic's own `min_entry_price` to `fill`.
- **I4 no look-ahead.** Nothing dated after the exit date is used; the helper enforces its own bounds.
  Don't widen `get_prices` windows.
- **I5 `both` is the drop book plus extras.** Every pre-existing field in `both` mode equals the
  `drop` run; `_ltp` fields are additions. Cash periods carry no `_ltp` field.
- **I6 downstream consumers.** Anything that filters or reinterprets period records (a
  `msg == "invested"` filter, state fields, per-track accounting, `period_accounting`, annual
  aggregation) must treat the LTP book the same way as the drop book. In `ltp` mode the downstream
  code sees LTP returns in the normal fields, so usually nothing else changes.
- **I7 several books per period.** If the topic computes more than one portfolio per period (tracks,
  buckets, quantiles, long/short legs), request fills once for the union of names that passed the
  entry guard and apply the same exit map to every book; add a per-book `_ltp` return in `both`
  mode (piotroski: `<track>_return_ltp`, one `results_block(..., key="<track>_return")` per
  published track, combined under `ltp`).
- **I8 no behaviour beyond this.** No refactors, renames, formatting sweeps, benchmark or screen
  changes. Minimal diff. Terse one-line comments at most.

The gate checks I1 (drop run has no LTP key) and I5 (drop == both minus LTP keys) directly; I2-I4 and
I6-I8 need code review.

## Determinism notes (D1-D4)

Running the same topic twice on identical data gave different results (measured 2026-10-09: 4 of 28
legs differed, by up to 0.58pp CAGR; one leg flipped a whole year between cash and invested). The
cause was selection SQL (and a few Python sorts) whose ORDER BY is not a total order: DuckDB runs it
in parallel and breaks ties differently each time. Two shapes account for almost all of it:

1. **Top-N cut with ties.** `ORDER BY metric LIMIT n` (or `QUALIFY ROW_NUMBER() OVER (ORDER BY metric) <= n`).
   Twin listings (SET `-R` NVDR lines, share classes, a company listed twice) carry identical
   fundamentals, so they tie exactly; a tie group straddling rank n picks arbitrary names.
2. **Latest-row dedup with duplicate keys.** `ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) = 1`
   when two rows for one symbol share the epoch: the surviving row's values are arbitrary.

The fix makes every ordering in the selection path a total order and changes nothing else:

- Top-N cuts: append `, symbol` (ascending) as the last ORDER BY key; Python `sorted(...)[:n]` cuts
  use a `(metric, symbol)` key (metric's direction kept, symbol ascending).
- Latest-row dedups: append tie-breakers after the existing key so that two rows that differ in any
  column the query goes on to read cannot tie: every such value column, `DESC NULLS LAST`. A finer
  natural key in the cache (`date`, `period`, `acceptedDate`) goes first. Arbitrary but fixed.
- `RANK()` / `DENSE_RANK()` / `PERCENT_RANK()` are deterministic under ties; leave them unless a
  LIMIT or ROW_NUMBER follows. `NTILE()` splits ties arbitrarily: order by `metric, symbol`.
- `GROUP BY` with `ANY_VALUE` / `FIRST` / an unaggregated pick: use `MIN`, `MAX`, or
  `arg_max(x, key)` over a total-order key.
- Epoch bounds built from naive local time move with the machine's timezone and daylight saving;
  bar epochs are UTC midnight. Use `data_utils.utc_epoch(d)` / `utc_date(epoch)`, without changing
  which bars a bound is meant to include.
- A rounded float aggregate feeding a ranking key (`ROUND(AVG/SUM(x), k)` over DOUBLE) flips with
  parallel add order when a group's exact value sits on a midpoint. Aggregate exactly:
  `ROUND(AVG(CAST(x AS DECIMAL(38,18))), k)`. Accepted exception to D1 (added 2026-10-10 after
  etf-concentration measured 23 flips in 200 synthetic midpoint groups). Unrounded float keys tie
  only within last-bit noise; leave them.
- Leave alone: ORDER BYs that only sort output for display, orderings that are already total, and
  anything outside the selection/pricing path.

- **D1** Only orderings and epoch bounds change. No new filters, no changed thresholds, no
  changed metric definitions, no dedup of twin listings in the determinism fix itself (ISIN dedup
  came later, in the rerun topics; see DATA_QUALITY_ISSUES.md).
- **D2** Wherever an ordering was already total, the result is unchanged.
- **D3** The cut size is unchanged (same n, same guard counts).
- **D4** No refactors, renames or formatting sweeps. Minimal diff, no new comments unless a
  tie-break rule needs one line of explanation.

The `det` check is the end-to-end test of D1-D3 on a leg. Offline, an ordering change can be tested
with an in-memory DuckDB and made-up rows.

## Offline checks before gating a code change

`python3 -m py_compile <file>`, `python3 <topic>/backtest.py --help`, `python3 scripts/test_ltp_exits.py`,
`python3 scripts/verify_floor_guard.py`, `python3 scripts/test_floor_guard.py`,
`python3 scripts/test_gate_ltp.py`. Only the gate runs backtests (two at a time, machine-wide).
