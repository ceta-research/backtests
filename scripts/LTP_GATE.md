# LTP exit-method gate

`scripts/gate_ltp.py` runs topic backtests one market leg at a time and checks the results JSON for
the `--exit-method drop|ltp|both` wiring (B005, last-traded-price exits). The checks are the ones that
passed all 75 wired topics on 2026-10-10.

The default exit method lives in one place, `data_utils.DEFAULT_EXIT_METHOD` (`ltp` since stage B
wave 1; it was `drop` before). The gate imports it, so nothing here changes when it flips.

## Checks

`--check` takes a comma list (default `stage-a`). Each check needs certain runs per leg; runs are
shared between checks, so `stage-a,det,flip` costs 4 runs per leg today (`both`, `drop`, `default`,
`drop.rep`), not 6.

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

Two gates on wave1, one before and one after `DEFAULT_EXIT_METHOD` flips to `ltp`. Both load an `ltp`
run, so the ltp-vs-both comparison runs; of the wave-1 topics only graham-net-net has had an ltp run
gated before (stage A record).

```bash
# before the flip: both, drop, ltp, ltp.rep = 4 runs/leg, 288 backtests
python3 scripts/gate_ltp.py run scripts/gate_legs/wave1.json --check stage-a,det --with-ltp --det-mode ltp --out-dir results/ltp_gate_b1
# after the flip, fresh out-dir: both, drop, ltp, default, ltp.rep = 5 runs/leg, 360 backtests
python3 scripts/gate_ltp.py run scripts/gate_legs/wave1.json --check stage-a,flip,det --out-dir results/ltp_gate_b2
```

The first proves, per leg: the both run publishes the drop book, the drop run equals the both run
minus LTP keys, the ltp run publishes the both run's LTP book, and the ltp run (fills and their order
included) is deterministic. The second repeats that on the flipped code (`det` now repeats the new
default, `ltp`) and adds: no flag == `--exit-method ltp`. At two backtests at a time these are long;
scope with `--topic`/`--leg` if needed.

The gate runs `<topic>/backtest.py <leg_args>` one market at a time. The stage B rerun goes through
the `--global` loops and `run_all_exchanges.py`, which the gate never runs: check those take
`--exit-method` from `add_exit_method_arg` and pass it to every market before trusting the rerun.
Exception: cash-conversion's `--global` writes only the combined file (keyed `NSE`, not `India`) and no
`ccc_metrics_*.json`, which are what its charts and posts read. Rerun it one market at a time:
`--preset <name> --output results/ccc_metrics_<published name>.json` (US_MAJOR, India, XETRA, China,
HKSE, JPX, Canada, LSE, SIX, STO, KSC, SAO, Taiwan, SES, JSE).

After a rerun, before anything is published: every per-market file the posts read must carry
`exit_method` equal to the intended mode, and the comparison JSON must hold no `error` entry. A
`--global` loop catches a market's exception, records it there, and leaves that market's old file on
disk.

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

When a topic's market list changes, change its legs to match; `plan` validates the file.

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
  changed metric definitions, no dedup of twin listings (a separate, pending decision).
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
