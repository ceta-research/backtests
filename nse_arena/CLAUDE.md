# nse_arena/

Intraday NSE strategy package. Each strategy's SQL finds candidates and their entry and exit prices on the server from NSE minute bars (`fmp.stock_prices_minute`); `IntradaySimulator` books the trades locally; `run.py` ranks the strategies by Calmar beside the India results of the EOD topic folders.

## Entry points

- `run.py`: the CLI and the `STRATEGIES` registry (`gap-up-scalp`, `orb`, `vwap`).
- `framework.py`: the `Strategy` base class, `IntradaySimulator`, `run_strategy`, `run_sweep`, `collect_eod_india_results` and `print_comparison`.
- `gap_up_scalp.py`, `orb.py`, `vwap_reversion.py`: one `Strategy` subclass each.
- `charges.py`: Zerodha NSE intraday (MIS) and delivery (CNC) charges. The simulator uses only `nse_intraday_charges`; the root `costs.py` is not used here.

## Run and check

From the repo root:

```bash
python3 -m nse_arena.run --help                    # no key needed
python3 -m py_compile nse_arena/*.py
python3 -m nse_arena.run --intraday-only           # not run here: needs CR_API_KEY
python3 -m nse_arena.run --strategy orb --sweep    # not run here: needs CR_API_KEY, many API calls
```

No test covers this package. The only check that reads it is `scripts/verify_floor_guard.py`.

## Conventions

- A new strategy subclasses `framework.Strategy`, implements `name`, `strategy_type`, `build_sql`, `default_config` and `sweep_grid`, and gets a key in `run.STRATEGIES`.
- `build_sql` returns one row per trade with `symbol, trade_date, entry_price, exit_price, exit_type`, the strategy's `sort_key` column (default `signal_strength`) and `bench_ret`.
- `--output` (default `nse_arena/results/comparison.json`) resolves against the repo root, not the current directory.

## What depends on it

- Gate 11 of `scripts/verify_floor_guard.py` requires `framework.py` to keep printing `MEASURED {r['window_label']}`. Removing that line fails the gate.
- `collect_eod_india_results` reads every `*/results/exchange_comparison.json` and takes the India node from the keys `BSE_NSE`, `BSE+NSE` or `India`, then `BSE` or `NSE`. A topic that renames its India key drops out of the comparison.

## Gotchas

- A run overwrites the tracked `nse_arena/results/comparison.json`. The tracked `orb_sweep.json` beside it has no writer in the current code.
- `gap-up-scalp/backtest.py` is a separate standalone version of the gap-up scalp with its own copy of `nse_intraday_charges`. A fix here doesn't reach it.
- Each strategy query asks the API for 16 GB of memory, 6 threads and a 600 s timeout (`run_strategy`).
