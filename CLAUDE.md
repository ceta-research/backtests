# backtests (ceta-research/backtests)

## What this is

Ceta Research's reproducible quantitative strategy backtests (MIT). Each topic folder is one strategy study: its `README.md` describes the study, and its tracked `results/` and `charts/` hold the numbers and charts the published posts use. Data comes from the Ceta Research API through `cr_client.py`.

## Layout

- `cr_client.py`: the API client `CetaResearch`, with Data Explorer SQL (`query()`, formats json, csv, parquet), Code Execution (`execute_code()`) and Projects (`create_project()`, `upsert_file()`, `run_project()`). The README's "API surface" table lists the methods and is current; `cr_client.py` holds the endpoint paths.
- `data_utils.py` (132 KB, search it): data loading, prices, rebalance dates, benchmarks, `DEFAULT_EXIT_METHOD`, `LtpExits` and the data checks (`DataChecks`).
- `metrics.py` (`compute_metrics`, stdlib only), `costs.py` (transaction costs), `cli_utils.py` (shared flags, `EXCHANGE_PRESETS`, `save_results`), `cloud_runner.py` (`--cloud` through the Projects API), `chart_utils.py` (chart benchmark helpers).
- `pairs_universe.py`: the per-year universe and pair exits of pairs-fundamentals, pairs-zscore and pairs-multi-pair.
- `<topic>/` (95 folders): `backtest.py`, `screen.py`, `generate_charts.py`, `README.md`, `results/`, `charts/`, and in 8 topics `run_all_exchanges.py`. Gaps: gap-up-scalp and trending-value hold only `backtest.py`; etf-overlap and pairs-screening have no `backtest.py`; etf-crowding and etf-underowned have no `screen.py`; rd-efficiency has no README.
- `scripts/`: offline tests, the LTP gate, results scanners, one-off fixers. See `scripts/CLAUDE.md`.
- `nse_arena/`: the intraday NSE strategy package. See `nse_arena/CLAUDE.md`.
- `examples/`: `code_execution_example.py` and `projects_example.py`.
- `METHODOLOGY.md` (119 KB, the canonical methodology) and `DATA_QUALITY_ISSUES.md` (165 KB): grep for the heading and read that section only.
- `RESULTS_PROVENANCE.md` in beat-streaks, stock-split and upgrade-cluster: read it before quoting a number from that topic's `results/`. dcf-threshold and graham-timing carry a `STRATEGY_CARD.yaml`.

Topic scripts add the repo root to `sys.path` themselves. Run them from the root:

```bash
python3 qarp/backtest.py --preset us --verbose    # not run here: needs CR_API_KEY
python3 qarp/screen.py --preset india             # not run here: needs CR_API_KEY
python3 qarp/generate_charts.py                   # reads qarp/results/, writes qarp/charts/ (not run here: rewrites tracked charts)
```

- Local mode is the default: fetch through the API, load into an in-memory DuckDB, compute on this machine.
- 84 of the 93 `backtest.py` take the shared flags from `cli_utils.add_common_args`: `--exchange BSE,NSE`, `--preset <EXCHANGE_PRESETS key>`, `--global`, `--api-key`, `--base-url`, `--output`, `--verbose`, `--risk-free-rate`, `--frequency`, `--no-costs`, `--no-next-day`. Screens define their own subset.
- `--exit-method drop|ltp|both` comes from `data_utils.add_exit_method_arg` (78 topics). The CLI default lives only in `data_utils.DEFAULT_EXIT_METHOD` (`ltp`). `run_backtest` signatures still default to `drop`, so a caller passes the resolved method.

## Setup from a fresh clone

```bash
git clone https://github.com/ceta-research/backtests.git
cd backtests
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
export CR_API_KEY=...    # every backtest, screen and nse_arena run needs it; the offline tests don't
```

- No Python version is pinned. Use 3.12 or later: `volume-confirmed-momentum/generate_charts.py` uses 3.12 f-string syntax, and `scripts/verify_floor_guard.py` parses it.
- `requirements.txt` holds requests, `duckdb>=0.10.0` and matplotlib; numpy arrives with matplotlib. `pairs-cointegration/backtest.py` also needs pandas, scipy and statsmodels, and `capex-efficiency/merge_results.py` needs pandas.
- Key lookup: `--api-key`, then `CR_API_KEY`, then `TS_API_KEY`. `--base-url`, `CR_BASE_URL` or `TS_BASE_URL` override `cr_client.DEFAULT_BASE_URL`.
- The checkout is about 300 MB: `*/results/` hold 223 MB and `*/charts/` 71 MB.

## Tests and CI gates

There is no CI, Makefile or pre-commit hook: nothing runs on a pull request. Run these from the root before you push; neither needs a key or the network.

```bash
for t in scripts/test_*.py; do .venv/bin/python "$t" > /dev/null 2>&1 || echo "FAIL $t"; done   # 30 offline tests, about 15 minutes
.venv/bin/python scripts/verify_floor_guard.py                                                   # static gates over every topic
```

On DuckDB 2.0.0, which `requirements.txt` now installs, the loop prints `FAIL scripts/test_domicile_dedup.py`: see `scripts/CLAUDE.md`.

The LTP gate's `run` and the rerun tools in `scripts/` call the live API and need `CR_API_KEY`. Details are in `scripts/CLAUDE.md`.

## How changes ship

- A merge to `main` builds and deploys nothing.
- The published posts quote the tracked `results/` and show the tracked `charts/`. Nothing copies them into the posts on merge: `scripts/copy_charts_to_content.py` does it by hand and is machine-specific.

<!-- shared:public-rules v1. Every public repo of ours carries this block word for word. -->
## Rules for every change

- No AI attribution: no Co-Authored-By trailer, no "Generated with Claude Code" line and no session link in commits or PR bodies.
- Never commit a secret, an API key, a server address, or anything that isn't meant to be public.
- One copy of everything. Never copy a block of code: reuse it, or move it into a shared helper. Instructions follow the same rule: each rule lives in one file and other files point to it. Keep files and functions small.
- Treat the default branch as released code: it can reach users without a further step.
- Write plainly: short sentences, lead with the point, no em-dash connectors.
<!-- /shared:public-rules -->

## Gotchas

- `results/` and `charts/` are in `.gitignore` but tracked (force-added), and published numbers depend on them. Superseded files are kept on purpose (see a `RESULTS_PROVENANCE.md`). Never `git add -A`, `git clean`, or `git rm --cached` there. Add a results file by path with `git add -f`.
- A run overwrites the tracked results file it writes. Check `git status` after every run.
- `--output` resolves against the current directory. Topic docstrings show `python3 <topic>/backtest.py ... --output results/<file>`, which from the root writes to an untracked root `results/`, while `generate_charts.py` reads `<topic>/results/`. Pass `--output <topic>/results/<file>`.
- `README.md` is stale in places: its strategy table and tree list 3 of the 95 topics, the tree lists `ts_client.py` (gone), and it says requirements are requests and duckdb only.
- `--cloud` is not on every script, and where it is the wiring differs by topic: `cloud_runner.run_backtest_cloud`, `cloud_runner.run_screen_cloud`, `CetaResearch.execute_code`, or nothing (a few screens accept the flag and ignore it). `run_backtest_cloud` uploads topic files only for the four topics in `cloud_runner.STRATEGY_FILES` (qarp, piotroski, low-pe, asset-growth). Read the code path before relying on `--cloud` anywhere else.
- `cloud_runner` writes the caller's API key into the uploaded `_run.py` or `_run_screen.py` wrapper, because the cloud run has no `CR_API_KEY` of its own.
- POST, PUT, PATCH and DELETE calls need a CSRF token even with an API key. `cr_client` fetches it from `GET /auth/csrf-token` itself.
- Ceta Research (`CR_*`, `CetaResearch`) is the primary name. `TS_*` names and the `ts-backtests-<topic>` cloud project names are Trading Studio backward-compat leftovers.
- Data checks (`data_utils.DataChecks`) are on only in cyclical-timing and deleveraging. Gate 15 of `scripts/verify_floor_guard.py` holds that list and fails a use anywhere else.
- Four `run_all_exchanges.py` (graham-number, low-pe, magic-formula, qarp) hard-code a local machine path: they are machine-specific.
- Every API key string in the repo is a placeholder (`your_key_here`) or a test fake.
