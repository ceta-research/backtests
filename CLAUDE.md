# backtests (ceta-research/backtests)

## What this is

Ceta Research's reproducible quantitative strategy backtests (MIT). Each topic folder is one strategy study: its `README.md` describes the study, and its tracked `results/` and `charts/` hold the numbers and charts the published posts use. Data comes from the Ceta Research API through `cr_client.py`.

## Layout

- `cr_client.py`: the API client `CetaResearch`, with Data Explorer SQL (`query()`, formats json, csv, parquet), Code Execution (`execute_code()`) and Projects (`create_project()`, `upsert_file()`, `run_project()`). The README's "API surface" table lists the main methods; `cr_client.py` holds the rest and the endpoint paths.
- `data_utils.py` (132 KB, search it): data loading, prices, rebalance dates, benchmarks, `DEFAULT_EXIT_METHOD`, `LtpExits` and the data checks (`DataChecks`).
- `metrics.py` (`compute_metrics`, stdlib only), `costs.py` (transaction costs), `cli_utils.py` (shared flags, `EXCHANGE_PRESETS`, `save_results`), `cloud_runner.py` (`--cloud` through the Projects API), `chart_utils.py` (chart benchmark helpers).
- `pairs_universe.py`: the per-year universe and pair exits of pairs-fundamentals, pairs-zscore and pairs-multi-pair.
- `<topic>/` (95 folders): `backtest.py`, `screen.py`, `generate_charts.py`, `README.md` and `results/`. Only 41 track a `charts/` and 8 have a `run_all_exchanges.py`. Gaps: gap-up-scalp and trending-value hold only `backtest.py`; etf-overlap and pairs-screening have no `backtest.py`; etf-crowding and etf-underowned have no `screen.py`; etf-overlap has no `generate_charts.py`; pairs-cointegration has no `results/`; rd-efficiency has no README.
- `scripts/`: offline tests, the LTP gate, results scanners, one-off fixers. See `scripts/CLAUDE.md`.
- `nse_arena/`: the intraday NSE strategy package. See `nse_arena/CLAUDE.md`.
- `examples/`: `code_execution_example.py` and `projects_example.py`.
- `METHODOLOGY.md` (119 KB, the canonical methodology) and `DATA_QUALITY_ISSUES.md` (165 KB): grep for the heading and read that section only.
- `RESULTS_PROVENANCE.md` in beat-streaks, stock-split and upgrade-cluster: read it before quoting a number from that topic's `results/`.

Topic `backtest.py` and `screen.py` add the repo root to `sys.path` themselves. Run them from the root:

```bash
python3 qarp/backtest.py --preset us --verbose    # not run here: needs CR_API_KEY
python3 qarp/screen.py --preset india             # not run here: needs CR_API_KEY
python3 qarp/generate_charts.py                   # offline: reads qarp/results/, writes qarp/charts/ (untracked for qarp)
```

- Local mode is the default: fetch through the API, load into an in-memory DuckDB, compute on this machine.
- 84 of the 93 `backtest.py` and 62 of the 91 `screen.py` take the shared flags from `cli_utils.add_common_args`: `--exchange BSE,NSE`, `--preset <EXCHANGE_PRESETS key>`, `--global`, `--api-key`, `--base-url`, `--output`, `--verbose`, `--risk-free-rate`, `--frequency`, `--no-costs`, `--no-next-day`. The rest define their own flags.
- `--exit-method drop|ltp|both` comes from `data_utils.add_exit_method_arg` (78 topics). The CLI default lives only in `data_utils.DEFAULT_EXIT_METHOD` (`ltp`). `run_backtest` signatures still default to `drop`, so a caller passes the resolved method.

## Setup from a fresh clone

```bash
git clone https://github.com/ceta-research/backtests.git
cd backtests
python3.12 -m venv .venv
echo .venv/ >> .git/info/exclude    # local only: gate 6 counts untracked files
.venv/bin/pip install -r requirements.txt
source .venv/bin/activate    # python3 in these files means the venv's Python; where activation doesn't persist, call .venv/bin/python
export CR_API_KEY=...    # every backtest, screen and nse_arena run needs it; the offline tests don't
```

- No Python version is pinned. Use 3.12 or later: `volume-confirmed-momentum/generate_charts.py` uses 3.12 f-string syntax, and `scripts/verify_floor_guard.py` parses it.
- `requirements.txt` holds requests, `duckdb>=0.10.0` and matplotlib; numpy arrives with matplotlib. `pairs-cointegration/backtest.py` also needs pandas, scipy and statsmodels, and `capex-efficiency/merge_results.py` needs pandas.
- Key lookup: `--api-key`, then `CR_API_KEY`, then `TS_API_KEY`. `--base-url`, `CR_BASE_URL` or `TS_BASE_URL` override `cr_client.DEFAULT_BASE_URL`.
- The checkout is about 300 MB: `*/results/` hold 223 MB and `*/charts/` 71 MB.

## Tests and CI gates

There is no CI, Makefile or pre-commit hook: nothing runs on a pull request. Run these from the root before you push; neither needs a key or the network.

```bash
for t in scripts/test_*.py; do .venv/bin/python "$t" > /dev/null 2>&1 || echo "FAIL $t"; done   # 30 offline tests, about 20 minutes
.venv/bin/python scripts/verify_floor_guard.py                                                   # static gates over every topic
```

Two known failures: `test_domicile_dedup.py` on DuckDB 2.0, and gate 6 of `verify_floor_guard.py` on a change to this file. See `scripts/CLAUDE.md`.

## How changes ship

- A merge to `main` builds and deploys nothing.
- The published posts quote the tracked `results/` and show the tracked `charts/`. Nothing copies them into the posts on merge: `scripts/copy_charts_to_content.py` does it by hand (see `scripts/CLAUDE.md`).

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
- A run overwrites the tracked results or chart files it writes. Check `git status` after every run.
- `--output` resolves against the current directory. Topic docstrings show `python3 <topic>/backtest.py ... --output results/<file>`, which from the root writes to an untracked root `results/`, while `generate_charts.py` reads `<topic>/results/`. Pass `--output <topic>/results/<file>`.
- `README.md` is stale in places: its strategy table and tree list 3 of the 95 topics, the tree lists `ts_client.py` (gone), and it says requirements are requests and duckdb only.
- `--cloud` is not on every script, and where it is the wiring differs by topic: `cloud_runner.run_backtest_cloud`, `cloud_runner.run_screen_cloud`, `CetaResearch.execute_code`, or nothing (a few screens accept the flag and ignore it). `run_backtest_cloud` uploads topic files only for the four topics in `cloud_runner.STRATEGY_FILES` (qarp, piotroski, low-pe, asset-growth). Read the code path before relying on `--cloud` anywhere else.
- deleveraging, high-yield-quality, low-debt and revenue-accel `screen.py` do not parse (bad f-string braces), and `stock-split/screen.py` imports a missing `cloud_runner.run_screen_on_cloud`. None of the five runs.
- `cloud_runner` writes the caller's API key into the uploaded `_run.py` or `_run_screen.py` wrapper.
- `cr_client` sends its Code Execution and Projects writes through the `_*_with_csrf` helpers, which fetch a token from `GET /auth/csrf-token`. A new write call to those APIs uses the same helpers.
- Ceta Research (`CR_*`, `CetaResearch`) is the primary name. `TS_*` names and the `ts-backtests-<topic>` cloud project names are Trading Studio backward-compat leftovers.
- Data checks (`data_utils.DataChecks`) are on only in cyclical-timing and deleveraging. Gate 15 of `scripts/verify_floor_guard.py` holds that list and fails a use anywhere else.
