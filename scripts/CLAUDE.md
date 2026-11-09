# scripts/

Checks and tools for the topic backtests: 34 offline tests, the LTP exit-method gate, results scanners and one-off fixers. Nothing outside `scripts/` imports from here. Run every command from the repo root with the root venv (see the root CLAUDE.md).

## Offline tests (`test_*.py`)

- Standalone files, no test framework: `python3 scripts/test_<name>.py`. Each exits non-zero on a failure.
- None needs a key or the network: 31 ran with sockets blocked and none tried to connect. The three run-window tests (`test_partial_metrics.py`, `test_run_window.py`, `test_latest_window_topics.py`) came later and use fake clients and local `git` only.
- Most load a topic's real `backtest.py` source and run it on made-up warehouse rows (a fake client, often an in-memory DuckDB). 26 of them also apply mutants (a text replacement on an anchor string of the code or doc under test) and require each to fail a check. A change to that code can move an anchor: fix the test's anchor in the same change.
- Each test's docstring says what it covers. `test_pairs_*.py` cover `pairs_universe.py` and the three pairs topics; `test_wave3_install.py` checks the wave 3 install block of `LTP_GATE.md` as written.
- Options: the tests that read `sys.argv[1]` take an optional repo root; `test_data_checks.py --base REF`; `test_partial_metrics.py` and `test_latest_window_topics.py --base REF` (default: the merge base with `origin/main`, so they need it fetched); `test_floor_guard.py --topic <topic> --verbose`; `test_build_output_synthetic.py -v`.
- `test_period_accounting.py` and `test_wave3_install.py` read tracked `<topic>/results/` files, so a checkout without them fails these two.
- `test_data_checks.py` runs `git show e1ecb7c:data_utils.py`: it needs the full history, not a shallow clone.
- `test_domicile_dedup.py` fails on DuckDB 2.0.0, which `requirements.txt` installs today: one mutant puts `uuid()` in a `PARTITION BY`, which 2.0 rejects at bind time. It passes after `.venv/bin/pip install "duckdb<2"` (1.5.6), but `test_pairs_zscore.py` ran about 4 times slower there.
- Slowest on 3.12.4 with DuckDB 2.0.0: `test_pairs_fundamentals.py` and `test_pairs_multi_pair.py` (about 5 minutes each), then `test_dogs_size.py` and `test_pairs_zscore.py` (under 3 minutes each). Run one test, not the loop, while iterating.

## LTP gate

- `gate_ltp.py` runs topic backtests one market leg at a time from the legs files `gate_legs/*.json` and checks the `--exit-method` wiring in the JSON they write. `LTP_GATE.md` is its runbook: checks, commands, waves and install steps. Read it before running or changing the gate.
- `plan` only prints the commands and runs offline: `python3 scripts/gate_ltp.py plan scripts/gate_legs/wave1.json --check stage-a,det,flip`.
- `run` runs the live backtests (not run here: needs `CR_API_KEY`). `report` re-reads what `run` wrote. Outputs go to the gitignored root `results/ltp_gate*/`.

## Scanners

- `verify_floor_guard.py`: 15 static gates over every topic (compile, import, floor guard, exit-method default, data checks and more).
- Its gate 6 (SCOPE) fails a branch that changes any file outside `*/backtest.py`, `*/generate_charts.py`, `scripts/`, rerun results and its `SCOPE_ALLOW` set. A change to any CLAUDE.md outside `scripts/`, the root README or most topic READMEs fails it.
- Gate 6 diffs against a local `main` branch (`git diff main...HEAD`). Without one it checks only the working tree and passes on committed changes. Either way it counts untracked files that `.gitignore` doesn't cover, such as a new `.venv/` (the root Setup excludes it).
- `scan_results_invariant.py`: impossible period counts across the committed results. Reads every topic's `results/`, writes nothing.
- `check_pairs_results.py FILE.json|TOPIC ...`: a pairs results file before it is published. `--self-test` runs offline.
- `benchmark_yield_audit.py`: which published "beat the local benchmark" claims survive a dividend adjustment. Reads the committed results and prints a report (`--json` for JSON).

## Fixers and live runners

- `rebuild_comparison.py <topic>` rebuilds `<topic>/results/exchange_comparison.json` from its per-market files, so it writes tracked results. `--keep`, `--drop` and `--only` shape it; the same line with `--check` added writes nothing and exits 1 on any disagreement. It refuses markets that end on different dates.
- `data_end.py <exchanges>...` or `--legs-file <file> --topic <topic>` prints each leg's latest traded day and the run window's end date, the date a run-window topic's rerun pins with `--end-date` (needs `CR_API_KEY`, one query).
- `patch_chart_benchmarks.py`, `patch_chart_titles.py` and `patch_chart_footers.py` rewrite `generate_charts.py` files. Each is a dry run unless given `--apply`.
- `copy_charts_to_content.py` copies charts into the published posts' content: dry run by default, `<topic> --apply` for one topic, `--apply --all` for every topic.
- `batch_rerun_diff.py`, `price_dedup_fix.py` and `detect_listing_dups.py` run live backtests or screens and need `CR_API_KEY`. `batch_rerun_diff.py` writes the tracked `data_quality_diff_report.csv`; the other two write gitignored `.jsonl` reports here.
- Machine-specific: `check_rerun_vintage.py`, `classify_chart_bug.py`, `copy_charts_to_content.py`, `patch_chart_benchmarks.py` and `patch_chart_titles.py` hard-code a local path, and `batch_rerun_diff.py` reads a content checkout beside this repo.
