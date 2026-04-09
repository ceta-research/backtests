#!/usr/bin/env python3
"""Batch re-run all backtests and diff against published blog numbers.

Runs each strategy's backtest.py --preset us, captures CAGR/Sharpe/MaxDD,
and compares to the published blog numbers (extracted from blog.md files
in ts-content-creator).

Usage:
    python scripts/batch_rerun_diff.py                    # US only (fast, ~90 min)
    python scripts/batch_rerun_diff.py --exchanges us,uk  # US + UK
    python scripts/batch_rerun_diff.py --strategy qarp    # single strategy
    python scripts/batch_rerun_diff.py --dry-run           # list strategies, don't run

Output: scripts/data_quality_diff_report.csv
"""

import argparse
import csv
import glob
import os
import re
import subprocess
import sys
import time

BACKTESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTENT_DIR = os.path.join(BACKTESTS_DIR, "..", "ts-content-creator", "content")

# Map strategy directory names to topic slugs in ts-content-creator
# Most follow the pattern: strategy-name → category-NN-strategy-name
# We'll search for matches dynamically


def find_blog_cagr(strategy_name, exchange="us"):
    """Search ts-content-creator for published blog CAGR for this strategy/exchange."""
    # Search _ready and _published for a blog.md matching this strategy
    for base in ["_ready", "_published"]:
        pattern = os.path.join(CONTENT_DIR, base, f"*{strategy_name}*", "blogs", exchange, "blog.md")
        matches = glob.glob(pattern)
        if not matches:
            # Try with hyphens removed
            alt_name = strategy_name.replace("-", "")
            pattern = os.path.join(CONTENT_DIR, base, f"*{alt_name}*", "blogs", exchange, "blog.md")
            matches = glob.glob(pattern)

        for blog_path in matches:
            try:
                with open(blog_path) as f:
                    text = f.read()

                # Look for CAGR in a markdown table: | CAGR | XX.XX% |
                cagr_match = re.search(r'\|\s*CAGR\s*\|\s*([\d.]+)%', text)
                sharpe_match = re.search(r'\|\s*Sharpe\s*(?:Ratio)?\s*\|\s*([\d.]+)', text)
                maxdd_match = re.search(r'\|\s*Max\s*Drawdown\s*\|\s*-?([\d.]+)%', text)

                return {
                    "blog_path": blog_path,
                    "blog_cagr": float(cagr_match.group(1)) if cagr_match else None,
                    "blog_sharpe": float(sharpe_match.group(1)) if sharpe_match else None,
                    "blog_maxdd": float(maxdd_match.group(1)) if maxdd_match else None,
                }
            except Exception:
                continue

    return {"blog_path": None, "blog_cagr": None, "blog_sharpe": None, "blog_maxdd": None}


def run_backtest(strategy_dir, preset="us", timeout=300):
    """Run a backtest and parse CAGR/Sharpe/MaxDD from stdout."""
    backtest_py = os.path.join(BACKTESTS_DIR, strategy_dir, "backtest.py")
    if not os.path.exists(backtest_py):
        return None

    try:
        result = subprocess.run(
            [sys.executable, backtest_py, "--preset", preset],
            capture_output=True, text=True, timeout=timeout,
            cwd=BACKTESTS_DIR,
        )
        output = result.stdout + result.stderr

        # Parse metrics from the standard output format:
        #   CAGR                              8.28%      8.02%
        #   Sharpe Ratio                      0.299      0.361
        #   Max Drawdown                    -52.88%    -43.86%
        cagr_match = re.search(r'CAGR\s+([\d.]+)%\s+([\d.]+)%', output)
        sharpe_match = re.search(r'Sharpe Ratio\s+([\d.]+)\s+([\d.]+)', output)
        maxdd_match = re.search(r'Max Drawdown\s+-([\d.]+)%\s+-([\d.]+)%', output)
        excess_match = re.search(r'Excess CAGR\s+([\d.-]+)%', output)
        osc_match = re.search(r'oscillation filter: removed (\d+) bad rows across (\d+) symbols', output)

        return {
            "cagr": float(cagr_match.group(1)) if cagr_match else None,
            "spy_cagr": float(cagr_match.group(2)) if cagr_match else None,
            "sharpe": float(sharpe_match.group(1)) if sharpe_match else None,
            "maxdd": float(maxdd_match.group(1)) if maxdd_match else None,
            "excess": float(excess_match.group(1)) if excess_match else None,
            "osc_rows_removed": int(osc_match.group(1)) if osc_match else 0,
            "osc_symbols": int(osc_match.group(2)) if osc_match else 0,
            "exit_code": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {"error": "timeout", "exit_code": -1}
    except Exception as e:
        return {"error": str(e), "exit_code": -1}


def main():
    parser = argparse.ArgumentParser(description="Batch re-run backtests and diff against blogs")
    parser.add_argument("--exchanges", default="us", help="Comma-separated presets (default: us)")
    parser.add_argument("--strategy", default=None, help="Run single strategy (directory name)")
    parser.add_argument("--dry-run", action="store_true", help="List strategies without running")
    parser.add_argument("--timeout", type=int, default=300, help="Timeout per backtest in seconds")
    args = parser.parse_args()

    exchanges = [e.strip() for e in args.exchanges.split(",")]

    # Find all strategies
    strategies = sorted([
        d for d in os.listdir(BACKTESTS_DIR)
        if os.path.isfile(os.path.join(BACKTESTS_DIR, d, "backtest.py"))
    ])

    if args.strategy:
        strategies = [s for s in strategies if args.strategy in s]

    if args.dry_run:
        print(f"Found {len(strategies)} strategies:")
        for s in strategies:
            blog = find_blog_cagr(s)
            has_blog = "✓" if blog["blog_cagr"] else "✗"
            print(f"  {has_blog} {s}")
        return

    # Output CSV
    output_path = os.path.join(BACKTESTS_DIR, "scripts", "data_quality_diff_report.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    rows = []
    total = len(strategies) * len(exchanges)
    count = 0

    for exchange in exchanges:
        for strategy in strategies:
            count += 1
            print(f"[{count}/{total}] {strategy} ({exchange})...", end=" ", flush=True)

            t0 = time.time()
            result = run_backtest(strategy, preset=exchange, timeout=args.timeout)
            elapsed = time.time() - t0

            if result is None:
                print("SKIP (no backtest.py)")
                continue

            if result.get("error"):
                print(f"ERROR: {result['error']} ({elapsed:.0f}s)")
                rows.append({
                    "strategy": strategy, "exchange": exchange,
                    "status": "error", "error": result["error"],
                })
                continue

            if result.get("cagr") is None:
                print(f"PARSE_FAIL ({elapsed:.0f}s)")
                rows.append({
                    "strategy": strategy, "exchange": exchange,
                    "status": "parse_fail",
                })
                continue

            # Get blog numbers
            blog = find_blog_cagr(strategy, exchange)

            cagr_delta = None
            sharpe_delta = None
            flag = ""
            if blog["blog_cagr"] is not None and result["cagr"] is not None:
                cagr_delta = result["cagr"] - blog["blog_cagr"]
                if abs(cagr_delta) > 0.5:
                    flag = "FLAG"
                elif abs(cagr_delta) > 0.3:
                    flag = "WARN"
            if blog["blog_sharpe"] is not None and result["sharpe"] is not None:
                sharpe_delta = result["sharpe"] - blog["blog_sharpe"]

            print(f"CAGR={result['cagr']:.2f}% (blog={blog['blog_cagr']}%) "
                  f"Δ={cagr_delta:+.2f}pp " if cagr_delta is not None else "",
                  f"osc={result['osc_rows_removed']} {flag} ({elapsed:.0f}s)")

            rows.append({
                "strategy": strategy,
                "exchange": exchange,
                "new_cagr": result["cagr"],
                "blog_cagr": blog["blog_cagr"],
                "cagr_delta": round(cagr_delta, 2) if cagr_delta is not None else None,
                "new_sharpe": result["sharpe"],
                "blog_sharpe": blog["blog_sharpe"],
                "sharpe_delta": round(sharpe_delta, 3) if sharpe_delta is not None else None,
                "new_maxdd": result["maxdd"],
                "blog_maxdd": blog["blog_maxdd"],
                "excess_cagr": result.get("excess"),
                "osc_rows_removed": result.get("osc_rows_removed", 0),
                "osc_symbols": result.get("osc_symbols", 0),
                "flag": flag,
                "status": "ok",
            })

    # Write CSV
    if rows:
        fieldnames = ["strategy", "exchange", "new_cagr", "blog_cagr", "cagr_delta",
                      "new_sharpe", "blog_sharpe", "sharpe_delta", "new_maxdd", "blog_maxdd",
                      "excess_cagr", "osc_rows_removed", "osc_symbols", "flag", "status", "error"]
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        # Summary
        flags = [r for r in rows if r.get("flag") == "FLAG"]
        warns = [r for r in rows if r.get("flag") == "WARN"]
        errors = [r for r in rows if r.get("status") in ("error", "parse_fail")]
        print(f"\n{'='*60}")
        print(f"Report: {output_path}")
        print(f"Total: {len(rows)} | FLAG (>0.5pp): {len(flags)} | WARN (>0.3pp): {len(warns)} | Errors: {len(errors)}")
        if flags:
            print(f"\nFLAGGED (>0.5pp CAGR change):")
            for r in flags:
                print(f"  {r['strategy']:30} {r['exchange']:5} blog={r['blog_cagr']}% new={r['new_cagr']}% Δ={r['cagr_delta']:+.2f}pp")


if __name__ == "__main__":
    main()
