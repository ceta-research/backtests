#!/usr/bin/env python3
"""Run QARP backtest across all exchanges and merge into exchange_comparison.json.

Usage:
    source /Users/swas/Desktop/Swas/Kite/ATO_SUITE/.venv/bin/activate
    cd /Users/swas/Desktop/Swas/Kite/ATO_SUITE/backtests
    python3 qarp/run_all_exchanges.py
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# Excluded exchanges (see DATA_QUALITY_ISSUES.md):
#   ASX, SAO — broken adjClose (unadjusted stock splits, >1000x price ratios)
#   JPX, LSE — no FY fundamental data in warehouse
EXCHANGES = [
    ("BSE", ["--exchange", "BSE"]),
    ("NSE", ["--exchange", "NSE"]),
    ("HKSE", ["--exchange", "HKSE"]),
    ("NYSE", ["--exchange", "NYSE"]),
    ("NASDAQ", ["--exchange", "NASDAQ"]),
    ("US_MAJOR", ["--preset", "us"]),
    ("XETRA", ["--exchange", "XETRA"]),
    ("SHH", ["--exchange", "SHH"]),
    ("SHZ", ["--exchange", "SHZ"]),
    ("AMEX", ["--exchange", "AMEX"]),
    ("KSC", ["--exchange", "KSC"]),
    ("TSX", ["--exchange", "TSX"]),
]

SCRIPT = str(Path(__file__).parent / "backtest.py")
PYTHON = sys.executable


def run_exchange(name, args):
    output_file = RESULTS_DIR / f"{name.lower()}_results.json"

    cmd = [PYTHON, SCRIPT] + args + [
        "--output", str(output_file),
        "--verbose",
    ]

    print(f"\n{'='*65}")
    print(f"  Running: {name}")
    print(f"  Command: {' '.join(cmd)}")
    print(f"{'='*65}")

    t0 = time.time()
    result = subprocess.run(cmd, capture_output=False)
    elapsed = time.time() - t0

    if result.returncode != 0:
        print(f"  FAILED: {name} (exit code {result.returncode})")
        return None

    print(f"  Completed: {name} in {elapsed:.0f}s")

    if output_file.exists():
        with open(output_file) as f:
            return json.load(f)
    return None


def merge_results(all_results):
    output_file = RESULTS_DIR / "exchange_comparison.json"
    with open(output_file, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nMerged results saved to {output_file}")


def main():
    print("QARP: Running backtests across all exchanges")
    print(f"Exchanges: {', '.join(name for name, _ in EXCHANGES)}")
    print(f"Results dir: {RESULTS_DIR}")

    all_results = {}
    total_t0 = time.time()

    for name, args in EXCHANGES:
        result = run_exchange(name, args)
        if result is not None:
            all_results[name] = result
        else:
            print(f"  Skipping {name} (no results)")

    merge_results(all_results)

    total_elapsed = time.time() - total_t0
    print(f"\nAll done. {len(all_results)}/{len(EXCHANGES)} exchanges completed in {total_elapsed:.0f}s")
    print(f"Results: {RESULTS_DIR / 'exchange_comparison.json'}")


if __name__ == "__main__":
    main()
