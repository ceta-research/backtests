#!/usr/bin/env python3
"""Run Magic Formula backtest across all exchanges and merge into exchange_comparison.json.

Usage:
    source /Users/swas/Desktop/Swas/Kite/ATO_SUITE/.venv/bin/activate
    cd /Users/swas/Desktop/Swas/Kite/ATO_SUITE/backtests
    python3 magic-formula/run_all_exchanges.py
"""

import json
import subprocess
import sys
import time
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# Each entry: (universe_name, backtest_args)
# universe_name matches the key in exchange_comparison.json["exchanges"]
EXCHANGES = [
    ("US_MAJOR",  ["--preset", "us"]),
    ("India",     ["--preset", "india"]),
    ("JKT",       ["--exchange", "JKT"]),
    ("SAO",       ["--preset", "brazil"]),
    ("KSC",       ["--preset", "korea"]),
    ("XETRA",     ["--preset", "germany"]),
    ("STO",       ["--preset", "sweden"]),
    ("China",     ["--preset", "china"]),
    ("HKSE",      ["--preset", "hongkong"]),
    ("TAI",       ["--preset", "taiwan"]),
    ("SET",       ["--exchange", "SET"]),
    ("ASX",       ["--preset", "australia"]),
    ("Canada",    ["--preset", "canada"]),
    ("TLV",       ["--exchange", "TLV"]),
    ("SAU",       ["--exchange", "SAU"]),
    ("PAR",       ["--exchange", "PAR"]),
]

SCRIPT = str(Path(__file__).parent / "backtest.py")
PYTHON = sys.executable


def run_exchange(name, args, skip_existing=True):
    output_file = RESULTS_DIR / f"magic_formula_{name}.json"

    # Skip if already complete (useful for resume after rate limit)
    if skip_existing and output_file.exists():
        try:
            with open(output_file) as f:
                data = json.load(f)
            if data.get("portfolio", {}).get("cagr") is not None:
                print(f"\n  Skipping {name} (result exists)")
                return data
        except Exception:
            pass  # Corrupt file, re-run

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


def build_comparison(all_results):
    """Build exchange_comparison.json with summary stats per exchange."""
    exchanges = {}
    for name, data in all_results.items():
        p = data.get("portfolio", {})
        c = data.get("comparison", {})
        exchanges[name] = {
            "cagr": p.get("cagr"),
            "total_return": p.get("total_return"),
            "max_drawdown": p.get("max_drawdown"),
            "sharpe_ratio": p.get("sharpe_ratio"),
            "sortino_ratio": p.get("sortino_ratio"),
            "excess_cagr": c.get("excess_cagr"),
            "win_rate": c.get("win_rate"),
            "beta": c.get("beta"),
            "alpha": c.get("alpha"),
            "cash_periods": data.get("cash_periods"),
            "total_periods": data.get("n_periods"),
            "avg_stocks": data.get("avg_stocks_when_invested"),
            "years": data.get("years"),
        }

    output = {
        "strategy": "magic-formula",
        "description": "Greenblatt Magic Formula: Rank(EY) + Rank(ROCE), top 30, ex. Financials/Utilities",
        "parameters": {
            "max_stocks": 30,
            "min_stocks": 10,
            "frequency": "quarterly",
            "costs": "size-tiered",
            "sector_exclusion": ["Financial Services", "Utilities"],
            "note": "min_market_cap per-exchange in local currency (see cli_utils.MKTCAP_THRESHOLD_MAP)",
        },
        "exchanges": exchanges,
    }

    out_file = RESULTS_DIR / "exchange_comparison.json"
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nMerged results saved to {out_file}")


def main():
    print("Magic Formula: Running backtests across all exchanges")
    print(f"Exchanges: {', '.join(name for name, _ in EXCHANGES)}")
    print(f"Results dir: {RESULTS_DIR}")

    all_results = {}
    total_t0 = time.time()

    for name, args in EXCHANGES:
        result = run_exchange(name, args, skip_existing=True)
        if result is not None:
            all_results[name] = result
        else:
            print(f"  Skipping {name} (no results)")

    build_comparison(all_results)

    total_elapsed = time.time() - total_t0
    print(f"\nAll done. {len(all_results)}/{len(EXCHANGES)} exchanges completed in {total_elapsed:.0f}s")
    print(f"Results: {RESULTS_DIR / 'exchange_comparison.json'}")


if __name__ == "__main__":
    main()
