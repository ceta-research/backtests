#!/usr/bin/env python3
"""
Run Graham Net-Net backtest across all eligible exchanges.

Eligible (based on inspection - avg 10+ qualifying stocks/year, 20+ years data):
- US_MAJOR (NYSE+NASDAQ+AMEX): avg ~180/yr
- HKSE: avg 142/yr
- JPX (Japan): avg 102/yr
- BSE+NSE (India): avg ~117/yr
- KSC (Korea): avg 38/yr
- NYSE: avg 27/yr
- NSE: avg 26/yr
- TSX: avg 25/yr
- LSE: avg 23/yr
- TAI: avg 20/yr (borderline)

Excluded:
- ASX: adjClose split issues
- SAO: adjClose split issues
- China (SHH/SHZ): avg only 4/yr — too thin

Usage:
    python3 graham-net-net/run_all_exchanges.py
    python3 graham-net-net/run_all_exchanges.py --output results/exchange_comparison.json
"""

import json
import os
import subprocess
import sys
import time

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

EXCHANGES = [
    ("US_MAJOR",  ["NYSE", "NASDAQ", "AMEX"]),
    ("JPX",       ["JPX"]),
    ("HKSE",      ["HKSE"]),
    ("India",     ["NSE"]),
    ("KSC",       ["KSC"]),
    ("LSE",       ["LSE"]),
    ("TSX",       ["TSX"]),
    ("TAI",       ["TAI", "TWO"]),
]

all_results = {}

for name, exchanges in EXCHANGES:
    ex_str = ",".join(exchanges)
    output_file = os.path.join(RESULTS_DIR, f"returns_{name}.json")
    print(f"\n{'='*60}")
    print(f"  Running: {name} ({ex_str})")
    print(f"{'='*60}")

    t0 = time.time()
    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(__file__), "backtest.py"),
        "--exchange", ex_str,
        "--output", output_file,
        "--verbose",
    ]
    result = subprocess.run(cmd, capture_output=False, text=True)
    elapsed = time.time() - t0
    print(f"  Completed in {elapsed:.0f}s (exit code: {result.returncode})")

    if result.returncode == 0 and os.path.exists(output_file):
        with open(output_file) as f:
            data = json.load(f)
        all_results[name] = data
        p = data.get("portfolio", {})
        c = data.get("comparison", {})
        print(f"  CAGR: {p.get('cagr', 'N/A')}%, ExcessCAGR: {c.get('excess_cagr', 'N/A')}%, "
              f"Sharpe: {p.get('sharpe_ratio', 'N/A')}, MaxDD: {p.get('max_drawdown', 'N/A')}%")

# Save combined results
if "--output" in sys.argv:
    idx = sys.argv.index("--output")
    out_path = sys.argv[idx + 1]
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nAll results saved to {out_path}")

# Print summary table
print("\n\n" + "="*80)
print("  SUMMARY: Graham Net-Net — All Exchanges")
print("="*80)
print(f"  {'Exchange':<15} {'CAGR':>8} {'SPY CAGR':>10} {'Excess':>8} {'Sharpe':>8} {'MaxDD':>8} {'AvgStocks':>10}")
print("  " + "-"*70)
for name, _ in EXCHANGES:
    output_file = os.path.join(RESULTS_DIR, f"returns_{name}.json")
    if os.path.exists(output_file):
        with open(output_file) as f:
            data = json.load(f)
        p = data.get("portfolio", {})
        b = data.get("spy", {})
        c = data.get("comparison", {})
        avg = data.get("avg_stocks_when_invested", "N/A")
        print(f"  {name:<15} {str(p.get('cagr','N/A')):>7}% {str(b.get('cagr','N/A')):>9}% "
              f"{str(c.get('excess_cagr','N/A')):>7}% {str(p.get('sharpe_ratio','N/A')):>8} "
              f"{str(p.get('max_drawdown','N/A')):>7}% {str(avg):>10}")
    else:
        print(f"  {name:<15} {'N/A':>7}  (no results file)")
