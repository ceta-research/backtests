#!/usr/bin/env python3
"""NSE Strategy Arena -- Run All & Compare
-------------------------------------------
Runs all intraday strategies on NSE minute data via CR API,
collects existing EOD India results from completed backtests,
and produces a unified comparison table ranked by Calmar ratio.

Usage:
    # Default configs only (fast, ~3 API calls)
    python3 -m nse_arena.run

    # Full parameter sweeps (slower, many API calls)
    python3 -m nse_arena.run --sweep

    # Single strategy
    python3 -m nse_arena.run --strategy gap-up-scalp
    python3 -m nse_arena.run --strategy orb --sweep

    # Skip EOD collection
    python3 -m nse_arena.run --intraday-only
"""

import argparse
import json
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from cr_client import CetaResearch
from nse_arena.framework import (
    run_strategy, run_sweep, collect_eod_india_results, print_comparison
)
from nse_arena.gap_up_scalp import GapUpScalp
from nse_arena.orb import OpeningRangeBreakout
from nse_arena.vwap_reversion import VWAPReversion


STRATEGIES = {
    "gap-up-scalp": GapUpScalp,
    "orb": OpeningRangeBreakout,
    "vwap": VWAPReversion,
}


def main():
    parser = argparse.ArgumentParser(description="NSE Strategy Arena")
    parser.add_argument("--strategy", type=str, choices=list(STRATEGIES.keys()),
                        help="Run a single strategy (default: all)")
    parser.add_argument("--sweep", action="store_true",
                        help="Run full parameter sweeps (default: single best config)")
    parser.add_argument("--intraday-only", action="store_true",
                        help="Skip collecting EOD results")
    parser.add_argument("--api-key", type=str,
                        help="CR API key (or set CR_API_KEY)")
    parser.add_argument("--output", type=str, default="nse_arena/results/comparison.json",
                        help="Output file for results")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    client = CetaResearch(api_key=args.api_key)

    # Determine which strategies to run
    if args.strategy:
        strats = {args.strategy: STRATEGIES[args.strategy]}
    else:
        strats = STRATEGIES

    # ── Run intraday strategies ──────────────────────────────────────────
    all_results = []
    best_per_strategy = {}

    for key, cls in strats.items():
        strategy = cls()
        print(f"\n{'#'*70}")
        print(f"  Strategy: {strategy.name} ({strategy.strategy_type})")
        print(f"{'#'*70}")

        if args.sweep:
            results = run_sweep(client, strategy, verbose=True)
            if results:
                all_results.extend(results)
                best_per_strategy[key] = results[0]  # Best by Calmar
                print(f"\n  Best config (by Calmar): "
                      f"CAGR={results[0]['cagr']:+.2f}%  "
                      f"MaxDD={results[0]['max_dd']:.1f}%  "
                      f"Calmar={results[0].get('calmar', 'N/A')}")
        else:
            r = run_strategy(client, strategy, verbose=True)
            if "error" not in r:
                all_results.append(r)
                best_per_strategy[key] = r

    # ── Collect EOD India results ────────────────────────────────────────
    eod_results = []
    if not args.intraday_only:
        print(f"\n{'#'*70}")
        print(f"  Collecting EOD India results from completed backtests...")
        print(f"{'#'*70}")
        eod_results = collect_eod_india_results()
        print(f"  Found {len(eod_results)} EOD strategies with India data")
        for r in eod_results:
            print(f"    {r['strategy']:<25} CAGR={r['cagr']:+.1f}%  "
                  f"MaxDD={r.get('max_dd', '?')}%  "
                  f"Calmar={r.get('calmar', 'N/A')}")

    # ── Comparison ───────────────────────────────────────────────────────
    # Use best config per intraday strategy for fair comparison
    comparison = list(best_per_strategy.values()) + eod_results
    if comparison:
        print_comparison(comparison, title="NSE Strategy Arena -- All Strategies")

    # ── Save results ─────────────────────────────────────────────────────
    output_path = Path(__file__).parent.parent / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Strip full_metrics for cleaner JSON
    save_data = []
    for r in comparison:
        r_copy = {k: v for k, v in r.items() if k != "full_metrics"}
        save_data.append(r_copy)

    with open(output_path, "w") as f:
        json.dump(save_data, f, indent=2, default=str)
    print(f"\n  Results saved to {output_path}")

    # ── Verdict ──────────────────────────────────────────────────────────
    valid = [r for r in comparison if r.get("calmar") is not None]
    if valid:
        best = max(valid, key=lambda x: x["calmar"])
        print(f"\n  {'='*60}")
        print(f"  VERDICT: Safest NSE strategy (best risk-adjusted return):")
        print(f"  {best['strategy']}")
        print(f"    CAGR:   {best['cagr']:+.2f}%")
        print(f"    MaxDD:  {best.get('max_dd', '?')}%")
        print(f"    Calmar: {best['calmar']:.3f}")
        print(f"    Sharpe: {best.get('sharpe', '?')}")
        print(f"  {'='*60}")


if __name__ == "__main__":
    main()
