#!/usr/bin/env python3
"""Run Piotroski F-Score backtest across all eligible exchanges.

Iterates through exchange presets, runs the backtest for each, and collects
results into a single exchange_comparison.json.

Usage:
    cd backtests
    python3 piotroski/run_all_exchanges.py
    python3 piotroski/run_all_exchanges.py --verbose
"""

import json
import os
import subprocess
import sys
import time

# Exchanges to test, ordered by expected data depth
EXCHANGES_TO_TEST = [
    # Tier 1: Large, deep data
    ("us", "US_MAJOR"),
    ("india", "India"),
    ("japan", "JPX"),
    ("uk", "LSE"),
    ("hongkong", "HKSE"),
    ("germany", "XETRA"),
    ("korea", "KSC"),
    ("australia", "ASX"),
    # Tier 2: Good data, smaller universe
    ("china", "China"),
    ("taiwan", "Taiwan"),
    ("canada", "Canada"),
    ("sweden", "STO"),
    ("thailand", "SET"),
    ("brazil", "SAO"),
    ("southafrica", "JSE"),
    ("switzerland", "SIX"),
    ("singapore", "SGX"),
    ("norway", "OSL"),
]


def main():
    verbose = "--verbose" in sys.argv or "-v" in sys.argv

    results_dir = os.path.join(os.path.dirname(__file__), "results")
    os.makedirs(results_dir, exist_ok=True)

    all_results = {}
    failed = []
    skipped = []
    total_start = time.time()

    for preset, name in EXCHANGES_TO_TEST:
        output_file = os.path.join(results_dir, f"piotroski_{name}.json")
        print(f"\n{'='*65}")
        print(f"  Running: {name} (--preset {preset})")
        print(f"{'='*65}")

        backtest_script = os.path.join(os.path.dirname(__file__), "backtest.py")
        cmd = [
            sys.executable, backtest_script,
            "--preset", preset,
            "--output", output_file,
        ]
        if verbose:
            cmd.append("--verbose")

        start = time.time()
        try:
            result = subprocess.run(
                cmd,
                cwd=os.path.dirname(os.path.dirname(__file__)),
                capture_output=not verbose,
                text=True,
                timeout=600,
            )

            elapsed = time.time() - start

            if result.returncode != 0:
                print(f"  FAILED ({elapsed:.0f}s)")
                if not verbose and result.stderr:
                    print(f"  Error: {result.stderr[-500:]}")
                failed.append((name, "non-zero exit"))
                continue

            # Load and store results
            if os.path.exists(output_file):
                with open(output_file) as f:
                    data = json.load(f)
                all_results[name] = data

                # Quick summary
                p = data.get("portfolios", {})
                high = p.get("score_8_9", {})
                low = p.get("score_0_2", {})
                spy = p.get("sp500", {})
                spread = data.get("spread_cagr", 0)

                print(f"  OK ({elapsed:.0f}s): Score 8-9={high.get('cagr', 'N/A')}% CAGR, "
                      f"Score 0-2={low.get('cagr', 'N/A')}%, "
                      f"SPY={spy.get('cagr', 'N/A')}%, "
                      f"Spread={spread:+.1f}%")
            else:
                skipped.append((name, "no output file"))
                print(f"  SKIPPED: no output file generated ({elapsed:.0f}s)")

        except subprocess.TimeoutExpired:
            print(f"  TIMEOUT (>600s)")
            failed.append((name, "timeout"))
        except Exception as e:
            print(f"  ERROR: {e}")
            failed.append((name, str(e)))

    # Save combined results
    comparison_file = os.path.join(results_dir, "exchange_comparison.json")
    with open(comparison_file, "w") as f:
        json.dump(all_results, f, indent=2)

    total_elapsed = time.time() - total_start

    # Summary
    print(f"\n{'='*65}")
    print(f"  PIOTROSKI F-SCORE: ALL EXCHANGES COMPLETE")
    print(f"{'='*65}")
    print(f"  Total time: {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")
    print(f"  Successful: {len(all_results)}")
    print(f"  Failed: {len(failed)}")
    print(f"  Skipped: {len(skipped)}")
    print(f"  Results: {comparison_file}")

    if all_results:
        print(f"\n{'Exchange':<12} {'Score 8-9':>10} {'Score 0-2':>10} {'All Value':>10} {'SPY':>8} {'Spread':>8}")
        print("-" * 62)
        for name, data in sorted(all_results.items(),
                                  key=lambda x: x[1].get("portfolios", {}).get("score_8_9", {}).get("cagr", 0),
                                  reverse=True):
            p = data.get("portfolios", {})
            h = p.get("score_8_9", {}).get("cagr", "N/A")
            l = p.get("score_0_2", {}).get("cagr", "N/A")
            a = p.get("all_value", {}).get("cagr", "N/A")
            s = p.get("sp500", {}).get("cagr", "N/A")
            sp = data.get("spread_cagr", 0)
            h_str = f"{h:.1f}%" if isinstance(h, (int, float)) else h
            l_str = f"{l:.1f}%" if isinstance(l, (int, float)) else l
            a_str = f"{a:.1f}%" if isinstance(a, (int, float)) else a
            s_str = f"{s:.1f}%" if isinstance(s, (int, float)) else s
            print(f"  {name:<12} {h_str:>10} {l_str:>10} {a_str:>10} {s_str:>8} {sp:>+7.1f}%")

    if failed:
        print(f"\nFailed exchanges:")
        for name, reason in failed:
            print(f"  {name}: {reason}")


if __name__ == "__main__":
    main()
