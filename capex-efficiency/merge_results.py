#!/usr/bin/env python3
"""Merge individual backtest results into exchange_comparison.json format."""
import json
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from metrics import compute_annual_returns

results_dir = Path(__file__).parent / "results"

# Mapping: chart_key -> (metrics_filename_stem, benchmark_name)
# chart_key is what generate_charts.py uses (must match exchange_comparison.json keys)
exchanges = {
    "US_MAJOR":    ("US_MAJOR", "S&P 500"),
    "India":       ("NSE", "Sensex"),
    "XETRA":       ("XETRA", "DAX"),
    "China":       ("China", "SSE Composite"),
    "LSE":         ("LSE", "FTSE 100"),
    "Switzerland": ("SIX", "SMI"),
}

output = {}

for chart_key, (metrics_stem, bench_name) in exchanges.items():
    print(f"Processing {chart_key} ({metrics_stem})...")

    metrics_file = results_dir / f"capex-efficiency_metrics_{metrics_stem}.json"
    if not metrics_file.exists():
        print(f"  Skipping: {metrics_file} not found")
        continue

    with open(metrics_file) as f:
        metrics = json.load(f)

    returns_file = results_dir / f"returns_{metrics_stem}.csv"
    if not returns_file.exists():
        print(f"  Skipping: {returns_file} not found")
        continue

    df = pd.read_csv(returns_file)

    period_returns = df["return"].tolist()
    period_dates = df["start_date"].tolist()
    bench_returns_per_period = df["spy_return"].tolist()  # actually local benchmark now

    annual_returns_data = compute_annual_returns(
        period_returns, bench_returns_per_period, period_dates, periods_per_year=1
    )

    annual_returns = [
        {
            "year": ar["year"],
            "portfolio": round(ar["portfolio"] * 100, 2),
            "spy": round(ar["benchmark"] * 100, 2),
            "excess": round(ar["excess"] * 100, 2),
        }
        for ar in annual_returns_data
    ]

    output[chart_key] = {
        "benchmark_name": bench_name,
        "portfolio": {
            "cagr": round(metrics["portfolio"]["cagr"] * 100, 2),
            "total_return": round(metrics["portfolio"]["total_return"] * 100, 2),
            "max_drawdown": round(metrics["portfolio"]["max_drawdown"] * 100, 2),
            "max_dd_duration_periods": metrics["portfolio"]["max_dd_duration_periods"],
            "annualized_volatility": round(metrics["portfolio"]["annualized_volatility"] * 100, 2),
            "sharpe_ratio": round(metrics["portfolio"]["sharpe_ratio"], 3) if metrics["portfolio"]["sharpe_ratio"] is not None else None,
            "sortino_ratio": round(metrics["portfolio"]["sortino_ratio"], 3) if metrics["portfolio"]["sortino_ratio"] is not None else None,
            "calmar_ratio": round(metrics["portfolio"]["calmar_ratio"], 3) if metrics["portfolio"]["calmar_ratio"] is not None else None,
            "var_95": round(metrics["portfolio"]["var_95"] * 100, 2),
            "cvar_95": round(metrics["portfolio"]["cvar_95"] * 100, 2),
            "best_period": round(metrics["portfolio"]["best_period"] * 100, 2),
            "worst_period": round(metrics["portfolio"]["worst_period"] * 100, 2),
            "pct_negative_periods": round(metrics["portfolio"]["pct_negative_periods"] * 100, 2),
            "max_consecutive_losses": metrics["portfolio"]["max_consecutive_losses"],
        },
        "spy": {  # key name kept for generate_charts.py back-compat; values are local benchmark
            "cagr": round(metrics["benchmark"]["cagr"] * 100, 2),
            "total_return": round(metrics["benchmark"]["total_return"] * 100, 2),
            "max_drawdown": round(metrics["benchmark"]["max_drawdown"] * 100, 2) if metrics["benchmark"]["max_drawdown"] != 0 else 0.0,
            "annualized_volatility": round(metrics["benchmark"]["annualized_volatility"] * 100, 2),
            "sharpe_ratio": round(metrics["benchmark"]["sharpe_ratio"], 3) if metrics["benchmark"]["sharpe_ratio"] is not None else None,
            "sortino_ratio": round(metrics["benchmark"]["sortino_ratio"], 3) if metrics["benchmark"]["sortino_ratio"] is not None else None,
            "calmar_ratio": round(metrics["benchmark"]["calmar_ratio"], 3) if metrics["benchmark"]["calmar_ratio"] is not None else None,
            "var_95": round(metrics["benchmark"]["var_95"] * 100, 2),
            "max_consecutive_losses": metrics["benchmark"]["max_consecutive_losses"],
            "pct_negative_periods": round(metrics["benchmark"]["pct_negative_periods"] * 100, 2),
        },
        "comparison": {
            "excess_cagr": round(metrics["comparison"]["excess_cagr"] * 100, 2),
            "win_rate": round(metrics["comparison"]["win_rate"] * 100, 2),
            "information_ratio": round(metrics["comparison"]["information_ratio"], 3) if metrics["comparison"]["information_ratio"] is not None else None,
            "tracking_error": round(metrics["comparison"]["tracking_error"] * 100, 2),
            "up_capture": round(metrics["comparison"]["up_capture"] * 100, 2) if metrics["comparison"]["up_capture"] is not None else None,
            "down_capture": round(metrics["comparison"]["down_capture"] * 100, 2) if metrics["comparison"]["down_capture"] is not None else None,
            "beta": round(metrics["comparison"]["beta"], 3) if metrics["comparison"]["beta"] is not None else None,
            "alpha": round(metrics["comparison"]["alpha"], 3) if metrics["comparison"]["alpha"] is not None else None,
        },
        "excess_cagr": round(metrics["comparison"]["excess_cagr"] * 100, 2),
        "win_rate_vs_spy": round(metrics["comparison"]["win_rate"] * 100, 2),
        "annual_returns": annual_returns,
        "invested_periods": len(period_returns),
    }

    print(f"  ✓ {chart_key}: {output[chart_key]['portfolio']['cagr']}% CAGR")

output_file = results_dir / "exchange_comparison.json"
with open(output_file, "w") as f:
    json.dump(output, f, indent=2)

print(f"\n✓ Merged results saved to {output_file}")
print(f"  Exchanges: {', '.join(output.keys())}")
