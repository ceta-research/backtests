# Cash Conversion Cycle (CCC)

Backtest sorting stocks by Cash Conversion Cycle efficiency across 13 exchanges from 2000-2025.

## Signal

CCC = Days Sales Outstanding + Days Inventory Outstanding - Days Payables Outstanding

| Portfolio | Threshold | Idea |
|-----------|-----------|------|
| Low CCC | < 30 days | Capital-efficient: collect fast, hold little inventory, pay slow |
| Mid CCC | 30-90 days | Average working capital efficiency |
| High CCC | > 90 days | Capital-intensive: slow collections, heavy inventory |
| Low + Decreasing | < 30 days + YoY decline | Improving efficiency (strongest signal hypothesis) |

**Universe:** Non-financial stocks above exchange-specific market cap threshold.
**Rebalancing:** Annual (April 1), equal weight.
**Benchmark:** S&P 500 (SPY).

## Key Finding

CCC does not reliably generate alpha as a standalone signal. Across most exchanges, Low CCC portfolios underperform or roughly match the S&P 500. Mid CCC (30-90 days) often outperforms Low CCC, suggesting the relationship between working capital efficiency and stock returns is not monotonic. The signal may work better as a secondary filter combined with quality or value factors.

## Usage

```bash
# Run backtest on US stocks (default)
python3 cash-conversion/backtest.py

# Run on Indian stocks
python3 cash-conversion/backtest.py --preset india --verbose

# Screen current low-CCC stocks (live TTM data)
python3 cash-conversion/screen.py
python3 cash-conversion/screen.py --preset india

# Generate charts from saved results
python3 cash-conversion/generate_charts.py
```

## Files

- `backtest.py` - Full historical backtest. Fetches data via API, caches in DuckDB, runs locally.
- `screen.py` - Live screen for low-CCC stocks using current TTM data. Single API call.
- `generate_charts.py` - Generate all charts from `results/ccc_metrics_*.json` files.
- `results/` - Pre-computed backtest results (per-exchange JSON, exchange comparison).
- `charts/` - PNG charts (cumulative growth, annual returns, cross-exchange comparison).
