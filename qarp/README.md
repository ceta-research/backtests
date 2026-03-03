# QARP: Quality at a Reasonable Price

A 7-factor stock selection signal that combines quality metrics with value constraints. Backtested across multiple exchanges from 2000-2025.

## Signal

All seven criteria must be met simultaneously:

| # | Filter | Threshold |
|---|--------|-----------|
| 1 | Piotroski F-Score | >= 7 |
| 2 | Return on Equity | > 15% |
| 3 | Debt-to-Equity | < 0.5 |
| 4 | Current Ratio | > 1.5 |
| 5 | Income Quality (OCF/NI) | > 1.0 |
| 6 | Price-to-Earnings | 5 to 25 |
| 7 | Market Cap | > $1B |

**Portfolio:** Equal weight all qualifying stocks. Cash if fewer than 10 qualify.
**Rebalancing:** Semi-annual (January and July), 2000-2025.
**Benchmark:** S&P 500 (SPY).

## Usage

```bash
# Configure your data source (see root README)
export CR_API_KEY="your_key_here"

# Screen current US stocks
python3 qarp/screen.py

# Screen Indian stocks
python3 qarp/screen.py --exchange BSE,NSE

# Full 25-year backtest on US stocks
python3 qarp/backtest.py

# Backtest German stocks with verbose output
python3 qarp/backtest.py --exchange XETRA --verbose

# Backtest all exchanges, save results
python3 qarp/backtest.py --global --output results_global.json
```

## Files

- `backtest.py` - Full historical backtest. Fetches data via API, caches in DuckDB, runs locally.
- `screen.py` - Instant screen on current (TTM) data. Single API call.
- `generate_charts.py` - Regenerate all 14 charts from `results/exchange_comparison.json`.
- `results/` - Pre-computed backtest results (exchange comparison JSON, per-exchange return CSVs).
- `charts/` - 14 PNG charts used in blog posts (cumulative growth, annual returns, comparison).

## How the backtest works

1. **Fetch** (30-60s): 7 SQL queries pull historical financials and prices for the target exchange
2. **Cache**: All data loaded into in-memory DuckDB
3. **Screen**: At each semi-annual rebalance date, compute Piotroski scores and apply all 7 filters
4. **Returns**: Equal-weight portfolio return for each period, compared to SPY

## Exchange presets

| Preset | Exchanges |
|--------|-----------|
| `--preset us` | NYSE, NASDAQ, AMEX |
| `--preset india` | BSE, NSE |
| `--preset germany` | XETRA |
| `--preset china` | SHZ, SHH |
| `--preset hongkong` | HKSE |

Or pass any exchange with `--exchange EXCHANGE_CODE`.

## Excluded Exchanges

| Exchange | Reason |
|----------|--------|
| ASX | Broken adjClose data. 314 stocks have >1000x price ratios from incorrect stock split adjustments. |
| SAO | Broken adjClose data. 20+ stocks with extreme price ratios from missed split adjustments. |
| JPX | No FY data in key_metrics/financial_ratios tables. TTM has 4,016 symbols but FY has 0. Data pipeline gap. |
| LSE | No FY data. Same as JPX. TTM has 3,745 symbols but FY has 0. |

See [DATA_QUALITY_ISSUES.md](../DATA_QUALITY_ISSUES.md) for details.
