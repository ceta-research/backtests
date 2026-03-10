# Analyst Upgrade Clusters

**Category:** Event Study
**Academic Reference:** Womack (1996), Barber, Lehavy, McNichols & Trueman (2001)

## Strategy

Measures abnormal stock returns after analyst rating upgrade clusters. An "upgrade cluster" fires when the aggregate bullish analyst count (StrongBuy + Buy) for a stock increases by 2 or more between consecutive monthly observations.

**Signal:** `bullish_count_t - bullish_count_t-1 >= 2`, where observations are 14–30 days apart.

**Data source:** `grades_historical` table (FMP aggregate analyst rating counts per symbol per date).

**Note:** The `analystRatings*` columns in this table are stored as `UINT16` in parquet. All delta computations must cast to `INTEGER` first to avoid underflow when counts decrease.

**Categories:**
- `upgrade_small` — delta = 2 (minimum cluster, most common)
- `upgrade_medium` — delta = 3–4
- `upgrade_large` — delta >= 5 (strongest consensus shift)
- `downgrade_cluster` — bearish delta >= 2 (Sell + StrongSell increase)

**Universe:** Stocks with market cap > exchange-specific threshold (local currency)

**Gap filter:** 14–30 days between consecutive observations. This is critical: FMP recorded daily updates for many symbols in 2022, producing a 10–100x spike in detected clusters vs other years. The minimum 14-day gap normalizes observation frequency across years.

**Windows:** T+1, T+5, T+21, T+63 trading days after the cluster

**Benchmark:** SPY (US), or regional country ETF for non-US exchanges

**Effective date range:** 2019–2025 (grades_historical is sparse before 2019 with <200 symbols)

## Usage

```bash
cd backtests/

# US market (default)
python3 upgrade-cluster/backtest.py

# Specific preset
python3 upgrade-cluster/backtest.py --preset india --verbose

# Single exchange
python3 upgrade-cluster/backtest.py --exchange JPX --output upgrade-cluster/results/upgrade_cluster_JPX.json

# All exchanges (global run)
python3 upgrade-cluster/backtest.py --global --output upgrade-cluster/results/exchange_comparison.json

# Live screen: current upgrade clusters
python3 upgrade-cluster/screen.py
python3 upgrade-cluster/screen.py --preset us --min-delta 3

# Generate charts (after running backtest)
python3 upgrade-cluster/generate_charts.py
python3 upgrade-cluster/generate_charts.py --all-exchanges
```

## Results Files

| File | Description |
|------|-------------|
| `results/upgrade_cluster_{EXCHANGE}.json` | Per-exchange results (CAR by cluster category) |
| `results/upgrade_cluster_{EXCHANGE}_events.csv` | Event-level returns (symbol, date, category, returns) |
| `results/exchange_comparison.json` | Global comparison across all exchanges |
| `charts/1_us_car_by_category.png` | Grouped bar: CAR by cluster size at each window |
| `charts/2_us_car_progression.png` | Line chart: CAR progression T+1 → T+63 by category |
| `charts/3_exchange_comparison.png` | Exchange comparison at T+1 |

## Data Quality Notes

1. **Pre-2019 gap:** grades_historical has <200 symbols with data before 2019. Effective analysis period is 2019–2025 (7 years).
2. **2022 observation frequency artifact:** FMP appears to have recorded daily updates for many symbols in 2022, causing a 10–100x spike in detected clusters vs adjacent years. The 14-day minimum gap filter eliminates this artifact.
3. **UINT16 overflow:** The `analystRatings*` columns are unsigned 16-bit integers. Computing deltas directly can underflow when counts decrease (e.g., 3 - 5 on UINT16 = 65534, not -2). Always cast to INTEGER.

## Academic Foundation

Womack (1996) documented that analyst upgrades to Strong Buy generate significant positive abnormal returns persisting for months after the recommendation change. Barber et al. (2001) extended this finding: changes in consensus recommendations — rather than individual calls — are the stronger predictor of future returns. Their logic: a single analyst changing a rating could reflect firm-specific reasons (commission generation, coverage initiation), but when multiple analysts independently reach the same conclusion in a short window, they're likely responding to the same fundamental catalyst.

## Key Findings (US, NYSE+NASDAQ+AMEX, 2019–2025)

See `results/upgrade_cluster_NYSE_NASDAQ_AMEX.json` for full metrics.
