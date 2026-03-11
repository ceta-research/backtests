# Value-Momentum Strategy

Combines two return factors with negative correlation — value (cheap P/E) and price momentum (12-month trend) — into a single composite signal.

## The Signal

**Step 1 — Value filter** (eliminates growth traps and money-losers):
- P/E ratio: 0 to 20 (positive earnings, reasonable valuation)
- ROE > 10% (basic profitability)
- Debt-to-Equity < 1.0 (not over-levered)
- Market cap > exchange threshold

**Step 2 — Momentum** (eliminates value traps — cheap but falling):
- 12-month price return (12M lookback, 1M skip)
- Only stocks with computable 12M momentum are ranked

**Step 3 — Composite ranking**:
- Value rank: percentile by P/E ascending (lower P/E = better value rank)
- Momentum rank: percentile by 12M return descending (higher return = better)
- Composite score: average of both percentile ranks
- Select top 30 stocks by composite score

## Portfolio Construction

- **Weighting**: Equal weight
- **Rebalancing**: Semi-annual (January 1, July 1)
- **Min stocks**: 10 (hold cash if fewer qualify)
- **Max stocks**: 30 (top 30 by composite score)
- **Transaction costs**: Size-tiered (0.1%–0.5% one-way)
- **Period**: 2000–2025

## Academic Basis

Asness, Moskowitz, and Pedersen (2013). "Value and Momentum Everywhere." *Journal of Finance*, 68(3), 929–985.

The paper documents that value and momentum factors work across asset classes and countries, and that their negative correlation makes them powerful diversifiers when combined. Long value + long momentum produces better risk-adjusted returns than either factor alone.

## Usage

```bash
# Current screen (live data)
python3 value-momentum/screen.py                     # US
python3 value-momentum/screen.py --preset india
python3 value-momentum/screen.py --preset japan

# Historical backtest
python3 value-momentum/backtest.py                   # US default
python3 value-momentum/backtest.py --preset india
python3 value-momentum/backtest.py --preset japan
python3 value-momentum/backtest.py --global --output results/exchange_comparison.json

# Generate charts (after running --global)
python3 value-momentum/generate_charts.py
```

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)

## Results

See `results/exchange_comparison.json` for full multi-exchange results after running `--global`.
