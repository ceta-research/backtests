# Magic Formula (Greenblatt)

Combined rank of **Earnings Yield** and **Return on Capital Employed**. Buy the top 30 stocks that are both cheap and high-quality.

## Strategy

Joel Greenblatt's Magic Formula ranks all stocks by two metrics:
- **Earnings Yield** (EBIT / Enterprise Value) - measures cheapness
- **ROCE** (EBIT / Capital Employed) - measures quality/efficiency

Each stock gets a rank for EY (highest = rank 1) and a rank for ROCE (highest = rank 1). The combined rank (EY rank + ROCE rank) determines the portfolio. Lowest combined rank = best.

**Source:** Greenblatt, Joel. *The Little Book That Beats the Market* (2005).

## Signal

| Parameter | Value |
|-----------|-------|
| Earnings Yield | > 0 (positive only) |
| ROCE | > 0 (positive only) |
| Market Cap | > $1B |
| Sector exclusion | Financial Services, Utilities (ROCE meaningless for financials) |
| Selection | Top 30 by combined EY + ROCE rank |
| Weighting | Equal weight |
| Rebalancing | Quarterly (Jan, Apr, Jul, Oct) |
| Min stocks | 10 (holds cash below this) |
| Transaction costs | Size-tiered (0.1-0.5% one-way) |

## Usage

```bash
# Backtest US stocks (default)
python3 magic-formula/backtest.py

# Backtest with verbose output
python3 magic-formula/backtest.py --verbose

# Backtest Indian stocks
python3 magic-formula/backtest.py --exchange BSE,NSE

# All exchanges
python3 magic-formula/backtest.py --global

# Without sector exclusion
python3 magic-formula/backtest.py --no-sector-filter

# Save results
python3 magic-formula/backtest.py --output results/magic_formula_us.json

# Screen current stocks (live TTM data)
python3 magic-formula/screen.py
python3 magic-formula/screen.py --exchange BSE,NSE
```

## Data Source

Requires a Ceta Research API key:

```bash
export CR_API_KEY="your-api-key"
# or
export TS_API_KEY="your-api-key"
```

Data tables used:
- `key_metrics` (FY) - `earningsYield`, `returnOnCapitalEmployed`, `marketCap`
- `profile` - `exchange`, `sector`
- `stock_eod` - `adjClose` (at rebalance dates)
- `key_metrics_ttm` - for live screen

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Live screen on current TTM data |
| `README.md` | This file |
| `results/` | Generated backtest results |
