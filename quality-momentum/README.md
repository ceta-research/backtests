# Quality Momentum

Combines two independently-documented return factors: quality and 12-month price momentum.

Quality filters eliminate fundamentally weak companies. Momentum selects the strongest recent performers within that universe. The result: financially sound companies that are already moving.

## Strategy

**Universe:** Full exchange (not index-constrained). NYSE+NASDAQ+AMEX for US.

**Quality filter** (FY annual data, 45-day filing lag):
- ROE > 15%
- Debt-to-Equity < 1.0 (and non-negative)
- Net income > 0
- Operating cash flow > 0
- Gross margin > 20%
- Market cap > exchange-specific threshold (~$200–500M USD-equivalent)

**Momentum signal:** 12-month price return (current price / price 365 days ago − 1)

**Portfolio:** Top 30 quality-passing stocks by 12-month momentum. Equal weight.

**Rebalancing:** Semi-annual (January and July)

**Cash rule:** Hold cash (0% return) if fewer than 10 stocks pass all filters

**Transaction costs:** Size-tiered model from `costs.py`

## Academic Basis

- **Quality factor:** Asness, C., Frazzini, A., & Pedersen, L.H. (2019). "Quality Minus Junk." *Review of Accounting Studies*, 24(1), 34–112.
- **Momentum:** Jegadeesh, N. & Titman, S. (1993). "Returns to Buying Winners and Selling Losers: Implications for Stock Market Efficiency." *Journal of Finance*, 48(1), 65–91.

## Usage

```bash
# From the backtests/ directory

# US backtest
python3 quality-momentum/backtest.py --preset us --output results.json --verbose

# India backtest
python3 quality-momentum/backtest.py --preset india --verbose

# All exchanges
python3 quality-momentum/backtest.py --global --output quality-momentum/results/exchange_comparison.json --verbose

# Current stock screen (live data)
python3 quality-momentum/screen.py --preset us
python3 quality-momentum/screen.py --preset india

# Generate charts (after running --global)
python3 quality-momentum/generate_charts.py
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000–2025) |
| `screen.py` | Current stock screen (live TTM data) |
| `generate_charts.py` | Create PNG charts from results |
| `results/` | Output JSON/CSV files (gitignored) |

## Data Source

Data via [Ceta Research](https://cetaresearch.com) (FMP financial data warehouse).
Full methodology: [backtests/METHODOLOGY.md](../METHODOLOGY.md)
