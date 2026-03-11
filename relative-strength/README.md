# Sector-Adjusted Momentum (Relative Strength)

Buys stocks that are outperforming their own sector peers, not just the market.

## Strategy

**Signal:** Stock 12-1 month return minus the equal-weighted sector average 12-1 month return.

A tech stock up 30% when tech is up 28% has 2% of genuine outperformance. A healthcare stock up 20% when healthcare is flat has 20% real momentum. Raw momentum ranks the tech stock higher. Relative strength fixes this.

Moskowitz & Grinblatt (1999) showed that about half of momentum profits come from industry momentum. Stocks ride hot sectors, not company-specific catalysts. Stripping out the sector effect isolates the part of momentum that's actually stock-specific.

**Portfolio construction:**
- Universe: All exchange stocks with MCap > threshold and known GICS sector
- Signal: (12M-1M return) − (equal-weighted sector average 12M-1M return)
- Min 5 stocks in sector to compute a valid sector average (else excluded)
- Selection: Top 30 by relative strength, equal weight
- Rebalancing: Quarterly (Jan, Apr, Jul, Oct)
- Costs: Size-tiered transaction cost model (see `costs.py`)

**Data lookback:** Prices from 12 months ago to 1 month ago (skip last month to avoid short-term reversal per Jegadeesh & Titman 1993). No financial filing lag needed — prices are real-time.

**Academic basis:**
- Moskowitz, T. & Grinblatt, M. (1999). "Do Industries Explain Momentum?" *Journal of Finance*, 54(4), 1249-1290.
- Jegadeesh, N. & Titman, S. (1993). "Returns to Buying Winners and Selling Losers." *Journal of Finance*, 48(1), 65-91.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (multi-exchange) |
| `screen.py` | Current stock screen using live TTM data |
| `generate_charts.py` | Chart generation from `results/exchange_comparison.json` |
| `results/` | JSON output files (gitignored) |
| `charts/` | PNG charts (gitignored, moved to ts-content-creator after generation) |

## Usage

```bash
# US backtest
python3 relative-strength/backtest.py --preset us --output results/returns_US.json --verbose

# India
python3 relative-strength/backtest.py --preset india --output results/returns_India.json --verbose

# All exchanges
python3 relative-strength/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen (live data)
python3 relative-strength/screen.py --preset us
python3 relative-strength/screen.py --preset india

# Generate charts (after running --global)
cd backtests
python3 relative-strength/generate_charts.py
```

## Excluded Exchanges

| Exchange | Reason |
|----------|--------|
| ASX | adjClose split-adjustment artifacts in FMP data |
| SAO | adjClose artifacts (suitable for event studies, not price strategies) |
| PAR | Pipeline gap: only 1 symbol with FY key_metrics data |

## Data Source

Ceta Research data warehouse (FMP financial data). Stock prices: `stock_eod` (~300M rows). Sector data: `profile.sector` (GICS). Market cap: `key_metrics` FY filings (point-in-time, 45-day lag).
