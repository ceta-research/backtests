# Sector Mean Reversion

Buy stocks in the worst-performing sectors. Sectors that have badly underperformed
over 12 months tend to mean-revert — capital flows back, valuations compress, and
sentiment extremes correct.

## Strategy

**Signal:** Rank all sectors by trailing 12-month equal-weighted return. Buy all stocks
in the bottom 2 sectors.

**Universe:** Full exchange universe (NYSE + NASDAQ + AMEX for US), filtered by market cap
threshold. Not constrained to S&P 500 or any index.

**Rebalancing:** Quarterly (January, April, July, October)

**Weighting:** Equal weight across all qualifying stocks in selected sectors

**Minimum requirements:**
- At least 5 sectors with valid data (else hold cash)
- At least 5 stocks per sector (else sector excluded)
- At least 10 stocks in portfolio (else hold cash)

## Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Sectors selected | Bottom 2 | Concentrated enough to capture reversion premium |
| Lookback | 12 months | Long enough to identify out-of-favor sectors, not noise |
| Rebalancing | Quarterly | Sector trends rotate on 3-6 month timescales |
| Market cap | Exchange-specific | Liquid mid-to-large cap only |
| Costs | Size-tiered | 0.1% (>$10B), 0.3% ($2-10B), 0.5% (<$2B) — one way |

## Academic Basis

Moskowitz, T.J. & Grinblatt, M. (1999). "Do industries explain momentum?" *Journal of
Finance*, 54(4), 1249-1290.

The paper documents that industry factors explain a significant portion of stock momentum.
This strategy applies the contrarian extension: at extreme underperformance, momentum
reverses. Sectors ranked in the worst 2 historically show mean reversion over the next
12 months, driven by valuation compression, capital rotation, and sentiment extremes.

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current stock screen (live data) |
| `generate_charts.py` | Chart generation from results JSON |
| `results/` | Computed results (JSON per exchange, exchange_comparison.json) |
| `charts/` | Generated PNG charts (gitignored) |

## Usage

```bash
# Run from backtests/ directory

# US backtest
python3 sector-rotation/backtest.py --preset us --output sector-rotation/results/returns_US_MAJOR.json --verbose

# India backtest
python3 sector-rotation/backtest.py --preset india --output sector-rotation/results/returns_BSE_NSE.json --verbose

# All exchanges
python3 sector-rotation/backtest.py --global --output sector-rotation/results/exchange_comparison.json --verbose

# Current screen (what to buy today)
python3 sector-rotation/screen.py --preset us

# Generate charts (after running global backtest)
python3 sector-rotation/generate_charts.py
```

## Data Notes

- Prices fetched at quarter-start dates only (Jan/Apr/Jul/Oct, days 1-15 of each month)
- 12-month trailing return = (price at Q) / (price at same month, prior year) - 1
- Sector mapping from `profile` table (current snapshot — sectors rarely change)
- `marketCap` in `profile` is in local currency per exchange
- No look-ahead bias: sector rankings computed using prices at rebalance date

## Exchange Eligibility

All major exchanges tested. Minimum: 5 qualifying sectors (each with 5+ stocks
with valid 12-month returns). See `results/exchange_comparison.json` for results.

Exchanges with fewer sector diversification (e.g., resource-heavy) may have fewer
cash periods but more concentrated sector bets.

## Data Attribution

*Data: Ceta Research (FMP financial data warehouse). Past performance does not
guarantee future results.*
