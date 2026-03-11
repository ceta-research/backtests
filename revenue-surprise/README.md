# Revenue Surprise Momentum Strategy

Quarterly momentum strategy based on positive revenue surprises: stocks that beat analyst revenue consensus estimates.

## Strategy

**Signal**: Quarterly revenue > analyst consensus estimate (positive surprise)

**Filters**:
- Revenue surprise: 0% < surprise < 50% (beat estimates, exclude outliers)
- ROE > 8% (profitable companies)
- D/E < 2.5 (reasonable leverage)
- Market cap > exchange-specific threshold

**Portfolio Construction**:
- Rank by revenue surprise % (highest first)
- Select top 30 stocks
- Equal weight
- Hold cash if < 10 stocks qualify
- Rebalance quarterly (Jan/Apr/Jul/Oct)

**Period**: 2000-2024 (quarterly data from analyst_estimates)

## Academic Foundation

Based on Jegadeesh & Livnat (2006) "Revenue Surprises and Stock Returns", *Journal of Accounting and Economics* 41(1-2), 147-166.

Revenue surprises are harder to manipulate than earnings (which can be inflated by cost cuts), making them a more reliable signal of genuine demand growth. The academic literature shows persistent positive drift following positive revenue surprises, especially in the 1-3 months after announcement.

## Key Results

### US (NYSE+NASDAQ+AMEX)
- **CAGR**: 10.11% vs SPY 8.01% (+2.10% excess)
- **Sharpe**: 0.376 vs 0.354
- **Max Drawdown**: -44.9% vs -45.5%
- **Up Capture**: 110.9% (captures more upside)
- **Down Capture**: 91.3% (slightly better than market in crashes)
- **Cash periods**: 0 / 103 quarters (always invested)
- **Period**: 2000-2024 (103 quarters)

### Other Markets
Limited quarterly analyst revenue estimate coverage in non-US markets leads to high cash periods:
- India: +1.03% excess but 45% cash (sparse quarterly estimates)
- Canada: -3.55% excess, 12% cash
- Japan/Taiwan: 75% cash (no quarterly signal data)
- China: 75% cash

**Key Finding**: Revenue surprise momentum works where quarterly analyst estimates are comprehensive (US). Other markets lack sufficient quarterly estimate data for signal generation.

## Data Requirements

The strategy requires:
1. **Quarterly revenue actuals**: `income_statement` (period = Q1/Q2/Q3/Q4)
2. **Quarterly revenue estimates**: `analyst_estimates` (period = 'quarter')
3. **Quality metrics**: `key_metrics` (ROE, market cap) - uses FY data for stability
4. **Leverage data**: `financial_ratios` (D/E) - uses FY data
5. **Price data**: `stock_eod` (for return computation)

**Critical**: The join between income_statement and analyst_estimates requires:
- Period matching: quarterly income → quarterly estimates
- Date proximity: filing_epoch within 90 days of estimate date
- Point-in-time compliance: 45-day lag after quarter-end for data availability

## Usage

```bash
# Screen current stocks (US)
python3 revenue-surprise/screen.py

# Backtest US
python3 revenue-surprise/backtest.py --preset us --output results/returns_US_MAJOR.json

# Backtest all exchanges
python3 revenue-surprise/backtest.py --global --output results/exchange_comparison.json --verbose

# Backtest India
python3 revenue-surprise/backtest.py --preset india

# Run on cloud
python3 revenue-surprise/backtest.py --cloud --preset us
```

## Files

- `backtest.py` - Historical backtest (2000-2024, quarterly)
- `screen.py` - Current stock screen (live data)
- `generate_charts.py` - Chart generation from results
- `results/exchange_comparison.json` - Multi-exchange results
- `results/returns_{EXCHANGE}.json` - Per-exchange results

## Notes

**Why quarterly rebalancing?**
Revenue surprise effects decay within months. Annual rebalancing (using FY data) produced 5.56% CAGR with -5.05% excess. Quarterly rebalancing (using quarterly data) captures the short-term drift: 10.11% CAGR with +2.10% excess.

**Data quality**:
- US: Analyst revenue estimates available quarterly from ~1992 onward (comprehensive)
- International: Quarterly analyst estimates sparse or forward-looking only (not historical)
- Asia-Pacific: Many markets report semi-annually or annually only, not quarterly

**Geographic conclusion**: This is a US-centric strategy due to data availability. Content should focus on US flagship blog + comparison showing limited international applicability.
