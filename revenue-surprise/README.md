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

August 2026 rerun. Non-US exchanges are measured against their own local index, not SPY.

### US (NYSE+NASDAQ+AMEX)
- **CAGR**: 11.55% vs S&P 500 8.02% (+3.54% excess, Jensen alpha 3.35%)
- **Sharpe**: 0.466 vs 0.361
- **Max Drawdown**: -45.3% vs -43.9% (deeper than the index)
- **Up Capture**: 114.0% (captures more upside)
- **Down Capture**: 85.2% (participates less in down quarters)
- **Cash periods**: 0 / 103 quarters (always invested)
- **Period**: 2000-2025 (103 quarters)

### Other Markets
Excess vs local benchmark, with cash periods driven by quarterly estimate coverage:
- UK: +2.38% vs FTSE 100, 6% cash (was 45%; FMP backfilled LSE coverage). Only 10.6 avg stocks, Sharpe 0.011
- Canada: -0.00% vs TSX Composite, 12% cash
- Hong Kong: -1.17% vs Hang Seng, 42% cash
- Japan: -1.30% vs Nikkei 225, 75% cash (first quarterly estimate 2016)
- Taiwan: -1.73% vs TAIEX, 75% cash (2011)
- India: -2.41% vs Sensex, 45% cash
- China: -3.59% vs SSE Composite, 75% cash (2011)
- Germany: -3.80% vs DAX, 0% cash

### Diagnostics (opt-in flags, default OFF)
- `--domicile-filter` on Germany: XETRA lists 2,665 symbols but only 715 are German-domiciled.
  Restricting to German companies moves CAGR 1.32% -> 3.50%, excess -3.80% -> -1.62%, Sharpe
  -0.037 -> +0.094, max drawdown -64.4% -> -31.1%, cash 0 -> 35 quarters, avg stocks 18.3 -> 27.1.
  Published numbers use the exchange-listed universe for consistency across markets.
- `--exclude-funds` on US: 11.44% CAGR / +3.43% excess vs 11.55% / +3.54% baseline. Closed-end
  funds and ETFs are not materially contaminating this screen; the $2B cap and ROE filters
  already exclude nearly all of them.
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
Revenue surprise effects decay within months, so a quarterly rebalance sits inside the drift
window. Measured on US data by holding the SAME quarterly signal for a year instead of a quarter
(`--frequency annual`):

| Rebalance | Gross excess | Net excess | Cost drag |
|-----------|-------------|-----------|-----------|
| Quarterly | +6.08% | +3.54% | 2.55 pp |
| Annual    | +3.93% | +3.33% | 0.60 pp |

The drift is real and worth 2.15 pp/yr before costs, but quarterly turnover is 4x higher and
transaction costs consume about nine tenths of that edge, leaving the two nearly tied net.

RETRACTED: an earlier version of this README claimed annual rebalancing produced 5.56% CAGR with
-5.05% excess. That test changed the signal as well as the holding period (full-year revenue vs
annual estimates rather than quarterly vs quarterly), and this backtest no longer implements that
variant, so the figure is withdrawn rather than restated.

Note: `--frequency annual` previously generated quarterly rebalance dates while annualizing as if
there were one period per year, which made this comparison untestable. Fixed August 2026; only the
quarterly default pins its own rebalance months.

**Data quality**:
- US: Analyst revenue estimates available quarterly from ~1992 onward (comprehensive)
- International: Quarterly analyst estimates sparse or forward-looking only (not historical)
- Asia-Pacific: Many markets report semi-annually or annually only, not quarterly

**Geographic conclusion**: This is a US-centric strategy due to data availability. Content should focus on US flagship blog + comparison showing limited international applicability.
