# Earnings Yield Value Strategy

Screen for profitable, financially stable companies with high earnings relative to price. Quarterly rebalancing. Equal weight.

## The Strategy

Earnings yield (E/P) is the inverse of the price-to-earnings ratio. A stock with P/E of 10 has a 10% earnings yield. High earnings yield means you're buying more earnings per dollar of price.

The screen adds quality filters to avoid value traps: companies that look cheap but are cheap because they're struggling.

**Signal filters:**
- Earnings yield > 0% (profitable, positive earnings)
- ROE > 12% (quality earnings, not just cheap)
- Debt/equity < 1.5 (manageable leverage)
- Interest coverage > 3x (can service existing debt)
- Market cap > exchange-specific threshold

**Portfolio construction:**
- Top 50 by earnings yield (highest E/P first)
- Equal weight
- Hold cash if fewer than 10 pass filters
- Quarterly rebalancing (Jan/Apr/Jul/Oct)

## Academic Reference

Fama, E.F. & French, K.R. (1992). "The Cross-Section of Expected Stock Returns." *Journal of Finance*, 47(2), 427-465.

Loughran, T. & Wellman, J. (2011). "New evidence on the relation between the enterprise multiple and average stock returns." *Journal of Financial and Quantitative Analysis*, 46(6), 1629-1650.

Greenblatt, J. (2005). *The Little Book That Beats the Market*. Wiley. (Earnings yield as one half of the Magic Formula.)

## Data

- Source: Ceta Research (FMP financial data warehouse)
- Period: 2000–2025
- Universe: Exchange-specific (see results)
- Rebalancing lag: 45 days after fiscal year end (point-in-time, no look-ahead bias)

## Results

See `results/exchange_comparison.json` for all exchange metrics.

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current qualifying stocks (TTM data) |
| `generate_charts.py` | Chart generation from results JSON |
| `results/exchange_comparison.json` | Computed results, all exchanges |

## Usage

```bash
# Backtest US stocks
python3 earnings-yield/backtest.py

# Backtest all exchanges
python3 earnings-yield/backtest.py --global --output earnings-yield/results/exchange_comparison.json --verbose

# Current screen (US stocks)
python3 earnings-yield/screen.py

# Advanced screen (adds Piotroski filter)
python3 earnings-yield/screen.py --advanced

# Generate charts (after running global backtest)
python3 earnings-yield/generate_charts.py
```

## Notes

- Piotroski score is snapshot-only (not available historically). TTM screen supports it; backtest does not.
- Excludes ASX and SAO (adjClose split artifacts affect returns).
- Market cap thresholds are in local currency (see `cli_utils.get_mktcap_threshold()`).
