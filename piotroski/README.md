# Piotroski F-Score Backtest

A multi-exchange backtest of Joseph Piotroski's 9-point quality checklist applied to value stocks. Annual rebalancing, three portfolio tracks, with decade-by-decade analysis.

## Strategy

**Universe:** Bottom 20% by price-to-book ratio, exchange-specific market cap thresholds.
**Signal:** Piotroski F-Score (9 binary criteria from financial statements).
**Portfolios:** Score 8-9 (long quality), Score 0-2 (avoid), All Value (baseline).
**Rebalancing:** Annual (April 1, after annual reports filed).
**Period:** 1985-2025.
**Benchmark:** S&P 500 (SPY).

### The 9 Piotroski Signals

**Profitability (4 points):**
1. Positive net income
2. Positive operating cash flow
3. Return on assets improved year-over-year
4. Cash flow exceeds net income (quality earnings)

**Leverage and Liquidity (3 points):**
5. Long-term debt decreased
6. Current ratio improved
7. No new shares issued (no dilution)

**Operating Efficiency (2 points):**
8. Gross margin improved
9. Asset turnover improved

## Key Finding

The F-Score doesn't consistently produce raw CAGR alpha on large-cap exchanges. The US spread (Score 8-9 minus Score 0-2) is -0.7%. But Score 8-9 has a better Sharpe ratio (0.282 vs 0.189) and lower max drawdown (-53.8% vs -67.8%). The F-Score is a risk filter, not a return booster.

| Exchange | Score 8-9 CAGR | Score 0-2 CAGR | Spread | Sharpe (8-9) |
|----------|---------------|---------------|--------|-------------|
| US (NYSE+NASDAQ+AMEX) | 10.3% | 11.0% | -0.7% | 0.282 |
| Japan (JPX) | 6.2% | 2.2% | +3.9% | 0.228 |
| India (BSE+NSE) | 1.7% | 8.1% | -6.4% | -0.110 |
| UK (LSE) | 7.4% | 9.2% | -1.8% | 0.129 |
| Australia (ASX) | 1.3% | -0.9% | +2.2% | -0.087 |
| Hong Kong (HKSE) | 18.8% | -5.6% | +24.4% | 0.192 |
| Korea (KSC) | 11.3% | 5.7% | +5.5% | 0.215 |

## Usage

```bash
export CR_API_KEY="your_key_here"

# Screen current US stocks with F-Score >= 8
python3 piotroski/screen.py

# Full backtest on US stocks
python3 piotroski/backtest.py --preset us --verbose

# Backtest Japanese stocks
python3 piotroski/backtest.py --preset japan --output results.json

# Run all eligible exchanges
python3 piotroski/run_all_exchanges.py

# Generate charts
python3 piotroski/generate_charts.py
```

## Files

- `backtest.py` - Historical backtest (fetches data via API, caches in DuckDB, runs locally)
- `screen.py` - Current stock screener (simple + value modes)
- `generate_charts.py` - Chart generation from result JSONs
- `run_all_exchanges.py` - Batch runner for all exchanges
- `results/` - Output JSONs and exchange_comparison.json

## Transaction costs

| Market Cap | Cost per trade |
|-----------|---------------|
| > $10B | 0.1% |
| $2-10B | 0.3% |
| < $2B | 0.5% |

Applied on both buy and sell (round-trip).

## References

- Piotroski, J. (2000). "Value Investing: The Use of Historical Financial Statement Information to Separate Winners from Losers." *Journal of Accounting Research*, 38 (suppl.), 1-41.
