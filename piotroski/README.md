# Piotroski F-Score Backtest

A 40-year backtest of Joseph Piotroski's 9-point quality checklist applied to value stocks. Annual rebalancing, three portfolio tracks, with decade-by-decade analysis.

## Strategy

**Universe:** Bottom 20% by price-to-book ratio, market cap > $100M.
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

## Usage

```bash
# Configure your data source (see root README)
export CR_API_KEY="your_key_here"

# Screen current US stocks with F-Score >= 8
python3 piotroski/screen.py

# Value screen: F-Score >= 7, P/B < 1.5, P/E < 20
python3 piotroski/screen.py --value

# Screen with lower threshold
python3 piotroski/screen.py --min-score 7

# Full 40-year backtest on US stocks
python3 piotroski/backtest.py

# Backtest with verbose year-by-year output
python3 piotroski/backtest.py --verbose

# Backtest Indian stocks, save results
python3 piotroski/backtest.py --exchange BSE,NSE --output results_india.json
```

## Files

- `backtest.py` - Full 40-year historical backtest. Fetches data via API, caches in DuckDB, runs locally.
- `screen.py` - Instant screen on current data. Two modes: simple (high F-Score) and value (F-Score + P/B + P/E filters).

## How the backtest works

1. **Fetch** (30-60s): 7 SQL queries pull historical financials, ratios, and prices
2. **Cache**: All data loaded into in-memory DuckDB
3. **Screen**: At each April 1 rebalance date:
   - Build value universe (bottom 20% P/B, > $100M market cap)
   - Compute Piotroski F-Score from raw financial statements
   - Split into Score 8-9, Score 0-2, and All Value portfolios
4. **Returns**: Equal-weight returns with size-tiered transaction costs

### Transaction costs

| Market Cap | Cost per trade |
|-----------|---------------|
| > $10B | 0.1% |
| $2-10B | 0.3% |
| $100M-2B | 0.5% |

Applied on both buy and sell (round-trip).

## Exchange presets

| Preset | Exchanges |
|--------|-----------|
| `--preset us` | NYSE, NASDAQ, AMEX |
| `--preset india` | BSE, NSE |
| `--preset germany` | XETRA |
| `--preset china` | SHZ, SHH |
| `--preset hongkong` | HKSE |

## References

- Piotroski, J. (2000). "Value Investing: The Use of Historical Financial Statement Information to Separate Winners from Losers." *Journal of Accounting Research*, 38 (suppl.), 1-41.
