# Graham Number Timing

Benjamin Graham's intrinsic value formula as a timing signal. Backtested across global exchanges from 2000-2025.

## Signal

**Graham Number = sqrt(22.5 × EPS × BVPS)**

Where:
- **EPS** = Earnings Per Share (Net Income / Shares Outstanding)
- **BVPS** = Book Value Per Share (Total Equity / Shares Outstanding)
- **22.5** = Graham's constant (P/E of 15 × P/B of 1.5)

**Buy signal:** Price < Graham Number (Price/Graham ratio < 1.0)

**Quality filters:**
| # | Filter | Threshold |
|---|--------|-----------|
| 1 | Price-to-Graham Ratio | < 1.0 |
| 2 | Return on Equity | > 10% |
| 3 | Net Income | > 0 |
| 4 | Total Equity | > 0 |
| 5 | Market Cap | Exchange-specific* |

*Exchange-specific thresholds (local currency): $1B USD (US), ₹20B (~$240M) India, €500M (~$545M) Germany, ¥2B (~$276M) China, etc. See cli_utils.py::MKTCAP_THRESHOLD_MAP.

**Portfolio:** Top 30 stocks by deepest discount to Graham Number. Equal weight. Cash if fewer than 10 qualify.

**Rebalancing:** Quarterly (Jan/Apr/Jul/Oct), 2000-2025.

**Benchmark:** S&P 500 (SPY).

## Rationale

Benjamin Graham (Warren Buffett's mentor) created this formula as a conservative estimate of a stock's intrinsic value. The 22.5 constant represents his criteria for a "defensive investor":
- P/E ratio no higher than 15
- P/B ratio no higher than 1.5
- Product of P/E × P/B should not exceed 22.5

When a stock's price falls below its Graham Number, it signals potential value. This backtest uses Graham's formula as a **timing tool** - buying stocks when they cross below fair value.

## Usage

```bash
# Configure data source
export CR_API_KEY="your_key_here"

# Screen current US stocks
python3 graham-timing/screen.py

# Screen Indian stocks
python3 graham-timing/screen.py --exchange BSE,NSE

# Full 25-year backtest on US stocks
python3 graham-timing/backtest.py

# Backtest German stocks with verbose output
python3 graham-timing/backtest.py --exchange XETRA --verbose

# Backtest all exchanges, save results
python3 graham-timing/backtest.py --global --output results/exchange_comparison.json
```

## Files

- `backtest.py` - Full historical backtest
- `screen.py` - Instant screen on current (TTM) data
- `generate_charts.py` - Regenerate charts from results JSON
- `results/` - Pre-computed backtest results
- `charts/` - PNG charts for blog posts

## How it works

1. **Fetch**: Historical financials (net income, equity, market cap) + prices
2. **Cache**: Load into in-memory DuckDB
3. **Screen**: At each quarterly rebalance:
   - Compute Graham Number for each stock
   - Select stocks where Price < Graham Number
   - Filter for quality (ROE > 10%, positive earnings/equity)
   - Take top 30 by lowest Price/Graham ratio
4. **Returns**: Equal-weight portfolio return vs SPY

## Exchange presets

| Preset | Exchanges |
|--------|-----------|
| `--preset us` | NYSE, NASDAQ, AMEX |
| `--preset india` | BSE, NSE |
| `--preset germany` | XETRA |
| `--preset china` | SHZ, SHH |
| `--preset hongkong` | HKSE |
| `--preset korea` | KSC |
| `--preset canada` | TSX |
| `--preset thailand` | SET |
| `--preset taiwan` | TAI |
| `--preset japan` | JPX |
| `--preset uk` | LSE |
| `--preset switzerland` | SIX |

Or use `--exchange CODE` for any exchange.

## Excluded Exchanges

| Exchange | Reason |
|----------|--------|
| ASX | Broken adjClose data (stock split artifacts) |
| SAO | Broken adjClose data (missed split adjustments) |

See [DATA_QUALITY_ISSUES.md](../DATA_QUALITY_ISSUES.md) for details.

## Academic Reference

Graham, Benjamin (1949). *The Intelligent Investor*. Harper & Brothers.

Graham Number formula appears in Chapter 14 ("Stock Selection for the Defensive Investor"):
> "We suggest as a requirement here that the product of the multiplier times the ratio of price to book value should not exceed 22.5. (This figure corresponds to 15 times earnings and 1½ times book value. It would admit an issue selling at only 9 times earnings and 2.5 times asset value, etc.)"

The square root formulation (Graham Number = sqrt(22.5 × EPS × BVPS)) is algebraically equivalent and easier to compute.

## Comparison to Other Strategies

**Graham Number vs Low P/E:**
- Low P/E: Buys stocks with lowest earnings multiples
- Graham Number: Combines earnings AND book value
- Graham is more conservative (requires both cheap P/E and P/B)

**Graham Number vs QARP:**
- QARP: 7-factor quality screen (Piotroski, ROE, D/E, CR, IQ, P/E)
- Graham: Simpler 2-factor value screen (EPS, BVPS)
- QARP is quality-first, Graham is value-first

**Graham Number vs DCF:**
- DCF: Future cash flows discounted to present value
- Graham: Current earnings + book value (no growth assumptions)
- Graham is more conservative (no optimistic growth projections)
