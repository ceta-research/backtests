# EV/EBITDA Sector-Relative Timing

Screen for stocks trading at a 30%+ discount to their sector's median EV/EBITDA, with positive earnings quality and reasonable leverage.

## Strategy

**Signal:** Stock's EV/EBITDA is less than 70% of its sector's median EV/EBITDA (same exchange), with ROE > 8% and D/E < 2.0.

**Logic:** EV/EBITDA compression relative to peers signals potential mean reversion. When a profitable company's enterprise multiple falls 30%+ below its sector median, the market may be mispricing it relative to competitors with similar business models.

**Why EV/EBITDA over P/E:**
- Not distorted by capital structure (captures debt load, unlike P/E)
- Comparable across geographies with different tax regimes
- Works for companies with high interest expenses that suppress EPS
- More stable within sectors than P/E (less variance = tighter signal)

**Filters:**
- EV/EBITDA: 0.5x to 25x (positive EBITDA, no distressed extremes)
- EV/EBITDA ratio to sector median < 0.70 (30%+ sector discount)
- ROE > 8% (quality: profitable business)
- D/E < 2.0 (not over-leveraged)
- Market cap threshold: exchange-specific (see cli_utils.py)
- Sector minimum: 5+ stocks required to compute a meaningful sector median

**Portfolio:** Top 30 stocks by deepest sector discount, equal weight. Cash if fewer than 10 qualify.

**Rebalancing:** Annual (January), 2000-2025.

**Benchmark:** each market against its own local index, not a US one. S&P 500 (SPY) for the US,
Sensex for India, Nikkei 225 for Japan, FTSE 100 for the UK, DAX for Germany, SMI for Switzerland,
TSX Composite for Canada, SSE Composite for China, Hang Seng for Hong Kong, KOSPI for Korea,
TAIEX for Taiwan, SET Index for Thailand, OMX Stockholm 30 for Sweden, Oslo All Share for Norway.
South Africa is the one exception: no local index in the data, so it is scored against the S&P 500.

**Execution:** next-day close after the January signal (market-on-close).

**Transaction costs:** size-tiered one-way by market cap, applied round-trip: 0.1% above $10B,
0.3% for $2-10B, 0.5% below $2B.

**Known gaps (see the content REVIEW.md for the full audit):**
- The universe does **not** exclude financials. EV/EBITDA does not describe a bank, whose
  liabilities are part of the operating business, and EBITDA skips net interest income. The
  live screens in `screen.py` and the published content drop the sector; the backtest never did.
- `tiered_cost()` is called without `fx_per_usd`, so non-US holdings are tiered on
  local-currency market caps against USD thresholds. Japan, India, Korea and Taiwan all land in
  the cheapest tier when most of their holdings belong in the most expensive one, worth roughly
  0.4-0.8pp of CAGR per year on those markets.

## Academic Reference

Loughran, T. & Wellman, J.W. (2011). New Evidence on the Relation between the Enterprise Multiple and Average Stock Returns. *Journal of Financial and Quantitative Analysis*, 46(6), 1629-1650.

The enterprise multiple (EV/EBITDA) predicts cross-sectional stock returns, particularly for value-oriented screens. Sector-relative versions of the signal reduce sensitivity to industry-level valuation regimes.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Live stock screen (current TTM data) |
| `generate_charts.py` | Chart generation from results JSON |
| `results/exchange_comparison.json` | Backtest results for all exchanges |

## Usage

```bash
# US backtest
python3 ev-ebitda-relative/backtest.py --verbose

# India backtest
python3 ev-ebitda-relative/backtest.py --preset india --verbose

# All exchanges
python3 ev-ebitda-relative/backtest.py --global --output results/exchange_comparison.json --verbose

# Live screen (current stocks)
python3 ev-ebitda-relative/screen.py
python3 ev-ebitda-relative/screen.py --preset india

# Generate charts (after running backtest)
python3 ev-ebitda-relative/generate_charts.py
```

## Exchanges Tested

| Exchange | Status | Notes |
|----------|--------|-------|
| NYSE+NASDAQ+AMEX | Included | US universe, primary backtest |
| NSE | Included | India (NSE only: BSE dual-lists the same companies) |
| JPX | Included | Japan (FY data available since ~2026-03 pipeline fix) |
| LSE | Included | UK (FY data available since ~2026-03 pipeline fix) |
| SHZ+SHH | Included | China A-shares |
| HKSE | Included | Hong Kong |
| TAI+TWO | Included | Taiwan |
| SET | Included | Thailand |
| XETRA | Included | Germany |
| KSC | Included | South Korea |
| TSX | Included | Canada |
| STO | Included | Sweden |
| SIX | Included | Switzerland |
| OSL | Included | Norway |
| JNB | Included | South Africa |
| ASX | Excluded | adjClose price data artifacts |
| SAO | Excluded | adjClose price data artifacts |

## Distinct From

**value-03-ev-ebitda** (absolute screen): Uses universal threshold (EV/EBITDA < 10x). Picks the cheapest stocks by absolute multiple regardless of what peers trade at. Fails in periods when entire sectors trade above 10x.

**timing-02-ev-ebitda-relative** (this strategy): Uses sector median as the baseline. A tech stock at 12x is "cheap" if its sector median is 20x. A utility at 8x is "expensive" if its sector median is 6x. Adapts to sector-level valuation regimes.

## Data Source

Ceta Research (FMP financial data warehouse). FY key_metrics (EV/EBITDA, ROE, market cap) and financial_ratios (D/E). Point-in-time with 45-day filing lag.
