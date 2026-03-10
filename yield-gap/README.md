# Yield Gap Screen

**Strategy slug:** `yield-gap`
**Category:** Mean Reversion / Value
**Rebalancing:** Annual (January)
**Universe:** NYSE + NASDAQ + AMEX (default)

## Strategy

The yield gap is the spread between a stock's earnings yield and the prevailing risk-free rate. When this gap is wide, equities are priced cheaply relative to government bonds. When it narrows, the margin of safety shrinks.

This strategy screens for stocks where the earnings yield offers at least 3 percentage points above the regional government bond yield, with quality filters to exclude leveraged or low-return companies.

## Signal

```
earnings_yield > max(6%, risk_free_rate + 3%)
earnings_yield < 50%  (cap: above this is distress or data error)
ROE > 8%
D/E < 2.0
market_cap > exchange-specific threshold
```

**Ranking:** Top 30 by highest earnings yield (widest gap), equal weight.

The effective threshold adapts per market:
- US (rfr=2.0%): EY > 6.0% (PE < ~16.7x)
- Germany (rfr=2.0%): EY > 6.0% (PE < ~16.7x)
- Japan (rfr=0.1%): EY > 6.0% (PE < ~16.7x)
- UK (rfr=3.5%): EY > 6.5% (PE < ~15.4x)
- India (rfr=6.5%): EY > 9.5% (PE < ~10.5x)
- Korea (rfr=3.0%): EY > 6.0% (PE < ~16.7x)

## Academic Foundation

The "yield gap" concept has roots in the Fed Model (Yardeni, 2000) and the equity risk premium (ERP) literature. At the stock level, high earnings yields relative to bonds have historically predicted outperformance (Campbell & Vuolteenaho, 2004; Damodaran, 2012).

**Key insight:** Quality companies (positive ROE, manageable debt) trading at high earnings yields are typically cheap for temporary reasons — a down cycle, sector rotation, or short-term earnings pressure — not structural deterioration.

## Usage

```bash
# Screen current qualifying stocks (US)
python3 yield-gap/screen.py

# Screen specific exchange
python3 yield-gap/screen.py --preset germany

# Backtest US
python3 yield-gap/backtest.py --preset us --output results/returns_US.json --verbose

# Backtest all exchanges
python3 yield-gap/backtest.py --global --output results/exchange_comparison.json

# Run on cloud
python3 yield-gap/backtest.py --cloud --preset us
```

## Data Source

Financial data: FMP (via Ceta Research warehouse)
- `earningsYield`: from `key_metrics` (FY, point-in-time with 45-day lag)
- `returnOnEquity`: from `key_metrics` (FY)
- `debtToEquityRatio`: from `financial_ratios` (FY)
- Prices: from `stock_eod` (adjusted close)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest |
| `screen.py` | Current qualifying stocks |
| `generate_charts.py` | Charts from results JSON |
| `results/exchange_comparison.json` | Multi-exchange results |
