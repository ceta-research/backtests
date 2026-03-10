# Cyclical Sector Timing

Buy quality cyclicals only when corporate revenues confirm economic expansion.

## Strategy Summary

**Universe:** Basic Materials + Industrials + Energy + Consumer Cyclical
**Signal (timing):** ≥50% of cyclical stocks with positive YoY revenue growth (FY data)
**Selection (when signal is on):** Top 30 by ROE, with positive revenue growth
**Rebalancing:** Annual (July), using FY data with 45-day lag
**Period:** 2001–2024

## Academic Basis

Sector rotation research (Fama & French, 1997; Moskowitz & Grinblatt, 1999) shows that sector-level signals contain information about future cross-sectional returns. The revenue-based timing signal is derived from: Nissim & Penman (2001), who document that revenue growth forecasts future profitability for industrial companies.

## Signal Logic

The expansion/contraction switch works as follows:

1. At each July rebalance, compute YoY revenue growth for every qualifying cyclical stock (Basic Materials, Industrials, Energy, Consumer Cyclical) with market cap above the exchange threshold
2. If ≥50% show positive YoY growth → **expansion confirmed → invest**
3. If <50% → **contraction signal → hold cash**

When invested, select top 30 by ROE (return on equity) among stocks with positive revenue growth. Quality-within-cyclicals approach avoids commodity peak-cycle concentration that pure revenue-momentum selection creates.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2001–2024) |
| `screen.py` | Current qualifying stocks |
| `generate_charts.py` | Charts from results JSON |
| `results/exchange_comparison.json` | Multi-exchange results |

## Usage

```bash
# Default (US)
python3 cyclical-timing/backtest.py

# India
python3 cyclical-timing/backtest.py --preset india

# All exchanges
python3 cyclical-timing/backtest.py --global --output results/exchange_comparison.json

# Current screen
python3 cyclical-timing/screen.py --preset us

# Generate charts
python3 cyclical-timing/generate_charts.py
```

## Key Results (US, 2001–2024)

| Metric | Cyclical Timing | S&P 500 |
|--------|----------------|---------|
| CAGR | 8.04% | 8.89% |
| Excess CAGR | -0.86% | — |
| Max Drawdown | -33.0% | -36.3% |
| Down Capture | 34.3% | 100% |
| Sharpe Ratio | 0.304 | 0.437 |
| Cash Periods | 3/24 (12%) | — |

**Key finding:** Near-market returns with only 34% down capture. The strategy sat in cash during 2010 (post-recession contraction), 2016 (energy downturn), and 2021 (COVID revenue impact), correctly avoiding two of three subsequent down markets.

**Split story:** 2001–2009 (+8 outperformance years, commodity supercycle tailwind). 2010–2024 (more mixed, fewer outperformance years as China/commodity cycle unwound).

## Signal History (US)

| Year | Signal | Expansion % | Action |
|------|--------|------------|--------|
| 2001 | ON | 83.9% | Invested (outperformed) |
| 2009 | ON | 78.5% | Invested (outperformed) |
| 2010 | **OFF** | 24.4% | **Cash** (missed +33% rally) |
| 2016 | **OFF** | 48.1% | **Cash** (missed +18% rally) |
| 2021 | **OFF** | 40.3% | **Cash** (avoided -10% drop) |
| 2022–2024 | ON | 85–90% | Invested (mixed) |

## Data Source

Ceta Research (FMP financial data warehouse). Revenue data from `income_statement` (FY periods). Quality metrics from `key_metrics` (FY). Prices from `stock_eod`.
