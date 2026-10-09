# Cyclical Sector Timing

Buy quality cyclicals only when corporate revenues confirm economic expansion.

## Strategy Summary

**Universe:** Basic Materials + Industrials + Energy + Consumer Cyclical
**Signal (timing):** ≥50% of cyclical stocks with positive YoY revenue growth (FY data)
**Selection (when signal is on):** Top 30 by ROE, with positive revenue growth
**Rebalancing:** Annual (July), using FY data with 45-day lag
**Period:** 2001–2025 (24 annual periods, July 2001 to July 2025)

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
| `backtest.py` | Full historical backtest (2001–2025) |
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

## Key Results (US, 2001–2025)

| Metric | Cyclical Timing | S&P 500 |
|--------|----------------|---------|
| CAGR | 9.43% | 8.91% |
| Excess CAGR | +0.52% | — |
| Max Drawdown | -32.63% | -38.01% |
| Down Capture | 39.03% | 100% |
| Sharpe Ratio | 0.36 | 0.427 |
| Cash Periods | 3/24 (12.5%) | — |

**Key finding:** A small edge over the S&P 500 (+0.52 points a year) with 39% down capture, but higher volatility (20.67% vs 16.2%), so the Sharpe ratio trails the index. The strategy sat in cash in 2010, 2016 and 2021, each time because the expansion signal read below 50%. Only 2021 avoided a down market (S&P 500 -10.68%). The 2010 and 2016 cash years missed rallies of +33.55% and +18.58%.

**Split story:** 2001–2009: beat the S&P 500 in all 9 years. 2010–2024: beat it in 5 of 15. The strategy holds no Technology or Communication Services stocks.

## Signal History (US)

| Year | Signal | Expansion % | Action |
|------|--------|------------|--------|
| 2001 | ON | 82.0% | Invested (outperformed: +5.69% vs -22.45%) |
| 2009 | ON | 79.0% | Invested (outperformed) |
| 2010 | **OFF** | 24.0% | **Cash** (missed +33.55% rally) |
| 2016 | **OFF** | 48.2% | **Cash** (missed +18.58% rally) |
| 2021 | **OFF** | 39.8% | **Cash** (avoided -10.68% drop) |
| 2022–2024 | ON | 60.6–89.9% | Invested (trailed the S&P 500 all three years) |

Across all 16 exchanges in `results/exchange_comparison.json`, 7 of the 14 markets with a local index beat it. South Africa and Saudi Arabia have no local index in the data and are measured against the S&P 500 in USD.

## Data Source

Ceta Research (FMP financial data warehouse). Revenue data from `income_statement` (FY periods). Quality metrics from `key_metrics` (FY). Prices from `stock_eod`.
