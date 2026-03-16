# ETF Underowned Quality Backtest

Buy quality stocks that ETFs overlook. Screen for high-ROE, low-debt, profitable companies held by fewer than 10 ETFs, then track their performance against the S&P 500.

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| Return on Equity | > 12% | key_metrics (FY) |
| Debt/Equity | 0 to 1.0 | financial_ratios (FY) |
| Current Ratio | > 1.5 | financial_ratios (FY) |
| Net Profit Margin | > 5% | financial_ratios (FY) |
| P/E Ratio | 0 to 40 | financial_ratios (FY) |
| ETF Count | < 10 | etf_holder (current snapshot) |
| Market Cap | > exchange threshold | key_metrics (FY) |

**Ranking:** ROE DESC (highest quality first)
**Portfolio:** Top 30, equal weight. Cash if fewer than 10 qualify.
**Rebalancing:** Annual (July)
**Period:** 2005-2025

## Academic Basis

- **Piotroski (2000):** F-Score spread was 13.4% among small-caps vs 5-6% among large-caps. Quality signals are 2x more effective among less-followed stocks.
- **Stambaugh, Yu & Yuan (2015):** Factor premiums are stronger in harder-to-arbitrage stocks.
- **Merton (1987):** Incomplete information model predicts higher expected returns for stocks with limited investor awareness.

## Key Results

The underowned quality thesis **largely fails across global markets**. Only Germany shows competitive returns.

| Exchange | CAGR | Excess | Sharpe | Max DD | Avg Stocks |
|----------|------|--------|--------|--------|------------|
| XETRA | 9.23% | -1.38% | 0.460 | -29.26% | 22.6 |
| SIX | 3.99% | -6.62% | 0.177 | -30.18% | 8.0 |
| NYSE+NASDAQ | 0.90% | -9.71% | -0.057 | -30.34% | 19.4 |
| LSE | 0.15% | -10.46% | -0.158 | -52.31% | 11.4 |
| BSE+NSE | -0.15% | -10.76% | -0.273 | -51.15% | 19.3 |

**Why it fails:** Being underowned by ETFs in efficient markets (US, UK) correlates with factors the quality screen doesn't capture: pending litigation, governance issues, sector misfit, or declining fundamentals that TTM metrics haven't reflected. The academic research on neglect premiums studied analyst coverage and institutional ownership broadly, not ETF ownership specifically.

**Why Germany works:** XETRA has a deep bench of profitable mid-cap industrials and specialty manufacturers that sit below major index thresholds. These companies have genuine quality but low ETF exposure because few thematic or factor ETFs target the German mid-cap space.

## Caveat

ETF holdings data (`etf_holder`) is a **current snapshot**, not historical. Ownership classifications are applied retrospectively. Quality filters use point-in-time FY data (no look-ahead bias). The ownership signal has look-ahead bias.

## Usage

```bash
# Single exchange
python3 etf-underowned/backtest.py --preset us --output results/returns_NYSE_NASDAQ.json --verbose

# All exchanges
python3 etf-underowned/backtest.py --global --output results/exchange_comparison.json --verbose

# No transaction costs
python3 etf-underowned/backtest.py --preset germany --no-costs --verbose

# Generate charts
python3 etf-underowned/generate_charts.py
```

## Files

```
etf-underowned/
├── backtest.py              # Main backtest
├── generate_charts.py       # Chart generation
├── README.md                # This file
├── results/                 # JSON results per exchange
│   ├── exchange_comparison.json
│   ├── returns_NYSE_NASDAQ.json
│   ├── returns_XETRA.json
│   └── ...
└── charts/                  # Generated PNGs (gitignored)
```

## Data

*Data: Ceta Research (FMP financial data warehouse). ETF holdings from etf_holder table. Quality metrics from key_metrics and financial_ratios (FY). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)*
