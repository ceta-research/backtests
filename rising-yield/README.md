# Rising Dividend Yield

Stocks with 3 consecutive fiscal years of increasing dividend yield, where the increase is driven by dividend growth (not price decline). Quality-filtered for ROE, payout sustainability, and market cap.

## Signal

| Parameter | Value |
|-----------|-------|
| Signal | dividendYield rising for 3 consecutive FY periods |
| Driver filter | DPS must also increase (excludes price-decline-driven yield expansion) |
| Quality: ROE | > 10% |
| Quality: Payout | < 75% |
| Size: Market Cap | > exchange-specific threshold (e.g., $1B for US) |
| Portfolio | Top 30 by 2-year yield expansion, equal weight |
| Rebalancing | Annual (July) |
| Cash rule | Hold cash if fewer than 10 stocks qualify |

## Academic Basis

- Campbell, J.Y. & Shiller, R.J. (1988). "The Dividend-Price Ratio and Expectations of Future Dividends and Discount Factors." *Review of Financial Studies* 1(3): 195-228.
- Cochrane, J.H. (2008). "The Dog That Did Not Bark: A Defense of Return Predictability." *Review of Financial Studies* 21(4): 1533-1575.

## Distinction from Dividend Growth

Rising dividend yield is not the same as dividend growth. A company can grow its dividend 10% per year and still see its yield *fall* if the stock price rises 15%. Rising yield tracks the trajectory of the yield ratio itself. This strategy specifically targets stocks where the yield is expanding because dividends are growing faster than prices.

## Usage

```bash
# Run backtest on US stocks
python3 rising-yield/backtest.py --preset us --output results/returns_US_MAJOR.json --verbose

# Run on all eligible exchanges
python3 rising-yield/backtest.py --global --output results/exchange_comparison.json --verbose

# Current stock screen
python3 rising-yield/screen.py
python3 rising-yield/screen.py --preset india

# Generate charts
python3 rising-yield/generate_charts.py
```

## Results (16 Exchanges Tested)

| Exchange | CAGR | Excess vs SPY | Sharpe | Max Drawdown | Avg Stocks |
|----------|------|---------------|--------|--------------|------------|
| India (BSE+NSE) | 13.08% | +5.25% | 0.278 | -14.57% | 27.8 |
| US (NYSE+NASDAQ+AMEX) | 9.23% | +1.40% | 0.394 | -26.93% | 27.2 |
| Germany (XETRA) | 9.00% | +1.17% | 0.377 | -37.12% | 22.2 |
| Australia (ASX) | 8.93% | +1.10% | 0.334 | -29.73% | 20.3 |
| Canada (TSX) | 8.81% | +0.98% | 0.375 | -29.74% | 23.8 |
| Brazil (SAO) | 8.73% | +0.90% | -0.088 | -25.37% | 20.3 |
| Japan (JPX) | 8.55% | +0.71% | 0.418 | -46.39% | 28.2 |
| Sweden (STO) | 7.88% | +0.05% | 0.295 | -45.02% | 25.6 |
| South Africa (JNB) | 7.42% | -0.41% | -0.105 | -32.12% | 22.1 |
| Taiwan (TAI) | 6.89% | -0.95% | 0.414 | -15.11% | 28.0 |
| UK (LSE) | 6.61% | -1.22% | 0.145 | -48.93% | 17.8 |
| Switzerland (SIX) | 6.20% | -1.64% | 0.343 | -35.14% | 18.2 |
| Hong Kong (HKSE) | 5.93% | -1.90% | 0.144 | -32.02% | 23.8 |
| China (SHZ+SHH) | 5.38% | -2.45% | 0.076 | -53.35% | 25.8 |
| Korea (KSC) | 4.09% | -3.74% | 0.068 | -28.97% | 26.5 |
| Singapore (SES) | 1.71% | -6.13% | -0.065 | -34.57% | 10.8 |

Thailand (SET) excluded due to unreliable data quality (-55.98% max drawdown from 3 consecutive years of losses).

Transaction costs applied using size-tiered model (0.1% for >$10B, 0.3% for $2-10B, 0.5% for <$2B, one-way).

## Data

Data: [Ceta Research](https://cetaresearch.com) (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md).
