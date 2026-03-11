# Market Share Gain

Companies beating their sector median on revenue growth tend to be taking share. This strategy screens for quality businesses with sector-relative revenue growth exceeding 10 percentage points, filtered for profitability and size.

## Academic Basis

The signal draws from three lines of evidence:

**Fundamental momentum:** Jegadeesh & Livnat (2006), "Revenue Surprises and Stock Returns", *Journal of Accounting and Economics* 41(1-2), 51-75. Revenue growth beyond expectations predicts returns more persistently than earnings surprises alone, because revenue manipulation is harder than earnings manipulation.

**Quality filter rationale:** Piotroski & So (2012), "Separating Winners from Losers Among Low Book-to-Market Stocks", *Journal of Financial Economics* 104(1), 1-28. High-growth firms with weak fundamentals underperform; the combination of strong growth and quality metrics (ROE, margins) is what generates alpha.

**Sector-relative framing:** This addresses the cross-sectional variation in base growth rates. Technology naturally grows faster than utilities. A 15% revenue growth rate is market-share gain in utilities but table stakes in SaaS. Sector medians make the signal comparable.

## Signal

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| Excess Rev Growth (vs sector median) | >= +10pp | Market share gain proxy |
| Return on Equity | > 8% | Profitable core business |
| Operating Profit Margin | > 5% | Monetizing the growth |
| Market Cap | > local threshold | Investable size |

**Sorting:** Highest excess growth first (descending). Top 30.

**Excess Growth** = YoY Revenue Growth - Sector Median YoY Revenue Growth
(Sector median computed over same-exchange universe, minimum 3 peers per sector)

## Parameters

- **Rebalancing:** Annual (July), filings available by then with 45-day lag
- **Min stocks:** 10 (holds cash if fewer qualify)
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Transaction costs:** Size-tiered (0.1% mega-cap to 0.5% mid-cap, round-trip)
- **Data lag:** 45 days point-in-time (current FY filing); 410 days for prior FY

## Results Summary (2000-2025)

| Exchange | CAGR | Excess CAGR | Sharpe | Max DD |
|----------|------|-------------|--------|--------|
| India (BSE+NSE) | 11.82% | +3.99% | 0.184 | -42.1% |
| Canada (TSX) | 6.72% | -1.11% | 0.222 | -26.6% |
| Switzerland (SIX) | 6.40% | -1.43% | 0.337 | -49.3% |
| UK (LSE) | 5.60% | -2.23% | 0.102 | -40.3% |
| Sweden (STO) | 4.46% | -3.37% | 0.112 | -46.4% |
| Germany (XETRA) | 3.53% | -4.30% | 0.080 | -46.2% |
| US (NYSE+NASDAQ+AMEX) | 3.34% | -4.50% | 0.072 | -41.4% |
| Japan (JPX) | 1.98% | -5.86% | 0.088 | -59.4% |

SPY benchmark: 7.83% CAGR, Sharpe 0.36. Only India generates statistically reliable alpha.

## Usage

```bash
# US stocks (default)
python3 market-share/backtest.py --verbose

# India
python3 market-share/backtest.py --preset india

# All exchanges
python3 market-share/backtest.py --global --output market-share/results/exchange_comparison.json --verbose

# Current screen
python3 market-share/screen.py
python3 market-share/screen.py --preset india

# Generate charts
python3 market-share/generate_charts.py
```

## Data Requirements

- `income_statement` (FY): revenue, dateEpoch (two consecutive years)
- `key_metrics` (FY): returnOnEquity, marketCap
- `financial_ratios` (FY): operatingProfitMargin
- `stock_eod`: adjClose at rebalance dates
- `profile`: exchange membership, sector

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen (FY revenue + TTM quality) |
| `generate_charts.py` | Generate PNG charts for blogs |
| `README.md` | This file |
| `results/` | Backtest output (exchange_comparison.json) |
| `charts/` | Generated chart PNGs |
