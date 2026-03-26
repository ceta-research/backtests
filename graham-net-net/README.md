# Graham Net-Net

Benjamin Graham's classic deep value strategy: buy stocks trading below their net current asset value (NCAV) — the liquidation value of current assets after paying off all liabilities.

## Strategy

**Signal:** Price < NCAV per share
**NCAV = Current Assets − Total Liabilities − Preferred Stock (per share)**
**Data source:** `key_metrics.grahamNetNet` (FMP pre-computed, per share)

A stock is a net-net when you can buy it for less than you'd theoretically recover if the company shut down tomorrow and sold only its liquid assets.

## Backtest Setup

| Parameter | Value |
|-----------|-------|
| Rebalance | Annual (April) |
| Hold period | 1 year |
| Position sizing | Equal weight |
| Max portfolio | 30 stocks |
| Cash rule | Hold cash if fewer than 5 qualifying |
| Financial data lag | 45 days (prevents look-ahead bias) |
| Benchmark | S&P 500 Total Return (SPY) |
| Period | 2001–2024 |

## Data Quality

Net-nets are inherently distressed, micro-cap stocks. Price data quality is lower than for large-caps. Returns are filtered to remove data artifacts:

- Minimum entry price: $0.50 (removes near-zero stocks with percentage distortions)
- Maximum single-year return: 300% (removes price data errors while preserving legitimate turnarounds)

Without this filter, single stocks like AWH (51,142%) and LMFA (23,187%) dominate results in crash-recovery years.

## Results Summary

| Exchange | CAGR | SPY CAGR | Excess | Sharpe | MaxDD | Avg Stocks |
|----------|------|----------|--------|--------|-------|------------|
| US (NYSE+NASDAQ+AMEX) | 5.02% | 8.84% | -3.81% | 0.078 | -54.4% | 28.5 |
| Japan (JPX) | 8.85% | 8.84% | +0.02% | 0.416 | -33.7% | 27.9 |
| India (NSE) | 8.28% | 8.84% | -0.55% | 0.042 | -57.7% | 26.0 |
| Korea (KSC) | 6.60% | 8.84% | -2.24% | 0.162 | -39.3% | — |
| Canada (TSX) | 7.49% | 8.84% | -1.34% | 0.133 | -55.9% | — |
| Hong Kong (HKSE) | -3.17% | 8.84% | -12.01% | -0.199 | -82.1% | 24.1 |
| UK (LSE) | -0.62% | 8.84% | -9.46% | -0.149 | -43.5% | — |

Japan is the only market with competitive risk-adjusted returns (Sharpe 0.416 vs S&P 500's 0.411). The net-net premium has largely eroded in developed markets. Hong Kong's -82% drawdown reflects real market deterioration (protests 2019, regulatory crackdowns 2020-2023), not a data artifact.

## Exchange Notes

**Excluded exchanges:**
- ASX (Australia): adjClose split adjustment issues
- SAO (Brazil): adjClose split adjustment issues
- SHH/SHZ (China): avg ~4 qualifying stocks/year — too thin for a portfolio
- Taiwan (TAI/TWO): borderline ~20/year, insufficient alpha to justify content
- BSE: BSE+NSE combined creates duplicate positions (same company on both exchanges)

**India:** Use NSE-only (`returns_NSE.json`). NSE has 8 cash periods (2001-2008) because the NCAV data didn't cover Indian stocks sufficiently before 2009.

## Usage

```bash
# Current screen — which stocks qualify today?
python3 graham-net-net/screen.py
python3 graham-net-net/screen.py --preset india

# Backtest a specific exchange
python3 graham-net-net/backtest.py --exchange JPX --verbose
python3 graham-net-net/backtest.py --exchange NYSE,NASDAQ,AMEX --output results/returns_US_MAJOR.json

# Run all exchanges (sequential, ~30-60 min total)
python3 graham-net-net/run_all_exchanges.py

# Generate charts (requires matplotlib)
python3 graham-net-net/generate_charts.py
```

## Market Cap Thresholds

Net-nets are by definition small/micro-cap. Standard $1B USD thresholds eliminate virtually every qualifying stock. Per-exchange thresholds used:

| Exchange | Threshold | Approx USD Equiv |
|----------|-----------|-----------------|
| NYSE/NASDAQ/AMEX | $50M | $50M |
| JPX (Japan) | ¥5B | ~$33M |
| HKSE | HK$200M | ~$25M |
| BSE/NSE | ₹500M | ~$6M |
| KSC (Korea) | ₩50B | ~$36M |
| LSE (UK) | £15M | ~$19M |
| TSX (Canada) | C$20M | ~$15M |

## Academic Background

- **Oppenheimer (1986):** 29% annual returns for US net-nets, 1970-1983. Benchmark: 11.5%.
- **Bildersee, Cheh, Zutshi (1993):** Japan net-nets returned 20.55%/yr vs 16.63% market, 1975-1988.
- **Xiao & Arnold (2008):** UK net-nets returned up to 19.7%/yr, 1980-2005.

Modern data shows significantly lower premiums. The anomaly has been partially arbitraged away, particularly in the US.

## Data Source

All data via [Ceta Research](https://cetaresearch.com) (FMP financial data warehouse). Historical financial statements, EOD adjusted prices, point-in-time data with 45-day filing lag.
