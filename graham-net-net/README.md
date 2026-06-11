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
| Benchmark | Local index per exchange (S&P 500 for US, Nikkei for JPX, etc.) |
| Period | 2001–2024 |

## Data Quality

Net-nets are inherently distressed, micro-cap stocks. Price data quality is lower than for large-caps. Returns are filtered to remove data artifacts:

- Minimum entry price: $0.50 (removes near-zero stocks with percentage distortions)
- Maximum single-year return: 300% (removes price data errors while preserving legitimate turnarounds)

Without this filter, single stocks like AWH (51,142%) and LMFA (23,187%) dominate results in crash-recovery years.

## Results Summary

Each exchange is benchmarked against its local index (run of 2026-06-11, with oscillation filter):

| Exchange | CAGR | Local Bench | Excess | Sharpe | MaxDD | Avg Stocks |
|----------|------|-------------|--------|--------|-------|------------|
| Canada (TSX) | 8.73% | TSX Comp 5.12% | +3.61% | 0.176 | -52.9% | 21.4 |
| Taiwan (TAI+TWO) | 9.02% | TAIEX 5.72% | +3.30% | 0.292 | -39.0% | 11.4 |
| Japan (JPX) | 7.39% | Nikkei 4.32% | +3.06% | 0.318 | -45.5% | 26.8 |
| Korea (KSC) | 5.70% | KOSPI 6.81% | -1.11% | 0.138 | -32.0% | 25.7 |
| US (NYSE+NASDAQ+AMEX) | 5.55% | S&P 500 8.86% | -3.32% | 0.091 | -60.4% | 28.7 |
| Hong Kong (HKSE) | -1.42% | Hang Seng 2.53% | -3.95% | -0.162 | -63.9% | 24.8 |
| UK (LSE) | -2.47% | FTSE 100 1.81% | -4.28% | -0.287 | -69.1% | 16.2 |
| India (NSE) | 8.56% | Sensex 13.63% | -5.07% | 0.049 | -58.1% | 24.9 |

Three markets beat their local benchmarks: Canada (largest premium, but beta 1.49), Taiwan (thin universe, 11.4 avg stocks), and Japan (best risk-adjusted, 45% down capture). Hong Kong's -64% drawdown reflects real market deterioration (protests 2019, regulatory crackdowns 2020-2023), not a data artifact. The UK is the worst absolute performer; with cleaned price data, three formerly-cash years (2008, 2010, 2012) now invest and lose.

## Exchange Notes

**Excluded exchanges:**
- ASX (Australia): adjClose split adjustment issues
- SAO (Brazil): adjClose split adjustment issues
- SHH/SHZ (China): avg ~4 qualifying stocks/year — too thin for a portfolio
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
