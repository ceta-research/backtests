# Graham Net-Net

Benjamin Graham's deep value screen: buy stocks trading below their net current asset value (NCAV), the current assets left after paying off every liability. A net-net is a company you can buy for less than you'd recover if it shut down and sold only its liquid assets.

## Strategy

**Signal:** price < NCAV per share
**NCAV = (current assets - total liabilities - preferred stock) / shares outstanding**
**Data source:** `key_metrics.grahamNetNet` (FMP, pre-computed per share, fiscal-year rows)

## How the screen works

On each April rebalance date, for every company on the market's exchanges (delisted ones included, up to the date they stopped trading):

1. Take the company's latest fiscal-year row whose fiscal year ended at least 45 days before the rebalance date and no more than 540 days before it. A company with no such row drops out for that year.
2. Keep it only if that row's NCAV is positive and its market cap (as of that fiscal year end) is above the market's floor (table below).
3. Restate NCAV in the currency the stock is quoted in: x100 for pence-quoted lines, and the newest USD exchange rate at most 7 days old when the reporting and quote currencies differ. A line with no currency, no fresh rate, or a depositary receipt quoted in a currency other than its reporting currency is excluded (FMP's per-share basis for receipts is inconsistent). In the US this excludes 873 receipt lines, summed over the 24 rebalances.
4. Require a price above 0.50 (local units) and below NCAV.
5. Keep one line per ISIN, so a security listed under two tickers counts once.
6. Rank by price/NCAV, cheapest first, and buy up to 30 names at equal weight. If fewer than 5 pass, hold cash for the year.

On the London Stock Exchange only pence-quoted (GBp) lines are screened. That drops the `0XXX.L` order-book lines FMP carries in their home currency and the USD and EUR depositary receipts.

A holding with no price at the exit date exits at its last traded price on or before that date (or on or before its delisting date plus 7 days, if that's earlier) instead of being dropped from the year. 25 exits were priced this way across the eight markets: 14 in the US, 5 in the UK, 3 in Korea, 2 in Hong Kong and 1 in Canada.

## Backtest Setup

| Parameter | Value |
|-----------|-------|
| Rebalance | Annual, April |
| Execution | Next-day close |
| Hold period | 1 year |
| Position sizing | Equal weight |
| Max portfolio | 30 stocks |
| Cash rule | Cash if fewer than 5 pass the screen or have an entry price |
| Financial data | Fiscal-year rows, used from 45 days after the fiscal year end, dropped after 540 days |
| Costs | Size-tiered transaction costs |
| Benchmark | Local index per market |
| Period | 24 annual periods, April 2001 to April 2025 |

## Results

Each market is measured in its own currency against its local index.

| Market | CAGR | Local index | Index CAGR | Excess | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----:|-------------|-----------:|-------:|-------:|-------:|-----------:|-----------:|
| UK (LSE, pence-quoted) | 5.87% | FTSE 100 | 1.81% | +4.06 | 0.081 | -41.02% | 4 | 11.4 |
| Taiwan (TAI+TWO) | 8.76% | TAIEX | 5.72% | +3.04 | 0.285 | -39.54% | 8 | 9.9 |
| Japan (JPX) | 7.34% | Nikkei 225 | 4.32% | +3.01 | 0.338 | -47.25% | 5 | 26.8 |
| Hong Kong (HKSE) | 3.19% | Hang Seng | 2.53% | +0.66 | 0.006 | -53.83% | 5 | 26.0 |
| Canada (TSX) | 5.72% | TSX Composite | 5.12% | +0.61 | 0.120 | -51.88% | 6 | 10.7 |
| US (NYSE+NASDAQ+AMEX) | 7.11% | S&P 500 (SPY) | 8.86% | -1.76 | 0.144 | -50.62% | 0 | 22.8 |
| India (NSE) | 11.11% | Sensex | 13.63% | -2.52 | 0.099 | -42.53% | 8 | 11.3 |
| Korea (KSC) | 4.25% | KOSPI | 6.81% | -2.56 | 0.059 | -33.80% | 7 | 20.9 |

Excess is CAGR minus index CAGR, in percentage points. Sharpe uses each market's own risk-free rate (US 2.0%, Japan 0.1%, Hong Kong 3.0%, India 6.5%, Korea 3.0%, UK 3.5%, Canada 2.5%, Taiwan 1.0%), so compare it with the index's Sharpe in the same market rather than across markets. Avg stocks is the average number held in invested years.

Five of eight markets beat their local index: the UK, Taiwan and Japan by about 3-4 points, Hong Kong and Canada by under one. All five indices are price indices that leave dividends out, while the portfolio's returns include them, so each margin is overstated by roughly the index's dividend yield. Hong Kong's and Canada's leads may not survive a total-return comparison, and Canada's Sharpe (0.120) is already below the TSX Composite's (0.141). The US trails the S&P 500, which does include dividends, and India and Korea trail too. India has the highest CAGR of the eight, 11.11%, but still trails the Sensex (13.63%) and holds 11.3 names on average. Details on dividends in benchmarks: [METHODOLOGY.md](../METHODOLOGY.md#benchmark-limitations).

### Cash years and thin books

A cash year is one where fewer than 5 names passed the screen. Years are the April each holding period starts.

| Market | Cash years |
|--------|-----------|
| US | none |
| Japan | 2001-2005 |
| Hong Kong | 2001-2005 |
| India | 2001-2008 (0 or 1 name passed each year) |
| Korea | 2001-2007 |
| UK | 2001, 2002, 2005, 2017 |
| Canada | 2001-2006 (2-4 names passed each year) |
| Taiwan | 2001-2007, 2021 |

Several invested years hold only 4-7 names, so one stock moves the year:

- India: 2015 held 5. 2017 averages 4: five passed the screen and had entry prices, and the return filter removed one. 2024 held 7 and lost 42.5%.
- Canada: 2013 held 6, 2014 held 5.
- UK: 2003, 2004 and 2006 held 5.
- Japan 2006 and Hong Kong 2007 held 5. Taiwan held 6-7 in 2009, 2014, 2018 and 2024.

## Changes from earlier versions

- **Latest filing first.** The screen used to look only at fiscal-year rows that already had positive NCAV and passed the size floor, then pick the newest. A company whose NCAV had since turned negative kept qualifying on its last positive filing, however old: PIF.TO stayed in the Canadian book through the 2024 rebalance on its FY2009 NCAV. The screen now takes the latest row first and tests it, and ignores rows more than 540 days old. About half the names in Canada's 2001-2006 books had qualified on such stale filings. Without them fewer than 5 pass, so those years are now in cash, and Canada's lead over the TSX Composite narrows to 0.61 points. Hong Kong moves from trailing the Hang Seng to beating it.
- **NCAV in the quote currency.** NCAV used to be compared with the price in whatever currency each was reported in, so pence prices were screened against NCAV in pounds and Canadian-dollar prices against NCAV in US dollars.
- **UK listings** are now pence-quoted lines only, and **same-ISIN twins** count once. Together with the currency fix, the UK moves from trailing the FTSE 100 to beating it.
- **Delisted holdings** exit at their last traded price instead of being dropped from the year.

## Data Quality

Net-nets are distressed micro-caps, and their price data is noisier than for large caps. Two filters apply to every year's returns:

- Minimum entry price: 0.50 in local units (removes near-zero prices where percentage moves are distorted)
- Maximum single-year return: 300% (removes price errors while keeping real turnarounds)

Known limits:

- Fiscal-year rows are used 45 days after the fiscal year end. Many companies publish annual results later than that, so a row can be used weeks to a couple of months before it was public.
- The size floor compares the filing's market cap, in the company's reporting currency, with a threshold set in the listing currency.
- `screen.py` (the current-day screen) compares price with trailing NCAV without the currency conversion above.

## Exchange Notes

**Excluded exchanges:**
- ASX (Australia): adjClose split adjustment issues
- SAO (Brazil): adjClose split adjustment issues
- SHH/SHZ (China): about 4 qualifying stocks a year, too thin for a portfolio
- BSE: India runs on NSE only; combining BSE and NSE put the same company in the book twice

## Market Cap Floors

Net-nets are small by definition, so the floors are far below the usual large-cap cutoffs.

| Market | Floor (local currency) |
|--------|-------|
| NYSE / NASDAQ / AMEX | $50M |
| JPX (Japan) | ¥5B |
| HKSE (Hong Kong) | HK$200M |
| NSE (India) | ₹500M |
| KSC (Korea) | ₩50B |
| LSE (UK) | £15M |
| TSX (Canada) | C$20M |
| TAI / TWO (Taiwan) | NT$500M |

## Usage

```bash
# Reproduce the results table: each market run on its own (about 10 minutes in total),
# writes results/returns_<MARKET>.json and the combined exchange_comparison.json
python3 graham-net-net/run_all_exchanges.py --output graham-net-net/results/exchange_comparison.json

# One market
python3 graham-net-net/backtest.py --exchange JPX --verbose
python3 graham-net-net/backtest.py --exchange NYSE,NASDAQ,AMEX --output graham-net-net/results/returns_US_MAJOR.json

# Charts (read results/exchange_comparison.json, write charts/)
python3 graham-net-net/generate_charts.py

# Current-day screen (see the limits above)
python3 graham-net-net/screen.py
python3 graham-net-net/screen.py --preset india
```

Last-traded-price exits are the default (`--exit-method ltp`). `backtest.py --global` pools every exchange into a single 30-stock portfolio; it is not what the table above reports.

`generate_charts.py` writes cumulative growth and year-by-year returns for each market against its index, plus CAGR and max drawdown by market, to `charts/`. Chart files aren't committed, so run it locally.

## Academic Background

- **Oppenheimer (1986):** 29% annual returns for US net-nets, 1970-1983. Benchmark: 11.5%.
- **Bildersee, Cheh, Zutshi (1993):** Japan net-nets returned 20.55%/yr vs 16.63% market, 1975-1988.
- **Xiao & Arnold (2008):** UK net-nets returned up to 19.7%/yr, 1980-2005.

Returns in this backtest are far below those figures, and the US, where Oppenheimer found the largest premium, trails its index.

## Data Source

All data via [Ceta Research](https://cetaresearch.com) (FMP financial data warehouse): fiscal-year key metrics and dividend-adjusted end-of-day prices.
