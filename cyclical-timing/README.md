# Cyclical Sector Timing

Buy quality cyclicals only when corporate revenues confirm economic expansion.

## Strategy Summary

**Universe:** Basic Materials + Industrials + Energy + Consumer Cyclical
**Signal (timing):** ≥50% of cyclical stocks with positive YoY revenue growth (FY data)
**Selection (when signal is on):** Top 30 by ROE, with positive revenue growth and ROE ≥ 5%
**Cash rule:** Cash when the signal is off, or when fewer than 10 names pass the screen or have a usable entry price
**Rebalancing:** Annual (July), using FY data with 45-day lag
**Costs and execution:** Size-tiered transaction costs, trades at the next day's close
**Data checks:** No return cap. The data checks (`dc-1`, [METHODOLOGY.md](../METHODOLOGY.md#data-checks-deleveraging-and-cyclical-timing)) move holiday rows, zero-volume rows of lines that traded, stale lines with no volume, a thin share class's replacement bar off its sibling's scale and non-positive prices out of each price window, hold a Thai NVDR line and its ordinary line once, test both market-cap floors (the signal's and the screen's) on a cap checked against later reverse splits, and flag adjustment-factor steps, splits and every return above +100% or below -50% for review. The results below come from the rerun with these checks, which replaced a 200% cap on a single stock's annual return; [Rerun October 2026](#rerun-october-2026) lists what moved.
**Period:** 2001–2025 (24 annual periods, July 2001 to July 2025)

## Academic Basis

Sector rotation research (Fama & French, 1997; Moskowitz & Grinblatt, 1999) shows that sector-level signals contain information about future cross-sectional returns. The revenue-based timing signal is derived from: Nissim & Penman (2001), who document that revenue growth forecasts future profitability for industrial companies.

## Signal Logic

The expansion/contraction switch works as follows:

1. At each July rebalance, compute YoY revenue growth for every qualifying cyclical stock (Basic Materials, Industrials, Energy, Consumer Cyclical) with market cap above the exchange threshold
2. If ≥50% show positive YoY growth → **expansion confirmed → invest**
3. If <50% → **contraction signal → hold cash**

When invested, select top 30 by ROE (return on equity) among stocks with positive revenue growth. Quality-within-cyclicals approach avoids commodity peak-cycle concentration that pure revenue-momentum selection creates. If fewer than 10 names qualify, or fewer than 10 of them have a usable entry price, the period is held in cash even with the signal on.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2001–2025) |
| `screen.py` | Current qualifying stocks |
| `generate_charts.py` | Charts from results JSON |
| `results/returns_<market>.json` | One market's results: metrics, every period, last-traded-price fills |
| `results/exchange_comparison.json` | All 16 markets in one file |
| `charts/` | Cumulative growth and annual returns per market, CAGR and drawdown comparisons |

## Usage

Run from the repository root. `--exit-method` defaults to `ltp` (last-traded-price exits). The listing rules apply automatically on XETRA, SIX and LSE runs; there is no flag for them.

```bash
# All 16 markets: writes returns_<market>.json for each and exchange_comparison.json
python3 cyclical-timing/backtest.py --global --output cyclical-timing/results/exchange_comparison.json

# One market: rewrites only that market's returns file
python3 cyclical-timing/backtest.py --exchange NYSE,NASDAQ,AMEX --output cyclical-timing/results/returns_NYSE_NASDAQ_AMEX.json
python3 cyclical-timing/backtest.py --exchange XETRA --output cyclical-timing/results/returns_XETRA.json   # Germany-domiciled only
python3 cyclical-timing/backtest.py --exchange SIX   --output cyclical-timing/results/returns_SIX.json     # Switzerland-domiciled only
python3 cyclical-timing/backtest.py --exchange LSE   --output cyclical-timing/results/returns_LSE.json     # pence (GBp) lines only

# Old exit rule (drops holdings with no exit price), or drop results with LTP beside them.
# drop doesn't reproduce the pre-rerun numbers: the listing rules and dedup changed selection too.
python3 cyclical-timing/backtest.py --exchange NSE --exit-method drop
python3 cyclical-timing/backtest.py --exchange NSE --exit-method both

# Every data check off and no return cap: measures what the checks change, never published
python3 cyclical-timing/backtest.py --exchange SAU --data-checks uncapped

# Current screen
python3 cyclical-timing/screen.py --preset us

# Generate charts
python3 cyclical-timing/generate_charts.py
```

The other markets use the same pattern with `--exchange NSE`, `TSX`, `SHZ,SHH`, `HKSE`, `JPX`, `KSC`, `ASX`, `TAI,TWO`, `STO`, `SAO`, `JNB` or `SAU`. Use `--exchange` rather than `--preset` to match the committed files. Five presets write a different universe name from the committed one (`us` writes US_MAJOR, `canada` Canada, `china` China, `taiwan` Taiwan and `southafrica` JSE), and `--preset canada` also adds TSXV.

`screen.py` doesn't apply the listing rules or the same-ISIN dedup, so on XETRA, SIX and LSE it can list lines the backtest excludes.

## Key Results (US, 2001–2025)

| Metric | Cyclical Timing | S&P 500 |
|--------|----------------|---------|
| CAGR | 12.13% | 8.91% |
| Excess CAGR | +3.22 pts | n/a |
| Max Drawdown | -37.19% | -38.01% |
| Volatility | 26.7% | 16.2% |
| Sharpe Ratio | 0.380 | 0.427 |
| Up Capture | 121.19% | n/a |
| Down Capture | 44.70% | n/a |
| Periods beating the index | 13/24 | n/a |
| Cash Periods | 3/24 (12.5%) | n/a |

**Key finding:** The strategy leads the S&P 500 by 3.22 points a year with 44.70% down capture, but its volatility is higher (26.7% vs 16.2%), so the Sharpe ratio trails the index. The strategy sat in cash in 2010, 2016 and 2021, each time because the expansion signal read below 50%. Only 2021 avoided a down market (S&P 500 -10.68%). The 2010 and 2016 cash years missed rallies of +33.55% and +18.58%.

**Split story:** 2001–2009: beat the S&P 500 in 8 of 9 periods; 2007 trailed it, -15.58% against -15.20%. 2010–2024: beat it in 5 of 15, which is 4 of the 12 invested periods plus the 2021 cash year. The strategy holds no Technology or Communication Services stocks.

## Signal History (US)

Year = the July rebalance; each period runs to the next July.

| Year | Signal | Expansion % | Action |
|------|--------|------------|--------|
| 2001 | ON | 81.9% | Invested (outperformed: +7.55% vs -22.45%) |
| 2009 | ON | 79.1% | Invested (outperformed: +45.57% vs +15.98%) |
| 2010 | **OFF** | 24.1% | **Cash** (missed +33.55% rally) |
| 2016 | **OFF** | 48.3% | **Cash** (missed +18.58% rally) |
| 2021 | **OFF** | 39.8% | **Cash** (avoided -10.68% drop) |
| 2022–2024 | ON | 60.7–90.2% | Invested (trailed the S&P 500 all three years) |

## Results by Market (2001–2025)

Returns in local currency, each market against its own index where the data has one. Sorted by excess CAGR.

| Market | Listing rule | Benchmark | CAGR | Benchmark CAGR | Excess (pts) | Sharpe | Max DD | Cash | Avg stocks |
|--------|--------------|-----------|-----:|---------------:|-------------:|-------:|-------:|-----:|-----------:|
| UK (LSE) | pence (GBp) lines | FTSE 100 | 8.76% | 1.80% | +6.96 | 0.299 | -33.68% | 2/24 | 18.5 |
| Sweden (STO) | none | OMX Stockholm 30 | 11.22% | 4.40% | +6.81 | 0.410 | -42.68% | 6/24 | 24.9 |
| Hong Kong (HKSE) | none | Hang Seng | 8.97% | 2.57% | +6.41 | 0.196 | -37.64% | 2/24 | 17.4 |
| Switzerland (SIX) | Switzerland-domiciled | SMI | 8.24% | 2.08% | +6.16 | 0.360 | -41.90% | 6/24 | 20.3 |
| Australia (ASX) | none | ASX 200 | 8.53% | 3.89% | +4.64 | 0.332 | -24.37% | 3/24 | 21.9 |
| US (NYSE+NASDAQ+AMEX) | none | S&P 500 | 12.13% | 8.91% | +3.22 | 0.380 | -37.19% | 3/24 | 23.7 |
| Canada (TSX) | none | TSX Composite | 7.61% | 5.32% | +2.29 | 0.235 | -32.57% | 3/24 | 24.7 |
| India (NSE) | none | Sensex | 14.94% | 14.23% | +0.71 | 0.273 | -37.05% | 5/24 | 21.2 |
| Germany (XETRA) | Germany-domiciled | DAX | 6.04% | 5.83% | +0.21 | 0.187 | -42.29% | 5/24 | 25.5 |
| Korea (KSC) | none | KOSPI | 6.46% | 7.07% | -0.61 | 0.160 | -23.33% | 9/24 | 27.9 |
| Japan (JPX) | none | Nikkei 225 | 3.79% | 4.85% | -1.06 | 0.168 | -65.58% | 6/24 | 27.5 |
| Taiwan (TAI+TWO) | none | TAIEX | 3.64% | 6.58% | -2.94 | 0.196 | -22.27% | 11/24 | 26.4 |
| China (SHZ+SHH) | none | SSE Composite | -2.60% | 1.89% | -4.49 | -0.119 | -67.84% | 6/24 | 15.4 |
| Brazil (SAO) | none | Bovespa | 4.82% | 9.92% | -5.10 | -0.211 | -43.62% | 9/24 | 13.8 |
| South Africa (JNB) | none | S&P 500 (USD) | 1.44% | 8.91% | -7.48 | -0.483 | -25.27% | 11/24 | 12.2 |
| Saudi Arabia (SAU) | none | S&P 500 (USD) | 0.84% | 8.91% | -8.08 | -0.134 | -45.49% | 11/24 | 22.7 |

Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the gap between the two rounded columns. Avg stocks = average holdings in invested periods.

9 of the 14 markets with a local index beat it: the UK, Sweden, Hong Kong, Switzerland, Australia, the US, Canada, India and Germany. South Africa and Saudi Arabia have no local index in the data and are measured against the S&P 500 in USD, so their excess includes currency moves and they aren't counted. Seven of the nine wins are against price indices that leave dividends out (all but the S&P 500 and the DAX), so those margins are overstated by roughly each index's dividend yield. The three thinnest of those, India (+0.71), Canada (+2.29) and Australia (+4.64), may not survive a total-return comparison. The DAX, Bovespa and S&P 500 include dividends, so Germany's +0.21 and the US's +3.22 are like for like. Details in METHODOLOGY.md (Benchmark Limitations).

## Cash Periods by Market

Every cash period counts as a 0% return in the CAGR. "No data" means no cyclical stock above the market-cap threshold had two years of FY revenue to compare, so the code reads the signal as off.

| Market | Signal off | No data | Too few names (fewer than 10 qualify or are buyable at entry) |
|--------|------------|---------|------------------------------------------|
| UK (LSE) | 2010, 2021 | none | none |
| Sweden (STO) | 2003, 2004, 2010, 2021 | none | 2001, 2002 |
| Switzerland (SIX) | 2003, 2004, 2010, 2021 | none | 2001, 2002 |
| Australia (ASX) | none | none | 2001–2003 |
| Canada (TSX) | 2010, 2016, 2021 | none | none |
| US (NYSE+NASDAQ+AMEX) | 2010, 2016, 2021 | none | none |
| Hong Kong (HKSE) | none | none | 2001, 2015 |
| Germany (XETRA) | 2003, 2010, 2021 | none | 2001, 2004 |
| Japan (JPX) | 2002, 2009, 2010, 2017, 2020, 2021 | none | none |
| India (NSE) | 2021 | 2001, 2002 | 2003, 2004 |
| Korea (KSC) | 2002, 2021 | none | 2001, 2003–2008 |
| Taiwan (TAI+TWO) | 2010, 2016, 2017, 2021, 2024 | 2001–2005 | 2006 |
| China (SHZ+SHH) | none | none | 2005, 2009, 2010, 2013, 2015, 2020 |
| Brazil (SAO) | 2010, 2017 | none | 2001–2005, 2008, 2009 |
| South Africa (JNB) | none | none | 2001–2007, 2010, 2011, 2021, 2024 |
| Saudi Arabia (SAU) | 2010, 2017, 2018, 2021 | 2001–2004 | 2005–2007 |

- Among the 14 markets with a local index, the signal switched off at the July 2010 rebalance in 9, and every one of those 9 indices rose over the following year.
- It switched off at the July 2021 rebalance in 10 of the 14, and the index fell in 8 of those 10 (all but India and the UK).
- Across all 14, the signal was off for 35 periods with data. The index fell in 12 of them and rose in 23.
- China's signal never switched off. All six of its cash years are price gaps: 30 names screened, 2 to 9 of them with a usable entry price.
- Thin early data drives most early cash years elsewhere. Korea's signal universe held 1 to 11 stocks from 2001 to 2008, so its 2002 signal-off rests on a single stock. Saudi Arabia's held 1 or 2 from 2005 to 2007, and South Africa's never held more than 31.

## Rerun October 2026

All results were rerun in October 2026 on refreshed data, with four code changes. The first three are described in METHODOLOGY.md (Universe Construction and Limitations: Survivorship Bias), the fourth in METHODOLOGY.md (Data Checks):

- **Last-traded-price exits.** A holding with no price in the window after the exit date now exits at its last traded price instead of being dropped from the period. 33 exits in 12 markets were filled this way; each fill is listed under `ltp` in that market's results file.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. Before, these books also held foreign companies' secondary lines.
- **Same-ISIN dedup.** On every market, a security listed under more than one ticker (same ISIN) is held once, and the freed slot goes to the next-ranked name. Share classes have their own ISINs, so both can be held: Sweden's book often holds Atlas Copco A and B together, and Canada's has held Teck A and B.
- **Data checks in place of the return cap.** The first October rerun still dropped a single stock's annual return above 200%. A second run replaced that cap with the data checks, and these results come from it. Against the first rerun, excess moved from +0.50 to +3.22 in the US, +0.33 to +6.41 in Hong Kong, -1.43 to +0.71 in India, -0.46 to +0.21 in Germany, -3.12 to -0.61 in Korea, -7.23 to -5.10 in Brazil, -6.15 to -4.49 in China and +1.43 to +2.29 in Canada; no other market moved by more than 0.53 points. Brazil gained two cash years (2008, 2009) and China one (2015), each with fewer than 10 names buyable after check T lost an entry (USIM6.SA in both Brazilian years, with one name under the floor on check M's cap in 2008; 603898.SS in China).

What moved, against the previous published results:

- **Germany:** CAGR 8.26% → 6.04% against the DAX's 5.83% (excess +2.44 → +0.21). Cash periods 4 → 5 and average holdings 19.2 → 25.5.
- **UK:** CAGR 0.67% → 8.76% against the FTSE 100's 1.80% (excess -1.13 → +6.96). The old run sat in cash for 17 of 24 periods, 15 of them because fewer than 10 of the 30 screened names had a usable entry price. Pence-only lines leave 2 cash periods, both signal-off.
- **Switzerland:** CAGR 7.33% → 8.24% (excess +5.25 → +6.16), cash periods 7 → 6, average holdings 15.8 → 20.3.
- **Hong Kong:** CAGR 3.05% → 8.97% (excess +0.48 → +6.41), and **the US** 9.43% → 12.13% (+0.52 → +3.22).
- **India** changed sides: CAGR 12.41% → 14.94% (excess -1.82 → +0.71).
- **Korea** 3.95% → 6.46% (excess -3.12 → -0.61), **China** -4.85% → -2.60% (-6.74 → -4.49), **Brazil** 2.80% → 4.82% (-7.13 → -5.10) and **Canada** 6.71% → 7.61% (+1.39 → +2.29).
- **Everywhere else:** CAGR moved by 0.53 points or less. In the first October rerun, before the data checks, every market but Germany, the UK and Switzerland moved by 0.58 points or less (US 9.43% → 9.42%) and none changed sides; those moves mix the dedup, LTP fills, the data refresh and tie-break noise in the old results, which came from before the selection path was made deterministic (METHODOLOGY.md, Reproducibility). The moves since then are the data checks'. The count of markets beating their index goes from 7 of 14 to 9, with the UK and India in.

## Data Source

Ceta Research (FMP financial data warehouse). Revenue data from `income_statement` (FY periods). Quality metrics from `key_metrics` (FY). Prices from `stock_eod`. Sector, country, quote currency and ISIN from `profile`. The data checks also read `income_statement` net income, EPS and weighted shares, `stock_eod` close and volume, `stock_prices_non_split_adjusted_daily` and `splits_calendar`.
