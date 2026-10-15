# DCF Discount Value Strategy

Buys companies whose market value is at least 20% below a simple discounted-cash-flow value: the Gordon Growth Model applied to the latest annual free cash flow. That condition reduces to a free cash flow yield (free cash flow / market cap) of at least 8.78%, so the screen ranks companies by FCF yield and holds the 50 highest, at equal weight, for a year.

## Academic Basis

- Gordon (1959), "Dividends, Earnings, and Stock Prices", *Review of Economics and Statistics* 41(2). The constant-growth valuation used here.
- Graham & Dodd (1934), *Security Analysis*. Buying below intrinsic value with a margin of safety.

## The Model

```
DCF value     = FCF × (1 + g) / (r − g) = FCF × 13.67      (g = 2.5%, r = 10%)
discount      = 1 − marketCap / (FCF × 13.67)
discount ≥ 20%  ⇔  FCF / marketCap ≥ 1.20 / 13.67 = 8.78%
```

`FCF` is the company's latest fiscal-year free cash flow. The signal doesn't depend on the share price beyond the market cap, and one set of assumptions is applied to every company and every market. A higher discount rate raises the threshold (r = 12% gives 11.12%), a lower one lowers it (r = 8% gives 6.44%).

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| FCF yield | 8.78% to 50% (a 20% or deeper discount; a yield above 50% is treated as a data error) | `cash_flow_statement` FY / `key_metrics` FY |
| Free cash flow | Above 0, reported in the market's reporting currency, on the latest filing | `cash_flow_statement` FY |
| Market cap | At least the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`): $1B in the US, C$500M in Canada, €500M on XETRA, ₹20B on NSE, ₩500B on KSC, SEK 5B on STO, NT$10B in Taiwan, ¥100B on JPX | `key_metrics` FY |
| Instrument | Not flagged as a fund or ETF | `profile` |

On each rebalance date, for every company on the market's exchanges:

1. Take the company's latest fiscal-year cash-flow row and its latest fiscal-year key-metrics row, each dated at least 45 days and at most 540 days before the rebalance date.
2. Keep the company if that cash-flow row shows free cash flow above 0 in the market's reporting currency, the market cap is above the floor, and the FCF yield is between 8.78% and 50%. A company whose latest filing fails is out for the year, even if an older filing passed.
3. Keep one line per ISIN.
4. Rank by FCF yield, highest first, and hold the top 50 at equal weight.

## Parameters

- **Rebalancing:** Annual, on April 1, executed at the next available close. 25 periods, April 2000 to April 2025.
- **Min stocks:** 10. The portfolio holds cash for the year if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price (a close of at least 1.00 in local units in the 10-day entry window). A cash year earns 0%. The floor is checked at entry only, after the one-line-per-ISIN step.
- **Max stocks:** 50.
- **Weighting:** Equal weight.
- **Return cap:** A single holding's yearly return above 200% is treated as a data error and left out of that year's average.
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Data lag:** 45 days after the fiscal year end (point-in-time), and no filing older than 540 days.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes, preferred lines and Thai NVDR lines have their own ISINs, so a company's two lines can both be held (see Known limits).
- **Universe:** every listing on the market's exchanges that isn't flagged as a fund or ETF. The universe isn't restricted to companies still listed today.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. Saudi Arabia has no local index configured and is measured against the S&P 500 (SPY) in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 0.5% Switzerland, 3.5% UK and Saudi Arabia, 10.5% Brazil).

## Results (April 2000 to April 2025)

Fourteen markets are compared. Of the 13 invested in at least half of the 25 years, 12 have a local index, and the screen beat it in 10: Sweden (+10.63pp a year), Germany (+7.03), Canada (+6.64), Taiwan (+6.39), Japan (+4.70), the UK (+4.16), Hong Kong (+3.42), China (+3.04), Korea (+1.78) and Thailand (+0.69). Germany's DAX includes dividends, like the portfolio. The other nine indices are price indices that leave dividends out, so each of those margins is overstated by roughly the index's dividend yield; Thailand's 0.69pp and Korea's 1.78pp are unlikely to survive a total-return comparison.

The US is level with SPY: 0.12pp ahead, which is a tie at under 0.2pp. India trails the Sensex by 1.29pp. Saudi Arabia is 0.74pp ahead of the S&P 500 in USD, a proxy rather than its own market, and over its 17 invested years it is 1.64pp behind it. Switzerland is too thin to test and is reported separately below.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| Sweden (STO) | OMX Stockholm 30 | 13.04% | 2.40% | +10.63 | 0.513 | -22.02% | 6 (2000-2002, 2007-2008, 2014) | 17.5 |
| Germany (XETRA) | DAX | 11.54% | 4.51% | +7.03 | 0.384 | -39.96% | 4 (2000-2002, 2016) | 27.9 |
| Canada (TSX + TSXV) | TSX Composite | 10.70% | 4.05% | +6.64 | 0.335 | -32.41% | 0 | 34.4 |
| India (NSE) | Sensex | 10.20% | 11.49% | -1.29 | 0.094 | -55.36% | 5 (2000-2004) | 31.7 |
| Taiwan (TAI + TWO) | TAIEX | 9.44% | 3.05% | +6.39 | 0.395 | -31.86% | 5 (2000-2004) | 42.9 |
| Saudi Arabia (SAU) | S&P 500 (SPY, USD) | 8.07% | 7.33% | +0.74 | 0.221 | -26.70% | 8 (2000-2006, 2008) | 21.8 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 7.44% | 7.33% | +0.12 | 0.165 | -61.51% | 0 | 35.4 |
| Japan (JPX) | Nikkei 225 | 6.90% | 2.20% | +4.70 | 0.273 | -45.04% | 1 (2000) | 42.2 |
| Korea (KSC) | KOSPI | 6.22% | 4.44% | +1.78 | 0.123 | -36.26% | 6 (2000-2004, 2007) | 33.8 |
| China (SHZ + SHH) | SSE Composite | 5.56% | 2.51% | +3.04 | 0.081 | -49.99% | 0 | 34.5 |
| UK (LSE) | FTSE 100 | 5.31% | 1.15% | +4.16 | 0.078 | -53.06% | 6 (2000-2005) | 28.8 |
| Thailand (SET) | SET Index | 5.10% | 4.41% | +0.69 | 0.105 | -42.72% | 5 (2000-2004) | 29.9 |
| Hong Kong (HKSE) | Hang Seng | 4.70% | 1.28% | +3.42 | 0.065 | -53.84% | 3 (2000-2002) | 28.8 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their April rebalance. Avg stocks is the average number of lines held in invested years; two lines of one company count as two (see Known limits).

- **US:** 7.44% a year against 7.33% for SPY, with a Sharpe of 0.165 against 0.253, a max drawdown of -61.51% against -39.33%, and no cash years. The return is the index's; the risk is higher.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). Three of the 13 markets are measured against a series that does too: the US and Saudi Arabia against SPY, and Germany against the DAX. The other ten are measured against price indices, so their excess is overstated by roughly the index's dividend yield. See METHODOLOGY.md (Benchmark Limitations).
- **Books at the floor:** Sweden 2006, 2012 and 2015, Taiwan 2005 and 2006, Korea 2005 and 2006, and Hong Kong 2004 were invested with exactly 10 buyable names. No invested year holds fewer than 10 lines.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets, because the risk-free rate differs (6.5% in India, 0.1% in Japan).

### Cash years

A year is in cash for one of two reasons, and each period record says which (`screened`, `entry_buyable`). The second column lists, in brackets, how many of the screened names had a usable entry price. The US, Canada and China were invested in all 25 years.

| Market | Screen passed fewer than 10 | Fewer than 10 buyable at entry (buyable of screened) |
|--------|-----------------------------|------------------------------------------------------|
| Sweden (STO) | 2000-2002, 2007-2008 | 2014 (8 of 13) |
| Germany (XETRA) | 2000-2002 | 2016 (9 of 15) |
| India (NSE) | 2000-2004 | none |
| Taiwan (TAI + TWO) | 2000-2004 | none |
| Saudi Arabia (SAU) | 2000-2006 | 2008 (9 of 11) |
| Japan (JPX) | 2000 | none |
| Korea (KSC) | 2000-2004, 2007 | none |
| UK (LSE) | 2000-2004 | 2005 (9 of 12) |
| Thailand (SET) | 2000-2003 | 2004 (9 of 13) |
| Hong Kong (HKSE) | 2000, 2002 | 2001 (5 of 10) |
| Switzerland (SIX) | 2000-2002, 2004 | 2003 (8 of 10), 2005 (8 of 14), 2006 (7 of 15), 2007 (8 of 15), 2008 (9 of 18), 2014 (8 of 11), 2015 (8 of 11), 2016 (9 of 14), 2017 (7 of 11), 2018 (7 of 11), 2019 (8 of 10), 2020 (9 of 14), 2021 (6 of 12) |

A cash year earns 0% and is counted in every 25-year figure above, against whatever the index did that year. The next table sets the 25-year excess beside the excess over the invested years only: the portfolio's returns in the years it held stocks, compounded and annualised over that many years, against the index over the same years. The second reading is not a return anyone earned. It shows how the selected stocks did when there was a book.

| Market | Cash years | Index in the cash years (% a year) | Excess, 25 years (pp) | Portfolio, invested years | Index, invested years | Excess, invested years (pp) |
|--------|------------|------------------------------------|-----------------------|---------------------------|-----------------------|-----------------------------|
| Sweden (STO) | 6 of 25 | -20.71 | +10.63 | 17.50% | 11.02% | +6.48 |
| Germany (XETRA) | 4 of 25 | -18.79 | +7.03 | 13.89% | 9.66% | +4.23 |
| India (NSE) | 5 of 25 | +5.50 | -1.29 | 12.91% | 13.04% | -0.13 |
| Taiwan (TAI + TWO) | 5 of 25 | -9.74 | +6.39 | 11.94% | 6.52% | +5.42 |
| Saudi Arabia (SAU) | 8 of 25 | -5.09 | +0.74 | 12.09% | 13.72% | -1.64 |
| Japan (JPX) | 1 of 25 | -37.58 | +4.70 | 7.20% | 4.32% | +2.88 |
| Korea (KSC) | 6 of 25 | +5.59 | +1.78 | 8.26% | 4.08% | +4.18 |
| UK (LSE) | 6 of 25 | -1.16 | +4.16 | 7.05% | 1.90% | +5.15 |
| Thailand (SET) | 5 of 25 | +11.34 | +0.69 | 6.41% | 2.74% | +3.67 |
| Hong Kong (HKSE) | 3 of 25 | -20.16 | +3.42 | 5.35% | 4.62% | +0.74 |

The sign changes in one market: Saudi Arabia goes from 0.74pp ahead of the S&P 500 over 25 years to 1.64pp behind it over its 17 invested years, because the S&P 500 fell 5.09% a year in the eight years the portfolio sat in cash. India trails on both readings. Sweden's and Hong Kong's leads are largely cash years in falling markets: their indices lost 20.71% and 20.16% a year in those years.

### Too thin to test

A market invested in fewer than half of the 25 years isn't a test of the screen. Its figures are listed so the files can be checked, not as results.

| Market | Years in cash | Returns in the invested years | Avg stocks when invested | CAGR incl. cash years |
|--------|---------------|-------------------------------|--------------------------|-----------------------|
| Switzerland (SIX) | 17 of 25 | 2009: +65.81% (10 names), 2010: +16.74% (23), 2011: -6.83% (16), 2012: +20.69% (12), 2013: +27.39% (10), 2022: -0.35% (10), 2023: +1.81% (12), 2024: +17.86% (11) | 13.0 | 4.91% |

- **Switzerland:** fewer than 10 Switzerland-domiciled companies passed the screen in 4 years. In 13 more, 10 to 18 passed but only 6 to 9 had a usable entry price. Its -6.83% max drawdown and 0.296 Sharpe describe 8 invested years and 17 flat ones. The two comparison charts leave it out and name it in their footer, and its own charts carry a "too thin to test" line.

### Australia and Brazil

The results directory holds `australia.json` and `brazil.json` because `generate_charts.py` reads them, but neither market is ranked here or in the comparison charts. The published Australian and Brazilian posts were withdrawn over broken split adjustments in the upstream ASX and SAO price data. This rerun reads the same price table, so it doesn't show whether that problem is gone. For checking only: Australia 5.80% a year against 3.67% for the ASX 200 (a price index), 9 years in cash; Brazil 10.25% against 8.58% for the Bovespa (which includes dividends), 6 years in cash, and 47% of the first ten names recorded in its invested years are preferred lines or units.

## Rerun October 2026

These results were rerun with the campaign's changes, described in METHODOLOGY.md (Universe Construction, Filing and Selection Rules, and Limitations: Survivorship Bias), and with one fix to this screen. The results published before this rerun were a May 2026 run (a March 2026 run for Australia and Brazil).

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 44 holdings in 11 of the 14 compared markets exited this way, 7 of them in the US; each results file lists them under its `ltp` key. On identical selection the exit rule moves CAGR by -0.10pp to +0.17pp.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market.
- **No `isActivelyTrading` filter.** The published run kept only companies still listed today, in every year. The universe now includes companies that stopped trading later, as far as the data holds them (see Known limits).
- **Entry-side cash rule.** The published files predated the check that at least 10 screened names have a usable entry price. They decided cash on the screen count alone.
- **Selection fix.** Each company is now tested on its latest fiscal-year filing. The published code took the latest filing that passed (positive free cash flow in the reporting currency), so a company whose cash flow had turned negative stayed in the screen on an older filing. The FCF yield is also capped at 50%, as the SQL screens printed in the topic's posts already were (`screen.py` isn't; see Known limits).

What moved by more than 1pp, and the verdict changes:

- **Switzerland** fell from 8.75% to 4.91% and became too thin to test: cash years rose from 3 to 17. Fewer than 10 Switzerland-domiciled companies passed in 2004, and in 13 other years 10 to 18 passed with 6 to 9 buyable. A replica of the screen run before the rerun (same wiring, old and new selection, raw price bars) gives 5 cash years under the campaign wiring alone, 12 once the latest filing is tested, and 17 with the 50% cap, the same 17 years as the rerun; in it, every Swiss name that wasn't buyable had no price bar in the entry window.
- **Hong Kong** fell from 6.77% to 4.70%. 2001 and 2002 are now cash (5 of 10 names buyable; 9 passed); the published files reported +30.52% and -4.44% for them. The rest comes from changed books.
- **Saudi Arabia** rose from 5.63% to 8.07% and went from 1.70pp behind the S&P 500 in USD to 0.74pp ahead. One year does most of it: 2008 passed 11 names with 9 buyable and is now cash, where the published file reported -38.80%.
- **Sweden** fell from 14.57% to 13.04%. 2014 passed 13 names with 8 buyable and is now cash, where the published file reported +28.77% in a year the OMX Stockholm 30 rose 21.64%.
- **US** fell from 7.91% to 7.44%, which turns a 0.58pp lead over SPY into a tie. No year is in cash; the books changed (the pre-rerun replica attributes most of the change to the 50% cap), and the max drawdown went from -59.79% to -61.51%.
- The other markets moved by -0.97pp to +0.57pp. Benchmark CAGRs are unchanged in all 16.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

The committed files were made one market at a time, each with `--preset` and an explicit `--output`:

```bash
# One market, written to the committed results file
python3 dcf-discount/backtest.py --preset germany --output dcf-discount/results/germany.json
python3 dcf-discount/backtest.py --preset us --output dcf-discount/results/us.json

# Charts from the region files, written to dcf-discount/charts/
python3 dcf-discount/generate_charts.py

# The old exit rule (doesn't reproduce old results: the listing rules, dedup, selection fix and cash rule still apply)
python3 dcf-discount/backtest.py --preset uk --exit-method drop

# Current-day screen (see Known limits)
python3 dcf-discount/screen.py --preset india
```

| Market | `--preset` | Exchanges | Output file | Listing rule |
|--------|------------|-----------|-------------|--------------|
| Sweden | `sweden` | STO | `sweden.json` | |
| Germany | `germany` | XETRA | `germany.json` | Germany-domiciled only |
| Canada | `canada` | TSX, TSXV | `canada.json` | |
| India | `india` | NSE | `india.json` | |
| Taiwan | `taiwan` | TAI, TWO | `taiwan.json` | |
| Saudi Arabia | `saudi` | SAU | `saudi.json` | |
| US | `us` | NYSE, NASDAQ, AMEX | `us.json` | |
| Japan | `japan` | JPX | `japan.json` | |
| Korea | `korea` | KSC | `korea.json` | |
| China | `china` | SHZ, SHH | `china.json` | |
| UK | `uk` | LSE | `uk.json` | GBp lines only |
| Thailand | `thailand` | SET | `thailand.json` | |
| Hong Kong | `hongkong` | HKSE | `hongkong.json` | |
| Switzerland | `switzerland` | SIX | `switzerland.json` | Switzerland-domiciled only |
| Australia | `australia` | ASX | `australia.json` | |
| Brazil | `brazil` | SAO | `brazil.json` | |

`generate_charts.py` reads these 16 files, and reads `returns_*.json` only when none of them exists.

`--global` isn't how these files were made. It runs Canada as TSX alone and Singapore as SGX (no rows), writes `returns_<exchanges>.json` next to the `--output` path, and records a market that fails as an error entry while its old file stays on disk.

## Known limits

- **Survivorship.** The price history holds almost no company that stopped trading before 2015 in the US or before 2020 elsewhere. Every result for the earlier years is measured on companies that survived at least that long, and last-traded-price exits only reach companies that stopped trading after that. See METHODOLOGY.md (Limitations: Survivorship Bias).
- The 45-day lag counts from the fiscal year end. Many companies publish annual results later than that, so a row can be used before it was public.
- Free cash flow and market cap are each taken from their own latest row, so they can come from different fiscal years when one table lacks the newest year.
- The size floor compares the filing's market cap, in the company's reporting currency, with a floor set in the listing currency. See DATA_QUALITY_ISSUES.md.
- The US market is every line on NYSE, NASDAQ and AMEX, so it holds foreign companies' depositary receipts as well as US companies.
- **Lines that aren't ordinary shares.** Preferred lines, Thai NVDR lines and other second lines of a company have their own ISINs, so they take their own slots. Among the first ten names recorded for each invested year (the highest-yielding ten), preferred lines take 78 of 250 slots in Canada (31.2%; MFC-PB.TO and POW-PC.TO, for example), preferred lines 43 of 190 in Korea (22.6%), preference shares 20 of 190 in the UK (10.5%; AV-A.L, AV-B.L, SANB.L, RSAB.L) and preferred lines 18 of 210 in Germany (8.6%; MAN3.DE, VOW3.DE). In Thailand NVDR lines take 70 of 200 (35.0%) and foreign-board lines 3 more, often beside the ordinary line (SCB.BK and SCB-R.BK). The US holds 10 preferred or note lines in 250 (4.0%). These were identified by ticker convention (for the named US and LSE lines, by name), not by a flag in the data. Each record lists ten names only, so these are counts over the highest-yielding names and the share in the whole book isn't known. Avg stocks counts lines, not companies.
- China's books hold Shenzhen B shares (200xxx.SZ, quoted in Hong Kong dollars) in 13 of 250 recorded slots.
- The entry floor of 1.00 is in local units. A name with no price bar in the 10-day entry window isn't buyable either; the pre-rerun replica found that this, not the floor, is what thins Switzerland's books.
- `screen.py` (the current-day screen) predates the rerun: it takes the latest filing with positive free cash flow, applies no reporting-currency rule, no fund or ETF filter, no 50% cap, no listing rule and no dedup. The backtest doesn't use it.

## Data Requirements

- `cash_flow_statement` (FY): freeCashFlow, reportedCurrency
- `key_metrics` (FY): marketCap
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, isFund, isEtf, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin (dedup)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current-day screen (see Known limits) |
| `generate_charts.py` | Charts from the 16 region files |
| `README.md` | This file |
| `results/` | `<region>.json` per market |
| `charts/` | Output of `generate_charts.py`: growth and yearly returns for each market, plus CAGR and max drawdown for the compared markets (Switzerland, too thin to test, is named in the footer, not ranked). Chart files aren't committed, so run it locally |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
