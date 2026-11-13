# FCF Conversion Quality

Systematic quality screen based on free cash flow conversion rate.

## Signal

Companies that convert a high percentage of reported earnings into actual free cash flow, with quality filters to eliminate statistical noise.

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| FCF / Net Income | 100% - 300% | Cash backs reported earnings. Cap at 300% filters tiny-NI noise |
| FCF / Revenue | > 10% | Ensures meaningful absolute cash flow, not ratio math alone |
| Return on Equity | > 10% | Profitable business, not high conversion on a low base alone |
| Operating Margin | > 10% | Real pricing power, sustainable cash generation |
| Net Income | > 0 | Negative denominators make ratio meaningless |
| Free Cash Flow | > 0 | Only companies generating cash |
| Market Cap | > exchange threshold | Liquid stocks only |

**Selection:** Top 30 by highest FCF conversion, equal weight.
**Rebalancing:** Annual (July). Cash if fewer than 10 stocks qualify, or fewer than 10 of them have a usable entry price.

On each rebalance date, for every ordinary share on the market's exchanges:

1. Take the company's latest fiscal-year `cash_flow_statement`, `key_metrics` and `financial_ratios` rows whose fiscal year ended at least 45 days and less than 410 days before the rebalance date: the (T-410, T-45] window.
2. Pair the cash-flow row with an `income_statement` row: the latest one in the same window whose fiscal year end is within 31 days of the cash-flow row's, either way, in the same `reportedCurrency` (two missing currencies count as equal). A company with none isn't ranked that year.
3. Keep the company if it passes the seven filters above, with FCF and net income read from that pair.
4. Keep one line per security (ISIN; a Thai NVDR shares its ordinary line's, and on SAO a company holds one line).
5. Rank by FCF conversion, highest first, with ties broken by symbol, and hold the top 30 at equal weight.

Each results file counts the lines step 2 leaves out, per rebalance date and by reason, under `statement_pair_guard`: `currency_mismatch` (income rows that fit the window and the 31 days, but only in another currency) or `no_income_in_window` (none at all). It counts the lines with positive free cash flow that pass the ROE, operating-margin and market-cap tests, before the one-line-per-security step ([METHODOLOGY.md](../METHODOLOGY.md#filing-and-selection-rules-rerun-topics), Filing and Selection Rules).

**Ordinary shares only** (universe rules `ur-3`, [METHODOLOGY.md](../METHODOLOGY.md#universe-rules)). Preferred and preference shares, notes, units, warrants, rights, funds and second lines at their own price on a company's financials never enter the universe, and neither do ADRs, royalty trusts and LPs on NYSE, NASDAQ and AMEX or BDRs on SAO. On SAO a company holds one line, its ordinary share, with its priced preferred or unit line standing in when the ordinary line has no entry price (`universe_rules.class_stand_ins`). ur-3 is asset growth's `ur-2` with more lines on the hand list: two preferreds and four notes no pattern reads (FITBP, SANB.L, three ANZ capital notes and CommBank PERLS), Rotork's and Sdiptech's preference lines (76ID.L, SDIP-PREF.ST), three depositary receipts listed abroad (6288.HK, ARION-SDB.ST, SAMPO-SDB.ST), a renamed copy and a second line (NTCO3.SA, 9512.SR) and an ADR the profile doesn't flag (JP). A line whose earlier bars or financial rows copy another line's enters on its first own day (`universe_rules.line_starts`: AZZA3.SA, L1G.AX, MATR.TO, TKO, AXIA6.SA, AXIA3.SA, BRSL, DOC and MAGN). A line whose later financial rows aren't its listing's leaves after its last own day (`universe_rules.line_ends`): Cabot Oil & Gas (COG) after 2021-10-01, when it became Coterra, Continental Resources (CLR) after 2022-11-22, when it went private, and two old tickers of renamed lines, PEAK after 2024-03-28 and GLT after 2024-12-03. A screen on the last own day keeps the line, and a screen after it gives the slot to the next name. Every results file records the rules and what they removed under `universe_rules`.

**Financials switch.** `--exclude-sector "Financial Services"` takes the lines whose profile sector is Financial Services out of the book after the screen's other tests and before the one-line-per-security step and the top-30 cut, and lists them per rebalance date under `sector_exclusion`. It's off by default, and the published markets run without it.

## Backtest Setup

| Parameter | Value |
|-----------|-------|
| Period | Annual rebalances from July 2000 to the latest close every market in the run traded, the last year partial (METHODOLOGY.md, "Run Window"). `--end-year 2025` stops at July 2025: 25 rebalances, July 2000 to July 2024, each held one year |
| Financial data lag | The (T-410, T-45] window and the cash-flow and income pair above |
| Execution | Next-day close after the rebalance date |
| Costs | Size-tiered, one way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The dollar breakpoints are converted to the leg's currency at a fixed rate per exchange (`costs.get_fx_per_usd`), recorded under `cost_model` |
| Entry floor | Entry price of at least 1.0 in the exchange's price unit (pence on LSE) |
| Data checks | No return cap. Data checks `dc-3` ([METHODOLOGY.md](../METHODOLOGY.md#data-checks)) clean the price windows, hold a Thai NVDR and its ordinary line once, test the market-cap floor on a checked cap, book reviewed rebuilds from a hand list, and list every return above +100% or below -50% |
| Missing exit price | Last traded price on or before the exit date |
| Benchmark | Local index per market, in local currency (S&P 500 in USD for Saudi Arabia) |

## Results (October 2026 run, July 2000 to July 2025)

These are the October 2026 run's results, and they predate the code above. That run had a 200% cap on a single stock's return, cost tiers in unconverted dollars, the 45-day lag with no lower bound and no pair test, no universe rules and a grid typed to end in 2025. The current code doesn't reproduce them, and a rerun on it replaces them.

The screen beat its local index in 10 of 15 markets. In the US it returned 9.91% a year against 7.85% for the S&P 500, with a Sharpe ratio of 0.402 against 0.352 and a slightly deeper maximum drawdown (-39.76% against -38.01%). India had the highest CAGR (14.94% against the Sensex's 12.06%) and Hong Kong the largest margin over its index (+5.78 percentage points). It trailed in Taiwan, Thailand, Germany, Brazil and Korea. Saudi Arabia is shown but not counted: there's no local index series, so it runs against the S&P 500 in USD.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max drawdown | Cash years | Avg stocks |
|---|---|---|---|---|---|---|---|---|
| Hong Kong (HKSE) | Hang Seng | 7.42% | 1.64% | +5.78 | 0.241 | -37.97% | 3 of 25 | 17.5 |
| UK (LSE) | FTSE 100 | 6.12% | 1.23% | +4.89 | 0.158 | -30.92% | 5 of 25 | 19.4 |
| Canada (TSX) | TSX Composite | 8.11% | 3.95% | +4.16 | 0.357 | -24.32% | 4 of 25 | 21.9 |
| China (SHZ, SHH) | SSE Composite | 6.01% | 2.43% | +3.58 | 0.099 | -43.71% | 0 of 25 | 20.0 |
| Switzerland (SIX) | SMI | 4.83% | 1.74% | +3.09 | 0.362 | -26.82% | 12 of 25 | 14.6 |
| India (NSE) | Sensex | 14.94% | 12.06% | +2.88 | 0.403 | -12.81% | 6 of 25 | 20.0 |
| Australia (ASX) | ASX 200 | 6.76% | 3.89% | +2.86 | 0.217 | -24.13% | 9 of 25 | 20.2 |
| US (NYSE, NASDAQ, AMEX) | S&P 500 | 9.91% | 7.85% | +2.06 | 0.402 | -39.76% | 0 of 25 | 24.0 |
| Japan (JPX) | Nikkei 225 | 4.99% | 3.31% | +1.68 | 0.261 | -41.64% | 6 of 25 | 24.7 |
| Sweden (STO) | OMX Stockholm 30 | 3.84% | 2.55% | +1.29 | 0.147 | -19.98% | 18 of 25 | 19.6 |
| Taiwan (TAI) | TAIEX | 3.56% | 4.09% | -0.53 | 0.199 | -24.16% | 7 of 25 | 24.1 |
| Thailand (SET) | SET Index | 3.69% | 5.13% | -1.45 | 0.088 | -39.26% | 9 of 25 | 19.2 |
| Germany (XETRA) | DAX | 0.21% | 5.04% | -4.83 | -0.120 | -48.00% | 12 of 25 | 14.9 |
| Brazil (SAO) | Bovespa | 2.93% | 8.70% | -5.77 | -0.653 | -13.07% | 13 of 25 | 14.0 |
| Korea (KSC) | KOSPI | -1.71% | 5.35% | -7.06 | -0.675 | -43.22% | 18 of 25 | 12.6 |
| Saudi Arabia (SAU) | S&P 500 (USD, not counted) | 1.43% | 7.85% | -6.42 | -0.137 | -33.21% | 10 of 25 | 15.7 |

Returns are in local currency. A cash year earns 0% and still counts in the CAGR. Avg stocks is the average book size in invested years; it sits below 30 when fewer than 30 names pass the screen or have a usable entry price, or when a realised return above 200% is dropped (29 names across 19 invested years).

Nine of the ten wins are against price indices that leave dividends out; the S&P 500 is the only total-return benchmark among them. Those margins are overstated by roughly each index's dividend yield, and the thinnest, Sweden (+1.29) and Japan (+1.68), may not survive a total-return comparison. The same omission flatters the Sharpe comparison below. The DAX and Bovespa include dividends, so two of the five losses are against total-return series. Details in [METHODOLOGY.md](../METHODOLOGY.md#benchmark-limitations) (Benchmark Limitations).

The Sharpe ratio beat the benchmark's in 11 of 15 markets, Taiwan included despite its lower CAGR, and the maximum drawdown was shallower in 12 of 15. Read both against the cash column. Sweden and Korea held cash in 18 of 25 years, Brazil in 13, Germany and Switzerland in 12, and a flat 0% year damps volatility and can't add to a drawdown. Those markets' figures rest on 7 to 13 invested years.

Cash years by market (the July rebalance that went to cash):

| Market | Cash rebalances (July of) |
|---|---|
| Korea (KSC) | 2000-2007, 2009-2015, 2018-2020 |
| Sweden (STO) | 2000-2009, 2011-2015, 2017, 2019, 2023 |
| Brazil (SAO) | 2000-2007, 2010-2012, 2015, 2022 |
| Germany (XETRA) | 2000-2006, 2009, 2012, 2014-2016 |
| Switzerland (SIX) | 2000-2009, 2012, 2019 |
| Saudi Arabia (SAU) | 2000-2007, 2009, 2019 |
| Australia (ASX) | 2000-2005, 2007, 2008, 2010 |
| Thailand (SET) | 2000-2005, 2007-2009 |
| Taiwan (TAI) | 2000-2006 |
| India (NSE) | 2000-2003, 2005, 2009 |
| Japan (JPX) | 2000-2004, 2009 |
| UK (LSE) | 2000-2004 |
| Canada (TSX) | 2000-2003 |
| Hong Kong (HKSE) | 2000-2002 |

The US and China were invested in all 25 years.

## Rerun October 2026

These results replace the previously published set. Three things changed, all described in [METHODOLOGY.md](../METHODOLOGY.md):

- **Last-traded-price exits.** A holding with no price in the 10-day window after the exit date now exits at its last traded price instead of being dropped from the period. 28 holdings across the 16 markets exited this way, and none was left without an exit price. See [Survivorship Bias](../METHODOLOGY.md#survivorship-bias).
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only. See [Universe Construction](../METHODOLOGY.md#universe-construction).
- **Same-ISIN dedup.** On every market, a security listed under more than one ticker (same ISIN) is held once, before the top-30 cut. Share classes have their own ISINs and can both be held: the US book holds both Wiley classes, WLY and WLYB, in 5 of its 25 years.

Separately, selection is now deterministic: ties at the top-30 cut break on the symbol (see [Reproducibility](../METHODOLOGY.md#reproducibility)).

The three markets under the listing rules moved the most. Germany fell from 6.88% to 0.21% CAGR and now trails the DAX; its cash years rose from 3 to 12. The UK rose from -0.30% to 6.12% and now beats the FTSE 100; its cash years fell from 17 to 5. Switzerland fell from 5.64% to 4.83% and still beats the SMI; its cash years rose from 9 to 12. Germany and Switzerland had no last-traded-price exits, so their moves come from the changes to the universe and selection, not from exits. The UK had 2 of the 28 last-traded-price exits. Every other market moved by 0.6 percentage points of CAGR or less. The count of markets beating their index is 10 of 15 before and after, with Germany and the UK trading places.

## Academic Basis

- Sloan (1996): Low-accrual firms outperform high-accrual firms by ~10% annually
- Richardson et al. (2005): Accrual reliability predicts earnings persistence
- Dechow (1994): Cash flows better measure firm performance than accruals

## Data Tables

| Table | Columns Used |
|-------|-------------|
| `cash_flow_statement` (FY) | `freeCashFlow`, `reportedCurrency` (the pair test) |
| `income_statement` (FY) | `netIncome`, `revenue`, `reportedCurrency` (the pair test) |
| `key_metrics` (FY) | `returnOnEquity`, `marketCap` |
| `financial_ratios` (FY) | `operatingProfitMargin` |
| `stock_eod` | `adjClose`, `close` and `volume` after each rebalance date, and the last traded price for exits |
| `income_statement`, `stock_prices_non_split_adjusted_daily`, `splits_calendar` | the market-cap check and the split flags (data checks M and F) |
| `profile` | `exchange`, `country` (XETRA and SIX rule), `currency` (LSE rule, dedup tie-break), `isin` and `isActivelyTrading` (dedup), `companyName`, `isFund`, `isEtf`, `isAdr` and `cik` (universe rules `ur-3`), `sector` (`--exclude-sector`) |

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

A run goes to the latest close every market in it traded, with the last year partial (METHODOLOGY.md, "Run Window"). `--end-year 2025` stops at July 2025, the committed files' window. The committed files are the October 2026 run's, made before the current code, so no command here reproduces them. A rerun for new figures gives every market one `--end-date` (`scripts/data_end.py --legs-file scripts/gate_legs/wave1.json --topic fcf-conversion` prints the common date), so the comparison file holds one window: `rebuild_comparison.py` refuses markets that end on different dates.

```bash
# US (default universe: NYSE + NASDAQ + AMEX), to the latest close
python3 fcf-conversion/backtest.py --verbose

# One market to a pinned end date, written to its results file
python3 fcf-conversion/backtest.py --end-date YYYY-MM-DD --exchange NSE --output fcf-conversion/results/returns_NSE.json

# One market to July 2025
python3 fcf-conversion/backtest.py --end-year 2025 --exchange NSE

# All 16 published markets on one end date, then fold them into exchange_comparison.json
for ex in NYSE,NASDAQ,AMEX NSE XETRA STO TSX SHZ,SHH HKSE JPX LSE ASX KSC SAO SIX TAI SET SAU; do
  python3 fcf-conversion/backtest.py --end-date YYYY-MM-DD --exchange "$ex" --output "fcf-conversion/results/returns_${ex//,/_}.json"
done
python3 scripts/rebuild_comparison.py fcf-conversion

# The US without financials (sector_exclusion in the file), outside the comparison file
python3 fcf-conversion/backtest.py --end-date YYYY-MM-DD --exchange NYSE,NASDAQ,AMEX --exclude-sector "Financial Services" --output fcf-conversion/results/diagnostics/financials_out_NYSE_NASDAQ_AMEX.json

# Every booked name per invested year (holdings_full), for review
python3 fcf-conversion/backtest.py --end-year 2025 --exchange LSE --full-holdings

# Screen current stocks (TTM data)
python3 fcf-conversion/screen.py

# Generate charts from the results files
python3 fcf-conversion/generate_charts.py
```

- `--exit-method drop` drops a holding with no exit price, where `ltp` exits it at its last traded price.
- `--data-checks on` is the default. `--data-checks uncapped` turns every data check off and sets no cap either: an attribution arm only, never published.
- `--no-costs` charges nothing and writes `"cost_model": null` in the results. A costed run writes `"cost_model": {"function": "costs.tiered_cost", "fx_per_usd": <the leg's rate>}`.
- Use `--exchange` as above, not `--preset`: the Canada and Taiwan presets add TSXV and TWO to the universe.
- `--global` runs these 16 markets on one end date and has no Singapore leg; no Singapore results are committed.
- `screen.py` reads TTM rows. It runs ur-3's removals and leaves out the lines ur-3's line starts and line ends keep out of a screen on the day it runs, but not the listing rules, the dedup, the class twins, the statement window or the data checks.
- `generate_charts.py` draws a run-window file's partial year as its last, hatched bar and takes each title's window from the file.
- Each run reads the live data warehouse, so a rerun can differ slightly from the committed results.

## Known limits

The committed results predate the current code, so these describe the code; the October books are cited as evidence.

- Survivorship and the 45-day lag from the fiscal year end apply here as in the other rerun topics: METHODOLOGY.md (Limitations).
- **The size floor reads a reporting-currency cap.** The floor tests the fiscal-year `key_metrics` market cap, in the company's reporting currency, against a threshold in the listing's currency (`get_mktcap_threshold`). At rough fixed rates, 22 of the October books' line-years would sit under the floor once the cap is converted. Universe rules ur-2, and so ur-3, remove 9 of them (US ADRs), and the other 13 are UK 10 (EVOK.L, BGEO.L in Georgian lari, PLUS.L, GKP.L, TBLD.L, TKO.L), US 2 (CAE 2000, and JP 2016, which ur-3's hand list now removes) and Australia 1 (SNZ.AX 2015). That counts one side only: names the raw cap wrongly kept out can't be priced offline. DATA_QUALITY_ISSUES.md has the count across topics.
- **The cost tiers' residual.** The breakpoints convert at the listing exchange's rate, while the cap they're compared with is in the reporting currency, so Hong Kong's CNY reporters are tiered at 7.8 for a currency near 7.25, and the UK's USD reporters at 0.79. The rates are fixed, one per exchange for every year.
- **Ordinary shares listed first elsewhere stay, disclosed.** They're ordinary shares at their own prices: Manulife's 0945.HK (the Hong Kong books of 2003 to 2008), Yancoal Australia's 3668.HK (2019, 2022 and 2023) and 20 US line-years of Canadian banks' and insurers' NYSE lines (RY 5, TD 4, BMO 3, SLF 3, MFC 3, CM 2). The depositary receipts listed abroad that the October books held, Fast Retailing's 6288.HK (Hong Kong 2022) and Arion banki's ARION-SDB.ST (Sweden 2022), and Sampo's SAMPO-SDB.ST, which the November gate's 2025 Sweden book held, are on ur-3's hand list and leave. Other receipts outside NYSE, NASDAQ, AMEX and SAO (TDRs, lines flagged `isAdr`) stay until a book holds one.
- **Share classes stay.** They have their own ISINs, so a company's two classes can both be held outside SAO, and Avg stocks counts lines, not companies. China's B shares stay too, and so do Investec's two lines (INVP.L and INVR.L, its two listed parents), held together in the UK books of 2008, 2009, 2016, 2017 and 2019.
- **TTB-F.BK stays.** TMB's Thai foreign-board line was the bank's only line in the 2015 Thailand book. Check N keys NVDRs to their ordinary lines, not foreign-board lines.
- **The rules read today's profile.** CAM in the 2015 US book was Cameron International; today's profile files the reused ticker as a municipal ETF, so the fund flag removes it. That year's screen passed 30 names, so the slot goes to the next one. JP, an ADR the profile doesn't flag, leaves only through its hand-list entry.
- **BCE's three TSX lines** carry one set of financials. ur-3 keeps BCE-PY.TO, BCE's common price series through 2025, and removes the preferred BCE-PA.TO. BCE-PY.TO prints a preferred's price in 2026. The October TSX books held it in 9 years, up to the 2024 rebalance. Gate 15 of `scripts/verify_floor_guard.py` refuses a committed results file that holds BCE-PY.TO in a period exiting on or after 2026-01-01.
- **Unpriced names.** A screened name with no usable entry price takes its top-30 slot and isn't replaced; each period's `entry_unpriced` lists them.
- The entry floor of 1.00 is in local units: one Hong Kong dollar, one penny on the LSE, one baht.

## Distinction from fcf-yield

| | FCF Conversion (this) | [FCF Yield](../fcf-yield/) |
|---|---|---|
| Formula | FCF / Net Income | FCF / Market Cap |
| Question | Is the cash real? | Is it cheap? |
| Signal type | Quality | Valuation |
| Academic root | Sloan (1996) accrual anomaly | Gray & Vogel (2012) |
