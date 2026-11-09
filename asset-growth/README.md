# Asset Growth Anomaly

Companies that grow their assets slowly tend to outperform those expanding aggressively. This strategy screens for quality companies with low total asset growth.

## Academic Basis

Cooper, Gulen & Schill (2008), "Asset Growth and the Cross-Section of Stock Returns", *Journal of Finance* 63(4), 1609-1651. Found that firms in the lowest asset growth decile outperformed the highest decile by ~20% annually over 1968-2003.

## Signal

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| Asset Growth (YoY) | -20% to +10% | Low growth, not distressed |
| Return on Equity | > 8% | Profitable |
| Return on Assets | > 5% | Capital efficient |
| Operating Profit Margin | > 10% | Operationally strong |
| Market Cap | > $500M in the US; elsewhere half the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`), in local currency (₹10B on NSE, ¥50B on JPX, €250M on XETRA) | Liquid, investable |

**Sorting:** Lowest asset growth first (ascending). Top 30.

**Asset Growth** = (Total Assets current FY minus Total Assets prior FY) / Total Assets prior FY. The prior FY is the company's own previous fiscal year: its balance sheet 300 to 430 days before the current one, in the same reporting currency.

On each rebalance date, for every ordinary share on the market's exchanges:

1. Take the company's latest fiscal-year `balance_sheet`, `key_metrics` and `financial_ratios` rows whose fiscal year ended at least 45 days and less than 410 days before the rebalance date.
2. Pair the balance sheet with the company's prior fiscal-year row: the latest one 300 to 430 days before it, in the same `reportedCurrency`. A company with none isn't ranked that year: a gap of more than 430 days (a skipped year or a moved year-end) or a switch of reporting currency leaves it out.
3. Keep the company if its asset growth is inside -20% to +10% and it passes the other four filters above.
4. Keep one line per security (ISIN; a Thai NVDR shares its ordinary line's).
5. Rank by asset growth, lowest first, and hold the top 30 at equal weight.

Each results file counts the lines step 2 leaves out among those that pass the other filters, before the one-line-per-security step, per rebalance date and by reason (`currency_switch`, `no_prior_in_window`), under `prior_year_guard` ([METHODOLOGY.md](../METHODOLOGY.md#filing-and-selection-rules-rerun-topics), Filing and Selection Rules).

**Ordinary shares only** (universe rules `ur-2`, [METHODOLOGY.md](../METHODOLOGY.md#universe-rules)). Preferred and preference shares, notes, bonds, units, warrants, rights, funds and second lines at their own price on a company's financials never enter the universe. On SAO a company holds one line, its ordinary share (ISIN class NO); where the ordinary line has no entry price, its priced preferred or unit line stands in, as an NVDR does for a Thai ordinary share. Many Brazilian ordinary lines have no prices in the warehouse before 2026, so the stand-ins keep those companies in the book, and `universe_rules.class_stand_ins` lists each one by rebalance date. A company with no ordinary line, a BDR and a fund stay out. A book also holds only what its market's main index can hold: no ADRs, royalty trusts or LPs on NYSE, NASDAQ and AMEX (the S&P 500 holds none), and no BDRs on SAO. A hand list takes out lines no pattern sees: the HKEX pilot-programme lines of US companies (4333.HK, 4335.HK, 4338.HK and three more), five KSC lines that repeat their company's KOSDAQ series, GUD.AX (since GUD Holdings became Amotiv in 2024, AOV.AX carries the same closes), and others such as SCHP.SW (a participation certificate), MAN3.DE (a preference line with no ISIN), PBR-A (a preferred ADR), BASA.DE (a second BASF line) and four US ADRs the profile doesn't flag (AZN, RDS-B, WIMI, JHX). Schindler and MAN leave the Swiss and German books with SCHP.SW and MAN3.DE: SCHN.SW has bars only from April 2026, and MAN.DE has none. A keep list holds ordinary shares that a pattern or a flag misreads, such as JUP.L (flagged as a fund) and BCE-PY.TO (BCE's common price series to 2025). Trust units and LPs outside the US, REITs, share classes and Thai NVDRs stay. Every results file records the rules and what they removed under `universe_rules`.

## Parameters

- **Rebalancing:** Annual, on July 1, executed at the next available close, from July 2000 to the latest close every market in the run traded, with the last year partial (METHODOLOGY.md, "Run Window"). `--end-year 2025` gives the published window, 25 periods from July 2000 to July 2025.
- **Min stocks:** 10 (holds cash if fewer qualify, or if fewer than 10 have a usable entry price). A cash year earns 0%. The floor is checked at entry only. Under `--exit-method drop`, a name with no exit price is left out of that year's average and not replaced, so an invested year can hold fewer than 10.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Data checks:** No return cap. The data checks (`dc-1`, [METHODOLOGY.md](../METHODOLOGY.md#data-checks)) move holiday rows, zero-volume rows of lines that traded, stale lines with no volume, a thin share class's bar off its sibling's scale and non-positive prices out of each price window, hold a Thai NVDR line and its ordinary line once, test the market-cap floor on a cap checked against later reverse splits, and flag adjustment-factor steps, splits and every return above +100% or below -50% for review. The October 2026 results below were made before them, with a 200% cap on a single stock's annual return. The cap left a capped name out of that year's average without replacing it: Brazil's 2007 book had 10 buyable names and averaged 9.
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit (0.2% to 1.0% round trip). The tier thresholds are in US dollars and market caps aren't converted, so on NSE, JPX and KSC, where the market-cap floor is at least 10 billion local units (₹10B, ¥50B, ₩250B), every holding pays the 0.1% tier.
- **Data lag:** 45 days after the fiscal year end (point-in-time). A fiscal year that ended 410 days or more before the rebalance isn't used.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so a company's two classes can both be held. The October 2026 books, made before the universe rules, also held preference lines beside their ordinary lines: Germany's held both lines of at least one of Henkel (HEN.DE, HEN3.DE), Fuchs (FPE.DE, FPE3.DE), Sartorius, Biotest or BMW in 17 of its 18 invested years, and Brazil's held COCE5.SA and COCE6.SA together in five.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. Saudi Arabia has no local index configured and is measured against the S&P 500 in USD. China (SHZ + SHH) is measured against the SSE Composite.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 10.5% Brazil).

## Results (October 2026 run, July 2000 to July 2025)

These are the October 2026 run's results. They were made before the data checks, the universe rules and the prior-year guard, so the current code doesn't reproduce them, and a rerun on it will replace them.

The strategy beat its own market's index in 12 of 15 markets. Germany, Australia and Thailand trailed. Saudi Arabia is left out of the count because its benchmark is the S&P 500 in USD; it trailed that by 5.19pp.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| India (NSE) | Sensex | 13.42% | 12.06% | +1.37 | 0.286 | -20.43% | 5 (2000-2004) | 22.4 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 12.97% | 7.85% | +5.12 | 0.691 | -29.07% | 0 | 25.9 |
| UK (LSE) | FTSE 100 | 9.38% | 1.23% | +8.15 | 0.333 | -21.88% | 1 (2001) | 20.6 |
| Brazil (SAO) | Bovespa | 8.73% | 8.70% | +0.03 | -0.124 | -15.78% | 7 (2000-2006) | 13.9 |
| Canada (TSX) | TSX Composite | 8.36% | 3.95% | +4.41 | 0.375 | -31.51% | 3 (2000, 2002-2003) | 24.0 |
| Japan (JPX) | Nikkei 225 | 7.34% | 3.31% | +4.03 | 0.389 | -27.17% | 5 (2000-2004) | 28.2 |
| Hong Kong (HKSE) | Hang Seng | 6.08% | 1.64% | +4.44 | 0.165 | -26.63% | 2 (2000, 2002) | 19.3 |
| Sweden (STO) | OMX Stockholm 30 | 5.88% | 2.55% | +3.32 | 0.241 | -36.52% | 7 (2000-2004, 2006, 2009) | 20.2 |
| Taiwan (TAI) | TAIEX | 5.72% | 4.09% | +1.64 | 0.404 | -10.94% | 8 (2000-2007) | 28.4 |
| Korea (KSC) | KOSPI | 5.57% | 5.35% | +0.21 | 0.193 | -22.01% | 10 (2000-2009) | 23.8 |
| Switzerland (SIX) | SMI | 5.51% | 1.74% | +3.77 | 0.314 | -35.82% | 5 (2000-2004) | 20.8 |
| China (SHZ + SHH) | SSE Composite | 5.25% | 2.43% | +2.82 | 0.084 | -64.17% | 0 | 19.9 |
| Thailand (SET) | SET Index | 3.49% | 5.13% | -1.64 | 0.059 | -32.71% | 5 (2000-2004) | 22.1 |
| Germany (XETRA) | DAX | 2.69% | 5.04% | -2.35 | 0.039 | -48.37% | 7 (2000-2006) | 20.7 |
| Saudi Arabia (SAU) | S&P 500 (USD) | 2.67% | 7.85% | -5.19 | -0.053 | -38.96% | 9 (2000-2008) | 23.1 |
| Australia (ASX) | ASX 200 | 2.17% | 3.89% | -1.72 | -0.101 | -44.51% | 7 (2000-2006) | 21.8 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their July rebalance.

- **US:** 12.97% a year against 7.85% for SPY, with a Sharpe of 0.691 against 0.352, a max drawdown of -29.07% against -38.01%, and no cash years.
- **Thin leads:** Brazil beats the Bovespa by 0.03pp with a lower Sharpe (-0.124 against -0.075), and Korea beats the KOSPI by 0.21pp, invested only from the 2010 rebalance.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). Four markets are measured against a series that does too: the US and Saudi Arabia against SPY, Germany against the DAX, Brazil against the Bovespa. The other twelve are measured against price indices, so their excess is overstated by roughly the index's dividend yield. See METHODOLOGY.md (Benchmark Limitations).
- **Cash years:** a year is in cash when fewer than 10 names pass the screen, or fewer than 10 of those have a usable entry price. Every cash year is a 2009 or earlier rebalance. The US and China were invested in all 25 years.
- **Sharpe:** Brazil's is negative at an 8.73% CAGR because its risk-free rate is 10.5%.

## Rerun October 2026

These results were rerun with three changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias):

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 26 holdings in 11 markets exited this way; each results file lists them under its `ltp` key.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market. Share classes have their own ISINs and can both be held, so Germany's ordinary and preference pairs (HEN.DE and HEN3.DE, for example) aren't a dedup failure.

The listing rules drive the large moves:

- **Germany** fell from 6.76% to 2.69% and now trails the DAX by 2.35pp, after leading it by 1.72pp. It's the one market whose verdict changed (13 of 15 beat before, 12 now). Cash years rose from 4 (2000-2003) to 7 (2000-2006): fewer than 10 Germany-domiciled companies passed the screen from 2000 to 2005, and in 2006 only 8 of the 11 that passed had a usable entry price.
- **UK** rose from 6.04% to 9.38%. Before, 12 of 25 rebalances sat in cash: the screen passed 30 names, but only 6 to 9 of them had a usable entry price. With pence lines only, that happens once (2001).
- **Switzerland** moved from 5.64% to 5.51%.
- The other 13 markets moved by -0.21pp to +0.09pp. Benchmark returns are unchanged.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market, as in the table below. A mixed leg such as `--exchange XETRA,LSE` or `--exchange LSE,NYSE` gets no listing rule.

A run goes to the latest close every market in it traded, with the last year partial (METHODOLOGY.md, "Run Window"). `--end-year 2025` gives the published July 2000 to July 2025 window. The committed files are the October 2026 run's, made before the data checks, the universe rules and the prior-year guard, so no command here reproduces them. A rerun for new figures gives every market one `--end-date` (`scripts/data_end.py --legs-file scripts/gate_legs/wave1.json --topic asset-growth` prints the common date), so the comparison file holds one window: `rebuild_comparison.py` refuses markets that end on different dates.

```bash
# US (default universe: NYSE + NASDAQ + AMEX), to the latest close
python3 asset-growth/backtest.py --verbose

# One market on the published window, written to its results file
python3 asset-growth/backtest.py --end-year 2025 --exchange XETRA --output asset-growth/results/returns_XETRA.json

# One market to a pinned end date (the date data_end.py prints)
python3 asset-growth/backtest.py --end-date YYYY-MM-DD --exchange XETRA --output asset-growth/results/returns_XETRA.json

# All 16 markets on one end date, plus exchange_comparison.json
python3 asset-growth/backtest.py --global --end-date YYYY-MM-DD --output asset-growth/results/exchange_comparison.json --verbose

# The old exit rule (doesn't reproduce old results: the listing rules, dedup, data checks, universe
# rules and prior-year guard still apply)
python3 asset-growth/backtest.py --end-year 2025 --exchange LSE --exit-method drop

# Every booked name per invested year (holdings_full), for review. --data-checks uncapped turns the data
# checks off (no cap either): an attribution arm only, never published
python3 asset-growth/backtest.py --end-year 2025 --exchange LSE --full-holdings

# Charts from results/exchange_comparison.json
python3 asset-growth/generate_charts.py

# Current screen
python3 asset-growth/screen.py
python3 asset-growth/screen.py --preset india
```

Per-market runs, each with `--end-year 2025` or one `--end-date`:

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `NSE` | `returns_NSE.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| Brazil | `SAO` | `returns_SAO.json` | |
| Canada | `TSX` | `returns_TSX.json` | |
| Japan | `JPX` | `returns_JPX.json` | |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| Sweden | `STO` | `returns_STO.json` | |
| Taiwan | `TAI` | `returns_TAI.json` | |
| Korea | `KSC` | `returns_KSC.json` | |
| Switzerland | `SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Thailand | `SET` | `returns_SET.json` | |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Saudi Arabia | `SAU` | `returns_SAU.json` | |
| Australia | `ASX` | `returns_ASX.json` | |

Use `--exchange` as shown. `--preset canada` adds TSXV and `--preset taiwan` adds TWO, so neither writes the TSX or TAI file's universe. `--global` runs these 16 markets on one end date and has no Singapore leg; no Singapore results are committed.

## Known limits

- Survivorship, the 45-day lag from the fiscal year end and a size floor read in the reporting currency apply here as in the other rerun topics: METHODOLOGY.md (Limitations) and DATA_QUALITY_ISSUES.md.
- **The ordinary-shares rule is a list of patterns plus a hand list and a keep list**, and it reads today's profile. A trust or LP that later converted carries its corporate name back, so the trust rule can't see Texas Pacific Land Trust's 2010 and 2011 line-years (a corporation since 2021). SATS's profile names the Strive preferred that took the ticker in 2026, so EchoStar's 2022 line is removed too. No rule sees KTH, NSS or plain-ticker notes such as TANNL, KKRS, DHCNL, OPINL, NYMTL, WHLRL and ADAMH.
- **Second lines of one company.** Renamed or legacy lines the hand list doesn't hold can put one company in a book twice: 76ID.L beside ROR.L, BC88.L, NTCO3.SA, AZZA3.SA, ABG.AX, SHA.DE, 9512.SR and 9511.SR. Two Stockholm `-PREF` lines that no pattern reads, CORE-PREF.ST and SDIP-PREF.ST, stay too. KOSDAQ companies whose only history before 2026 is a KSC line stay in the Korea leg (013030, 073490, 085370.KS). Share classes have their own ISINs, so both can be held, and Avg stocks counts lines, not companies.
- **BCE's three TSX lines** carry one set of financials. BCE-PY.TO is BCE's common price series through 2025 and prints a preferred's price in 2026, when BCE.TO's bars start, so a run past July 2025 that holds BCE isn't handled yet. DATA_QUALITY_ISSUES.md (Asset growth's data fixes) has this and the entries above.
- **Unpriced names.** A screened name with no usable entry price takes its top-30 slot and isn't replaced; each period's `entry_unpriced` lists them.
- The entry floor of 1.00 is in local units: one Hong Kong dollar, one penny on the LSE, one baht.

## Data Requirements

- `balance_sheet` (FY): totalAssets and reportedCurrency (the current year and the prior year 300 to 430 days before it, in one currency)
- `key_metrics` (FY): returnOnEquity, returnOnAssets, marketCap
- `financial_ratios` (FY): operatingProfitMargin
- `stock_eod`: adjClose, close and volume after each rebalance date, and the last traded price for exits
- `income_statement`, `stock_prices_non_split_adjusted_daily`, `splits_calendar`: the market-cap check and the split flags
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup), companyName, isFund, isEtf, isAdr, isin and cik (ordinary-shares rule)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest, from July 2000 to the latest close or to a typed end year |
| `screen.py` | Current stock screen. Growth reads the FY balance sheets with the backtest's row windows and currency test; ROE, ROA, margin and size read TTM rows. It keeps ordinary shares only (`ur-2`), but not the listing rules, the dedup or the data checks. It also excludes the Asset Management, Shell Companies and Closed-End Fund industries, which the backtest doesn't |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py` |
