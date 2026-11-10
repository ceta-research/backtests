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

**Ordinary shares only** (universe rules `ur-2`, [METHODOLOGY.md](../METHODOLOGY.md#universe-rules)). Preferred and preference shares, notes, bonds, units, warrants, rights, funds and second lines at their own price on a company's financials never enter the universe. On SAO a company holds one line, its ordinary share (ISIN class NO); where the ordinary line has no entry price, its priced preferred or unit line stands in, as an NVDR does for a Thai ordinary share. Many Brazilian ordinary lines have no prices in the warehouse before 2026, so the stand-ins keep those companies in the book, and `universe_rules.class_stand_ins` lists each one by rebalance date. A company with no ordinary line, a BDR and a fund stay out. A book also holds only what its market's main index can hold: no ADRs, royalty trusts or LPs on NYSE, NASDAQ and AMEX (the S&P 500 holds none), and no BDRs on SAO. A hand list takes out lines no pattern sees: the HKEX pilot-programme lines of US companies (4333.HK, 4335.HK, 4338.HK and three more), five KSC lines that repeat their company's KOSDAQ series, GUD.AX (since GUD Holdings became Amotiv in 2024, AOV.AX carries the same closes), and others such as SCHP.SW (a participation certificate), MAN3.DE (a preference line with no ISIN), PBR-A (a preferred ADR), BASA.DE (a second BASF line) and four US ADRs the profile doesn't flag (AZN, RDS-B, WIMI, JHX). Schindler and MAN leave the Swiss and German books with SCHP.SW and MAN3.DE: SCHN.SW has bars only from April 2026, and MAN.DE has none. A line whose earlier bars and financial rows copy another line's enters the universe on its first own day (`universe_rules.line_starts`): AZZA3.SA, Azzas 2154 since Arezzo's merger with Soma, carries ARZZ3.SA's closes up to 2024-08-26, so a screen before 2024-08-27 holds Arezzo once, as ARZZ3.SA. A keep list holds ordinary shares that a pattern or a flag misreads, such as JUP.L (flagged as a fund) and BCE-PY.TO (BCE's common price series to 2025). Trust units and LPs outside the US, REITs, share classes and Thai NVDRs stay. Every results file records the rules and what they removed under `universe_rules`.

## Parameters

- **Rebalancing:** Annual, on July 1, executed at the next available close, from July 2000 to the latest close every market in the run traded, with the last year partial (METHODOLOGY.md, "Run Window"). The published results run to 2026-11-09 (`--end-date 2026-11-09`): 26 full years and a partial year from July 2026. `--end-year 2025` stops at July 2025, 25 full years.
- **Min stocks:** 10 (holds cash if fewer qualify, or if fewer than 10 have a usable entry price). A cash year earns 0%. The floor is checked at entry only. Under `--exit-method drop`, a name with no exit price is left out of that year's average and not replaced, so an invested year can hold fewer than 10.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Data checks:** No return cap. The data checks (`dc-1`, [METHODOLOGY.md](../METHODOLOGY.md#data-checks)) move holiday rows, zero-volume rows of lines that traded, stale lines with no volume, a thin share class's bar off its sibling's scale and non-positive prices out of each price window, hold a Thai NVDR line and its ordinary line once, test the market-cap floor on a cap checked against later reverse splits, and flag adjustment-factor steps, splits and every return above +100% or below -50% for review (`data_checks.large_moves` lists them, and `ltp.large_moves` those of exits at the last traded price).
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit (0.2% to 1.0% round trip). The tier thresholds are in US dollars and market caps aren't converted, so on NSE, JPX and KSC, where the market-cap floor is at least 10 billion local units (₹10B, ¥50B, ₩250B), every holding pays the 0.1% tier.
- **Data lag:** 45 days after the fiscal year end (point-in-time). A fiscal year that ended 410 days or more before the rebalance isn't used.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so a company's two classes can both be held, except on SAO, where a company holds one line (Signal).
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. Saudi Arabia has no local index configured and is measured against the S&P 500 in USD. China (SHZ + SHH) is measured against the SSE Composite.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 10.5% Brazil).

## Results (July 2000 to 9 November 2026)

Every market runs to 2026-11-09 (`--end-date 2026-11-09`): 26 full years from the July 2000 rebalance and a partial year from 2026-07-01, 130 days or 36% of a year. CAGR, max drawdown and excess count the partial year. Sharpe divides by the full years' volatility (METHODOLOGY.md, "Run Window"). Each file's `partial_year` holds the partial year's returns.

The strategy beat its own market's index in 10 of 15 markets. Brazil, Korea, Thailand, Germany and Australia trailed. Saudi Arabia is left out of the count because its benchmark is the S&P 500 in USD; it trailed that by 6.06pp.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 14.24% | 8.23% | +6.01 | 0.695 | -35.51% | 0 | 26.6 |
| India (NSE) | Sensex | 12.56% | 11.28% | +1.28 | 0.238 | -20.43% | 5 (2000-2004) | 23.2 |
| Hong Kong (HKSE) | Hang Seng | 10.32% | 1.59% | +8.72 | 0.334 | -31.43% | 2 (2000, 2002) | 20.0 |
| UK (LSE) | FTSE 100 | 10.11% | 1.96% | +8.15 | 0.347 | -28.28% | 1 (2001) | 21.1 |
| Canada (TSX) | TSX Composite | 9.79% | 4.95% | +4.83 | 0.445 | -32.53% | 3 (2000, 2002-2003) | 24.5 |
| Japan (JPX) | Nikkei 225 | 7.91% | 4.68% | +3.24 | 0.426 | -27.17% | 5 (2000-2004) | 28.2 |
| Taiwan (TAI) | TAIEX | 7.07% | 6.31% | +0.75 | 0.417 | -10.94% | 8 (2000-2007) | 28.5 |
| Sweden (STO) | OMX Stockholm 30 | 6.74% | 3.48% | +3.26 | 0.283 | -36.52% | 7 (2000-2004, 2006, 2009) | 21.1 |
| Korea (KSC) | KOSPI | 6.40% | 7.04% | -0.64 | 0.215 | -22.33% | 10 (2000-2009) | 21.6 |
| China (SHZ + SHH) | SSE Composite | 5.95% | 2.53% | +3.43 | 0.099 | -64.21% | 0 | 20.4 |
| Brazil (SAO) | Bovespa | 5.91% | 9.12% | -3.21 | -0.429 | -12.04% | 12 (2000-2007, 2011, 2013, 2016, 2021) | 16.9 |
| Switzerland (SIX) | SMI | 5.55% | 2.50% | +3.04 | 0.306 | -36.28% | 5 (2000-2004) | 20.9 |
| Thailand (SET) | SET Index | 4.39% | 5.97% | -1.58 | 0.110 | -33.93% | 6 (2000-2005) | 22.5 |
| Germany (XETRA) | DAX | 2.95% | 5.19% | -2.25 | 0.055 | -48.61% | 7 (2000-2006) | 20.0 |
| Saudi Arabia (SAU) | S&P 500 (USD) | 2.17% | 8.23% | -6.06 | -0.071 | -38.96% | 9 (2000-2008) | 23.8 |
| Australia (ASX) | ASX 200 | 2.06% | 3.88% | -1.82 | -0.112 | -44.78% | 7 (2000-2006) | 21.9 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years count the 26 full years and are labelled by their July rebalance; every market was invested in the partial year.

- **US:** 14.24% a year against 8.23% for SPY, with a Sharpe of 0.695 against 0.379, a max drawdown of -35.51% against -38.01%, and no cash years.
- **Behind:** Korea trails the KOSPI by 0.64pp, invested only from the 2010 rebalance. Over the 2025 year the KOSPI rose 148.7% and the book 19.1%. Brazil trails the Bovespa by 3.21pp, invested in 14 of the 26 full years. In 41 of its holdings a preferred or unit line stood in for an unpriced ordinary line; `universe_rules.class_stand_ins` lists all 58 the screens picked, cash years included.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). Four markets are measured against a series that does too: the US and Saudi Arabia against SPY, Germany against the DAX, Brazil against the Bovespa. The other twelve are measured against price indices, so their excess is overstated by roughly the index's dividend yield. See METHODOLOGY.md (Benchmark Limitations).
- **Cash years:** a year is in cash when fewer than 10 names pass the screen, or fewer than 10 of those have a usable entry price. Every cash year is a 2009 or earlier rebalance except Brazil's 2011, 2013, 2016 and 2021. The US and China were invested in all 26 full years.
- **Sharpe:** Brazil's is negative at a 5.91% CAGR because its risk-free rate is 10.5%.

## Rerun November 2026

These results replace the October 2026 files. The screen's filters didn't change; four things around them did:

- **Data checks in place of the 200% cap.** A holding's return above +200% used to be dropped from its year's average. It's now kept, and the data checks remove the errors (METHODOLOGY.md, Data Checks). 35 held returns above +200% are kept across the 16 files. 30 are in the 2000-2024 rebalances: Hong Kong 6, China 5, the US 4, India 3, Korea 2, the UK 2, Sweden 2, Saudi Arabia 2, and Canada, Switzerland, Taiwan and Thailand 1 each. The other 5 are in the 2025 year: Taiwan 2, Hong Kong, Japan and Korea 1 each.
- **Ordinary shares only** (universe rules `ur-2`, Signal). Preferreds, notes, units, funds, US ADRs, US royalty trusts and LPs, BDRs and the hand-listed second lines leave the books, and on SAO a company holds one line.
- **The prior-year guard.** Asset growth compares the company's own previous fiscal year, 300 to 430 days earlier, in the same reporting currency (Signal).
- **The run window.** Every market runs to 2026-11-09, so the 2025 rebalance is a full year and a partial year from July 2026 follows.

What moved, for each market whose excess moved more than half a point. "Code" is the change on the same 25 years (the 2000-2024 rebalances); "Window" is what the 2025 year and the partial year add.

| Market | October 2026 | Same 25 years | To 2026-11-09 | Code | Window |
|--------|--------------|---------------|---------------|------|--------|
| Hong Kong | +4.44 | +7.96 | +8.72 | +3.52 | +0.76 |
| US | +5.12 | +6.50 | +6.01 | +1.38 | -0.49 |
| China (SHZ + SHH) | +2.82 | +3.56 | +3.43 | +0.74 | -0.13 |
| Brazil | +0.03 | -2.69 | -3.21 | -2.72 | -0.52 |
| Korea | +0.21 | +1.12 | -0.64 | +0.91 | -1.76 |
| Taiwan | +1.64 | +1.92 | +0.75 | +0.28 | -1.17 |
| Saudi Arabia | -5.19 | -4.77 | -6.06 | +0.42 | -1.29 |
| Japan | +4.03 | +4.16 | +3.24 | +0.13 | -0.92 |
| Switzerland | +3.77 | +3.66 | +3.04 | -0.11 | -0.62 |

- **Hong Kong** gains mostly on the code: the returns above +200% the cap had dropped (0027.HK in 2010, 1230.HK in 2012, 8005.HK in 2014, 3306.HK in 2017, 0336.HK in 2020, 1428.HK in 2024), and the US companies' pilot-programme lines are out. In the 2025 year the book rose 28.2% while the Hang Seng fell 4.8%.
- **The US** gains on AppLovin, iHeartMedia and Quidel, each above +200%, with the ADRs, royalty trusts and LPs out. In the 2025 year the book rose 6.8% and SPY 20.4%.
- **China** gains on five returns above +200%.
- **Brazil** goes from a tie to 3.21pp behind. With one line per company, 2007, 2011, 2013, 2016 and 2021 had 8 or 9 buyable lines, where the second lines had made 10 to 14, and they sit in cash. In 2016 Arezzo now counts once: AZZA3.SA's bars before 2024-08-27 copy ARZZ3.SA's (Signal).
- **Korea** goes from a near tie to 0.64pp behind. The code adds 0.91pp: the preferreds and the five KOSDAQ duplicates are out, and names from below the cut take their slots. The 2025 year takes 1.76pp: the KOSPI rose 148.7% and the book 19.1%.
- **Taiwan, Saudi Arabia, Japan and Switzerland** move on the 2025 year: TAIEX rose 107.0% against the book's 49.0%, SPY 20.4% against Saudi's -4.9%, the Nikkei 72.9% against 22.3%, the SMI 19.7% against 5.4%.
- Canada, India, the UK, Sweden, Germany, Australia and Thailand moved by 0.42pp or less. Korea and Brazil now trail, so 10 of 15 markets beat their index, against 12.

## Rerun October 2026

The October 2026 files, which the November rerun replaced, came from three changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias):

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

A run goes to the latest close every market in it traded, with the last year partial (METHODOLOGY.md, "Run Window"). The committed files are the runs pinned to `--end-date 2026-11-09`, and the same pin rebuilds them; it fixes the prices, not the vendor's later restatements. `--end-year 2025` stops at July 2025. A rerun for new figures gives every market one `--end-date` (`scripts/data_end.py --legs-file scripts/gate_legs/wave1.json --topic asset-growth` prints the common date), so the comparison file holds one window: `rebuild_comparison.py` refuses markets that end on different dates.

```bash
# US (default universe: NYSE + NASDAQ + AMEX), to the latest close
python3 asset-growth/backtest.py --verbose

# One market on the published window, written to its results file
python3 asset-growth/backtest.py --end-date 2026-11-09 --exchange XETRA --output asset-growth/results/returns_XETRA.json

# One market to July 2025
python3 asset-growth/backtest.py --end-year 2025 --exchange XETRA

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
- **Second lines of one company.** Renamed or legacy lines the hand list doesn't hold can put one company in a book twice: 76ID.L beside ROR.L, BC88.L, NTCO3.SA, ABG.AX, SHA.DE, 9512.SR and 9511.SR. Two Stockholm `-PREF` lines that no pattern reads, CORE-PREF.ST and SDIP-PREF.ST, stay too. Sweden's 2016 and 2020 books hold CORE-PREF.ST (booked returns of +6.02% and +6.74%, dividends included); without it Sweden reads 6.84% a year and +3.36pp, against a published 6.74% and +3.26pp. KOSDAQ companies whose only history before 2026 is a KSC line stay in the Korea leg (013030, 073490, 085370.KS). Share classes have their own ISINs, so both can be held, and Avg stocks counts lines, not companies.
- **PTNM's stale tail** (US, 2025 book). Pitanium listed in May 2025. Its bars repeat one close, 10.39, on the same monthly volume from November 2025 to February 2026, then show no volume until its delisting on 2026-07-16. Its exit reads that series at the last traded bar, 2026-02-09: +18.6% from the 8.76 entry. The data checks don't catch it, because the repeated bars carry volume. At a 0% return the US CAGR would read 14.21%, at a total loss 14.06%: at most 0.18pp of its +6.01pp lead. Data checks dc-2 catch it from the topic's next rerun (METHODOLOGY.md, check S): the repeated bars copy its last real trade, 2025-10-03 at 10.39, so the exit moves to that date at the same +18.6%.
- **013030.KS's 2024 exit.** The KSC feed for this KOSDAQ company stops on 2024-08-14, with a delisting row that day, so its July 2024 holding exits at that bar (26,900 against 26,300 at entry), not at the July 2025 close.
- **BCE's three TSX lines** carry one set of financials. BCE-PY.TO is BCE's common price series through 2025 and prints a preferred's price in 2026, when BCE.TO's bars start. The published books hold BCE-PY.TO up to the 2020 rebalance and no BCE line after it, so the 2026 prices don't reach them. A later run that holds BCE past July 2025 isn't handled yet. DATA_QUALITY_ISSUES.md (Asset growth's data fixes) has this and the entries above.
- **Adjusted prices the data checks flag but keep.** Check F flags a holding whose adjClose drifts from its close during the holding (`factor_flag`, with `band` among its `reasons`) and changes no return. Four flagged returns, on three lines, are adjustment errors in the price feed that move their leg by 0.15pp or more. Each figure below rebuilds the holder's return from traded prices, the cash the company paid and its ordinary dividends, and swaps it into the published period.
  - **KINV-B.ST** (Sweden, 2014 and 2020 books). Its adjClose applies dividend rows about 9.7 times Kinnevik's real payouts: on 2020-08-25 a SEK 67.55 row, where the real event was a SEK 7 redemption. It books +70.43% and +250.49%, against KINV-A.ST's -4.99% and +61.89% in the same books. At KINV-A's returns Sweden reads 6.33% a year and +2.85pp, against 6.74% and +3.26pp. The KINV-A figure is a lower bound: KINV-A's adjClose misses the 2021 Zalando distribution in kind (about SEK 166 a share, ex-date 2021-05-17). With that value reinvested in KINV-A at the ex-date close, both lines return +131.51% in 2020 and Sweden reads 6.51% and +3.03pp.
  - **DLAR.L** (UK, 2008 book). De La Rue's November 2008 return of cash paid 305p a share with a 25-for-39 consolidation set to keep the share price level. Both series misread it: the close steps up on 2008-11-17, the day the new shares started dealing, and adjClose books the cash as a dividend on top. It books +151.70%. Priced from the new shares at that day's open and close, with the cash and the ordinary dividends, the holding returned +1.57% to +5.38%, and the UK reads 9.76% to 9.77% a year and +7.80pp to +7.81pp, against 10.11% and +8.15pp.
  - **3003.SR** (Saudi Arabia, 2020 book). City Cement's 2020 capital cut cancelled one share in every 3.85 and paid SAR 10 per cancelled share. The close treats the cut as a reverse split and carries no cash; adjClose books SAR 10 on every share, where only the cancelled ones were paid. It books +205.32%. With the cash per share held and the ordinary dividends, the holding returned +58.91%, and Saudi Arabia reads 1.98% a year and -6.26pp to the S&P 500, against 2.17% and -6.06pp.
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
| `screen.py` | Current stock screen. Growth reads the FY balance sheets with the backtest's row windows and currency test; ROE, ROA, margin and size read TTM rows. It runs `ur-2`'s removals, but not the listing rules, the dedup, the class-twin stand-ins, the line starts (each listed line has started by today) or the data checks, so on SAO it can list a company's ordinary line beside its preferred line (PETR3.SA and PETR4.SA). It also excludes the Asset Management, Shell Companies and Closed-End Fund industries, which the backtest doesn't |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py` |
