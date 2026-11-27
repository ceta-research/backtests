# 52-Week High Proximity

Buys the 30 stocks trading closest to their 52-week high, every quarter, at equal weight. George & Hwang (2004) found that nearness to the 52-week high predicts returns and explains much of what is called price momentum. The proposed reason is anchoring: investors treat the old high as a ceiling and are slow to price good news as a stock approaches it.

## Academic Basis

- George, T. & Hwang, C. (2004). "The 52-Week High and Momentum Investing." *Journal of Finance*, 59(5), 2145-2176.
- Li, J. & Yu, J. (2012). "Investor Attention, Psychological Anchors, and Stock Return Predictability." *Journal of Financial Economics*, 104(2), 401-419.
- Jegadeesh, N. & Titman, S. (1993). "Returns to Buying Winners and Selling Losers." *Journal of Finance*, 48(1), 65-91.

## Signal

```
proximity_ratio = close / MAX(high over the 252 bars ending at the signal bar)
```

- **Signal bar:** the latest bar on or up to 10 days before the rebalance date with volume above 0. A line with no traded bar in those 10 days is not ranked. The 52-week high is the highest `high` over the 252 bars ending at the signal bar, untraded (zero or NULL volume) bars included, and a line needs at least 100 of them. A line on ur-4's lookback-clip list counts its bars from its first own day (Universe, below).
- **Price basis:** `close` and `high` are both split-adjusted and neither is dividend-adjusted, so the ratio compares like with like. A ratio of 1.0 means the stock closed at its 52-week high; 0.95 means 5% below it. The ratio has no cap.
- **Ranking:** highest ratio first. Exact ties at the cut (names sitting on their high, such as Chinese limit-up closes) go to the larger checked market cap, then to the symbol.

On each rebalance date, for every ordinary share on the market's exchanges:

1. Keep it if its checked market cap is above the market's floor: the cap on its latest `key_metrics` fiscal-year row readable on the rebalance date under lag rule `lr-1` (Filing lag, under Parameters), as data checks dc-3's check M reads it (divided where a later price scaling shows the vendor's cap sits on another basis, and failing where the row is ineligible). Check M reads every fiscal-year row over the floor, one with no return on equity included.
2. Compute the ratio as above.
3. Keep one line per security: one per ISIN, a Thai NVDR line keyed to its ordinary line (check N), and on SAO one line per company.
4. Take the 30 highest ratios and hold them at equal weight for the quarter.

Each results file counts the lines over the floor that step 2 leaves unranked, per rebalance date and by reason, under `signal_guard`: `no_traded_bar` (no traded bar in the 10 days) or `short_history` (fewer than 100 of the 252 bars), per line before step 3 ([METHODOLOGY.md](../METHODOLOGY.md#filing-and-selection-rules-rerun-topics), Filing and Selection Rules).

**Universe: ordinary shares only** (universe rules `ur-4`, [METHODOLOGY.md](../METHODOLOGY.md#universe-rules)). Preferred and preference shares, notes, units, warrants, rights, funds and second lines at their own price never enter the universe, and neither do ADRs, trusts and LPs on NYSE, NASDAQ and AMEX, BDRs on SAO, or J-REITs on JPX (the Nikkei 225 holds no REITs). The hand list adds lines no pattern reads: blank-check shells, Stockholm preference and D shares, funds the profile doesn't flag, receipts, and lines that print another line's bars. On SAO a company holds one line, its ordinary share, with its priced preferred or unit line standing in when the ordinary line has no entry price (`universe_rules.class_stand_ins`). A line whose earlier bars aren't its own listing's enters on its first own day (`universe_rules.line_starts`, 80 lines). For 45 of them the earlier bars are another company's, fund's or instrument's (1972.HK's are Swire Pacific's, 145990.KS's Samyang Holdings', HUSQ-B.ST's and HUSQ-A.ST's Electrolux's, HLM.JO's Tongaat-Hulett's, AIP.JO's Tiger Brands', 6888.KL's Telekom Malaysia's, VOD.JO's Telkom's and CALY's a fund's, GETY's and 29 de-SPAC lines' their blank-check shells' (CO's from 2009-07-01 its own OTC quotes), LUMI.ST's and five US lines' depositary receipts'), and the 252-bar window starts on the first own day too (`universe_rules.lookback_clip`). A renamed line keeps its old ticker's bars in its window. A line whose later rows aren't its listing's leaves after its last own day (`universe_rules.line_ends`): COG, CLR, PEAK and GLT, and BCE-PY.TO after 2025-09-30, when its bars turn into a preferred's. COG's last own day, 2021-10-01, is a rebalance date: that quarter's screen keeps COG, which has no bar after that day, so a pick of it has no entry price and its slot stays empty. Every results file records the rules and what they removed under `universe_rules`.

## Parameters

- **Rebalancing:** quarterly, on 1 January, April, July and October, executed at the next available close, from January 2000 to the latest close every market in the run traded, the last quarter partial and priced at that close (METHODOLOGY.md, "Run Window"). `--end-year 2025` stops at the 1 October 2025 exit: 103 quarters, January 2000 to July 2025, the committed files' grid.
- **Min stocks:** 10. The quarter is held in cash if fewer than 10 names pass the screen, or if fewer than 10 of them pass the entry test: a price of at least 1.00 in local units in the 10-day entry window. The entry test reads `adjClose`, which is back-adjusted for every later dividend, not the `close` the ratio uses. In an invested quarter, a name that fails the entry test is dropped from the book and its slot isn't refilled from lower ranks, so a book can hold fewer than 30 names. A cash quarter earns 0%. The floor is checked at entry only.
- **Max stocks:** 30, equal weight.
- **Size floor:** the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`), in local currency: $1B in the US, ₹20B on NSE, ¥100B on JPX, ₩500B on KSC, NT$10B on TAI, ฿10B on SET, €500M on XETRA and MIL, £500M on the LSE, A$500M on ASX, R$1B on SAO. It's read against the checked cap (check M, step 1).
- **Filing lag:** lag rule `lr-1`. On the US exchanges a fiscal-year row is read from its earliest filing date within 180 days of its period end, and a row with no such date 105 days after it. Elsewhere a row waits its market's deadline for full-year results, counted from the period end: 60 days on NSE, KLS, ASX and SES; 90 on JPX, HKSE, KSC, TAI, TSX, SET, SAO and JNB; 120 on LSE, XETRA, STO, OSL, MIL, SIX, SHZ and SHH. Three markets carry an era with an earlier, longer deadline: HKSE 120 days for a period end before 31 December 2010, SES 90 before 31 December 2003 (a 12-month year begun before 2003) and ASX 75 before 30 June 2004. Each results file records the rule, its eras and its counts under `lag_rule`; the sources are in [METHODOLOGY.md](../METHODOLOGY.md#filing-lag) (Filing Lag).
- **Data checks:** no return cap. Data checks `dc-3` ([METHODOLOGY.md](../METHODOLOGY.md#data-checks)) clean each rebalance date's 12-day price window before the oscillation filter (check T: closed days, untraded and stale rows, the rebalance date's own bar included), hold a Thai NVDR and its ordinary line once (N), test the size floor and the tie-break on a checked cap (M), book reviewed rebuilds from the hand list (F), and list every return above +100% or below -50% (L). Each results file carries them under `data_checks`.
- **Transaction costs:** size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, on each booked name's checked cap. Only real trades are charged, each sized to the trade (the `trades` convention, METHODOLOGY.md, "Transaction Costs"): a name that enters pays on its whole weight, a name that leaves on the weight it drifted to, at the rate it last carried, and a name that stays on the gap between its drifted weight and the new equal weight. The trades are those of the names the run booked: a bought name it can't book pays no entry, a held one it can't book is charged as leaving, and under `--exit-method drop` the costs read the exit side as the returns do. There is no sale at the window's end; a quarter that goes to cash sells the book, and that sale is charged to the quarter before it. A market whose benchmark starts after its first invested quarter has its first buy-in on a quarter the metrics don't read, so its measured figures start with the book already held. A name with no cap pays the highest rate, 0.5%, as in fcf-conversion; the size floor leaves no booked name without one. The dollar breakpoints are converted to the leg's currency at a fixed rate per exchange (`costs.get_fx_per_usd`), recorded under `cost_model` with `"turnover": "trades"` (`null` on a `--no-costs` run). With `--full-holdings`, each booked name in `holdings_full` carries `cost_rate`, its one-way tier at that rebalance as a fraction: the rate charged on what it trades there, and on its sale at the next rebalance if it leaves.
- **Cost convention sensitivity:** the topics that charge through `costs.apply_costs` charge each booked name a full round trip every period, kept or not. `scripts/cost_conventions.py` reads a market's `--full-holdings` results file, recomputes each quarter from the listed names under the file's own convention (each must match the stored return to 0.000002), and prints the market's CAGR and excess under both conventions and the difference. The committed results predate this convention, so no figure is given here yet.
- **Exits:** a holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default), read from the warehouse with check T's rules (a stale copy of the last trade doesn't count as one), and check F's flags on those fills sit in the `ltp` block.
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so two classes of one company can both be held (see Known limits).
- **Benchmark:** each market's own index, in local currency. The US is measured against SPY. South Africa, Italy and Malaysia have no local index in the data and are measured against the S&P 500 (SPY) in USD. China (SHZ + SHH) is measured against the SSE Composite, Norway against the Oslo All Share (`^OSEAX`), Australia against the ASX 200 (`^AXJO`) and Brazil against the Bovespa (`^BVSP`).
- **Risk-free rate (Sharpe):** fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 0.5% Switzerland, 3.5% UK).

## Results (October 2026 run, January 2000 to October 2025)

These are the October 2026 run's results, and they predate the code above. That run had a 200% cap on a single stock's return and a 1.1 cap on the ratio, no data checks and no universe rules, the market cap's floor and tie-break on the unchecked cap of a fiscal-year row read 45 days after its period end, cost tiers in unconverted dollars, a full round trip charged on every booked name every quarter, a grid typed to end in 2025, and no Australia or Brazil leg. The current code doesn't reproduce them.

Of the 14 markets measured against their own index over the full window, the screen beat it in 10: Taiwan (+7.61pp a year), Hong Kong (+6.81), Korea (+4.84), Sweden (+4.80), Switzerland (+4.39), Thailand (+4.29), India (+3.00), the UK (+2.83), Canada (+2.16) and Singapore (+1.48). Germany ties the DAX (0.11pp behind, under the 0.2pp line). The US (-3.45pp against SPY), China (-0.93) and Japan (-0.84) trail. Norway beat the Oslo All Share by 3.60pp a year, measured 2013-2025 only, so it isn't ranked with them. Every market was invested in at least half of its quarters (Norway in all 50 of its measured quarters), so none is too thin to test.

Three benchmarks include dividends, as the portfolio's returns do: SPY (US), the DAX (Germany) and the Oslo All Share GR (Norway). Norway's +3.60pp is like for like; Germany's tie and the US shortfall are too. The other twelve local indices are price indices, so those margins are overstated by roughly the index's dividend yield. India's +3.00, the UK's +2.83, Canada's +2.16 and Singapore's +1.48 are small enough that they may not survive a total-return comparison, and Japan and China would trail by more.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash quarters | Avg names |
|--------|-----------|------|----------------|-------------|--------|--------|---------------|-----------|
| India (NSE) | Sensex | 14.12% | 11.12% | +3.00 | 0.336 | -57.08% | 17 (2000-01 to 2004-01) | 29.8 |
| Taiwan (TAI) | TAIEX | 11.99% | 4.38% | +7.61 | 0.670 | -47.69% | 21 (2000-01 to 2005-01) | 30.0 |
| South Africa (JNB) | S&P 500 in USD | 10.00% | 8.02% | +1.99 | 0.067 | -36.62% | 18 (2000-01 to 2004-04) | 26.7 |
| Korea (KSC) | KOSPI | 9.64% | 4.81% | +4.84 | 0.354 | -42.92% | 20 (2000-01 to 2004-10) | 28.8 |
| Hong Kong (HKSE) | Hang Seng | 8.58% | 1.77% | +6.81 | 0.267 | -52.83% | 2 (2000-01, 2000-04) | 27.6 |
| Malaysia (KLS) | S&P 500 in USD | 8.50% | 8.02% | +0.48 | 0.529 | -34.26% | 24 (2000-01 to 2005-10) | 26.1 |
| Thailand (SET) | SET Index | 8.05% | 3.76% | +4.29 | 0.326 | -44.90% | 17 (2000-01 to 2004-01) | 29.3 |
| Sweden (STO) | OMX Stockholm 30 | 7.98% | 3.17% | +4.80 | 0.284 | -52.29% | 2 (2000-01, 2000-04) | 28.2 |
| Canada (TSX) | TSX Composite | 7.24% | 5.08% | +2.16 | 0.367 | -34.94% | 0 | 29.8 |
| Italy (MIL) | S&P 500 in USD | 6.51% | 8.02% | -1.51 | 0.196 | -58.72% | 17 (2000-01 to 2004-01) | 27.6 |
| Switzerland (SIX) | SMI | 6.50% | 2.10% | +4.39 | 0.354 | -48.70% | 1 (2000-01) | 27.9 |
| Germany (XETRA) | DAX | 5.01% | 5.12% | -0.11 | 0.154 | -47.94% | 0 | 28.6 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 4.57% | 8.02% | -3.45 | 0.173 | -53.96% | 0 | 29.7 |
| UK (LSE) | FTSE 100 | 4.19% | 1.36% | +2.83 | 0.050 | -39.02% | 0 | 29.9 |
| Singapore (SES) | STI | 3.65% | 2.17% | +1.48 | 0.074 | -52.89% | 14 (2000-01 to 2002-04, 2003-04 to 2004-01) | 17.6 |
| China (SHZ + SHH) | SSE Composite | 3.26% | 4.19% | -0.93 | 0.025 | -72.47% | 0 | 29.3 |
| Japan (JPX) | Nikkei 225 | 2.56% | 3.40% | -0.84 | 0.157 | -59.03% | 2 (2000-01, 2000-04) | 30.0 |
| Norway (OSL), 2013-2025, short window, not ranked | Oslo All Share (`^OSEAX`) | 14.51% | 10.91% | +3.60 | 0.829 | -22.93% | 0 of 50 measured | 28.3 |

Returns are in local currency, after costs. Cash quarters are counted over the 103 rebalances and labelled by their rebalance month. Avg names is the average number of lines held in invested quarters; two lines of one company count as two. A figure below 30 means fewer than 30 names passed the screen, or the entry test (see Parameters) dropped some of them. The entry test dropped 40.2% of screened names in Singapore (17.6 held of 29.4 screened), 12.4% in Malaysia, 8.0% in Hong Kong, 7.1% in Italy and 2.3% in China. Norway's 28.3 is over its 50 measured quarters; the file's `avg_stocks_when_invested`, 24.4, averages all 74 invested rebalances of the run, including the 24 before its window.

- **US:** 4.57% a year against 8.02% for SPY, with a Sharpe of 0.173 against 0.361 and a maximum drawdown of -53.96% (2008-01-01 to 2009-07-01) against -43.86%. It was invested in all 103 quarters and beat SPY in 43.69% of them.
- **Norway** is measured from April 2013, when `^OSEAX` starts: 50 quarters, all invested. Its 29 cash quarters (2000-01 to 2006-01 and 2009-04 to 2010-01) fall before that and aren't in its figures. Its CAGR covers 12.5 years, not 25.8, so it doesn't rank against the other rows.
- **South Africa, Italy, Malaysia** set a local-currency return against a USD index, so their excess mixes in the currency move and isn't a verdict either way.
- **Cash quarters** are early: apart from Norway's, which fall before its measured window, the last one in any market is 2005-10 (Malaysia). In every market but three, all of them are quarters when fewer than 10 names passed the screen. Italy's 2003-04 to 2004-01 (8 or 9 of 10 to 12 names buyable), Malaysia's 2005-10 (8 of 12) and Singapore's 2001-04 to 2002-04 and 2003-04 to 2004-01 (5 to 9 of 11 to 21) are quarters when 10 or more passed but fewer than 10 passed the entry test (see Parameters).
- **Singapore** holds 17.6 names on average in its 89 invested quarters, though 29.4 pass the screen. The entry test drops 1,053 of its 2,620 screened names (40.2%), at 35% to 48% in each five-year stretch, and the dropped slots aren't refilled (2024-10: 30 screened, 16 held). The test reads the dividend-adjusted price, so how many of those names had a printed close under S$1 isn't determined from these files.
- **China's drawdown**, -72.47%, runs from October 2007 to January 2025: the book did not regain its October 2007 value for 69 quarters. The SSE Composite's own maximum is -66.96%.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets, because the risk-free rate differs (6.5% in India, 0.1% in Japan).

## Rerun October 2026

These results replace a run from June 2026. The whole topic was rerun with the campaign's changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias), and four changes to this screen:

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the quarter. 176 holdings in 16 markets exited this way (62 in the US); 8 had no bar to exit at. Each results file lists them under its `ltp` key. On identical selection the exit rule moves CAGR by -0.21pp (Japan) to +0.05pp (US).
- **Listing rules and same-ISIN dedup**, as under Parameters. The dedup runs before the top-30 cut, so a removed twin frees its slot.
- **Entry-side cash rule.** A quarter is cash when fewer than 10 screened names pass the entry test (an entry `adjClose` of at least 1.00, see Parameters). The June run decided cash on the screen count alone.
- **Price basis of the ratio.** The June run divided `adjClose` by the 52-week high. `adjClose` is back-adjusted for every later dividend, so a company scored lower the more it paid out after the rebalance date: the ranking used future dividends and shut dividend payers out. The ratio is now `close` over the high, both on the same basis. Before the rerun this was measured to change 45.4% of book slots across the 18 markets, from 4.9% in Norway to 76.8% in the US.
- **Signal bar.** The June run took the latest bar up to 5 days after the rebalance date, while entry is the first close after it, so the first days of a rise counted towards both the ranking and the return. The signal bar is now the last traded bar in the 10 days up to the rebalance date. Lines that didn't trade (fund share classes, suspended stocks) had sat on their high and won ties; the traded-bar rule changes 4.2% of holdings.
- **Ties** at the cut go to the larger market cap, then the symbol.

The selection changes drive the large moves, and no run separates them. The US fell from 12.52% to 4.57% (from 4.51pp ahead of SPY to 3.45pp behind), Japan from 8.43% to 2.56% (from 5.03pp ahead of the Nikkei to 0.84pp behind), India from 18.96% to 14.12%, Germany from 8.23% to 5.01% (from 3.11pp ahead of the DAX to a tie), Korea from 12.07% to 9.64% and Thailand from 10.47% to 8.05%. China rose from -2.25% to 3.26%, Taiwan from 9.18% to 11.99%, Switzerland from 4.47% to 6.50% and Malaysia from 6.42% to 8.50%. Cash quarters rose by 2 in Korea and 3 in Thailand, and by 4 in Italy, 9 in Singapore and 1 in Malaysia (all from the entry-side rule); Switzerland's fell by 2. Benchmark returns are unchanged in all 18 markets.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

A run goes to the latest close every market in it traded, with the last quarter partial (METHODOLOGY.md, "Run Window"). `--end-year 2025` stops at the October 2025 exit, the committed files' grid. The committed files are the October 2026 run's, made one market at a time before the current code, so no command here reproduces them. A rerun for new figures gives every market one `--end-date` (`scripts/data_end.py --legs-file scripts/gate_legs/wave3.json --topic 52-week-high` prints the common date), so the comparison file holds one window: `rebuild_comparison.py` refuses markets that end on different dates.

```bash
# US (NYSE + NASDAQ + AMEX), to the latest close
python3 52-week-high/backtest.py --preset us --verbose

# One market to a pinned end date, written to its results file. --full-holdings: gate 15 of scripts/verify_floor_guard.py recomputes each row from the lists, and refuses a results file without them
python3 52-week-high/backtest.py --end-date YYYY-MM-DD --exchange XETRA --full-holdings --output 52-week-high/results/returns_XETRA.json
python3 52-week-high/backtest.py --end-date YYYY-MM-DD --preset us --full-holdings --output 52-week-high/results/returns_NYSE_NASDAQ_AMEX.json

# What a market returns under each cost convention: a --full-holdings run's file, which lists every booked name and its cost rate (here on the committed files' grid, in the untracked root results/), then the script
python3 52-week-high/backtest.py --end-year 2025 --exchange SAO --full-holdings --output results/52wh_SAO_full.json
python3 scripts/cost_conventions.py results/52wh_SAO_full.json

# After all 20: rebuild the comparison file the charts read, then check it against the per-market files
python3 scripts/rebuild_comparison.py 52-week-high
python3 scripts/rebuild_comparison.py 52-week-high --check

# Charts from results/exchange_comparison.json, written to 52-week-high/charts/
python3 52-week-high/generate_charts.py

# The old exit rule
python3 52-week-high/backtest.py --exchange LSE --exit-method drop

# The 30 names a rebalance today would pick, before the entry test and cash rule; run it after the market's close
python3 52-week-high/screen.py --preset us
```

| Market | Arguments | Output file | Listing rule |
|--------|-----------|-------------|--------------|
| US | `--preset us` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `--exchange NSE` | `returns_NSE.json` | |
| UK | `--exchange LSE` | `returns_LSE.json` | GBp lines only |
| Germany | `--exchange XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Japan | `--exchange JPX` | `returns_JPX.json` | |
| China | `--exchange SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Hong Kong | `--exchange HKSE` | `returns_HKSE.json` | |
| Korea | `--exchange KSC` | `returns_KSC.json` | |
| Taiwan | `--exchange TAI` | `returns_TAI.json` | |
| Canada | `--exchange TSX` | `returns_TSX.json` | |
| Switzerland | `--exchange SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| Sweden | `--exchange STO` | `returns_STO.json` | |
| Thailand | `--exchange SET` | `returns_SET.json` | |
| South Africa | `--exchange JNB` | `returns_JNB.json` | |
| Norway | `--exchange OSL` | `returns_OSL.json` | |
| Italy | `--exchange MIL` | `returns_MIL.json` | |
| Malaysia | `--exchange KLS` | `returns_KLS.json` | |
| Singapore | `--exchange SES` | `returns_SES.json` | |
| Australia | `--exchange ASX` | `returns_ASX.json` | |
| Brazil | `--exchange SAO` | `returns_SAO.json` | |

The arguments set the universe name written into the file: `--preset us` writes `US_MAJOR`, as the committed file has. `--preset taiwan` adds TWO, so it doesn't reproduce the TAI file. `--preset canada` adds TSXV, and `--preset mexico` and `--preset israel` run exchanges lag rule `lr-1` doesn't list, so the current code refuses all three.

`--global` loops over the same 20 exchange sets, on one end date for all of them, and writes `returns_<exchanges>.json` next to the `--output` path, plus a comparison file. It isn't how these files were made: its US file carries the universe name `NYSE_NASDAQ_AMEX`, and a market that fails inside the loop is recorded as an error entry while its old file stays on disk. With `--full-holdings` its comparison file also holds every `holdings_full` list, which `rebuild_comparison.py` leaves to the per-market files, so rebuild it. Run the `--check` above after any rerun.

## Known limits

- **Survivorship.** The price data holds almost no company that stopped trading before 2015 (US) or 2020 (other markets), so the earlier years are measured on companies that survived at least that long. Last-traded-price exits can only capture companies that stopped trading after those dates. See METHODOLOGY.md (Limitations: Survivorship Bias).
- **Not only ordinary shares, in the October books.** That run had no instrument filter, so income lines (funds, preferred shares, notes, REITs), which sit near their 52-week high much of the time, and Thai NVDR lines beside their ordinary lines took slots. Its period records list the first 10 names held, in rank order. Among those, by ticker pattern and name (not by a fund flag in the data), the share of slots held by funds, ETFs, preferred or note lines, warrants and NVDRs is: Thailand 27.4% (NVDR lines 216 of 860, infrastructure and property funds 20), Korea 20.7% (preferred lines, 172 of 830), the UK 20.4% (investment trusts 182 of 1,030, preference shares 28), Canada 19.4% (preferred lines 185 of 1,030, closed-end funds and bullion trusts 15), the US 9.1% (closed-end funds 46 of 1,030, notes and debentures 26, preferred lines 19, warrants 3) and Germany 8.9% (preference shares, 92 of 1,030). REIT and trust units, not counted there, are 16.4% of Singapore's recorded slots. The current code removes the preferred, preference, fund, note and warrant lines ur-4 names, holds an NVDR with its ordinary line once, and lists every booked name; REITs and their units outside Japan stay (ur-4's keep list).
- **Two lines of one company.** One line per ISIN isn't one line per company. Among the October books' recorded names, at least one company has two lines in the book in 81 quarters in Sweden (A and B shares), 73 in Thailand (ordinary and NVDR), 50 in Korea (ordinary and preferred, or two preferred series), 40 in Canada, 37 in Germany (HEN.DE with HEN3.DE in 20 of them) and 28 in Switzerland (Roche's RO.SW with ROG.SW in 24, Swatch's UHR.SW with UHRN.SW in 16). Under the current code share classes still stay, so Sweden's A and B shares and Swatch's two lines can both be held. Avg names counts lines.
- **Size floor.** The floor compares the checked `key_metrics` market cap, in the company's reporting currency, with a floor set in the listing currency, unconverted, and takes the latest row readable under `lr-1` with no upper age limit.
- **Filing lag.** Month deadlines are stored as days from a 31 December year end, so `screen.py` run on a day other than the 1st of a month can read a row up to 3 days early. ASX's two months are 60 days, and its usual 30 June year end is due at 62, so `screen.py` run on 29 or 30 August reads such a row before its deadline. On four legs some companies have a longer deadline than their market's count, and the vendor's exchange codes don't say which segment a line sits in, so their rows are read before that deadline:
  - AIM companies on the UK leg (six months);
  - Open Market companies on the Germany leg (six months on Scale, none for the rest);
  - foreign issuers on the Brazil leg (four months);
  - since September 2024, JSE general-segment companies on the South Africa leg (four months for the first required release). Read at 90 days, a year end in March, June, September or December moves to an earlier rebalance on this quarterly grid.

  The full list is in METHODOLOGY.md (Filing Lag).
- **Splits inside the lookback.** The ratio reads the vendor's split-adjusted close and high. A split the vendor left unadjusted inside a line's 252 bars puts the window's high on another price scale until the split leaves the window, so the line ranks on a ratio that is too low or too high for up to a year. In the October books 38 booked line-quarters ranked on such a window, China 16 and Taiwan 15, mostly bonus issues and stock dividends. No check cleans a lookback: the data checks and `scripts/split_check.py` read the held windows, and a hand-list entry rebuilds a held return, not a ranking.
- **Closed-day rows outside the windows.** Check T cleans the 12-day window from each rebalance date, so a closed-day copy on the rebalance date moves and the signal falls back to the last traded bar. A closed-day row anywhere else in the 252 bars stays and can set the high: rows on another price scale (CM.TO's and TIH.TO's on 2003-12-26, DB1.DE's on 2002-12-24) set those lines' highs for the year after. The signal bar sits in a cleaned window only when the market traded on the rebalance date. When it didn't, as on 1 January in most markets, the signal bar comes from the 10 days before, which check T doesn't clean, so a traded copy on a closed day there can be it.
- **The oscillation filter reads later bars.** `remove_price_oscillations` cleans the price cache before the screen. It flags a row using up to two bars after it, and its tier 2 counts a line's mild oscillations over the whole run, so the 252-bar frame at a rebalance date is cleaned with hindsight. On ASX and SAO the filter over the whole run removes frame rows that a filter cut at each rebalance date would keep: 4,087 distinct rows on ASX and 518 on SAO, mostly through tier 2. The held book changes by one name at 2 of 103 rebalances on ASX and at 9 of 103 on SAO, in both directions and by under 0.1pp a year. A plain cut at each date would rank on phantom holiday prints, so the filter stays. The other legs aren't measured yet.
- **Bars with no high.** The price fetch keeps only bars with a high above 0, and the same cache prices entries and exits, so a bar with no high is dropped: the signal then reads the traded bar before it, and an entry or an exit the next bar in its window. Nothing counts these drops yet.
- **China's B shares.** They are ordinary shares and stay, priced in US dollars (Shanghai) or Hong Kong dollars (Shenzhen) inside a leg measured in yuan against the SSE Composite, so their returns carry the currency move.
- **Lines listed first elsewhere.** A company's ordinary share listed on this market as well as its home one stays, at its local price: AZN.ST, ABB.ST, NDA-SE.ST, 0945.HK, AD.MI and FERG.L are among the October books' holdings.
- **Clipped starts.** A line on ur-4's lookback-clip list ranks on its own bars only, so it can't rank until it has 100 of them, about five months after its first own day. A renamed line keeps its old ticker's bars in its window, as do the two first listings off the list (BNS, BILL.ST), and a shell, copy or receipt line the list doesn't name ranks on its predecessor's bars.
- **Entry floor.** The 1.00 entry test reads `adjClose`, which is back-adjusted for every dividend paid after the rebalance date, so it sits below the printed close, by more the earlier the quarter and the higher the payout. A name whose printed close was above 1.00 fails the test when later dividends push its adjusted price under 1.00. That is the same dependence on later dividends the rerun removed from the ratio. Here it decides which screened names are held and, in 14 quarters (Italy 4, Malaysia 1, Singapore 9), whether the book is invested at all. In invested quarters the test drops 40.2% of screened names in Singapore, 12.4% in Malaysia, 8.0% in Hong Kong, 7.1% in Italy, 2.3% in China and 1.1% or less in the other 13 markets (none in Japan, Korea, Taiwan or Germany). These counts include the rare name with no entry bar in the window. How many dropped names had a printed close above 1.00, and what that does to the results, isn't measured.
- **Charts.** The CAGR and drawdown comparison charts leave out a leg measured over a shorter window (Norway's CAGR and drawdown cover 2013-2025 only), one never invested and one with no benchmark CAGR, and name each in a footnote. Norway's own growth chart is titled "measured 2013-2025", and its "Cash" footer counts the 50 measured quarters (0%). On a run-window file the partial final year is the last point and the last, hatched bar, and the titles carry the file's window.
- **`screen.py`** runs one market at a time: it fetches the market's last 390 days of bars and its rules' inputs, as one rebalance of the backtest does. It lists the 30 names the screen picks, before the entry test and the cash rule (Parameters), which need an entry bar the day doesn't have yet, so a rebalance may hold fewer of them or cash. Run it after the market's close: it ranks on the day's own bar when the vendor has one, and before the close that bar can be unfinished. On the day itself no later bar exists, so check T's closed-day test is skipped (a vendor row on a market holiday that day can be the signal bar), and the dedup's preference for the line buyable at entry has no entry bar to read.

## Data Requirements

- `stock_eod`: close, high and volume for the signal; adjClose for the 1.00 entry test, for entry and exit returns, and for the last traded price at exit
- `key_metrics` (FY): marketCap
- `income_statement`, `stock_prices_non_split_adjusted_daily`, `splits_calendar`: the market-cap check and the split flags (data checks M and F)
- `income_statement` (FY `filingDate`, on US legs only): lag rule `lr-1`'s filing dates
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup tie-break), companyName, isFund, isEtf, isAdr, cik and industry (universe rules `ur-4`)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (January 2000 to the latest close) |
| `screen.py` | The 30 names a rebalance today would pick, on the backtest's own fetch and screen, before its entry test and cash rule |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py`: growth and yearly returns for each market, plus CAGR and maximum drawdown across markets. Chart files aren't committed, so run it locally |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
