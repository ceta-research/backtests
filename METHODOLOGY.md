# Backtesting Methodology

This document describes the complete methodology used for all strategy backtests in this repository. Every backtest follows the same framework to ensure consistency, reproducibility, and comparability across strategies.

## Table of Contents

1. [Overview](#overview)
2. [Universe Construction](#universe-construction)
3. [Rebalancing Frequencies](#rebalancing-frequencies)
4. [Portfolio Construction](#portfolio-construction)
5. [Transaction Costs](#transaction-costs)
6. [Benchmarks](#benchmarks)
7. [Metrics Suite](#metrics-suite)
8. [Exchange Data Coverage](#exchange-data-coverage)
9. [Output Artifacts](#output-artifacts)
10. [Risk-Free Rate](#risk-free-rate)
11. [Limitations](#limitations)
12. [Reproducibility](#reproducibility)
13. [References](#references)

---

## Overview

All backtests in this repo share a common structure:

1. **Fetch** historical financial data via the [Ceta Research](https://cetaresearch.com) SQL API
2. **Cache** data locally in an in-memory DuckDB database
3. **Screen** stocks at each rebalance date using the strategy's signal
4. **Rebalance** the portfolio (buy/sell to target weights)
5. **Calculate** returns accounting for transaction costs
6. **Aggregate** to annual/cumulative metrics
7. **Compare** to benchmark(s)

**Determinism:** Same data + same parameters = same results. That has held since 2026-10-10, not before; see [Reproducibility](#reproducibility). All code is open source. Any user with a Ceta Research API key can reproduce our findings.

**Data source:** Financial Modeling Prep (FMP) data warehouse, accessed via Ceta Research's SQL API:
- Income statements, balance sheets, cash flow statements (1985-present, varies by exchange)
- Financial ratios (P/E, P/B, ROE, D/E, current ratio, etc.)
- Daily EOD prices (adjusted for splits and dividends)
- Company profiles (sector, industry, exchange, market cap)
- Piotroski F-Scores (pre-computed 9-signal quality scores)

---

## Universe Construction

### Security Selection

Securities are sourced from the `profile` table, which contains approximately 71,000 publicly traded stocks across 72 global exchanges.

**Exchange filtering:**
- Each strategy specifies target exchanges (e.g., NYSE+NASDAQ+AMEX for US-focused strategies)
- See [Exchange Data Coverage](#exchange-data-coverage) below for complete exchange list and data quality

**Listing rules (topics rerun with last-traded-price exits):**
- XETRA legs hold Germany-domiciled companies only, and SIX legs Switzerland-domiciled companies only.
- LSE legs hold lines quoted in pence (GBp) only. That drops the `0XXX.L` lines of foreign companies (carried in their home currency) and USD or EUR depositary receipts, and keeps LSE-primary companies registered abroad that quote in pence, such as Experian, Coca-Cola HBC and Wizz Air.
- On every leg, a security listed under more than one ticker (same ISIN) is held once: the line buyable at entry wins, then a line still marked as actively trading, then the line quoted in the market's own currency (CAD over USD on TSX, HKD over CNY on HKSE), then the alphabetically first symbol. This happens before the top-N cut, so the freed slot goes to the next-ranked name. Share classes, preference shares, Thai `-R` lines and mutual-fund share classes have their own ISINs and stay, so one company can still hold two or more slots. The three pairs topics go further and keep one line per company as well (see [Pairs Universe](#pairs-universe-rerun-topics)).
- Why: foreign companies' secondary listings, mostly US large caps, made up 74-78% of XETRA "Germany" holdings on average, and at least half in every invested year. SIX books were 22-34% foreign on average, and LSE books 51-81% foreign lines on average from 2018. Those returns belonged to the listing venue, not to German, Swiss or UK companies. Stockholm is left as listed: its books are almost entirely domestic, and a country filter would drop ABB, AstraZeneca and Nokia.
- Three rerun waves apply these rules. The first is cyclical-timing, asset-growth, fcf-conversion, cash-conversion and graham-net-net. The second is deleveraging, fcf-compounders, fcf-yield, revenue-accel, dividend-sustainability, price-to-sales, dogs-of-dow, low-debt, dividend-growth, pe-mean-revert, rising-yield and graham-number. The third is 52-week-high, dcf-discount, defensive-quality, pairs-fundamentals, pairs-zscore and pairs-multi-pair. Those twenty-three topics' results carry `listing_filter` and `isin_dedup` keys (the pairs topics also `company_dedup`), except pairs-multi-pair's `diversification_analysis.json`, a summary table of its US run that carries none of these keys or `exit_method`. Any other results file without them predates the rules, or is one of the 13 Norway legs rerun on their topics' own code in October 2026 (see [Survivorship Bias](#survivorship-bias), Which results use it). Topics outside the three waves haven't been changed: their XETRA, SIX and LSE legs still hold every listed line. Caveats in DATA_QUALITY_ISSUES.md (Exchange-listed universes are not domicile universes).

**Market capitalization filter:**
- Strategy-specific minimum, typically $100M - $1B
- Prevents micro-cap illiquidity and data quality issues
- Applied at each rebalance date. In the rerun topics the figure tested is the market cap on the company's latest fiscal-year `key_metrics` row, and the threshold is set per exchange in that exchange's currency (`cli_utils.get_mktcap_threshold`; graham-net-net keeps its own lower table).
- Known limit, not fixed: that market cap is in the company's reporting currency, so a company that reports in a different currency from its listing is tested in the wrong unit. This holds in every rerun topic except the non-US legs of dogs-of-dow, which restate size (see [Filing and Selection Rules](#filing-and-selection-rules-rerun-topics)). Measured in DATA_QUALITY_ISSUES.md.

### Data Quality Requirements

To be included in a backtest at a given rebalance date, a stock must have:

1. **Financial statements** for the screening period (varies by strategy):
   - **Income statement** (period=FY) - required for profitability metrics (ROE, net income, revenue, gross profit)
   - **Balance sheet** (period=FY) - required for leverage/liquidity metrics (D/E, current ratio, total assets)
   - **Cash flow statement** (period=FY) - required if strategy uses Piotroski scoring or income quality (OCF/NI) metrics
   - **Minimum:** 2 consecutive years of fiscal year statements (current year + prior year for year-over-year comparisons)

2. **Price data** for return calculation:
   - Daily EOD prices in `historical_price_full` table
   - Adjusted for splits and dividends (`adjClose` field)
   - Must have a price in a 10-day forward window from each rebalance date (15 days for exits in pairs-fundamentals), taking the first available price. Under the default next-day execution the window starts the day after the rebalance date. The exit price at the end of the period uses the same window. A held name with no price in it exits at its last traded price under the default `--exit-method ltp`, and is dropped from the period under `--exit-method drop`, the rule results produced before a topic's rerun used, and the 13 Norway legs rerun in drop mode (see [Survivorship Bias](#survivorship-bias)).

3. **Price sanity filters** (applied per holding, per period, in the periodic-rebalance portfolio topics; pairs topics and event studies use their own rules):
   - **Minimum entry price** (`min_entry_price`, 1.0 in most topics, 0.50 in a few; qarp, low-pe and the topics that don't use `filter_returns` apply none). It's compared to `adjClose` in the exchange's own price units, sub-units included: most LSE lines quote in pence (GBp), JNB in cents (ZAc), TLV in agorot. It's meant as a guard against zero, near-zero and broken prices, not a US$1 rule. Company size is handled by the market-cap filter above, and price spikes by the oscillation filter ([Price Data Quality](#price-data-quality-fmp-eod-anomalies)) and the return cap below. A US$1-equivalent floor was rejected because it would remove large caps in years when their shares traded below US$1, which ICBC, PetroChina and Lloyds all have. One side effect in early years: `adjClose` is back-adjusted for later splits and dividends. On 2003-01-02 the dividend part alone puts the median NYSE and NASDAQ name at 0.52-0.56x its split-adjusted close, and names that split heavily afterwards sit far lower (AAPL 0.22, NVDA 0.09, AMZN 0.98), so a 1.0 floor drops them from early rebalances. Whether a name clears the floor then depends on splits after the rebalance date, which is a look-ahead element.
   - **Maximum single return** (`max_single_return`, 200% per rebalance period in most topics, 300% in small-cap, small-value and trending-value; qarp, low-pe and sector-correlation apply none). A holding whose period return exceeds it is dropped as a likely price artifact. It's upside-only: large losses are kept.

**Survivorship bias:** Not removed. The data holds almost no company that stopped trading before 2015 (US) or 2020 (other markets), so every result for 2000-2014 in the US, and for 2000-2019 elsewhere, is measured on companies that survived at least to 2015 or 2020. The counts are under [Limitations: Survivorship Bias](#survivorship-bias). Companies that were acquired or went bankrupt after those dates are in the data (how completely isn't established), and in most topics such a company can be *selected* at any rebalance date before it stopped trading. dcf-discount, defensive-quality and the three pairs topics (pairs-zscore, pairs-multi-pair, pairs-fundamentals) used to build their universe from currently active listings (`isActivelyTrading = true`), so delisted names couldn't be selected in any year. Their October 2026 reruns don't use that filter. The pairs topics now rebuild their candidates each year from the lines listed and traded in the formation year, which removes the size look-ahead of ranking on today's market cap; survivorship is reduced, not fixed. On the exit side, a holding with no price in the 10-day window after the exit date exits at its last traded price on or before the exit date (`--exit-method ltp`, the default), so a company that traded down before delisting counts its loss and an acquired one counts its last price. Because of the gap in the data, that rule can only capture a company that stopped trading from about 2015 in the US and 2020 elsewhere. Under the older rule (`--exit-method drop`) such a holding was dropped from the period, so the period return averaged the names that still traded. A holding with no bar between its entry and the exit date is dropped either way. The three pairs topics instead close a pair at the last bar both legs traded, so a pair whose leg stopped trading closes there; the same gap in the data limits that to companies that stopped trading from about 2015 in the US and 2020 elsewhere. Committed results produced before a topic's rerun used the drop rule, and so do the 13 Norway legs rerun in drop mode in October 2026. Details, and how to tell which rule a results file used, under [Limitations: Survivorship Bias](#survivorship-bias).

**Look-ahead bias:** Partly mitigated, by a 45-day lag. A screen reads only fiscal-year statements whose period ended at least 45 days before the rebalance date. The lag counts from the fiscal period end, not from the date the statement was filed: the screens read each statement by its period-end date (`dateEpoch`, named `filing_epoch` in the code), and none reads a filing date. A company whose year ended 45 to 90 days before a rebalance can therefore be screened on figures that weren't public yet. Details under [Limitations: Look-Ahead Bias](#look-ahead-bias).

### Filing and Selection Rules (Rerun Topics)

The audits run before each rerun found screens that picked a company on an old filing, in the wrong currency unit, or on a broken streak, and one that ranked on a ratio that mixed two price bases. The rules below replace them. The seven second-wave topics named here (price-to-sales, graham-number, rising-yield, pe-mean-revert, dogs-of-dow, low-debt and dividend-growth) and the third wave's 52-week-high and dcf-discount have been rerun on their rules, and their committed results carry them. All of them keep the 45-day lag from the fiscal period end. What each defect did to the old books, and what each rerun showed, is in DATA_QUALITY_ISSUES.md (Rerun topics apply listing rules on XETRA, SIX and LSE). The three pairs topics' new universe has its own section below.

- **Latest filing first, then the tests** (price-to-sales, graham-number, rising-yield, pe-mean-revert, dcf-discount and the yield side of dogs-of-dow; graham-net-net since the first wave). The screen takes each company's latest fiscal-year row with a period end on or before the 45-day cutoff and at most 540 days before the rebalance date, and only then applies its tests. A company whose latest row fails is out, and so is one with no row in that window. Before, the screens loaded only passing rows, or tested before picking the latest row, so a company kept qualifying on its last passing year, however old. One limit: price-to-sales and pe-mean-revert load only rows that carry the ratio (P/S or P/E), ROE and market cap, so there "latest" means the latest row with those figures, still within the 540 days.
  - price-to-sales: the six tests (P/S above 0 and below 1.0, gross margin above 20%, operating margin above 5%, ROE above 10%, the size floor) run on the latest `financial_ratios` and `key_metrics` rows.
  - graham-number: the latest row must carry a positive Graham number. A year with a loss or negative book value has none, and the company is out.
  - rising-yield: the latest row's dividend yield must be above the previous fiscal year's, and that one above the year before it, on three consecutive fiscal years with none missing. A zero or missing yield breaks the run.
  - pe-mean-revert: the P/E band (3 to 50) is tested on the latest row instead of choosing it. The sector median is still taken over the companies whose latest row passes the band and the quality and size tests, and before the same-ISIN dedup, so a twin line counts twice in its sector's median.
  - dogs-of-dow: the latest ratios row must show a dividend yield above zero, on the Dow 30 leg and on the non-US legs. The market-cap row has the same 540-day limit.
  - dcf-discount: the latest fiscal-year cash-flow row and the latest market-cap row in that window are each taken. Free cash flow must then be above zero and filed in the market's reporting currency, the market cap must clear the size floor, and the FCF yield (free cash flow over market cap) must be at least 8.78%, the equivalent of a 20% discount to the topic's DCF value, and at most 50% (next item). Before, the fetch loaded only rows with positive free cash flow in that currency, so a company kept qualifying on its last positive year.
- **graham-number: the Graham number in the price's unit.** `grahamNumber` is per share in the filing's reporting currency, and `adjClose` is in the listing's quote currency. Before the price test and the ranking, the Graham number is restated in the price's unit: times 100 on lines quoted in a sub-unit (pence, South African cents, Israeli agorot), and converted at the newest USDxxx close on or before the price bar, at most 7 days old, when the two currencies differ. A line with no currency on either side or no fresh rate is dropped, and so is a depositary receipt quoted in a different currency from the one it reports in. Dropped lines are counted in the results under `gn_ccy_excluded`. This is graham-net-net's rule from the first wave. Before, a pence-quoted UK line was tested against a pound figure, and won and yen Graham numbers were compared with dollar prices.
- **low-debt: the prior year is the company's own previous fiscal year.** The Piotroski score compares two fiscal years. The current row is the latest with a period end on or before the 45-day cutoff and less than 595 days before the rebalance date. The prior row is the latest one whose period end is 300 to 430 days before the current row's. That fits 52-week and 53-week years and a moved year-end, and leaves a company with a skipped year unscored. Before, the prior row had to fall in a window fixed to the rebalance date, which no company with a year-end between May 18 and November 13 could meet.
- **dividend-growth: the streak ends at the last complete year.** Dividends are summed per calendar year. A year counts as an increase only if its total is above the total of the calendar year immediately before it. The run of increases must be at least 5 years long and must end in the last complete calendar year before the rebalance. A year with no dividend, a cut, a flat year or a missing year breaks it. Before, the streak counted increases since the last recorded cut and skipped missing years, so a company that had stopped paying kept its streak.
- **dogs-of-dow: size on the non-US legs.** Outside the US the topic takes the 30 largest lines on an exchange as its blue chips and holds the 10 highest yielders among them. Size is now the smaller of two figures, both in the leg's currency: the latest fiscal-year market cap restated from its reporting currency, and the entry bar's close times the latest fiscal-year weighted average share count (times 0.01 on pence and cent quotes). The rate is the newest USDxxx close on or before the entry bar, at most 7 days old. One figure alone is used when the other is missing. A line takes a blue-chip slot only if its entry bar passes the entry-price test (adjusted close of at least 1.0) and its size is at or above the leg's floor and at most 15,000 times the floor. The ceiling is a data-error limit: the largest real size on any leg is about 7,100 times its floor (Saudi Aramco), and the smallest corrupt one about 27,000 times. Then come the same-ISIN dedup, the 30 largest and the 10 highest positive yields. Results record what the size rule dropped under `size_excluded`. Before, the cut ranked raw reported market caps, in mixed currencies and with corrupt values included. The US leg is unchanged: it ranks the Dow 30 by yield, using today's index members for every year.
- **dcf-discount: the FCF yield is capped at 50%.** A yield above 50% takes no slot, the band every SQL screen printed in the topic's posts uses (the repository's `dcf-discount/screen.py` has no cap). Before, the backtest had no upper bound: in a replica of the US screen made before the rerun, 529 of the 1,250 held slot-years had a yield above 50%. The reporting-currency test keeps the universe to companies that file in the market's currency. `key_metrics.marketCap` is in the reporting currency too, so it doesn't fix a unit mismatch; it restricts who can be picked.
- **52-week-high: one price basis, the last traded bar, ties by size.** The proximity ratio is the close on the signal bar over the highest high of the 252 bars ending there. Both are split-adjusted and neither is adjusted for dividends; `adjClose` prices the returns only. The signal bar is the latest bar from 10 days before the rebalance date up to that date with volume above zero, and a line with no such bar isn't ranked. The 252-bar window includes untraded bars. A line needs at least 100 price rows in the window, and a ratio above 1.1 is treated as a data error. The same-ISIN dedup runs before the top-30 cut, and exact ties at the cut go to the larger market cap, then the symbol. Before, the ratio divided `adjClose`, which is back-adjusted for every later dividend, by an unadjusted high, so a company scored lower the more it paid out after the rebalance date. The published June 2026 run also took its signal bar from bars up to 5 days after the rebalance date while entry was the next day, a look-ahead, with no test of how old the bar was, and cut ties in no fixed order.
- **defensive-quality: the campaign rules only.** No selection rule of its own changed in the third wave. Since earlier in October 2026 it doesn't filter on `isActivelyTrading` and reads only filings at most 540 days old; its rerun results are the first to carry both.

### Pairs Universe (Rerun Topics)

pairs-fundamentals, pairs-zscore and pairs-multi-pair take their candidates from `pairs_universe.py`, rebuilt for every trading year from the calendar year before it (the formation year). A line is a candidate for a trading year when all of these hold:
- It is in the leg's `profile` rows with a sector, isn't flagged as a fund or ETF, and passes the leg's listing rule (XETRA Germany-domiciled, SIX Switzerland-domiciled, LSE pence-quoted).
- It has at least one fiscal-year `key_metrics` row with a market cap, the operating-company test. That row may be dated at any time, so a line whose first filing comes years after the formation year still counts in it. The test isn't point-in-time: a point-in-time version would empty the early Korean and Taiwanese years. Every results file says so under `universe_rule`.
- It traded through the formation year: at least 200 bars in it, the last one within 10 days of the year end, and a median daily close times volume above zero.
- It is the one line kept for its security (same ISIN, the campaign rule) and for its company. Share classes, preference lines and Thai NVDR lines of one company are collapsed to one line, matched on ISIN, CIK, normalised company name or ticker root.
- It is among the 30 largest lines of its sector by the formation year's median daily traded value (close times volume), converted to US dollars at the year-end rate. There is no other size floor, so a sector with fewer than 30 lines admits them all.

Pairs are formed within a sector from those candidates. A pair whose correlation isn't a number can't rank, and no pair joins two lines of one company. The cost tier comes from the latest fiscal-year market cap whose fiscal year ended 45 to 540 days before the trading year, in US dollars at the formation year-end rate; a candidate with none pays the smallest-cap tier. Each pair is charged one tiered cost per leg per round trip, on the pair's return basis. A year is in cash when fewer than 3 pairs are entered in the 10-day entry window, decided before the year trades. A pair is one position: when a leg stops trading, both legs close at the last bar both traded, bounded by a recorded delisting the same way as single-stock exits. pairs-fundamentals takes `--exit-method drop|ltp|both` through a pair-level helper; pairs-zscore and pairs-multi-pair accept only `ltp`, so they have no drop book.

Before the rerun each topic built one list from today's actively trading names, the 30 largest of each sector by today's market cap, and used it for every year from 2005. A name had to be among today's 30 largest to be picked in 2005, a look-ahead on size, and a name delisted since couldn't be picked at all. The old code also let pairs with an undefined correlation rank first, charged each round trip's cost twice, set the cost tier from today's market cap in the reporting currency, let pairs-multi-pair exit on the bar that gave the signal, and decided cash from outcomes known only at the year end. The per-year list removes the size look-ahead. Survivorship is reduced, not fixed: the price history holds almost no company that stopped trading before 2015 (US) or 2020 (elsewhere), so in earlier years the candidates are still companies that survived (see [Survivorship Bias](#survivorship-bias)). Known gaps, listed in DATA_QUALITY_ISSUES.md: the company key misses some twins listed under different names and ISINs, and a delisted leg's zero-volume repeat bars can keep it in the formation-year universe.

---

## Rebalancing Frequencies

All backtests support four rebalancing frequencies via the `--frequency` CLI flag. Each strategy documents its canonical frequency (the one used in blog posts).

| Frequency | Periods/Year | Typical Use Case | Turnover | Transaction Cost Impact | Data Requirement |
|-----------|------------:|------------------|----------|------------------------|------------------|
| **Monthly** | 12 | Momentum, technical signals, mean reversion | Very high (12× annual) | Significant drag on returns | Needs monthly or daily price updates |
| **Quarterly** | 4 | Earnings-driven signals, moderate turnover | Moderate-high (4× annual) | Moderate drag | Works well with quarterly filers (US stocks) |
| **Semi-annual** | 2 | Quality/value hybrids (QARP default) | Moderate (2× annual) | Low-moderate drag | Annual statements sufficient |
| **Annual** | 1 | Deep value, fundamental (Piotroski default) | Low (1× annual) | Minimal drag | Aligns with annual filing schedule |

**Trade-offs:**
- **Higher frequency:** Fresher signals (captures trends faster), but higher transaction costs and turnover often reduce after-cost returns
- **Lower frequency:** Lower costs and tax-efficient, but slower to adapt to changing fundamentals

**Academic evidence:** Vanguard research (2025) found annual rebalancing delivers +51 basis points risk-adjusted benefit vs daily rebalancing for typical factor strategies, primarily due to lower transaction costs outweighing signal staleness.

**Sensitivity testing:** Each strategy reports results at all supported frequencies to demonstrate the impact of rebalancing choice. Turnover and transaction cost drag are reported separately for transparency.

---

## Portfolio Construction

### Weighting Schemes

| Scheme | Calculation | Use Case | Trade-off |
|--------|-------------|----------|-----------|
| **Equal weight** | 1/N per stock | Default for all strategies | Simple, prevents concentration, standard in academic factor literature |
| Market-cap weight | weight_i = mcap_i / Σ(mcap) | Large-cap tilt | Mimics index construction, lower turnover, concentrates in mega-caps |

**Default:** Equal weight. This is the standard approach in academic factor research because it isolates the factor effect without allowing mega-caps to dominate.

### Position Limits

| Parameter | Typical Range | Strategy Examples | Rationale |
|-----------|--------------|-------------------|-----------|
| **Min stocks** | 10-30 | QARP: 10, Low P/E: 10 | Diversification floor. Below this, portfolio is under-diversified. |
| **Max stocks** | Unlimited or fixed cap | QARP: unlimited, Low P/E: 30 | Concentration vs diversification trade-off. Unlimited allows signal to breathe. |

**Cash rule:** If fewer than `min_stocks` qualify at a rebalance date, the portfolio holds cash (0% return) for that period. This prevents under-diversified concentrated bets that violate the strategy's risk profile. Since September 2026 the rule is checked twice in the topics with a minimum book size: on the number of names that pass the screen, and on the number of those with a usable entry price. A period where enough names pass but too few can be bought is cash as well. That second check, the entry-side cash guard, reads entry prices only, never exit prices. Results produced before it averaged whatever names were priced, however few. Rerun results record `screened`, `entry_buyable` and `min_stocks` for every period, so each cash period can be traced to the screen or to the guard.

**Reading a market's result:**
- A market invested in fewer than half of its rebalance periods is too thin to test. The count is `invested_periods` against `total_rebalances`, over the whole requested window. Its CAGR mostly measures which periods cleared the floor, so it must not be read or ranked as a normal result. The second-wave topics' charts label such a market "too thin to test".
- An excess return of less than about 0.2 percentage points a year against a benchmark, in either direction, is a tie.
- Most local benchmarks are price indices and the portfolios include dividends, so an excess against one is overstated by roughly the index's dividend yield (see [Benchmark Limitations](#benchmark-limitations)).

**Alternatives (not currently implemented):**
- Hold previous portfolio until `min_stocks` threshold met again
- Fall back to broader universe (e.g., relax one filter criterion)
- Hold benchmark proxy (e.g., SPY) instead of cash

### Turnover Calculation

**Definition:** Percentage of portfolio value replaced at each rebalance.

**Formula:**
```
turnover = Σ |new_weight_i - old_weight_i| / 2
```

Where:
- `new_weight_i` = target weight of stock i after rebalance (0 if not in new portfolio)
- `old_weight_i` = weight of stock i before rebalance (0 if not in old portfolio)
- Division by 2 avoids double-counting (a stock sold and bought back counts as 100% turnover, not 200%)

**Example:** If 40% of stocks are replaced (20% sold, 20% bought), turnover = 40%.

**Reported:** Average turnover per rebalance. Varies by frequency even with same signal (higher frequency → more turnover).

---

## Transaction Costs

All backtests include transaction costs by default (disable via `--no-costs` flag for theoretical comparison).

### Cost Model: Size-Tiered

Based on empirical trading costs in liquid markets:

| Market Capitalization | One-Way Cost | Round-Trip Cost | Rationale |
|----------------------|-------------|----------------|-----------|
| **> $10 billion** | 0.10% | 0.20% | Mega-cap stocks: tight bid-ask spreads, high liquidity, minimal market impact |
| **$2B - $10B** | 0.30% | 0.60% | Large-cap: moderate spreads, good liquidity |
| **< $2 billion** | 0.50% | 1.00% | Mid/small-cap: wider spreads, lower liquidity, higher impact cost |

**Application:**
- **Entry cost:** Charged when buying a stock at rebalance
- **Exit cost:** Charged when selling a stock at rebalance or period exit
- **Round-trip:** Entry + exit (e.g., 0.10% + 0.10% = 0.20% total for mega-cap held one period)

**Net return calculation:**
```python
raw_return = (exit_price - entry_price) / entry_price
cost_rate = tiered_cost(market_cap)  # returns 0.001, 0.003, or 0.005
net_return = raw_return - (entry_cost + exit_cost)
```

**Example:** A $5B stock with +15% raw return:
- Cost tier: $2-10B → 0.30% one-way
- Round-trip cost: 0.60%
- Net return: 15.00% - 0.60% = 14.40%

**Reporting:** All published results include transaction costs (realistic). Some strategies additionally report no-cost results for academic comparison.

**Alternative models (via CLI flags):**
- `--flat-cost 0.001` - Flat 0.10% for all stocks (simplicity)
- `--no-costs` - Zero costs (academic baseline)

---

## Benchmarks

Every backtest compares the strategy portfolio to up to three benchmarks to provide context on performance.

### 1. Primary Benchmark (Always Included)

**S&P 500 (SPY)**
- Universal reference point for all strategies
- Available 1993-01-29 to present (33+ years of data)
- USD-denominated
- Used regardless of strategy geography

**Rationale:** Provides a consistent baseline across all strategies. Even for non-US strategies, SPY represents the "do nothing, just buy US large-cap" alternative available to any investor.

**Limitation:** For non-US exchanges, SPY is USD-denominated while local strategy returns are in local currency (INR, EUR, CNY, etc.). This introduces currency effects into the comparison. See [Limitations: Currency Effects](#currency-effects).

### 2. Factor Benchmarks (Strategy-Appropriate)

Match the strategy to a factor ETF for apples-to-apples comparison:

| Strategy Type | Benchmark ETF | Ticker | History | Why This Benchmark |
|--------------|--------------|--------|---------|-------------------|
| **Value** (Low P/E, Low P/B, High Dividend Yield) | Russell 1000 Value | IWD | 2000-05-26 to present (26 years) | US large-cap value stocks. Tracks companies with low P/B and P/E. |
| **Quality** (High ROE, Piotroski, QARP) | iShares MSCI USA Quality Factor | QUAL | 2013-07-18 to present (13 years) | Stocks with high ROE, stable earnings, low debt. |
| **Small-cap** | Russell 2000 | IWM | 2000-05-26 to present (26 years) | US small-cap broad market. |
| **Momentum** | iShares MSCI USA Momentum Factor | MTUM | 2013-04-18 to present (13 years) | Stocks with strong price momentum. |

**Usage:** Included in metrics comparison if the strategy's backtest start date ≥ ETF inception date.

### 3. Regional Benchmarks (Exchange-Specific)

When testing non-US exchanges, we include the corresponding regional ETF as a third benchmark:

| Exchange(s) | Regional Benchmark | Ticker | History | Notes |
|------------|-------------------|--------|---------|-------|
| **BSE, NSE** (India) | iShares MSCI India | INDA | 2012-02-03 to present (14yr) | India large/mid-cap |
| **XETRA, FSX** (Germany) | iShares MSCI Germany | EWG | 1996-03-18 to present (30yr) | German equities |
| **SHZ, SHH** (China) | iShares China Large-Cap | FXI | 2004-10-08 to present (21yr) | China H-shares + red chips |
| **HKSE** (Hong Kong) | iShares MSCI Hong Kong | EWH | 1996-03-18 to present (30yr) | Hong Kong equities |
| **JPX** (Japan) | iShares MSCI Japan | EWJ | 1996-03-18 to present (30yr) | Japanese equities |
| **KSC, KOE** (South Korea) | iShares MSCI South Korea | EWY | 2000-05-12 to present (26yr) | South Korean equities |
| **ASX** (Australia) | iShares MSCI Australia | EWA | 1996-03-18 to present (30yr) | Australian equities |
| **LSE** (UK) | iShares MSCI United Kingdom | EWU | 1996-03-18 to present (30yr) | UK equities |
| **TSX, TSXV** (Canada) | iShares MSCI Canada | EWC | 1996-03-18 to present (30yr) | Canadian equities |
| **PAR, AMS, BRU, MIL, STO, OSL, CPH, HEL** (Europe) | Vanguard FTSE Europe | VGK | 2005-03-10 to present (21yr) | Broad European exposure |
| **Global Emerging** (fallback) | iShares MSCI Emerging Markets | EEM | 2003-04-11 to present (23yr) | Emerging markets broad |

All ETFs verified available in Ceta Research `historical_price_full` table (verified 2026-02-13).

**Additional regional ETFs (to be verified):** EWT (Taiwan), THD (Thailand), EIDO (Indonesia), EWZ (Brazil), EWS (Singapore), EWW (Mexico), EZA (South Africa), KSA (Saudi Arabia). These will be added as we expand to additional regions.

**Multi-benchmark reporting:** Each backtest computes all metrics against:
1. SPY (primary, always)
2. Factor ETF (strategy-appropriate, if backtest period overlaps)
3. Regional ETF (exchange-specific, if available)

---

## Metrics Suite

Every backtest computes a comprehensive set of performance, risk, and comparative metrics. Results are organized into two tiers.

### Tier 1: Core Metrics (Always Computed)

These 17 metrics are computed for every strategy on every exchange:

#### Return Metrics

| Metric | Formula | Unit | Interpretation |
|--------|---------|------|----------------|
| **CAGR** (Compound Annual Growth Rate) | (cumulative_return)^(1/years) - 1 | % | Annualized growth rate. Primary performance metric. |
| **Total Return** | cumulative_return - 1 | % | Total growth over entire backtest period (not annualized). |

#### Risk Metrics

| Metric | Formula | Unit | Interpretation |
|--------|---------|------|----------------|
| **Max Drawdown** | min(cumulative_value / peak_value - 1) | % (negative) | Worst peak-to-trough loss. Maximum pain an investor would have experienced. |
| **Drawdown Recovery** | Periods from trough to new peak | count | How long capital was underwater. `None` if never recovered by end of backtest. |
| **Annualized Volatility** | std(returns) × √(periods_per_year) | % | Annual standard deviation of returns. Measures variability. Sample std (n-1 denominator). |
| **VaR 95%** | 5th percentile of period returns | % | Historical Value-at-Risk. "95% confidence of not losing more than X% in a period." |
| **Max Consecutive Losses** | Longest streak of negative-return periods | count | Psychological/operational risk. Longest losing run. |
| **Pct Negative Periods** | count(returns < 0) / total_periods | % | Hit rate. Fraction of periods with losses. |

#### Risk-Adjusted Metrics

| Metric | Formula | Benchmark | Interpretation |
|--------|---------|-----------|----------------|
| **Sharpe Ratio** | (CAGR - Rf) / annualized_vol | >1.0 good, >2.0 excellent | Return per unit of total risk. Standard metric for risk-adjusted performance. |
| **Sortino Ratio** | (CAGR - Rf) / downside_dev | >2.0 good | Return per unit of downside risk. Only penalizes downside volatility, ignores upside. Preferred for asymmetric strategies. |
| **Calmar Ratio** | CAGR / abs(max_drawdown) | >2.0 good | Return per unit of maximum drawdown. Focuses on worst-case scenario. |

**Formulas (detailed):**

**Sharpe:**
```
annualized_vol = std(period_returns) × √(periods_per_year)
sharpe = (CAGR - Rf) / annualized_vol
```

**Sortino:**
```
downside_deviations = [min(r - Rf/ppy, 0) for r in period_returns]
downside_variance = mean(downside_deviations²)
downside_dev = √(downside_variance) × √(periods_per_year)
sortino = (CAGR - Rf) / downside_dev
```

**Calmar:**
```
calmar = CAGR / abs(max_drawdown)
```
Returns `None` if max_drawdown == 0 (all positive returns).

#### Relative Metrics (vs Benchmark)

These compare the portfolio to the primary benchmark (SPY) and optionally to factor/regional benchmarks.

| Metric | Formula | Benchmark | Interpretation |
|--------|---------|-----------|----------------|
| **Information Ratio** | mean(excess) × ppy / tracking_error | >0.5 good, >1.0 excellent | Risk-adjusted outperformance vs benchmark. Measures consistency of alpha. |
| **Tracking Error** | std(excess_returns) × √(ppy) | Lower = tracks benchmark closely | Annualized volatility of excess returns. Measures deviation from benchmark. |
| **Up Capture** | mean(port\|bench>0) / mean(bench\|bench>0) | >100% desired | Upside participation. Captures X% of benchmark's up moves. 120% = captures 1.2× upside. |
| **Down Capture** | mean(port\|bench<0) / mean(bench\|bench<0) | <100% desired | Downside protection. Captures X% of benchmark's down moves. 80% = only 0.8× downside. |
| **Win Rate** | count(portfolio > benchmark) / total_periods | >50% = more wins than losses | Percentage of periods where portfolio outperformed benchmark. |

**Formulas (detailed):**

**Information Ratio:**
```
excess_returns = [port_r - bench_r for each period]
tracking_error = std(excess_returns) × √(periods_per_year)
information_ratio = mean(excess_returns) × periods_per_year / tracking_error
```

**Up/Down Capture:**
```
up_periods = [(port_r, bench_r) where bench_r > 0]
up_capture = mean(port_r in up_periods) / mean(bench_r in up_periods)

down_periods = [(port_r, bench_r) where bench_r < 0]
down_capture = mean(port_r in down_periods) / mean(bench_r in down_periods)
```

Returns 0 if no up/down periods exist.

#### Portfolio Metrics

| Metric | Calculation | Unit | Interpretation |
|--------|-------------|------|----------------|
| **Avg Positions** | mean(stocks_held \| stocks_held > 0) | count | Average number of stocks in portfolio when invested (excludes cash periods). |
| **Cash Periods** | count(stocks_held < min_stocks) | count | Number of periods where portfolio held cash due to insufficient qualifying stocks. |
| **Invested Periods** | total_periods - cash_periods | count | Number of periods with actual stock positions. |

---

### Tier 2: Advanced Metrics (Strategy-Specific)

These metrics are computed when relevant or requested via CLI flags.

| Metric | Formula | When Computed | Interpretation |
|--------|---------|---------------|----------------|
| **Alpha (CAPM)** | α from regression: R_p - Rf = α + β(R_m - Rf) + ε | Always if benchmark available | Excess return not explained by market exposure (beta). Intercept of regression. |
| **Beta (CAPM)** | β from same regression. Also: cov(R_p, R_m) / var(R_m) | Always if benchmark available | Systematic risk vs market. β=1.0 is market-like, <1.0 defensive, >1.0 aggressive. |
| **CVaR 95%** | mean(returns \| returns ≤ VaR_95) | Always | Conditional VaR. Expected loss given we're in worst 5% of outcomes. Tail risk measure. |
| **Rolling 3-Year CAGR** | CAGR over rolling 3-year windows | If backtest ≥ 5 years | Time series showing stability over time. Helps identify regime changes. |
| **Annual Turnover** | mean(Σ\|new_wt - old_wt\| / 2) | Always | Average % of portfolio replaced per rebalance. Transaction cost driver. |
| **Decade Breakdown** | Avg annual returns per decade | If backtest ≥ 20 years | Performance across market regimes (2000s crisis, 2010s bull, 2020s, etc.). |
| **Pre/Post Publication** | Avg returns before/after strategy published | For academic strategies only | Tests if publication reduces alpha due to crowding. Example: Piotroski (2000). |
| **Skewness** | Third standardized moment | Optional | Return distribution asymmetry. Positive = more extreme gains than losses. |
| **Kurtosis** | Fourth standardized moment | Optional | Tail thickness. High kurtosis = fat tails (extreme outcomes more common). |

**Formulas (detailed):**

**Alpha & Beta (CAPM):**
```
Regression: R_portfolio,t - Rf,t = α + β × (R_benchmark,t - Rf,t) + ε_t
```
- α (alpha): annualized excess return after adjusting for systematic risk
- β (beta): slope = cov(R_p, R_m) / var(R_m)

Solved via ordinary least squares on period returns.

**CVaR 95%:**
```
VaR_95 = 5th percentile of returns
CVaR_95 = mean(returns where returns ≤ VaR_95)
```

**Rolling CAGR (3-year window):**
```
For each period t where t ≥ 3 years from start:
  rolling_cagr_t = (cumulative_t / cumulative_{t-36months})^(1/3) - 1
```

**Decade Breakdown:** (for backtests spanning 1985-2025)
- Buckets: 1985-1989, 1990s, 2000s, 2010s, 2020-2025
- For each bucket: average annual return for portfolio and benchmark, spread (portfolio - benchmark)

---

## Exchange Data Coverage

As of 2026-02-13, FMP data via Ceta Research covers **72 exchanges** globally. Data quality varies significantly.

### Classification

Exchanges are classified into three tiers based on the number of symbols with complete financial data (income + balance + prices):

- **Tier 1** (1,000+): Backtesting-ready for most strategies. Deep liquidity, good data quality.
- **Tier 2** (250-1,000): Suitable for strategies with looser filters or regional focus. Moderate depth.
- **Tier 3** (<250): Limited. Viable for niche regional strategies but low diversification.

### Tier 1: Backtesting-Ready (1,000+ Symbols)

15 exchanges with rich financial data:

| Exchange | Code | Country/Region | Symbols with Financials† | Regional Benchmark | Notes |
|----------|------|----------------|------------------------|-------------------|-------|
| **OTC** | OTC | US Over-the-Counter | 12,518 | SPY | Richest dataset but includes illiquid penny stocks. Advanced use only. |
| **NASDAQ** | NASDAQ | US Technology | 7,712 | SPY | Tech-heavy, high quality |
| **NYSE** | NYSE | US Main | 4,173 | SPY | Blue-chip focus |
| **JPX** | JPX | Japan (Tokyo) | 3,972 | EWJ | 2nd largest non-US |
| **BSE** | BSE | India (Bombay) | 3,913 | INDA | Largest Indian exchange |
| **LSE** | LSE | UK (London) | 3,410 | EWU | European financial center |
| **HKSE** | HKSE | Hong Kong | 2,703 | EWH | Asia financial hub |
| **SHZ** | SHZ | China (Shenzhen) | 2,447 | FXI | Tech/growth focus |
| **NSE** | NSE | India (National) | 2,201 | INDA | Modern Indian exchange |
| **ASX** | ASX | Australia | 2,148 | EWA | Pacific exposure |
| **SHH** | SHH | China (Shanghai) | 2,008 | FXI | Established Chinese exchange |
| **TWO** | TWO | Taiwan OTC | 1,149 | EWT (TBD) | Taiwan secondary |
| **TAI** | TAI | Taiwan (Main) | 1,033 | EWT (TBD) | Taiwan primary |
| **SET** | SET | Thailand | 1,011 | THD (TBD) | Southeast Asia |
| **KSC** | KSC | South Korea (KOSPI) | 1,009 | EWY | Korea large-cap |

† Symbols with income statement + balance sheet + price data.

### Tier 2: Solid Coverage (250-1,000 Symbols)

19 exchanges with moderate depth. Suitable for strategies with looser filters.

| Exchange | Country | Symbols with Financials | Regional Benchmark |
|----------|---------|------------------------|-------------------|
| TSX | Canada | 998 | EWC |
| TSXV | Canada Venture | 969 | EWC |
| XETRA | Germany | 958 | EWG |
| JKT | Indonesia | 887 | EIDO (TBD) |
| STO | Sweden (Stockholm) | 882 | VGK |
| PAR | France (Euronext Paris) | 809 | VGK |
| CNQ | Canada (NEO) | 702 | EWC |
| TLV | Israel | 537 | - |
| SAO | Brazil (B3) | 459 | EWZ (TBD) |
| WSE | Poland (Warsaw) | 429 | - |
| AMEX | US (mostly ETFs) | 400 | SPY |
| SIX | Switzerland | 387 | EWL (TBD) |
| SAU | Saudi Arabia (Tadawul) | 372 | KSA (TBD) |
| KOE | South Korea (KOSDAQ) | 366 | EWY |
| SES | Singapore | 315 | EWS (TBD) |
| KLS | Malaysia | 313 | - |
| OSL | Norway | 292 | VGK |
| MIL | Italy (Milan/Borsa) | 276 | VGK |
| FSX | Germany (Frankfurt) | 257 | EWG |

### Tier 3: Limited Coverage (50-250 Symbols)

39 additional exchanges with 50-250 symbols. Suitable for niche regional strategies but limited diversification.

Examples: MCX (Mexico, 239), JNB (South Africa, 225), CPH (Denmark, 186), HEL (Finland, 173), BME (Spain, 171), BRU (Belgium, 148), IST (Turkey, 143), NZE (New Zealand, 140), MEX (Mexico, 139), AMS (Netherlands, 132), VIE (Austria, 102), ATH (Greece, 94), NEO (Canada, 77), plus 26 smaller exchanges (<50 symbols).

### Exchange Selection Criteria (Per Strategy)

Not all strategies run on all 72 exchanges. Each strategy documents eligible exchanges based on:

1. **Minimum universe size:** Must have N symbols with complete financial data (typically 50-100, varies by strategy strictness)
2. **Minimum qualifying stocks:** The strategy's signal must find stocks on average across backtest periods (typically 5-10+)
3. **Data coverage period:** Must have 10+ years of historical data for statistical robustness

**Example: QARP (strict 7-factor filter)**
- Eligible: 12 exchanges (US_MAJOR, BSE, NSE, XETRA, SHZ, SHH, HKSE, KSC, ASX, plus NYSE/NASDAQ/AMEX individually)
- Excluded: TSXV (0 stocks qualify), TSX (0 stocks qualify), JPX/LSE (not yet tested)
- Reason: Stringent filters (Piotroski ≥7, ROE >15%, low D/E, moderate P/E, etc.) eliminate most stocks

**Example: Low P/E (projected, looser filter)**
- Eligible: Likely all 15 tier-1 + most tier-2 exchanges (needs verification run)
- Reason: Looser filter (just P/E <15, ROE >10%, D/E <1.0) should find stocks on most exchanges

**OTC Exchange Note:** OTC has the richest dataset (12,518 symbols) but includes many illiquid penny stocks, pink sheet stocks, and ADRs. Recommend flagging as "advanced users only" or excluding from default comparison reports. Results will be noisy and likely not investable at scale.

---

## Output Artifacts

Each strategy produces standardized result files and charts.

### Result Files

Located in `{strategy}/results/` (e.g., `qarp/results/`, `piotroski/results/`):

| File | Format | Contents | Purpose |
|------|--------|----------|---------|
| **exchange_comparison.json** | JSON | All metrics for all tested exchanges | Master metrics file. Used by chart generation scripts. Contains full metrics dict per exchange. |
| **returns_{EXCHANGE}.csv** | CSV | Period-level returns for one exchange | Columns: `rebalance_date`, `exit_date`, `portfolio_return`, `spy_return`, `stocks_held`, `holdings` (comma-separated symbols or "CASH"). Audit trail for deep-dive analysis and verification. |
| **sensitivity_{FREQUENCY}.json** | JSON | Metrics at each rebalancing frequency | Shows impact of monthly vs quarterly vs semi-annual vs annual. Used for frequency sensitivity analysis. |
| **summary_metrics.json** | JSON | Primary configuration metrics only | Quick reference. Subset of exchange_comparison.json for the canonical configuration (usually US_MAJOR at default frequency). |

**exchange_comparison.json structure example:**
```json
{
  "US_MAJOR": {
    "universe": "US_MAJOR",
    "n_periods": 50,
    "years": 25.0,
    "cash_periods": 0,
    "invested_periods": 50,
    "avg_stocks_when_invested": 44.2,
    "portfolio": {
      "total_return": 425.67,
      "cagr": 9.96,
      "max_drawdown": -28.54,
      "max_drawdown_recovery": 8,
      "annualized_volatility": 15.23,
      "sharpe_ratio": 0.523,
      "sortino_ratio": 0.745,
      "calmar_ratio": 0.349,
      "var_95": -12.34,
      "max_consecutive_losses": 3,
      "pct_negative_periods": 28.0
    },
    "spy": { ... same fields ... },
    "comparison": {
      "excess_cagr": 2.32,
      "win_rate": 56.0,
      "information_ratio": 0.421,
      "tracking_error": 8.34,
      "up_capture": 98.5,
      "down_capture": 87.2,
      "beta": 0.92,
      "alpha": 1.45
    },
    "annual_returns": [
      {"year": 2000, "portfolio": 12.34, "spy": -10.50, "excess": 22.84},
      {"year": 2001, "portfolio": -5.23, "spy": -11.89, "excess": 6.66},
      ...
    ]
  },
  "BSE": { ... same structure ... },
  ...
}
```

### Charts

Located in `{strategy}/charts/`:

**Per-exchange charts:**
- `{exchange}_cumulative_growth.png` - Line chart showing growth of $10,000 invested over time (portfolio vs SPY benchmark)
- `{exchange}_annual_returns.png` - Grouped bar chart of year-by-year returns (portfolio vs SPY)

**Cross-exchange comparison charts:**
- `comparison_cagr.png` - Horizontal bar chart of CAGR by exchange, sorted descending, with SPY reference line
- `comparison_drawdown.png` - Horizontal bar chart of max drawdown by exchange
- `comparison_sortino.png` - Sortino ratio by exchange (NEW with Tier 2 metrics)
- `comparison_capture.png` - Scatter plot: up_capture (x-axis) vs down_capture (y-axis), exchanges as labeled points. Ideal zone = upper-left quadrant (high up capture, low down capture). (NEW with Tier 2 metrics)

**Chart specifications:**
- DPI: 200 (high-res for blog embedding)
- Format: PNG with white background
- Dimensions: 12×6 for line charts, 10×7 for bar charts, 10×10 for scatter
- Footer attribution: "Data: Ceta Research | {strategy}, {rebalance frequency}, equal weight, {date range}"

---

## Risk-Free Rate

**Default:** Fixed 2.0% annual (configurable via `--risk-free-rate` CLI flag)

**Usage:** Used in:
- Sharpe ratio denominator
- Sortino ratio denominator
- CAPM alpha calculation (R_p - Rf = α + β(R_m - Rf))

**Rationale for 2% default:**
- Approximate long-term average of 3-month US Treasury bills (1990-2020)
- Conservative for modern low-rate environment (2010-2021 had 0-0.5% T-bill rates)
- Avoids external API dependency for simplicity

**Period adjustment:** For sub-annual rebalancing, the annual rate is divided by `periods_per_year`:
- Annual: use 2.0% directly
- Semi-annual: use 2.0% / 2 = 1.0% per period
- Quarterly: use 2.0% / 4 = 0.5% per period
- Monthly: use 2.0% / 12 = 0.167% per period

**Future enhancement:** Integrate historical 3-month Treasury bill rates from FRED API (series: TB3MS, available 1934-present). This would use the actual risk-free rate for each backtest period, improving Sharpe/Sortino precision. When implemented, both fixed (2%) and historical Sharpe ratios will be reported.

**CLI examples:**
```bash
# Conservative: assume 0% risk-free rate (all returns are "excess")
python3 qarp/backtest.py --risk-free-rate 0.0

# Historical average
python3 qarp/backtest.py --risk-free-rate 0.02

# Higher-rate environment (2022-2024)
python3 qarp/backtest.py --risk-free-rate 0.04
```

---

## Limitations

### Survivorship Bias

**Status:** Not removed. The data holds almost no company that stopped trading before 2015 (US) or 2020 (other markets), so earlier years are measured on survivors whatever the exit rule. After those dates it's partly mitigated: for selection in most topics, and for exits in results produced with last-traded-price exits.

**What the data holds (measured 2026-10-13).** We counted the symbols in the daily price table (`stock_eod`) that have a full year of price bars in 2004, by the year of their last bar:

| Market | Still trading | Last bar 2020-2025 | Last bar 2015-2019 | Last bar before 2015 |
|---|---:|---:|---:|---:|
| US | 3,707 | 470 | 174 | 10 |
| Japan (JPX) | 1,916 | 109 | 0 | 0 |
| UK (LSE) | 416 | 121 | 0 | 0 |
| India (NSE) | 465 | 47 | 0 | 0 |
| Hong Kong (HKSE) | 545 | 31 | 0 | 0 |
| Canada (TSX) | 300 | 53 | 0 | 0 |
| Germany (XETRA) | 284 | 45 | 0 | 0 |

Of the 4,361 US symbols that traded through 2004, 10 (0.2%) have a last bar before 2015. In the six other markets none ends before 2020. The vendor's delisted-company list, as we hold it, has 10,012 symbols, and only 25 US and 11 non-US ones were delisted before 2015. Companies are acquired, go bankrupt or delist every year, so a complete record would hold many such symbols for 2005-2014. Ten in the US and none in six other markets means the record of them is missing.

What follows from that:
- Every result for 2000-2014 in the US, and for 2000-2019 in the other markets, is measured on companies that survived at least to 2015 or 2020. A screen run for 2003 can only pick companies that were still trading more than a decade later. Survivor-only data usually overstates returns. The size of the effect here isn't measured.
- Last-traded-price exits can only capture a company that stopped trading from about 2015 in the US and 2020 elsewhere. Before that there is almost no such company in the data to exit.
- The US count for 2015-2019 (174) is well under the one for 2020-2025 (470), so the record for those years may be partial too. Not established.
- The price-table count covers the US and six other markets. The other markets weren't counted one by one; for them the delisted-company list (11 non-US symbols delisted before 2015) is the evidence.
- Whether older delistings can be loaded is being investigated. We haven't established whether they're missing at the vendor or only from our copy.

**Selection:** Companies that were acquired, went bankrupt or otherwise stopped trading from about 2015 (US) or 2020 (other markets) on are in the data, though the US record for 2015-2019 may be partial (above). In most topics such a company can be picked at any rebalance date before it stopped trading. A company that stopped trading earlier is almost never in the data and can't be picked in any year. dcf-discount, defensive-quality, pairs-zscore, pairs-multi-pair and pairs-fundamentals used to go further, filtering the universe to currently active listings (`isActivelyTrading = true`), which excluded every delisted name in every year. Their October 2026 reruns removed that filter; in these five backtests the field is now read only to break ties in the same-ISIN dedup (the current-day screens of defensive-quality and the three pairs topics still filter on it). The three pairs topics rebuild the universe each year from what was listed and traded in the formation year (see [Pairs Universe](#pairs-universe-rerun-topics)), so a name that later stopped trading is a candidate in the years it was listed. That removes the size look-ahead. Survivorship is reduced, not fixed: in the years the data doesn't cover, these topics are still measured on companies that survived. The US leg of dogs-of-dow still ranks today's Dow 30 members in every year, a survivorship bias of the kind the removed filter carried.

**Exits:** The exit price is the first price in the 10-day forward window after the exit date. Under `--exit-method ltp`, a holding with no price in that window (delisted, acquired, or a coverage gap) exits at its last traded price: the last `adjClose`, cleaned by the same oscillation filter as the topic's other prices, after its entry bar and on or before the exit date, or on or before its delisting date plus 7 days if that's earlier. A delisting more than 30 days before the entry bar belongs to an earlier holder of the ticker and is ignored. A holding with no bar in that window is still dropped, and the period return is the average of the rest. A filled exit goes through the same entry floor, return cap and costs as any other. Because the data holds almost no company that stopped trading before 2015 (US) or 2020 (other markets), a fill in an earlier year is seldom a recorded delisting. It can be a suspension, a gap in the price rows or a delisting the list doesn't hold. LTP is wired into the periodic-rebalance portfolio backtests (75 topics). The three pairs topics are different: a pair is one position, and when one leg stops trading both legs close at the last bar both traded, with the same delisting bound. pairs-fundamentals takes all three methods through a pair-level helper, where `drop` drops the pair from the year. pairs-zscore and pairs-multi-pair accept only `ltp`, so there is no drop book. Their universe is rebuilt each year (see [Pairs Universe](#pairs-universe-rerun-topics)), so a pair can hold a company that later stopped trading, within the same data gap. Event studies use their own return windows and aren't covered here.

`--exit-method drop|ltp|both` picks the rule, and `ltp` is the default. Rerun topics carry `"exit_method": "ltp"` in their results. A committed results file with no `exit_method` key used drop exits: it predates the change, until its topic is rerun, or it is one of the 13 Norway legs rerun with `--exit-method drop` in October 2026 (below). `drop` is the old rule, which drops every holding without an exit price. It doesn't reproduce old numbers, because selection changed at the same time (see [Reproducibility](#reproducibility) and the listing rules under [Universe Construction](#universe-construction)). `both` publishes the drop numbers and reports the LTP numbers beside them.

**Which results use it:** a topic rerun with last-traded-price exits writes `"exit_method": "ltp"` in its results JSON and lists every fill (symbol, entry bar, last bar, price, delisting date) under the `ltp` key. The first wave is cyclical-timing, asset-growth, fcf-conversion, cash-conversion and graham-net-net. The second wave is deleveraging, fcf-compounders, fcf-yield, revenue-accel, dividend-sustainability, price-to-sales, dogs-of-dow, low-debt, dividend-growth, pe-mean-revert, rising-yield and graham-number. The third is 52-week-high, dcf-discount, defensive-quality, pairs-fundamentals, pairs-zscore and pairs-multi-pair (the pairs topics list each pair closed at its last common bar). Those twenty-three topics' results carry `"exit_method": "ltp"` (all but pairs-multi-pair's `diversification_analysis.json`, a summary table of its US `ltp` run). A results file with no `exit_method` key used drop exits, and that's every other topic's committed results until it's rerun.

The third wave also reran the Norway (OSL) leg of 13 other topics: earnings-consistency, etf-rebalancing, etf-underowned, ev-ebitda, ev-ebitda-relative, garp, oversold-quality, pe-compression, peg-ratio, price-momentum, price-to-book, relative-strength and value-momentum. Their other markets weren't rerun and use drop exits, so the Norway leg was run with `--exit-method drop` to stay comparable with them. It ran on each topic's own code, which has no listing rule keys and, in these 13 topics, no same-ISIN dedup, so the records carry neither `exit_method` nor the listing keys. What it repaired is the period accounting: see "The Norway repairs" below.

Rules that still apply:
- Holdings with a period return above `max_single_return` are dropped (200% in most topics, 300% in small-cap, small-value and trending-value; qarp, low-pe and sector-correlation apply no cap), while losses of any size are kept. This can remove genuine acquisition premiums, filled exits included.
- Under `ltp`, a holding whose trading stopped before its price reached zero exits at its last trade, not at -100%, so a total loss is understated wherever trading stopped first.
- The delisting bound needs a delisting record. Most LSE fills have none, so there the window runs to the exit date.
- If every holding in a period ends up without an exit price or trips the return cap, the period records 0% and is counted as a cash period. Under `ltp` a period whose names all lost their exit prices can count as invested rather than cash, because fills give it holdings and the cash and invested counts read `stocks_held`.

**Direction:** Strategy-specific. Measured 2026-10-09 on eight legs with both methods in one run, on the books before the listing rules, ISIN dedup and graham-net-net's NCAV currency fix (so the graham-net-net figures describe books that no longer exist), the largest moves were -0.99pp CAGR (graham-net-net, LSE), +0.72pp (52-week-high, US) and -0.59pp (graham-net-net, US). Distress and net-net books lose, because their dead names were real losses. US momentum gains, because acquired names that sat flat through bad quarters count under LTP. History in DATA_QUALITY_ISSUES.md (Exit-side survivorship).

Measured again 2026-10-13 on the 96 market legs of the first six second-wave topics (deleveraging, fcf-compounders, fcf-yield, revenue-accel, dividend-sustainability and price-to-sales), with both methods on identical selection in one run per leg: the exit rule moved CAGR by 0.14 points or less on 93 legs. The three larger moves were revenue-accel Germany +0.45 points (7 exits filled), price-to-sales China +0.26 (6) and revenue-accel Singapore -0.21 (1, in a market too thin to test). By topic the range was -0.13 to +0.08 points for deleveraging, -0.12 to +0.14 for fcf-compounders, -0.14 to +0.09 for fcf-yield, -0.09 to +0.08 for dividend-sustainability, -0.10 to +0.26 for price-to-sales and -0.21 to +0.45 for revenue-accel. 142 exits were filled at a last traded price and 1 stayed unpriced. All 27 US fills were priced at a last bar in 2017 or later. Of the 115 outside the US, 89 were priced at one in 2021 or later. The other 26 (17 of them on China legs) have no delisting recorded at the time of the fill. They may be suspensions, gaps in the price rows or delistings the list doesn't hold; that wasn't checked symbol by symbol.

The other six second-wave topics (low-debt, dividend-growth, graham-number, pe-mean-revert, rising-yield and dogs-of-dow, 93 market legs) were measured the same way: the exit rule moved CAGR by 0.14 points or less on 86 legs. The seven larger moves were dogs-of-dow Shenzhen +0.46 points (2 exits filled, both of 000002.SZ), graham-number Singapore -0.29 and low-debt Singapore -0.27 (1 each, the same stock, HKB.SI, bought in 2024; the low-debt leg is too thin to test), dividend-growth China +0.21 (1, in a market too thin to test), pe-mean-revert Canada +0.19 (6), pe-mean-revert Hong Kong +0.19 (2) and pe-mean-revert India -0.19 (4). By topic the range was -0.27 to +0.07 points for low-debt, -0.14 to +0.21 for dividend-growth, -0.29 to +0.12 for graham-number, -0.19 to +0.19 for pe-mean-revert, -0.10 to +0.12 for rising-yield and -0.12 to +0.46 for dogs-of-dow. 230 exits were filled at a last traded price and none stayed unpriced. Of the 60 US fills, 59 were priced at a last bar in 2017 or later and one at a bar in May 2016. Of the 170 outside the US, 134 were priced at one in 2021 or later. The other 36 (26 of them on China legs) have no delisting recorded at the time of the fill, with the same possible causes, and weren't checked symbol by symbol either.

The third wave was measured the same way where a drop book exists: 52-week-high (18 legs), dcf-discount (16), defensive-quality (17) and pairs-fundamentals (15), 66 legs. The exit rule moved CAGR by 0.14 points or less on 63 of them. The three larger moves were pairs-fundamentals China +0.28 points (one pair, closed at its last common bar on 2015-12-18, when trading in one leg, China Vanke, was suspended; it wasn't a delisting, and the pair couldn't have been closed at that price), 52-week-high Japan -0.21 (15 exits filled) and dcf-discount US +0.17 (7). By topic the range was -0.21 to +0.05 points for 52-week-high, -0.10 to +0.17 for dcf-discount, -0.07 to +0.08 for defensive-quality and -0.10 to +0.28 for pairs-fundamentals. 245 exits were filled at a last traded price (176 holdings in 52-week-high, 50 in dcf-discount, 15 in defensive-quality and 4 pairs in pairs-fundamentals) and 8 stayed unpriced, all in 52-week-high. Of the 74 US fills, 68 were priced at a last bar in 2017 or later. The other six are four 2016 bars (three in 52-week-high, one in dcf-discount) and two older ones in 52-week-high: UB in November 2008, with a delisting record dated 2021, and BNI in February 2010, delisted that day, one of the 25 US delistings before 2015 that the list holds. Of the 171 fills outside the US, 149 were priced at a last bar in 2021 or later. The other 22 (17 of them on China legs) carry no delisting record, with the same possible causes. pairs-zscore and pairs-multi-pair have no drop book, so there is no exit-rule effect to measure: a pair closes at the last bar both legs traded either way.

**What moved the second wave's numbers.** The published files of its twelve topics were written between March and August 2026. They predate the entry-side cash guard (September 2026, see [Position Limits](#position-limits)), the fix to the price oscillation filter (August 2026), the deterministic tie-breaks, the listing rules and the same-ISIN dedup, and seven of the twelve also predate a selection rule of their own ([Filing and Selection Rules](#filing-and-selection-rules-rerun-topics)). The exit rule is the one change measured alone, and it moved no leg by more than 0.46 points (ranges above), so the larger moves come from the others. The files show two of those directly:
- The cash guard. In the first six topics' new files 338 of 2,023 cash periods are periods where 10 or more names passed the screen and fewer than 10 could be bought. In the other six topics' files it's 128 of 755. The published code stayed invested through periods like those, on whatever was priced. dogs-of-dow's floor is 5 names, not 10, and none of its 30 cash years is a guard year.
- The listing rules. German and Swiss legs go to cash for years. deleveraging's Germany leg is in cash for 26 of 103 quarters and its Switzerland leg for 33, from none in the published files; in 25 and 21 of those quarters fewer than 10 home-country names passed the screen. In dividend-growth the years with fewer than 10 names passing went from 7 to 16 of 25 in Germany and from 7 to 13 in Switzerland, while in its other 12 markets they're exactly the published cash years.

The selection rules are a third cause in the seven topics that have one. The result files can't isolate them, because every leg ran on the fixed code only. Where a figure below is credited to a fix, it comes from a replay of the screen with and without the fix, made on the warehouse before the rerun. A replay isn't a backtest. For the six topics rerun last (price-to-sales' rule is sized in DATA_QUALITY_ISSUES.md):
- rising-yield: the fix is the main driver. Cash years went from 65 to 227 of 425 market-years, CAGR fell in 15 of 17 markets, and nine of the 17 became too thin to test. By the replay about 123 of the 162 added cash years come from the fix and about 36 would have appeared without it. The replay modelled a draft of the fix that kept the published screen's bound on the fiscal year, so for a company with a March year-end, common in India and Japan, it tested an older fiscal year than the rerun did. Its cash years differ from the rerun's in 9 of 17 markets and its counts by up to 2 a market, and it says least about India and Japan.
- graham-number: its two rules replaced books that had been ranked on mixed units and old filings. The US leg went from 7.01% to 11.80% a year with no cash year in either file; by the fix's screen check, 71 of the 750 top-30 slots the old screen picked survive the fix, and the new file counts 2,762 cross-currency depositary-receipt line-dates dropped over the 25 rebalances. The UK leg went from 5 cash years to none now that pence-quoted lines can pass, and South Africa from 25 cash years to 7. The US figure can't be read as a stock result yet: tickers in the mutual-fund format are among the ten cheapest names its file records in every year from 2014, and no run has left funds out (DATA_QUALITY_ISSUES.md).
- pe-mean-revert: cash years went from 76 to 136 of 325 market-years. By the replay the fix replaces 760 of 5,501 top-30 slots (13.8%) and accounts for 5 of the 60 added cash years (China 2, Germany 1, Sweden 2). In the replay without the fix, 17 of the other 55 are cash-guard years and 34 fail the screen count on the German, Swiss and UK legs, where the listing rule is the evident cause. The last 4 (Taiwan 2012; Sweden 2007, 2012 and 2014) fail the screen count on legs with no listing rule and no twin removed, so they come from data revised since August or another code change, not determined which. The dedup decides none of the 60.
- low-debt: the fix mostly widens books. The US leg holds 232.4 names on average against 196.0, and its CAGR went from 7.57% to 7.64%. By the replay the fix changes the cash-year count in 3 of 15 markets (Germany 8 to 6, Canada 3 to 2, South Africa 25 to 22). The moves above 2 points (Germany -2.55, Sweden -2.68, Switzerland -5.85) come mostly from elsewhere: 40 market-years went from invested to cash across the topic, 31 on the cash guard and 9 on the screen count, all 9 on the German and Swiss legs, and Germany's book is about half its published size (22.1 names against 41.9).
- dividend-growth: by the replay the fix alone sends three market-years to cash (Germany 2024, Sweden 2023, Switzerland 2021) and none out. The moves above 2 points come mostly from the other changes: Germany's (-5.01) from the listing rule, Switzerland's (-2.38) from the listing rule and the cash guard together, Sweden's (-2.32) from the cash guard in three years and the fix in one. All three legs are now too thin to test, so those figures mostly measure years going to cash, not the screen.
- dogs-of-dow: books that were short are full. Average lines held when invested rose on all 16 non-US legs (3.6 to 9.8 in the published files, 9.3 to 10.0 now). A line now needs an entry price to take a blue-chip slot, and the new files count 29,990 line-years that took none for want of one and 73 above the size ceiling, 71 of them in Japan. Five legs moved by more than 2 points (UK +5.61, South Africa -3.77, Germany +2.94, Canada +2.73, India on BSE -2.10). On the German, Swiss and UK legs the files can't split the size rule from the listing rule and the dedup. Two of the fuller books aren't ordinary blue chips: the UK's, the largest move, holds four preference-share lines in 52 of its 250 slots, and Canada's holds preferred-share lines in 96 of 249. Four of the topic's published per-market files (Germany, Switzerland, South Africa and Saudi Arabia) were never committed to this repository; their published figures come from the same April 2026 run as the 13 that were.

Inside fully invested periods the files can't separate the dedup, the price-filter fix, the tie-breaks, a topic's selection rule and data revised since the published run, so that part of each move isn't attributed. 62 of the second wave's 189 legs are invested in fewer than half their periods and are too thin to test: 38 of the first six topics' 96, 17 of them in fcf-compounders, and 24 of the other 93 (rising-yield 9, dividend-growth 7, pe-mean-revert 5, low-debt 2, graham-number 1, dogs-of-dow none). 16 of those 24 were invested in at least half their periods in the published files and became too thin in the rerun: Germany, Sweden, Switzerland, Australia, Korea, South Africa, Singapore, Brazil and Thailand in rising-yield; Germany, Sweden and Switzerland in pe-mean-revert and again in dividend-growth; Singapore in low-debt. Four of the 24 have no invested period at all (South Africa in dividend-growth and in rising-yield, Singapore in rising-yield, France in graham-number). Each topic's README has its own numbers under "Rerun October 2026".

**What moved the third wave's numbers.** Its published files were written between March and June 2026: 52-week-high in June, dcf-discount in May (Australia and Brazil in March), defensive-quality in April (its China SHZ+SHH file in March) and the three pairs topics in March. All of them predate the entry-side cash guard, the fix to the price oscillation filter, the deterministic tie-breaks, the listing rules and the same-ISIN dedup, and each also predates its topic's own changes ([Filing and Selection Rules](#filing-and-selection-rules-rerun-topics), [Pairs Universe](#pairs-universe-rerun-topics)). Every leg ran on the fixed code only, so where a figure below is credited to one change, it comes from a replay of the screen made on the warehouse before the rerun, not from a backtest.
- 52-week-high: CAGR fell in 10 of 18 markets and rose in 8, and moved by more than 2 points in 10. The US went from 12.52% to 4.57% a year (from 4.51 points ahead of SPY to 3.45 behind), Japan from 8.43% to 2.56% (now behind the Nikkei 225), and Germany's lead over the DAX became a tie (-0.11 points). The US had no cash quarter in either file, so its move is the book. By the replay the price basis alone changes 45.4% of the invested slots across the 18 legs (76.8% in the US), and the traded-bar rule changes 4.2% of holdings, mostly fund lines and suspended shares that sat on their high. The end of the 5-day look-ahead acts on the same quarters and isn't measured. The entry-side cash guard decides 14 of the 184 cash quarters. No market is too thin to test.
- dcf-discount: of the 14 markets its comparison post covers, 10 beat their index, the US ties (+0.12 points, published +0.58), India trails, Saudi Arabia is measured against the S&P 500 in USD, and Switzerland is too thin to test, in cash in 17 of 25 years against 3 in the published file. By the replay the latest-filing rule alone takes Switzerland from 5 cash years to 12 and the cap to 17, both through the cash guard on a universe of Swiss companies only. Across the 16 legs 35 years went from invested to cash, 26 of them on the guard. CAGR moved by more than 2 points in 4 legs (Switzerland -3.84, Hong Kong -2.07, Saudi Arabia +2.44, Brazil -7.80).
- defensive-quality: 42 market-years went from invested to cash, 31 on the cash guard and 11 on the screen count, all 11 on the UK and German legs under the listing rule. The UK fell from 11.19% to 5.73% with 7 new cash years. Five of its 17 files are too thin to test: Switzerland and Malaysia newly, Taiwan, Sweden and South Africa already.
- The pairs topics: every market trails its benchmark in both vintages, except South Africa in pairs-zscore, which went from 27.94% to -0.55% against the S&P 500 in USD. Its published file held four years above +100% for a market-neutral book; the new one forms 0 to 2 pairs in 9 years and is in cash in them. CAGR moved by more than 2 points in 4 of the 15 pairs-fundamentals markets, 3 of the 12 pairs-zscore markets and 4 of the 12 pairs-multi-pair markets, and changed sign in 10 of the 15 pairs-fundamentals markets (China's change rests on the one suspension fill above). The result files can't split a move between the universe, the halved cost charge, the dollar cost tier and the other fixes, because the old files record no pairs.

10 of the third wave's 90 legs are invested in fewer than half their periods and are too thin to test: dcf-discount Switzerland; defensive-quality Taiwan, Switzerland, Sweden, Malaysia and South Africa; pairs-fundamentals South Africa, Thailand, Switzerland and Singapore. Four of them became too thin in the rerun (dcf-discount Switzerland, defensive-quality Switzerland and Malaysia, pairs-fundamentals South Africa), and three pairs-fundamentals markets that were too thin in the published files are testable now (UK, Taiwan and Germany). Each topic's README has its own numbers.

**The Norway repairs.** Norway's benchmark, the Oslo All-share index (`^OSEAX`), starts in 2013. The published Norway records of the 13 topics named above measured returns only from 2013 or 2014, while their cash counts often covered rebalances back to the topic's start (2000, or 2005 in the two ETF topics). They carried no `total_rebalances`, so the two populations couldn't be told apart, and 8 of them had a negative `invested_periods`. The new records count cash and invested periods over all rebalances, carry `total_rebalances`, and state in `window_label` and `window_truncated` that their metrics cover 2013 or 2014 to 2025. Returns are unchanged in 7 of the 13 (earnings-consistency, etf-underowned, ev-ebitda-relative, oversold-quality, pe-compression, price-momentum, value-momentum) and changed in 6, from the entry-side cash guard, the price-filter fix, the tie-breaks and data revised since the published run, not separated: garp fell from 9.24% to 3.56% a year (12 measured quarters are now guard cash), ev-ebitda rose from 11.15% to 12.83%, etf-rebalancing from 15.78% to 16.29%, price-to-book from 13.61% to 13.79% and relative-strength from 9.23% to 9.39%, and peg-ratio fell from 13.78% to 13.53%. Over all rebalances, 9 of the 13 legs are invested in fewer than half their periods and are too thin to test: earnings-consistency, etf-underowned and ev-ebitda-relative never invest, oversold-quality invests in 2 of 103 quarters, pe-compression in 2 of 25 years, garp in 14 of 103 quarters, and ev-ebitda (8 of 25 years), relative-strength (38 of 103 quarters) and value-momentum (22 of 51 half-years) are invested in most of their measured window but not in half of all their periods. etf-rebalancing, peg-ratio, price-momentum and price-to-book are testable on their truncated windows (2013 or 2014 to 2025). `^OSEAX` includes dividends, so Norway's excess needs no dividend allowance.

**Limitation:** Companies that stopped trading before 2015 (US) or 2020 (other markets) are almost entirely missing from the data, so those years are measured on survivors under either exit rule. The numbers are under "What the data holds" above.

### Look-Ahead Bias

**Status:** Partly mitigated

The fundamental screens apply a 45-day lag: at a rebalance date a screen reads only fiscal-year statements whose period ended at least 45 days earlier.

**Implementation:** Screening queries filter on the statement's period-end date (`dateEpoch`, named `filing_epoch` in the code): `filing_epoch <= rebalance_date - 45 days`. No screen reads the date a statement was filed or accepted. Earlier versions of this document said the screens used a filing date. They didn't.

**Limitation:** The lag counts from the fiscal period end, not from the filing. US annual reports are filed 60 to 90 days after the fiscal year end, and deadlines can be longer in other markets, so a screen can read figures that weren't public on the rebalance date. The rerun topics' published runs rebalance on the first day of January, April, July or October. A year that ended on December 31 is first used on April 1, 91 or 92 days later, in the quarterly and April topics, and later than that in the July and January topics. A year that ended 45 to 90 days before a rebalance is the exposed case in the US. How many holdings that affects, and what it does to a result, isn't measured. Not fixed.

### Currency Effects

**Status:** Not adjusted

Returns are denominated in the local currency of the exchange:
- US exchanges (NYSE, NASDAQ, AMEX, OTC): USD
- Indian exchanges (BSE, NSE): INR
- German exchanges (XETRA, FSX): EUR
- Chinese exchanges (SHZ, SHH): CNY
- Hong Kong (HKSE): HKD (pegged to USD 1983-present, minimal currency effect)
- Japanese exchange (JPX): JPY
- UK exchange (LSE): GBP
- Canadian exchanges (TSX, TSXV): CAD
- Australian exchange (ASX): AUD
- European exchanges: EUR or local (SEK, NOK, CHF, etc.)

**Benchmark comparison:** SPY is USD-denominated. When comparing a non-US strategy to SPY, the "excess return" includes currency appreciation/depreciation effects.

**Example (India):**
- QARP BSE returned 24% CAGR in INR (local currency)
- SPY returned 7.64% CAGR in USD
- Reported "excess" = 16.36%
- But: INR depreciated ~60% vs USD over 2000-2025
- Currency-adjusted alpha is lower (but still substantial)

**Disclosure:** All blog posts for non-USD strategies explicitly mention currency effects and provide context on currency moves over the backtest period.

**Future enhancement:** Adjust all returns to a common currency (USD) using daily FX rates (available from FRED or similar sources). This would make cross-border comparisons cleaner.

### Data Coverage Variability

**By exchange:**
- **US exchanges:** Best coverage. Comprehensive quarterly + annual filings since 1990s. SEC EDGAR data.
- **Developed markets** (Japan, UK, Germany, Australia, Canada): Good coverage. Mostly annual filings, some quarterly. Standards vary.
- **Emerging markets** (India, China, Brazil, Southeast Asia): Improving coverage. Gaps exist pre-2010. Cash flow statements sometimes missing.
- **Frontier markets:** Sparse. Often missing cash flow, incomplete balance sheets, or annual-only data.

**By financial statement type:**
- Income statements: ~90% coverage for market cap > $100M
- Balance sheets: ~90% coverage
- Cash flow statements: ~75% coverage (especially lower for non-US)

**Impact:** Strategies requiring cash flow statements (Piotroski F-Score, QARP's income quality metric) may have smaller universes on emerging/frontier exchanges.

### Rebalancing Execution Assumptions

**Price:** Assumes fills at closing price on rebalance date. No slippage beyond modeled transaction costs.

**Liquidity:** Does not model market impact beyond tiered transaction costs. A large portfolio ($100M+) buying 30 small-cap stocks would face additional slippage in practice. Our transaction cost tiers (0.1-0.5%) are conservative estimates for retail-scale trading.

**Dividends:** Included via `adjClose` field in `historical_price_full`. Dividends are assumed reinvested at the ex-dividend date closing price (implicitly captured by the adjustment factor).

**Corporate actions:** Splits, reverse splits, spinoffs handled via `adjClose` price adjustments, assuming continuity of ownership. A holding that's acquired or delisted mid-period isn't carried through: by default (`--exit-method ltp`) it exits at its last traded price, or is dropped if it has no bar to exit at; results produced before a topic's rerun, and the 13 Norway legs rerun in drop mode, dropped it from that period (see [Survivorship Bias](#survivorship-bias)).

### Financial Statement Timing

**Fiscal year heterogeneity:** Companies use different fiscal year ends (calendar year, fiscal year ending March/June/Sept, etc.). The backtest dates each FY statement by its period end and makes it usable 45 days later (see [Look-Ahead Bias](#look-ahead-bias)), so how fresh a company's figures are at a rebalance depends on its year-end.

**Restatements:** If a company restates historical financials (corrections, accounting changes), FMP updates the historical data in the warehouse. Backtests use the current (restated) data, which may differ from what investors saw in real-time. This is a limitation shared by all retrospective backtests and generally improves data quality (corrects errors).

**Quarterly vs annual:** Most strategies use annual fiscal year (FY) statements only. This ensures global consistency (most non-US companies don't file quarterly) but means the figures a screen reads are old: at least 45 days past the period end, and up to about 410 days (a year plus the 45-day lag) for a company that reports every year. In the topics with the latest-filing rule the limit is 540 days.

### Price Data Quality (FMP EOD Anomalies)

**Status:** Known issue, mitigation in progress

FMP's EOD price data contains phantom rows on non-trading days (exchange holidays, ex-dividend dates) for some symbols. These rows have identical OHLC values (open = high = low = close, indicating no real trading occurred) and incorrect split/dividend adjustment factors. The pattern: a stock's normal split factor (e.g., 3.37x) flips to ~1.0x on the phantom day, then reverts the next trading day. This creates false price spikes of 50-400%+ followed by equivalent reversals.

**Scope (as of April 2026):** ~2,500 symbols across all 19 exchanges. Most severe on LSE (252 symbols, 30,268 affected days, concentrated around UK bank holidays in 2004-2005), NASDAQ (821 symbols), and JNB/Johannesburg (46 symbols). US large-cap occurrences are mostly isolated to 1-2 days in 1970s-1980s data. Affected symbols include blue chips (ExxonMobil, Barclays, Rio Tinto, Wipro).

**Root cause:** FMP inserts rows for dates when the exchange was closed (holidays, special closures). These rows carry unadjusted prices rather than the correctly adjusted values used on actual trading days. Confirmed by querying FMP's `/stable/eod-bulk` API directly.

**Impact on backtests:**
- US (2000+): Minimal. Most US flips are outside typical backtest windows. The 200% per-period return cap provides additional protection.
- LSE/UK: Moderate. ~100 FTSE constituents have affected days in 2004-2005.
- JNB/South Africa: Severe. Thin universe + widespread errors. SA excluded from published content.

**Mitigation (backtest layer):** Backtests should exclude symbols with any day where `open = high = low = close` AND the day-over-day price ratio exceeds 5x, within the backtest period. This removes phantom holiday rows while preserving legitimate low-volatility days (which have non-zero intraday ranges).

**Mitigation (warehouse layer):** The data pipeline cleanup worker detects and removes rows with extreme day-over-day ratios (>5x threshold).

### Benchmark Limitations

**SPY for non-US:** The S&P 500 (SPY) is used as the primary benchmark for all strategies, even those on non-US exchanges. This provides consistency but introduces two issues:
1. **Currency mismatch** (documented above)
2. **Apples-to-oranges comparison:** SPY represents large-cap US growth, which may not be comparable to, say, Chinese value stocks or Indian mid-caps

**Mitigation:** Regional benchmarks (EWJ, INDA, etc.) provide a more appropriate comparison for non-US strategies. Both comparisons are reported.

**Dividends in local index benchmarks:** Topics that compare a market with its own index use `data_utils.LOCAL_INDEX_BENCHMARKS`. Four of those series are total return, like the portfolio's dividend-adjusted `adjClose`: SPY, the DAX (`^GDAXI`), the Ibovespa (`^BVSP`) and the Oslo All-share GR (`^OSEAX`). The rest (Sensex, FTSE 100, Nikkei 225, Hang Seng, KOSPI, TAIEX, SSE Composite, OMX Stockholm 30, SMI, SET, TSX Composite, STI, ASX 200) are price indices that leave dividends out, so excess return against them is overstated by roughly the index's dividend yield.

---

## Reproducibility

To reproduce any result in this repository:

### Steps

1. **Get API key:** Sign up at [cetaresearch.com](https://cetaresearch.com) and create an API key at Settings > API Keys
2. **Clone repo:** `git clone https://github.com/ceta-research/backtests.git && cd backtests`
3. **Install dependencies:** `pip install -r requirements.txt` (requires Python 3.8+)
4. **Set API key:** `export CR_API_KEY="your_key_here"`
5. **Run backtest:** `python3 qarp/backtest.py` (or any other strategy)

**Determinism:** Since 2026-10-10, two runs on the same data select the same holdings and produce the same results:
- Every ordering in the selection path is a total order. Top-N cuts end on the symbol (ascending), and latest-row picks break ties on the values they go on to read.
- Date bounds are built at UTC midnight, the same as the price bars.

Before that, neither held. The SQL engine runs in parallel and broke ties differently from run to run: twin listings with identical fundamentals tied at the top-N cut, and duplicate filings tied in latest-row picks. Date bounds were built from the machine's local time, so which bars fell inside a window moved with its timezone and daylight saving. On 28 legs run twice on identical data, 4 differed, by up to 0.58pp CAGR, and one flipped a whole year between cash and invested. Committed results produced before 2026-10-10 can carry that noise until rerun.

**Data versioning:** Financial data in Ceta Research updates nightly as companies file new reports or restate historical data. To guarantee exact reproduction:
- Note the date you ran the backtest
- Results may vary slightly if run on a different date and companies have restated financials in the interim
- For academic research, snapshot the data or document the date

**Code versioning:** Each blog post references a specific git commit SHA. Check out that commit for exact code match.

### Expected Variations

Runs on different dates can differ because of:
- Financial restatements by companies
- Exchange membership changes (stocks moving between NASDAQ/NYSE)
- Price data corrections

These are unavoidable in live data and do not indicate methodology issues. They grow with the time between runs. A difference between two runs on the same day and the same code means the data changed between the fetches.

---

## References

**Academic papers:**
- Piotroski, J. (2000). "Value Investing: The Use of Historical Financial Statement Information to Separate Winners from Losers." *Journal of Accounting Research*, 38 (Supplement), 1-41.
- Basu, S. (1977). "Investment Performance of Common Stocks in Relation to Their Price-Earnings Ratios: A Test of the Efficient Market Hypothesis." *Journal of Financial Economics*, 12(3), 129-156.
- Fama, E. F., & French, K. R. (1992). "The Cross-Section of Expected Stock Returns." *Journal of Finance*, 47(2), 427-465.
- Lakonishok, J., Shleifer, A., & Vishny, R. W. (1994). "Contrarian Investment, Extrapolation, and Risk." *Journal of Finance*, 49(5), 1541-1578.

**Industry research:**
- Vanguard (2025). "Best Practices for Portfolio Rebalancing: Frequency and Threshold Analysis."
- BlackRock (2024). "Factor Investing: Performance Attribution and Risk Management."

**Metrics methodology:**
- LuxAlgo (2024). "Top 7 Metrics for Backtesting Results." https://www.luxalgo.com/blog/top-7-metrics-for-backtesting-results/
- QuantStart (2023). "Sharpe Ratio for Algorithmic Trading Performance Measurement." https://www.quantstart.com/articles/Sharpe-Ratio-for-Algorithmic-Trading-Performance-Measurement/
- TrendSpider (2024). "Advanced Backtesting Metrics: Sortino, Calmar, and Information Ratio." https://trendspider.com/learning-center/advanced-backtesting-metrics/

**Data sources:**
- Financial Modeling Prep (FMP) financial data: https://financialmodelingprep.com/
- Ceta Research SQL API: https://cetaresearch.com/docs/api
- FRED (Federal Reserve Economic Data): https://fred.stlouisfed.org/ (for future T-bill integration)
