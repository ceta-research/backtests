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

**Determinism:** Same data + same parameters = same results. All code is open source. Any user with a Ceta Research API key can reproduce our findings.

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

**Market capitalization filter:**
- Strategy-specific minimum, typically $100M - $1B
- Prevents micro-cap illiquidity and data quality issues
- Applied at each rebalance date using trailing-twelve-month (TTM) market cap

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
   - Must have a price on or within 10 days of each rebalance date (uses first available price in window)

**Survivorship bias:** Mitigated by including delisted stocks. FMP maintains historical financial data for companies that were acquired, went bankrupt, or otherwise stopped trading. Backtest portfolios include companies that "didn't survive," capturing the full distribution of outcomes.

**Look-ahead bias:** Mitigated by using `filing_date` (when the financial statement was filed with regulators) rather than `period_end_date` (when the fiscal period ended). For US companies, 10-K/10-Q filings occur 60-90 days after period end. Using `filing_date` ensures we only use information publicly available to investors at the rebalance date. Screening queries filter `WHERE filing_date <= rebalance_date - 45 days`.

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

**Cash rule:** If fewer than `min_stocks` qualify at a rebalance date, the portfolio holds cash (0% return) for that period. This prevents under-diversified concentrated bets that violate the strategy's risk profile.

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

**Status:** Mitigated (but not eliminated)

FMP data includes delisted stocks with historical financial statements. When a company is acquired, goes bankrupt, or otherwise stops trading, its historical data remains in the warehouse. Backtest portfolios include companies that "didn't survive," capturing the full distribution of outcomes (winners and losers).

**Limitation:** Very old delistings (pre-1990) may have incomplete financial statement history. The earlier the backtest start date, the higher the potential survivorship bias.

**Impact:** Minimal for backtests starting 2000+. Moderate for 1985+ backtests.

### Look-Ahead Bias

**Status:** Mitigated

All backtests use `filing_date` (when the financial statement was publicly filed with regulators) rather than `period_end_date` (when the fiscal period ended). For US companies, 10-K annual filings occur 60-90 days after fiscal year end. Using `filing_date` ensures we only use information that was publicly available to investors at the rebalance date.

**Implementation:** Screening queries filter `WHERE filing_date <= rebalance_date - 45 days` to allow time for data to propagate to investors.

**Limitation:** Filing date metadata precision varies by exchange. Some international exchanges have less reliable filing dates or delayed reporting. This may introduce small timing errors.

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

**Corporate actions:** Splits, reverse splits, spinoffs, mergers handled automatically via `adjClose` price adjustments. Assumes continuity of ownership through corporate actions.

### Financial Statement Timing

**Fiscal year heterogeneity:** Companies use different fiscal year ends (calendar year, fiscal year ending March/June/Sept, etc.). The backtest treats all FY statements as occurring at their `filing_date`, which varies by company. This introduces timing noise but reflects real-world information arrival patterns.

**Restatements:** If a company restates historical financials (corrections, accounting changes), FMP updates the historical data in the warehouse. Backtests use the current (restated) data, which may differ from what investors saw in real-time. This is a limitation shared by all retrospective backtests and generally improves data quality (corrects errors).

**Quarterly vs annual:** Most strategies use annual fiscal year (FY) statements only. This ensures global consistency (most non-US companies don't file quarterly) but means signals can be up to 15 months stale in the worst case (fiscal year ended 12 months ago + 90-day filing delay + waited 45 days for next rebalance).

### Benchmark Limitations

**SPY for non-US:** The S&P 500 (SPY) is used as the primary benchmark for all strategies, even those on non-US exchanges. This provides consistency but introduces two issues:
1. **Currency mismatch** (documented above)
2. **Apples-to-oranges comparison:** SPY represents large-cap US growth, which may not be comparable to, say, Chinese value stocks or Indian mid-caps

**Mitigation:** Regional benchmarks (EWJ, INDA, etc.) provide a more appropriate comparison for non-US strategies. Both comparisons are reported.

---

## Reproducibility

To reproduce any result in this repository:

### Steps

1. **Get API key:** Sign up at [cetaresearch.com](https://cetaresearch.com) and create an API key at Settings > API Keys
2. **Clone repo:** `git clone https://github.com/tradingstudio-hq/ts-backtests.git && cd ts-backtests`
3. **Install dependencies:** `pip install -r requirements.txt` (requires Python 3.8+)
4. **Set API key:** `export CR_API_KEY="your_key_here"`
5. **Run backtest:** `python3 qarp/backtest.py` (or any other strategy)

**Determinism:** All randomness eliminated. Portfolio holdings are fully deterministic based on:
- SQL `ORDER BY` clauses (stable sort order)
- Tie-breaking: If two stocks have identical scores, SQL engine sorts by symbol alphabetically (stable)

**Data versioning:** Financial data in Ceta Research updates nightly as companies file new reports or restate historical data. To guarantee exact reproduction:
- Note the date you ran the backtest
- Results may vary slightly if run on a different date and companies have restated financials in the interim
- For academic research, snapshot the data or document the date

**Code versioning:** Each blog post references a specific git commit SHA. Check out that commit for exact code match.

### Expected Variations

Minor variations (<0.1% CAGR) may occur due to:
- Financial restatements by companies
- Exchange membership changes (stocks moving between NASDAQ/NYSE)
- Price data corrections

These are unavoidable in live data and do not indicate methodology issues.

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
