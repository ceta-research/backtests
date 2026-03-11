# Pairs Trading: Multi-Pair Portfolio Construction

**Strategy ID:** pairs-06-multi-pair
**Series:** Pairs Trading (pairs-01 through pairs-06)
**Status:** Final chapter — portfolio construction layer on top of pairs-zscore

---

## What This Strategy Does

Pairs-05 (pairs-zscore) showed that z-score mean reversion works on individual pairs. But running a single pair at a time produces lumpy, high-variance returns. This strategy answers the next question: how does building a portfolio of simultaneous pairs change the risk-return profile, and which allocation method works better?

The pair-finding logic is identical to pairs-zscore. The new layer is portfolio construction:

- **Formation:** Each year, rank all same-sector pairs by 252-day returns correlation. Apply half-life filter (AR(1) half-life 5–60 days). Keep the top N.
- **Trading:** For each selected pair, run the daily z-score strategy (enter at |z| > 2.0, exit at |z| < 0.5 or 60-day stop).
- **Allocation:** Weight pairs by equal weight or inverse-volatility (1 / annualised spread return vol from the formation period).
- **Portfolio sizes tested:** N = 5, 10, 15, 20 simultaneous pairs.

The key finding: going from 5 to 15 pairs roughly halves the maximum drawdown. Beyond 15, returns stabilise. Inverse-volatility allocation consistently improves Sharpe ratio versus equal weight at every portfolio size.

---

## Signal

| Parameter | Value |
|---|---|
| Minimum correlation | 0.70 (252-day formation period) |
| Minimum common days | 200 |
| Half-life filter | AR(1) half-life 5–60 days |
| Z-score entry | \|z\| > 2.0 (40-day rolling window) |
| Z-score exit | \|z\| < 0.5 |
| Time stop | 60 trading days |
| Loss stop | -5% pair P&L |
| Min active pairs | 3 (cash period if fewer pairs trade) |

---

## Portfolio Construction

**Equal weight:** Each of the N pairs gets weight 1/N. Pairs with no trades contribute 0 to the portfolio return but still count in the denominator — this naturally penalises years where the strategy fires infrequently.

**Inverse-volatility:** Weight each pair by 1 / spread_vol_ann, where spread_vol_ann is the annualised standard deviation of daily spread changes (log spread = log(P_A) - β × log(P_B)) during the formation period. Pairs with missing vol data fall back to equal weight. Normalised so weights sum to 1.

**Sector cap (soft):** No pair universe hard-caps sectors, but the screen tool marks pairs that push any sector over 3 pairs in the portfolio. Over-concentration in a single sector defeats the diversification purpose.

**Pair replacement:** Formation runs annually (Jan 1). If a selected pair lacks prices at trading start or either leg is below $1.00, it is skipped. The next-ranked pair does not replace it — the portfolio runs with however many pairs pass.

---

## Rebalancing

- **Pair formation:** Annual (previous calendar year).
- **Position monitoring:** Daily z-score calculation; continuous entry/exit.
- **Portfolio rebalancing:** Once per year (formation period boundaries).

---

## Backtest Details

- **Period:** 2005–2024 (20 years)
- **Exchanges:** 12 (US, Japan, Canada, Hong Kong, China, Korea, Taiwan, Sweden, South Africa, India, UK, Germany)
- **Primary output:** 20-pair inverse-vol configuration
- **Benchmark:** SPY (S&P 500 ETF)
- **Transaction costs:** Size-tiered, 4 legs per trade (entry and exit for both legs)
- **Data:** FMP warehouse via Ceta Research API

---

## Key Finding

Single-pair trading is high-variance. Running 5 pairs simultaneously cuts maximum drawdown significantly versus a single pair. The curve flattens around 15–20 pairs: each additional pair beyond 15 provides diminishing diversification benefit.

Inverse-volatility allocation outperforms equal weight at every portfolio size on a risk-adjusted basis. The improvement is largest at small N (5–10 pairs), where a single volatile pair can dominate an equal-weight portfolio.

---

## Usage

**Run the backtest (US, default):**
```bash
python3 pairs-multi-pair/backtest.py --verbose \
    --output pairs-multi-pair/results/exchange_comparison.json
```

**Run for a specific exchange:**
```bash
python3 pairs-multi-pair/backtest.py --preset india \
    --output pairs-multi-pair/results/exchange_comparison.json --verbose
```

**Run all exchanges (global mode):**
```bash
python3 pairs-multi-pair/backtest.py --global \
    --output pairs-multi-pair/results/exchange_comparison.json --verbose
```

**Run without transaction costs (academic baseline):**
```bash
python3 pairs-multi-pair/backtest.py --preset us --no-costs --verbose
```

**Screen current signals (US):**
```bash
python3 pairs-multi-pair/screen.py
```

**Screen with more pairs:**
```bash
python3 pairs-multi-pair/screen.py --preset japan --top-n 30
```

**Generate charts:**
```bash
python3 pairs-multi-pair/generate_charts.py
```

---

## Output Files

| File | Description |
|---|---|
| `results/exchange_comparison.json` | Standard metrics (primary: 20-pair inv-vol) + `diversification_analysis` key |
| `results/diversification_analysis.json` | Standalone per-size/allocation breakdown (N × allocation combinations) |
| `charts/1_us_cumulative_growth.png` | $1,000 growth: inv-vol vs equal vs SPY |
| `charts/2_us_annual_returns.png` | Annual return bars vs SPY |
| `charts/3_diversification_curve.png` | Sharpe and MaxDD vs N (5/10/15/20) |

---

## Relationship to Other Pairs Strategies

| Strategy | What It Covers |
|---|---|
| pairs-01-screening | Pair identification: which stocks move together |
| pairs-02-cointegration | Statistical tests for mean reversion (ADF, half-life) |
| pairs-03-fundamentals | Fundamental filters on top of statistical pairs |
| pairs-04-cointegration (backtest) | Single-pair Bollinger-band entry, annual formation |
| pairs-05-zscore | Single-pair daily z-score signal |
| **pairs-06-multi-pair** | **Portfolio of N simultaneous pairs; diversification analysis** |

---

## Academic Reference

Gatev, E., Goetzmann, W. & Rouwenhorst, K. (2006). "Pairs Trading: Performance of a Relative-Value Arbitrage Rule." *Review of Financial Studies*, 19(3), 797–827.
