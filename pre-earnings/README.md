# Pre-Earnings Announcement Runup

**Type:** Event Study
**Signal:** Cumulative Abnormal Return (CAR) in T-10, T-5, T-1 trading days before earnings
**Dataset:** FMP earnings surprises (historical), stock_eod (prices)
**Period:** 2000-2025

---

## What This Measures

Stocks tend to drift upward in the days before earnings announcements. This is the "earnings announcement premium," first documented by Barber, De George, Lehavy & Trueman (2013). The effect is strongest for companies with a history of beating analyst estimates.

This event study measures:
- **Pre-event returns**: from T-10, T-5, T-1 to announcement (T=0)
- **Announcement day** (T+1): for comparison with PEAD
- **Stratification by historical beat rate**: habitual beaters vs missers vs mixed

This is an event study — results are CARs and t-statistics, not CAGR/Sharpe.

---

## Strategy Card

```yaml
strategy:
  name: Pre-Earnings Announcement Runup
  slug: pre-earnings
  description: Measures abnormal returns in the window before earnings announcements,
    stratified by each stock's historical beat rate.
  academic_reference: |
    Barber, De George, Lehavy & Trueman (2013) "The Earnings Announcement Premium
    and Trading Volume", Journal of Accounting Research 51(1).
    So & Wang (2014) "News-Driven Return Reversals: Liquidity Provision Ahead of
    Earnings Announcements", Journal of Financial Economics 114(1).

signal:
  event: earnings announcement date (from earnings_surprises table)
  pre_windows: [-10, -5, -1]  # trading days before T=0
  post_windows: [1]           # T+1 = announcement day reaction
  categories:
    - habitual_beater: beat_rate > 75%, >= 8 prior quarters
    - mixed: beat_rate 25-75%, >= 4 prior quarters
    - habitual_misser: beat_rate < 25%, >= 8 prior quarters
  beat_rate: point-in-time (only prior events counted for each event)

data:
  events: earnings_surprises (epsActual, epsEstimated, date columns)
  prices: stock_eod (adjClose)
  market_cap: key_metrics FY (historical, matched to event date)
  benchmark: SPY (US) or regional ETF
  filters:
    - |epsEstimated| > $0.01
    - min 4 prior reports for classification
    - market cap > exchange-specific threshold

universe_standard: full exchange universe, not index-constrained
```

---

## Usage

```bash
# US event study (default)
python3 pre-earnings/backtest.py

# Specific exchange
python3 pre-earnings/backtest.py --preset india
python3 pre-earnings/backtest.py --preset canada

# All exchanges (takes 30-60 min)
python3 pre-earnings/backtest.py --global --output results/exchange_comparison.json --verbose

# Screen for upcoming earnings with beat rate
python3 pre-earnings/screen.py
python3 pre-earnings/screen.py --category habitual_beater
python3 pre-earnings/screen.py --days 21

# Generate charts (after running backtest)
python3 pre-earnings/generate_charts.py --comparison results/exchange_comparison.json
```

---

## Key Findings (US, 2000-2025)

| Category | T-10 CAR | T-5 CAR | T-1 CAR | T+1 CAR |
|----------|----------|---------|---------|---------|
| Overall | +0.23% | +0.22% | +0.10% | +0.05% |
| Habitual Beater | +0.62% | +0.41% | +0.25% | +0.14% |
| Mixed | +0.21% | +0.21% | +0.08% | +0.06% |
| Habitual Misser | -0.09% | +0.11% | +0.07% | -0.16% |

Statistical significance (t-stats): T-10 overall t=6.27**, habitual beater t=5.37**

---

## Methodology Notes

- **Point-in-time beat rates**: For each event at date T, only events before T count toward that symbol's beat rate. This prevents look-ahead bias.
- **Winsorization**: Returns winsorized at 1st/99th percentile to reduce outlier influence on means.
- **Benchmark**: SPY for US; iShares country ETFs for international (see REGIONAL_BENCHMARKS in data_utils.py).
- **Market cap**: Exchange-specific thresholds (see cli_utils.py MKTCAP_THRESHOLD_MAP).
- **Deduplication**: Multiple records for the same symbol/date are deduplicated before analysis.
- **No transaction costs**: This is an event study measuring population drift, not a trading strategy backtest.

---

## Data Source

Data: Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md).
