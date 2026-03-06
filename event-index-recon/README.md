# Index Reconstitution Event Study

**Type:** Event study (not a portfolio screener)
**Metric:** Cumulative Abnormal Return (CAR) vs benchmark
**Indices:** S&P 500 (SPY benchmark), NASDAQ-100 (QQQ benchmark)
**Period:** 2000–2025
**Events:** 679 S&P 500 events (465 additions, 214 removals) + 406 NDX events (237 additions, 169 removals)

---

## What This Measures

When stocks enter or exit a major index, passive funds must buy or sell regardless of price. That forced trading creates predictable price pressure.

- **Additions:** Funds buy the new member. Speculators front-run the announcement, so prices often peak at or before inclusion and drift lower after.
- **Removals:** Funds sell the removed stock. Prices drop on forced selling, then partially recover as selling pressure abates.

This study measures cumulative abnormal return (CAR) at T+1, T+5, T+21, and T+63 trading days after the event date.

---

## Results Summary (2000–2025)

### S&P 500 Additions (N=465) — Significant

| Window | Mean CAR | t-stat | Sig? |
|--------|----------|--------|------|
| T+1    | -0.14%   | -0.92  |      |
| T+5    | **-0.98%**   | **-3.68**  | **  |
| T+21   | **-1.06%**   | **-2.11**  | *   |
| T+63   | -1.23%   | -1.56  |      |

### S&P 500 Removals (N=191–214) — Noisy (outlier-driven)

| Window | Mean CAR | Median CAR | t-stat | Sig? |
|--------|----------|------------|--------|------|
| T+5    | +3.72%   | -0.15%     | 0.97   |      |
| T+21   | +7.22%   | +0.73%     | 1.95   |      |
| T+63   | +8.60%   | -0.19%     | 2.16   | *    |

Mean is inflated by 4 extreme outliers (SOV +591%, PCG +173%, NCR +115%, OI +110%). Winsorized mean = +2.10%. The median (+0.73%) is the most honest estimate.

### NASDAQ-100 Additions (N=237) — Weak

| Window | Mean CAR | t-stat | Sig? |
|--------|----------|--------|------|
| T+5    | -0.76%   | -1.99  | *    |
| T+21   | +0.19%   | 0.25   |      |

### NASDAQ-100 Removals (N=163–169) — Cleanest finding

| Window | Mean CAR | Median CAR | t-stat | Sig? |
|--------|----------|------------|--------|------|
| T+5    | +0.83%   | 0.01%      | 1.06   |      |
| T+21   | **+5.13%**   | **+2.61%**     | **3.29**   | **   |
| T+63   | **+7.41%**   | **+6.40%**     | **3.17**   | **   |

Consistent across years. Only 1 outlier >100%. The NASDAQ-100 removal effect is the strongest, cleanest result in this dataset.

---

## Data Note: Critical Bug in Older Code

The `date` column in all three constituent tables (`historical_sp500_constituent`, `historical_nasdaq_constituent`, `historical_dowjones_constituent`) is an FMP snapshot date (e.g., 2025-11-30 for all rows), **not** the historical change date.

The correct event date for **both additions and removals** is:
```sql
TRY_STRPTIME(dateAdded, '%B %d, %Y')
```

Removals use the same `dateAdded` as the corresponding addition row, because the removed stock was replaced on the same date its replacement was added.

Old code using `CAST(date AS DATE)` for removals produces 5,050 events all clustered in 2025–2026. Those results are invalid.

---

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Full event study: CAR + portfolio simulation for S&P 500 and/or NASDAQ-100 |
| `screen.py` | Live screen: recent additions/removals with current return vs benchmark |
| `generate_charts.py` | Generate PNG charts from backtest results |
| `results/` | Output from backtest.py (authoritative) |
| `results/results_SP500.json` | S&P 500 CAR summary + portfolio metrics |
| `results/results_NDX.json` | NASDAQ-100 CAR summary + portfolio metrics |
| `results/index_comparison.json` | Cross-index comparison |
| `results/event_returns_SP500.csv` | Event-level returns (696 rows) |
| `results/event_returns_NDX.csv` | Event-level returns (406 rows) |

---

## Run the Backtest

```bash
cd backtests

# S&P 500
python3 event-index-recon/backtest.py

# NASDAQ-100
python3 event-index-recon/backtest.py --index nasdaq100

# Both (saves to results/)
python3 event-index-recon/backtest.py --global --verbose
```

## Run the Screen

```bash
cd backtests

# Recent S&P 500 changes (last 90 days)
python3 event-index-recon/screen.py

# Both indices, last 180 days
python3 event-index-recon/screen.py --global --days 180
```

## Generate Charts

```bash
cd backtests
python3 event-index-recon/generate_charts.py
# Charts saved to event-index-recon/charts/
```

Requires: `pip install matplotlib numpy`

---

## Academic Background

- **Chen, Noronha & Singal (2004)** — "The Price Response to S&P 500 Index Additions and Deletions: Evidence of Asymmetry and a New Explanation." *Journal of Finance.*
- **Shleifer (1986)** — downward-sloping demand curves for stocks; index addition creates permanent price pressure.
- **Harris & Gurel (1986)** — temporary price pressure hypothesis.

The negative addition effect is consistent with Shleifer's downward-sloping demand: index funds must buy, pushing prices up, which then partially revert. The removal recovery is consistent with overshooting: forced selling pushes prices below fundamental value.

---

## Data Source

All data via [Ceta Research](https://cetaresearch.com) data warehouse (FMP financial data).

Tables used:
- `historical_sp500_constituent` — S&P 500 constituent history
- `historical_nasdaq_constituent` — NASDAQ-100 constituent history
- `stock_eod` — daily adjusted close prices
