# Earnings Growth Consistency

Screen for companies that grew net income every year for 3 consecutive years. Consistent earners attract stable institutional ownership, command lower risk premiums, and tend to sustain their earnings trajectory.

## Strategy

**Signal:** Net income grew year-over-year in each of the last 3 fiscal years, positive in all 4 periods, ROE > 8%, D/E < 2.0, market cap above exchange threshold.

**Selection:** Top 30 by ROE, equal weight.

**Rebalancing:** Annual (July). Annual frequency matches the fiscal year data cadence. July rebalance allows a 45-day lag from December/March FY-end filings.

**Universe:** Full exchange universe (not index-constrained). Backtested 2000–2025 across 17 exchanges.

## Signal Logic

```sql
-- 3-year earnings growth streak
WHERE y1.netIncome > y2.netIncome   -- Year 0 > Year 1
  AND y2.netIncome > y3.netIncome   -- Year 1 > Year 2
  AND y3.netIncome > y4.netIncome   -- Year 2 > Year 3
  AND y4.netIncome > 0              -- All periods profitable
```

## Usage

```bash
# Run US backtest
python3 earnings-consistency/backtest.py --verbose

# Run on India
python3 earnings-consistency/backtest.py --preset india --verbose

# Run all exchanges (saves exchange_comparison.json)
python3 earnings-consistency/backtest.py --global --output results/exchange_comparison.json --verbose

# Current stock screen (live data)
python3 earnings-consistency/screen.py
python3 earnings-consistency/screen.py --preset india

# Generate charts (after running --global)
python3 earnings-consistency/generate_charts.py
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000–2025) |
| `screen.py` | Current qualifying stocks using TTM + FY data |
| `generate_charts.py` | Chart generation from results |
| `results/exchange_comparison.json` | Multi-exchange backtest results |
| `charts/` | Generated PNGs (gitignored) |

## Academic Basis

- **Dichev & Tang (2009)** — "Earnings Volatility and Earnings Predictability." *Journal of Accounting and Economics.* Documents that earnings consistency (low volatility, steady growth) predicts future earnings and is associated with lower cost of capital.
- **Sloan (1996)** — "Do Stock Prices Fully Reflect Information in Accruals and Cash Flows About Future Earnings?" *The Accounting Review.* Higher earnings quality (persistent, cash-backed earnings) predicts positive future returns.
- **Chan, Karceski & Lakonishok (2003)** — "The Level and Persistence of Growth Rates." *Journal of Finance.* Finds that past earnings growth has limited ability to predict future growth — making *consistently* growing companies a rare and valuable subset.

## Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Streak length | 3 years | 4 data points needed; shorter is noisy, longer = too few stocks |
| ROE minimum | 8% | Quality floor; screens out low-margin pass-throughs |
| D/E maximum | 2.0 | Manageable leverage; growth companies may carry moderate debt |
| Max stocks | 30 | Concentrated enough for factor signal, diversified enough for stability |
| Rebalance | Annual (July) | Matches FY filing cadence with 45-day lag buffer |
| Costs | Size-tiered | Per `costs.py` model |

## Data Notes

- **Point-in-time:** Filing date used, not fiscal year end, with 45-day lag to simulate real-world availability.
- **Stale cutoff:** FY filings older than 5 years are ignored (avoids zombie symbols with no recent filings).
- **Survivorship:** FMP data includes delisted companies. Results are not survivorship-biased.
- **ASX/SAO excluded:** Adjusted close price artifacts per `DATA_QUALITY_ISSUES.md`.
