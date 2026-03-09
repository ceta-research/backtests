# M&A Announcement Return Patterns

**Category:** Event Study
**Data:** FMP `mergers_acquisitions_latest` (SEC-sourced M&A filings)
**Universe:** US stocks (NYSE/NASDAQ/AMEX), market cap > $1B
**Period:** 2000–2025
**Benchmark:** SPY

---

## What This Measures

This event study measures cumulative abnormal returns (CAR) for two groups after M&A deal announcements:

- **Target companies** (`targetedSymbol`): the company being acquired
- **Acquirer companies** (`symbol`): the company doing the acquiring

Each deal creates two events analyzed separately. Abnormal return = stock return minus SPY return over the same window.

Windows measured: T+1, T+5, T+21, T+63 trading days post-announcement.

---

## Data Notes

**Source:** FMP `mergers_acquisitions_latest` — SEC deal filing data.

**Important limitations:**
1. No deal price, premium, or deal type (cash/stock) in the data
2. `transactionDate` is the SEC filing date, which may differ from the press announcement date
3. Coverage is selective: not all public M&A deals appear in this dataset
4. Targets with no US price data (private companies, foreign companies) are excluded

**Deduplication:** Each deal generates multiple filings (average ~2.9 per deal, from different share classes). We deduplicate to one event per (symbol, transactionDate) pair.

---

## Screening for Current M&A Activity

```sql
-- Recent M&A deals (last 90 days)
WITH recent AS (
    SELECT
        symbol AS acquirer,
        targetedSymbol AS target,
        companyName AS acquirer_name,
        targetedCompanyName AS target_name,
        CAST(transactionDate AS DATE) AS deal_date,
        ROW_NUMBER() OVER (
            PARTITION BY targetedSymbol, CAST(transactionDate AS DATE)
            ORDER BY acceptedDate DESC
        ) AS rn
    FROM mergers_acquisitions_latest
    WHERE CAST(transactionDate AS DATE) >= CURRENT_DATE - INTERVAL '90' DAY
      AND targetedSymbol IS NOT NULL AND TRIM(targetedSymbol) != ''
)
SELECT acquirer, target, acquirer_name, target_name, deal_date
FROM recent
WHERE rn = 1
ORDER BY deal_date DESC
LIMIT 30
```

---

## Running the Backtest

```bash
# From the backtests/ directory

# Default run (US, $1B+ market cap)
python3 ma-arbitrage/backtest.py --output ma-arbitrage/results --verbose

# Lower market cap threshold ($500M)
python3 ma-arbitrage/backtest.py --min-mktcap 500000000 --output ma-arbitrage/results --verbose

# Specific date range (post-2010 only)
python3 ma-arbitrage/backtest.py --start-year 2010 --output ma-arbitrage/results --verbose

# Generate charts (after backtest)
python3 ma-arbitrage/generate_charts.py

# Run current screen
python3 ma-arbitrage/screen.py --days 90
```

---

## Results

Results are saved to `ma-arbitrage/results/`:
- `summary_metrics.json` — CAR by role (target/acquirer) and window
- `event_returns.csv` — Event-level data
- `event_frequency.csv` — Yearly event counts

Charts are saved to `ma-arbitrage/charts/`:
- `1_car_by_role.png` — CAR by window for targets vs acquirers (feature image)
- `2_event_frequency.png` — Annual event count
- `3_car_overall.png` — Overall CAR bar chart

---

## Academic References

- Mitchell, M. & Pulvino, T. (2001). "Characteristics of Risk and Return in Risk Arbitrage." *Journal of Finance*, 56(6), 2135-2175.
- Baker, M. & Savasoglu, S. (2002). "Limited Arbitrage in Mergers and Acquisitions." *Journal of Financial Economics*, 64(1), 91-115.
- Roll, R. (1986). "The Hubris Hypothesis of Corporate Takeovers." *Journal of Business*, 59(2), 197-216.

---

## Data Source

All queries run against the Ceta Research data warehouse (FMP financial data). API access at [cetaresearch.com](https://cetaresearch.com).
