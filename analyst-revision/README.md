# Analyst Rating Revision Momentum

Event study measuring Cumulative Abnormal Returns (CAR) after individual analyst
rating upgrades and downgrades. When an analyst raises their rating from Hold to Buy,
prices move — but not all at once. This study measures how much, how fast, and how
persistently.

## Signal

**Data source:** FMP `stock_grade` table — individual analyst grade changes, one row
per analyst firm per stock per date. Different from `grades_historical` (which tracks
aggregate consensus counts).

**Events:**
- Upgrades: `action = 'upgrade'` (e.g., Hold → Buy, Sell → Buy)
- Downgrades: `action = 'downgrade'` (e.g., Buy → Hold, Buy → Sell)

**Cluster detection:** An upgrade is "clustered" when 2+ distinct analyst firms upgrade
the same stock within 30 days of each other.

**Magnitude:**
- Small (+2): Hold → Buy (most common)
- Large (+4): Sell → Buy (rare, stronger signal)

**Market cap filter:** Exchange-specific threshold (see `cli_utils.MKTCAP_THRESHOLD_MAP`).
Excludes micro-caps where analyst coverage is unreliable.

## Methodology

**Study type:** Event study. Each event (analyst revision) is measured independently.
Not a portfolio backtest — no rebalancing, no portfolio construction.

**Windows:** T+1, T+5, T+21, T+63 trading days after event.

**Abnormal return:** Stock return minus regional benchmark return (SPY for US, EWU
for UK, EWG for Germany, EWL for Switzerland, EWC for Canada).

**Deduplication:** If the same analyst firm revises the same stock on the same date
multiple times (data fetch artifact), keep the most recent record only.

**Winsorization:** 1st/99th percentile applied before computing statistics to reduce
outlier impact on mean CAR.

**Period:** 2012–2025. Data before 2012 is sparse in FMP's stock_grade.

## Academic Basis

- Stickel, S. (1995). "The Anatomy of the Performance of Buy and Sell
  Recommendations." *Financial Analysts Journal*, 51(5), 25–39.
- Womack, K. (1996). "Do Brokerage Analysts' Recommendations Have Investment Value?"
  *Journal of Finance*, 51(1), 137–167.
- Barber, B., Lehavy, R., McNichols, M. & Trueman, B. (2001). "Can Investors Profit
  from the Prophets?" *Journal of Finance*, 56(2), 531–563.

## Key Findings (2012–2025)

### US (NYSE+NASDAQ+AMEX, n=66,742 upgrades)
| Window | Upgrade CAR | Downgrade CAR |
|--------|-------------|---------------|
| T+1    | +0.652%**   | -0.822%**     |
| T+5    | +0.664%**   | -0.876%**     |
| T+21   | +0.492%**   | -1.004%**     |
| T+63   | +0.178%**   | -1.477%**     |

Cluster vs single (T+21): **+0.923% vs +0.176%** — cluster effect is 5x larger.

### Germany (XETRA, n=12,585 upgrades)
| Window | Upgrade CAR | Downgrade CAR |
|--------|-------------|---------------|
| T+1    | +0.569%**   | -0.621%**     |
| T+21   | +1.458%**   | -0.087% (ns)  |
| T+63   | +2.634%**   | +0.824%**     |

Most persistent drift of any market. Downgrades fully revert by T+63.

### UK (LSE, n=9,059 upgrades)
| Window | Upgrade CAR |
|--------|-------------|
| T+1    | +0.579%**   |
| T+21   | +0.745%**   |
| T+63   | +0.621%**   |

Cluster at T+21: **+1.373%** vs single: +0.322%.

## Exchange Eligibility

FMP's `stock_grade` data is concentrated in Western markets. Asian markets (India,
Japan, Korea, Taiwan, China) have negligible analyst grade coverage in this dataset.

| Exchange | Events | Eligible |
|----------|--------|----------|
| NYSE+NASDAQ+AMEX (US) | 66,742 upgrades | Yes — flagship |
| LSE (UK) | 9,059 | Yes — blog |
| XETRA (Germany) | 12,585 | Yes — blog |
| SIX (Switzerland) | 2,402 | Comparison only |
| TSX (Canada) | 2,232 | Comparison only |
| BSE/NSE, JPX, KSC, etc. | <50 | Excluded (insufficient data) |

## Usage

```bash
# US market (default)
python3 analyst-revision/backtest.py

# Specific exchange
python3 analyst-revision/backtest.py --preset uk --verbose
python3 analyst-revision/backtest.py --preset germany

# All eligible exchanges
python3 analyst-revision/backtest.py --global \
  --output analyst-revision/results/exchange_comparison.json

# Live screen: recent upgrades
python3 analyst-revision/screen.py --preset us

# Live screen: upgrade clusters (2+ analysts in 30 days)
python3 analyst-revision/screen.py --clusters

# Generate charts
python3 analyst-revision/generate_charts.py --all-exchanges
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full event study (all eligible exchanges) |
| `screen.py` | Live screen: recent upgrades and clusters |
| `generate_charts.py` | Chart generation from results JSON |
| `results/exchange_comparison.json` | All exchange results |
| `results/analyst_revision_{EXCHANGE}.json` | Per-exchange results |
| `results/analyst_revision_{EXCHANGE}_events.csv` | Event-level returns |
