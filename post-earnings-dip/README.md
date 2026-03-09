# Post-Earnings Dip Mean Reversion

**Strategy type:** Event study
**Signal:** Beat earnings estimates, stock drops 5%+ on announcement
**Question:** Does the sell-off reverse, or persist?

## The Idea

A stock beats earnings estimates — a seemingly good result — but the market sells it off 5% or more. The instinct is to "buy the dip": if the fundamentals just proved out, the sell-off is an overreaction.

This backtest tests that instinct across 12 exchanges and 25 years of data.

## Signal

1. `epsActual > epsEstimated` (earnings beat)
2. `|epsEstimated| > $0.01` (avoid tiny/meaningless estimates)
3. Stock drops ≥ 5% from T-1 close to T+1 close (sell-the-news reaction)
4. Market cap above exchange-specific threshold (liquid mid-to-large caps)

Event windows measured from T+1 (dip bottom): T+5, T+10, T+21, T+63 trading days.

### Dip categories
- `dip_5`: 5-10% drop (moderate sell-off)
- `dip_10`: 10-20% drop (sharp sell-off)
- `dip_20`: 20%+ drop (severe sell-off)

## Methodology

- **Metric:** Cumulative Abnormal Return (CAR) = stock return minus benchmark return
- **Benchmark:** SPY (US), or regional ETF (EWJ, EPI, EWG, etc.)
- **Winsorization:** 1st/99th percentile to reduce outlier noise
- **Market cap filter:** Exchange-specific thresholds (₹20B for India, $1B for US, etc.)
- **Data:** FMP earnings surprises, daily adjusted prices

See `backtests/METHODOLOGY.md` for full methodology details.

## Results Summary (2000-2025)

| Exchange | Events | T+21 CAR | t-stat | T+63 CAR | t-stat |
|----------|--------|----------|--------|----------|--------|
| US (NYSE/NASDAQ/AMEX) | 13,950 | -0.22% | -2.12* | -0.84% | -4.53** |
| Canada (TSX) | 737 | -0.18% | -0.39 | -1.45% | -1.93 |
| Japan (JPX) | 776 | -0.26% | -0.89 | 0.00% | 0.00 |
| Taiwan (TAI/TWO) | 384 | +1.76% | +2.73** | +3.04% | +2.47* |
| Sweden (STO) | 319 | -0.27% | -0.54 | -2.58% | -2.48* |
| India (BSE/NSE) | 467 | -0.17% | -0.42 | +2.54% | +3.55** |
| Germany (XETRA) | 322 | +0.55% | +0.91 | -0.94% | -0.81 |

*p<0.05, **p<0.01

**Key finding:** "Buy the dip after a beat" does not work in most markets. The sell-off
continues rather than reverting. Taiwan and India are notable exceptions.

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run US event study
python3 post-earnings-dip/backtest.py

# Specific exchange
python3 post-earnings-dip/backtest.py --preset india

# All exchanges
python3 post-earnings-dip/backtest.py --global --output results/exchange_comparison.json

# 10% dip threshold only
python3 post-earnings-dip/backtest.py --min-dip 0.10

# Live screen (recent beats)
python3 post-earnings-dip/screen.py

# Cloud execution
python3 post-earnings-dip/backtest.py --cloud
```

## Data Notes

- **LSE (UK):** Earnings data sparse before 2022. Results reflect 2022-2025 data primarily.
- **China (SHZ/SHH):** Beat rate (32-36%) lower than other markets (41-60%). Results included
  with caveat about data coverage.
- **Excluded:** Australia (8yr history), Thailand/Norway/Switzerland (<100 events each).

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Event study: 2000-2025, all exchanges |
| `screen.py` | Live screen: recent beat+dip candidates |
| `generate_charts.py` | Chart generation from results |
| `results/` | JSON and CSV output files |

## Academic Context

Post-earnings announcement drift (PEAD) shows that stocks continue moving in the direction
of the earnings surprise. This strategy tests the opposite hypothesis — that stocks
overreact in the short term and mean-revert.

The data mostly supports PEAD over mean reversion: beats followed by sell-offs continue
underperforming in the majority of markets tested. The exceptions (Taiwan, India) may
reflect market structure differences, information diffusion speed, or sentiment dynamics.

Reference: Bartov, Radhakrishnan & Krinsky (2000), "Investor Sophistication and Patterns
in Stock Returns after Earnings Announcements," *The Accounting Review*, 75(1), 43-63.
