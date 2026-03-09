# Earnings Beat Streaks

**Category:** Event Study
**Academic Reference:** Loh & Warachka (2012), Myers, Myers & Skinner (2007)

## Strategy

Measures abnormal stock returns following consecutive earnings beats. A "beat streak" occurs when a company beats analyst EPS estimates for 2 or more consecutive quarters. We track whether the **Nth** beat in a streak predicts future returns.

**Beat definition:** `epsActual > epsEstimated` with `|epsEstimated| > $0.01`

**Categories:**
- `streak_2` — 2nd consecutive beat (where streak momentum begins)
- `streak_3` — 3rd consecutive beat (sweet spot per academic literature)
- `streak_4` — 4th consecutive beat
- `streak_5plus` — 5th or longer beat (market may have priced in the pattern)

**Universe:** Stocks with market cap > exchange-specific threshold (local currency)

**Windows:** T+1, T+5, T+21, T+63 trading days after the streak-extending announcement

**Benchmark:** SPY (US), or regional country ETF for non-US exchanges

## Usage

```bash
cd backtests/

# US market (default)
python3 beat-streaks/backtest.py

# Specific preset
python3 beat-streaks/backtest.py --preset india --verbose

# Single exchange
python3 beat-streaks/backtest.py --exchange JPX --output results/beat_streaks_JPX.json

# All exchanges (global run)
python3 beat-streaks/backtest.py --global --output beat-streaks/results/exchange_comparison.json

# Live screen: current active streaks
python3 beat-streaks/screen.py
python3 beat-streaks/screen.py --preset us --min-streak 5

# Generate charts (after running backtest)
python3 beat-streaks/generate_charts.py
```

## Results Files

| File | Description |
|------|-------------|
| `results/beat_streaks_{EXCHANGE}.json` | Per-exchange results (CAR by streak category) |
| `results/beat_streaks_{EXCHANGE}_events.csv` | Event-level returns (symbol, date, streak, returns) |
| `results/exchange_comparison.json` | Global comparison across all exchanges |
| `charts/1_us_car_by_streak.png` | Grouped bar: CAR by streak length at each window |
| `charts/2_us_car_progression.png` | Line chart: CAR progression T+1 → T+63 by streak |
| `charts/3_exchange_comparison.png` | Exchange comparison at T+21 |

## Key Findings (US, NYSE+NASDAQ+AMEX, 2000–2025)

- All streak categories (2+) show statistically significant positive T+1 CAR
- Signal is strongest at `streak_2` and `streak_3` (largest abnormal returns)
- `streak_5plus` shows weaker incremental returns (market partially prices in long streaks)
- T+1 reaction has remained relatively stable over time; T+63 drift has compressed
- Mean CAR diverges from median → right-skewed distribution, driven by outlier beats

## Academic References

- **Loh, R. & Warachka, M. (2012).** "Streaks in Earnings Surprises and the Cross-Section of Stock Returns." *Management Science*, 58(7), 1305-1321.
- **Myers, L., Myers, J. & Skinner, D. (2007).** "Earnings Momentum and Earnings Management." *Journal of Accounting, Auditing & Finance*, 22(2), 249-284.
- **Bernard, V. & Thomas, J. (1989).** "Post-Earnings-Announcement Drift: Delayed Price Response or Risk Premium?" *Journal of Accounting Research*, 27(Supplement), 1-36.
