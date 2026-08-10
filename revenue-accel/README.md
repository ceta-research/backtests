# Revenue Acceleration Growth

**Strategy slug:** `revenue-accel`
**Category:** Growth
**Rebalancing:** Annual (April)
**Universe:** Full exchange (NYSE+NASDAQ+AMEX for US), not index-constrained

---

## What It Does

Revenue acceleration identifies companies whose revenue growth rate is speeding up, not just growing. A company going from 10% to 15% YoY growth is accelerating. One going from 20% to 15% is decelerating, even though it still has strong growth.

The strategy buys the top 30 accelerating companies by acceleration magnitude each year, filtering for quality (ROE, D/E) and a minimum growth threshold to exclude companies accelerating from near-zero or negative growth.

---

## Signal

| Filter | Value | Source |
|--------|-------|--------|
| Revenue growth current > prior | acceleration > 0 | `income_statement` FY |
| Current YoY growth | > 5% | `income_statement` FY |
| Return on equity | > 10% | `key_metrics` FY |
| Debt-to-equity | < 1.5 | `financial_ratios` FY |
| Market cap | > exchange threshold | `key_metrics` FY |
| Selection | Top 30 by acceleration magnitude | ranked DESC |

**Acceleration = growth_current - growth_prior** where:
- `growth_current = (rev_t - rev_t1) / rev_t1` (most recent FY vs prior FY)
- `growth_prior   = (rev_t1 - rev_t2) / rev_t2` (prior FY vs two years prior)

Requires 3 consecutive FY revenue filings per symbol.

---

## Portfolio Construction

- **Rebalancing:** Annual, April 1st each year
- **Data lag:** 45 days (filings dated on/before Feb 15 used for April 1 rebalance)
- **Execution:** Next-day close. `get_prices(..., offset_days=1)` for both entry and exit,
  so no fill uses a price observed on the same bar as the signal. `--no-next-day` restores
  the old same-day behaviour.
- **Weighting:** Equal weight
- **Cash rule:** Hold cash (0% invested) if fewer than 10 stocks qualify
- **Transaction costs:** Size-tiered (see `costs.py`)

## Benchmarks

Each run reports two benchmarks:

- **Primary:** the local index for the exchange set (`get_local_benchmark`), e.g. the DAX for
  XETRA, the TSX Composite for TSX, the Sensex for NSE. Stored under the `spy` key and
  `comparison` block for backward compatibility, with `benchmark_symbol` / `benchmark_name`
  naming what it actually is.
- **Secondary:** SPY in USD, stored under `spy_usd` / `comparison_vs_spy` / `excess_cagr_vs_spy`.
  This is the cross-market comparable, since it is the same total-return series for every run.

Every local index in `LOCAL_INDEX_BENCHMARKS` is a **price index except the DAX**, which is a
performance index that reinvests dividends. Portfolio returns include dividends, so excess vs a
local index overstates the real edge by roughly that market's dividend yield everywhere except
Germany. Cross-market claims should use the SPY column.

## Data quality guards

- `remove_price_oscillations()` runs after the price load, dropping rows where adjClose spikes
  and reverts within 1 to 2 days (bad split adjustments, phantom holiday rows).
- `filter_returns()` drops positions entering below $1 (`MIN_ENTRY_PRICE`) or posting a
  single-period return above 200% (`MAX_SINGLE_RETURN`).
- `--exclude-funds` is an **opt-in diagnostic**, off by default. Closed-end funds report
  investment income in the `revenue` field, so they can post enormous "revenue acceleration"
  and crowd the top 30 (18 of 30 US and UK slots in April 2018). Enabling it moves CAGR by
  +0.70pp US, +0.58pp UK, +0.13pp Canada, +0.07pp Germany. It deliberately does **not** filter
  on `isActivelyTrading`, which would add survivorship bias to a historical run. Guarded
  outputs live in `results/diagnostics/`.
- ASX, SAO and SGX are excluded from all runs. See `DATA_QUALITY_ISSUES.md`.

---

## Academic Basis

- Chan, Karceski & Lakonishok (1996) "Momentum Strategies." *Journal of Finance* 51(5), 1681-1713.
  Documents that earnings and revenue growth momentum predicts future returns.
- Lakonishok, Shleifer & Vishny (1994) "Contrarian Investment, Extrapolation, and Risk."
  *Journal of Finance* 49(5), 1541-1578.
  Analysts systematically underextrapolate fundamental momentum, creating predictable mispricings.

---

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Live stock screen (current qualifying stocks) |
| `generate_charts.py` | Chart generation from results JSON |
| `results/exchange_comparison.json` | Multi-exchange backtest results (15 exchanges) |
| `results/diagnostics/` | `--exclude-funds` sensitivity runs |

---

## Usage

```bash
# Default: US (NYSE+NASDAQ+AMEX)
python3 revenue-accel/backtest.py

# Specific exchange
python3 revenue-accel/backtest.py --preset india --verbose

# All exchanges
python3 revenue-accel/backtest.py --global \
  --output results/exchange_comparison.json --verbose

# Without transaction costs (academic baseline)
python3 revenue-accel/backtest.py --no-costs

# Live screen (current stocks)
python3 revenue-accel/screen.py
python3 revenue-accel/screen.py --preset india

# Generate charts (after global run)
python3 revenue-accel/generate_charts.py
```

---

## Data Notes

- Revenue acceleration requires 3 consecutive annual filings. Exchanges with sparse FY data
  (< 3 filings for most symbols) produce high cash periods and are excluded from content.
- 45-day lag applied for point-in-time accuracy.
- Market cap stored in local currency. Thresholds set per exchange via `cli_utils.get_mktcap_threshold()`.
- ASX and SAO excluded from `--global` due to adjClose split/adjustment artifacts.
- SES excluded: historically high cash periods due to sparse FY filing coverage.
