# Defensive Sector Quality

Buy quality dividend-paying stocks in defensive sectors (Consumer Defensive, Utilities,
Healthcare). The portfolio holds income-generating businesses that protect capital during
market downturns and rate-rising cycles.

## Strategy

**Signal:** Screen stocks in defensive sectors for quality fundamentals. Rank by dividend
yield and hold the top 30.

**Sectors:** Consumer Defensive · Utilities · Healthcare

**Quality filters:**
- ROE > 6% (lower bar for capital-intensive utilities)
- Operating margin > 8%
- Debt/Equity < 2.5 (utilities carry structural leverage)
- Dividend yield > 0.5% (income-generating requirement)

**Universe:** Full exchange universe filtered by market cap threshold (exchange-specific).
Not constrained to any index.

**Rebalancing:** Annual (July). FY annual data with 45-day look-ahead lag.

**Weighting:** Equal weight. Cash if fewer than 10 stocks qualify.

**Portfolio size:** Top 30 by dividend yield.

## Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Sectors | Consumer Defensive, Utilities, Healthcare | Classic defensive classification |
| ROE threshold | > 6% | Utilities: ~8% avg, Consumer Defensive: ~15% avg |
| OPM threshold | > 8% | Excludes unprofitable "defensive" names |
| D/E threshold | < 2.5 | Utilities carry more debt than other sectors |
| Dividend yield | > 0.5% | Confirms income-generating status |
| Ranking | Dividend yield DESC | Income-first selection within quality universe |
| Rebalancing | Annual (July) | Annual FY data. July: filings available, settled markets |
| Costs | Size-tiered | 0.1% (>$10B), 0.3% ($2-10B), 0.5% (<$2B) — one way |

## Academic Basis

Novy-Marx, R. (2013). "The Other Side of Value: The Gross Profitability Premium."
*Journal of Financial Economics* 108(1), 1-28.

Defensive sector quality stocks exhibit persistent risk-adjusted outperformance because
they combine two known premiums: the quality premium (high-ROE companies retain
pricing power) and the low-beta premium (defensive stocks absorb less market drawdown).
During rate-rising cycles, utilities and consumer staples benefit from regulated pricing
and inelastic demand, making dividend yield a useful forward return signal.

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current stock screen (live TTM data) |
| `generate_charts.py` | Chart generation from results JSON |
| `results/` | Computed results (exchange_comparison.json + per-exchange JSON) |
| `charts/` | Generated PNG charts (gitignored) |

## Usage

```bash
# Run from backtests/ directory

# US backtest
python3 defensive-quality/backtest.py --preset us --output defensive-quality/results/returns_US_MAJOR.json --verbose

# India backtest
python3 defensive-quality/backtest.py --preset india --output defensive-quality/results/returns_BSE_NSE.json --verbose

# All exchanges
python3 defensive-quality/backtest.py --global --output defensive-quality/results/exchange_comparison.json --verbose

# Current screen (what to buy today)
python3 defensive-quality/screen.py --preset us

# Generate charts (after running global backtest)
python3 defensive-quality/generate_charts.py
```

## Exchange Eligibility

Tested on 16 exchanges. Excluded: ASX (adjClose split artifacts), SAO/Brazil (same issue).

JPX (Japan) and LSE (UK) include a minority of stocks with extreme historical price ratios,
but these are filtered by the market cap threshold (JPX: ¥100B, LSE: £500M) and by
`filter_returns()` (max single-period return: 200%).

Exchanges with fewer than 10 qualifying stocks in most periods run in cash and are
excluded from dedicated regional content (still included in comparison JSON for transparency).

## Data Notes

- Sector classification from `profile` table (current snapshot)
- Annual FY data with 45-day point-in-time lag
- `marketCap` in `profile` is in local currency per exchange
- Exchange-specific market cap thresholds via `cli_utils.get_mktcap_threshold()`

## Data Attribution

*Data: Ceta Research (FMP financial data warehouse). Past performance does not
guarantee future results.*
