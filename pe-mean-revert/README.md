# P/E Mean Reversion (Sector-Relative)

**Strategy type:** Value timing
**Universe:** Full exchange (not index-constrained)
**Rebalancing:** Annual (January)
**Period:** 2000–2025

---

## The Signal

A stock's P/E ratio mean-reverts toward its sector's median. When a quality stock trades at a deep discount to its sector peers — not just its own history — the market tends to re-rate it toward the sector average.

**Entry criteria:**
- Current P/E < 60% of sector median P/E (at least 40% cheaper than peers)
- P/E in range 3–50 (profitable, not speculative)
- ROE > 8% (quality earnings, not just cheap-for-a-reason)
- D/E < 2.0 (not excessively leveraged)
- Market cap above exchange threshold (liquid mid-to-large cap)

**Portfolio construction:** Top 30 by lowest (stock P/E / sector median P/E), equal weight. Hold cash if fewer than 10 qualify.

---

## Distinct from P/E Compression (reversion-05)

| Dimension | P/E Compression | P/E Mean Reversion |
|-----------|-----------------|-------------------|
| Baseline | Stock's own 5-year average | Sector median P/E |
| Question | "Is this stock cheap vs its own history?" | "Is this stock cheap vs peers right now?" |
| History required | 3–5 years of own P/E data | None (uses current cross-section) |
| Signal type | Intrinsic mean reversion | Relative mean reversion |

Both test P/E mean reversion — one longitudinally (own history), one cross-sectionally (sector peers).

---

## Academic Reference

Fama, E.F. & French, K.R. (1992). The Cross-Section of Expected Stock Returns. *Journal of Finance*, 47(2), 427–465.

Value premium within industries: sector-relative valuation captures expected returns beyond market-level P/E alone.

---

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000–2025) |
| `screen.py` | Current stock screen (live TTM data) |
| `generate_charts.py` | Chart generation from results JSON |
| `results/exchange_comparison.json` | Full multi-exchange results |

---

## Usage

```bash
# Run US backtest
python3 pe-mean-revert/backtest.py --preset us --verbose

# Run all exchanges
python3 pe-mean-revert/backtest.py --global --output pe-mean-revert/results/exchange_comparison.json --verbose

# Live screen (current qualifying stocks)
python3 pe-mean-revert/screen.py --preset us

# Generate charts (requires results/exchange_comparison.json)
python3 pe-mean-revert/generate_charts.py
```

---

## Content Location

`ts-content-creator/content/_current/timing-01-pe-mean-revert/`
