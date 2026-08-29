# Results provenance: stock-split

Read this before quoting any number from `results/`.

## Wrong-market benchmarks in the international runs (UNPUBLISHED, gate before use)

Three of the `results/international/*_summary.json` files measure a local-currency equity leg against
**SPY**:

| File | Universe | Benchmark | Why |
|---|---|---|---|
| `set_summary.json` | Thailand (SET) | SPY | Results written 2026-03-25 01:15:47. The `"SET": "^SET.BK"` registry entry landed in `e27660c` at 02:34:29, **79 minutes later**. Re-running today gives `^SET.BK`, so this file disagrees with current code. |
| `saudi_summary.json` | Saudi (SAU) | SPY | No `LOCAL_INDEX_BENCHMARKS` entry for SAU. Fallback is SPY. |
| `shz_summary.json` | Shenzhen (SHZ) | SPY | `399001.SZ` has no price data in FMP `stock_eod` (`data_utils.py:73`), so SHZ is deliberately absent from the registry and falls back to SPY. |

**None of these three is published.** They are the retraction class if they ever are, so gate them
before any publication.

⚠️ Related trap in `get_local_benchmark` (`data_utils.py:222-233`): unmapped exchanges contribute
nothing to the symbol set rather than forcing the mixed-region SPY fallback. A `["SHZ","SHH"]`
universe therefore resolves to the Shanghai Composite alone, measuring a two-venue universe against
one venue's index.

## The published 5-for-1+ claim decomposes

The live US post's headline, that 5-for-1-and-larger splits underperform by -11.7% and the result is
highly significant, is carried by two artifact groups:

- fund and ETF share splits: -12.4 (n=98)
- mis-encoded reverse splits and IPO conversions: -40.9 (n=11)

The genuine non-fund 5:1 to 25:1 leg is **-7.1, t=-1.5, not significant**. US-domiciled common stock
is **-2.2, t=-0.4**.

There is no fund or ETF guard in `backtest.py`, which is why fund splits enter the sample at all.

See `docs/sessions/completed/2026-08-29/EVENT_STUDY_DIRECTION_SWEEP.md` in the ATO_SUITE docs tree.
