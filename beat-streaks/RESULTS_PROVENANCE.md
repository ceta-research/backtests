# Results provenance: beat-streaks

Read this before quoting any number from `results/`.

## Superseded benchmark file

`beat_streaks_BSE_NSE.json` (written 2026-03-09) uses **INDA**, a regional ETF. It is superseded by
`beat_streaks_NSE.json` (2026-04-05), which uses the local index `^BSESN`. Both cover India and both
are committed.

**Kept, not deleted**, because a published blog may quote either and the filename does not say which.
Re-running with today's `data_utils.LOCAL_INDEX_BENCHMARKS` gives `^BSESN`, so the INDA numbers are
not reproducible from current code.

## Wrong-market benchmarks, published

`TAI_TWO` (Taiwan) and `SET` (Thailand) are benchmarked against **SPY** in the published run. Both
have local indices available today (`^TWII`, `^SET.BK`). This is the same class as the
`momentum-05-analyst-revision` retraction: a local-currency equity leg measured against a US index.

## The inverse leg exists as of 2026-08-29

**The miss-streak mirror leg was computed 2026-08-29** (it had never been run before; the topic's own
`REVIEW.md` originally waived the direction test by citing academic literature). `backtest.py` now
takes `--leg beat|miss|break`. The mirror was run as fresh PAIRED runs (beat + miss, same code/data
vintage, local price indices, T+0 = last pre-announcement close) on 10 markets, all under
`results/mirror-2026-08/` — deliberately a separate directory so the canonical published
`beat_streaks_*.json` files above are untouched.

Direction-test verdicts at T+21 (overall streak>=2 rows; `results/mirror-2026-08/_verdicts_t21.json`):
9 of 10 markets PASS (miss streaks drift significantly negative while beat streaks drift positive):
US, Canada, Japan, Taiwan, India, Thailand, Brazil, Hong Kong, Korea. **China FAILS** — both legs
positive and significant (beats +1.28 t=7.3, misses +0.39 t=3.5) against a Shanghai-Composite-only
benchmark for a SHZ+SHH universe; treat China's absolute levels as universe drift (the beat-minus-miss
spread stays positive). A US streak-break leg also exists (`us_break.json`): -1.20% at T+1 (t=-36.9),
-3.09% at T+63.

⚠️ The mirror pairs' beat legs are NOT the published table's numbers (different benchmark for
Taiwan/Thailand, different T+0 convention vs the comparison post, fresh data). Compare legs only
within the same paired run.

## Domicile contamination

LSE events are **61.5% US-domiciled** and XETRA **49% US-domiciled**. `WHERE exchange = X` selects
listings, not companies, and the London and Frankfurt lines of US issuers dominate the earnings-based
event tables. Any per-market claim for the UK or Germany measures a majority-foreign universe against
a GBP or EUR national index.

See `docs/sessions/completed/2026-08-29/EVENT_STUDY_DIRECTION_SWEEP.md` in the ATO_SUITE docs tree.
