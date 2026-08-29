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

## No inverse leg exists

**A miss-streak leg was never computed for this topic**, so the direction test cannot be run on it at
all. The topic's own `REVIEW.md` waived the test by citing academic literature rather than measuring
it. This means beat-streaks cannot be cleared, only caveated, until someone funds the inverse leg.

## Domicile contamination

LSE events are **61.5% US-domiciled** and XETRA **49% US-domiciled**. `WHERE exchange = X` selects
listings, not companies, and the London and Frankfurt lines of US issuers dominate the earnings-based
event tables. Any per-market claim for the UK or Germany measures a majority-foreign universe against
a GBP or EUR national index.

See `docs/sessions/completed/2026-08-29/EVENT_STUDY_DIRECTION_SWEEP.md` in the ATO_SUITE docs tree.
