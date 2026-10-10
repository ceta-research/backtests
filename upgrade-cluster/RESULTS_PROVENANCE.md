# Results provenance: upgrade-cluster

Two different benchmark generations are committed side by side in `results/`. Read this before
quoting any number from that directory.

## Two benchmark sets

| File group | Written | Benchmarks |
|---|---|---|
| `exchange_comparison.json` | 2026-03-10 | **Regional ETFs**: EWJ (JPX), EWU (LSE), EWG (XETRA), EWH (HKSE), EWY (KSC), FXI (China), EWC (TSX), INDA (India) |
| `upgrade_cluster_BSE_NSE.json`, `upgrade_cluster_SHZ_SHH.json` | 2026-03-10 | **Regional ETFs**: INDA, FXI |
| every other `upgrade_cluster_*.json` | 2026-04-05 | **Local indices**: `^BSESN`, `^N225`, `^FTSE`, `^GDAXI`, `^HSI`, `^KS11`, `000001.SS`, `^GSPTSE` |

The 2026-03-10 files are superseded by newer siblings that cover the same venues with different
benchmarks:

- `upgrade_cluster_BSE_NSE.json` (INDA) is superseded by `upgrade_cluster_NSE.json` (`^BSESN`)
- `upgrade_cluster_SHZ_SHH.json` (FXI) is superseded by `upgrade_cluster_SHH_SHZ.json` (`000001.SS`)

Note the near-identical filenames. `SHZ_SHH` and `SHH_SHZ` differ only in venue order.

**They are kept, not deleted, because a published blog may quote either one and the filename alone
does not tell you which.** Establish which file backs a given published table before you change it.
Re-running these legs with today's `data_utils.LOCAL_INDEX_BENCHMARKS` would produce the local-index
benchmark, not the ETF, so the committed numbers are not reproducible from current code.

## Wrong-market benchmark, published

`upgrade_cluster_JNB.json` uses **SPY (S&P 500)** for a Johannesburg universe. `JNB` has no entry in
`LOCAL_INDEX_BENCHMARKS` (`data_utils.py:71` records why: `^J203.JO` has no price data in FMP
`stock_eod`), so `get_local_benchmark` falls back to SPY. Sample is `n_upgrade_events=36`,
`n_downgrade_events=2`.

## Direction test

`T+21` fails the direction test on the US leg: upgrades `+0.7637` (t=4.418, n=5640) and downgrade
clusters `+1.4799` (t=3.576, n=1002) both beat SPY, and the downgrades beat it by more. `T+1` and
`T+5` fail the same way, both legs significantly negative. **`T+63` is the only window that passes**:
upgrades `+0.7635` (t=2.632), downgrades `-0.8091` (t=-1.128, not significant), opposite signs.

Domicile is clean for this topic. XETRA is 96.9% German-domiciled, because `grades_historical` is an
aggregate table that attaches to local lines. The contamination that forced the
`momentum-05-analyst-revision` retraction does not apply here.

See `docs/sessions/completed/2026-08-29/EVENT_STUDY_DIRECTION_SWEEP.md` in the ATO_SUITE docs tree.

## Same-day duplicates in the cluster LAG

Since 2026-10-10 `backtest.py` detects clusters with `LAG()` over
`PARTITION BY symbol ORDER BY obs_date, bullish_count DESC NULLS LAST, bearish_count DESC NULLS LAST`.
The two count keys make the order total. Before, same-day rows came back in whatever order DuckDB's
parallel run produced.

Where `(symbol, obs_date)` is unique, the count keys change nothing. Where one date has several rows
with different counts, only the group's first row passes the gap filter (the others get
`gap_days = 0`), and LAG compares it with the **last** row of the previous date. With DESC that is
the date's max-bullish row against the prior date's min-bullish row, so `upgrade_delta` is max minus
min: the most upgrade-friendly comparison there is. No ordering removes this, because a group's first
and last rows always differ; ASC gives min minus max instead. On a synthetic fixture with same-day
duplicates on 25% of observations (offline harness; old-code ranges pooled over the verifier's runs
and three reruns), the old code gave 16,592 to 16,765 upgrade events and 5,968 to 6,017 `upgrade_large`; DESC gives a fixed 20,216 and
8,499, ASC 15,615 and 4,718. Removing the bias means collapsing each `(symbol, date)` to one row
before the LAG. That changes the metric, so it is a separate decision.

Whether it fires on real data:

- Local snapshot `data/data_source=fmp/analyst/grades_historical/1765992124.334719.parquet`
  (17 Dec 2025, 1,463,596 rows, 39,574 symbols, 2012-02-01 to 2025-12-01): 0 `(symbol, date)` groups
  with more than one row. Every `date` is the 1st of a month and `dateEpoch = epoch(CAST(date AS DATE))`
  on every row.
- The pipeline (`ts-data-pipeline/configs/endpoints/fmp/analyst/grades_historical.yaml`) dedupes on
  `(symbol, dateEpoch)`, keeping the newest `fetchedAtEpoch`. That is the LAG's partition key, so
  repacked data should be unique on it.
- **Prod is unverified.** The backtest reads prod at query time, and fetch files not yet repacked
  (refetched every 6 days) could carry duplicates. Run this before publishing any result from this code
  (through `cr_client` as written; the backtest uses the same unqualified table name):

```sql
SELECT COUNT(*) AS dup_groups FROM (
  SELECT symbol, CAST(date AS DATE) AS d
  FROM grades_historical
  WHERE CAST(date AS DATE) BETWEEN '2019-01-01' AND '2025-12-31'
  GROUP BY 1, 2
  HAVING COUNT(DISTINCT (
    CAST(analystRatingsStrongBuy AS INTEGER) + CAST(analystRatingsBuy AS INTEGER),
    CAST(analystRatingsSell AS INTEGER) + CAST(analystRatingsStrongSell AS INTEGER))) > 1
)
```

At 0, the tie-break never fires and results match the pre-fix code. Above 0, upgrade counts and CARs
are biased upward by the rule above: don't publish until the collapse decision is made.
