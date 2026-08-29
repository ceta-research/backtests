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
