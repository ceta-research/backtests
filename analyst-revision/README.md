# Analyst Rating Revision Event Study

Measures cumulative abnormal returns after individual analyst upgrades and downgrades,
using FMP's `stock_grade` table (one row per firm, per stock, per grade change).

> **⚠️ Correction, August 2026. The non-US results published from this study were
> withdrawn.** They measured foreign companies' secondary listings against a local
> index. XETRA is 88.1% US-domiciled companies by analyst event count and 1.5% German;
> the LSE is 85.7% US-domiciled. Read [Universe contamination](#universe-contamination-read-this-before-using-a-non-us-preset)
> before running any non-US preset. The US result is unaffected.

**Not to be confused with `event-08-upgrade-cluster`.** That study uses
`grades_historical` (aggregate consensus counts: "2 analysts moved to Buy"). This one
uses `stock_grade` (individual grade changes: "one analyst went Hold to Buy"). Related
signals, different tables.

## Methodology

**Study type:** Event study. Each revision is measured independently. No rebalancing,
no portfolio construction.

**Windows:** T+1, T+5, T+21, T+63 trading days after the event.

**Entry:** Next-day close (market on close). The announcement lands during the session,
so the same-day close is not tradable. In `compute_event_returns` the ASOF join is
`trade_date > event_date`, not `>=`.

**Benchmark:** the exchange's own **local index**, via `data_utils.get_local_benchmark`:
SPY (US), `^GDAXI` (Germany), `^FTSE` (UK), `^SSMI` (Switzerland), `^GSPTSE` (Canada).
Earlier versions used regional ETF proxies (EWU, EWG, EWL, EWC), which introduced
currency and tracking error. See the correction note above for why a local index is
necessary but not sufficient.

**Abnormal return:** stock return minus benchmark return over the same window.

**Market cap filter:** exchange-specific threshold, see `cli_utils.MKTCAP_THRESHOLD_MAP`.

**Deduplication:** FMP re-fetches create duplicate rows. Keep the most recent record per
(symbol, date, gradingCompany).

**Winsorization:** 1st/99th percentile before computing statistics.

**Data quality guards:**
- `remove_price_oscillations()` strips rows whose adjusted close spikes and reverts
  within a day or two. Runs **before** the trading-day calendar is built from the
  benchmark series, so a deleted phantom row cannot shift every event's day numbering.
- `filter_returns()` runs per window, keyed on `symbol|event_date`, dropping entry
  prices below $1 and single-window returns above +200%. Per window, so a junk T+63
  print does not discard that event's clean T+1 observation.
- The price fetch keeps `adjClose > 0`, not `> $1`. `MIN_ENTRY_PRICE` is an entry-side
  test; applying it to the whole price table also deletes exit rows for stocks that fell
  under a dollar, which censors the worst outcomes and inflates downgrade CARs.

**Period:** 2012–2025. FMP's `stock_grade` is sparse before 2012, and non-US coverage is
thin before 2018.

## Universe contamination (read this before using a non-US preset)

`--preset germany` resolves to `WHERE exchange = 'XETRA'`, which selects every company
**listed** in Frankfurt. Outside the US that is mostly foreign companies' secondary lines.

| Exchange | Domiciled locally | US-domiciled | Other foreign |
|----------|-------------------|--------------|---------------|
| XETRA | **1.5%** | **88.1%** | 10.4% |
| LSE | 6.2% | 85.7% | 8.1% |
| SIX | 8.2% | 74.2% | 17.6% |
| TSX | **90.5%** | 7.4% | 2.0% |

The most-graded XETRA tickers are `NFC.DE` (Netflix, 647 events), `INL.DE` (Intel, 609),
`TL0.DE` (Tesla, 557) and `APC.DE` (Apple, 526). Scoring those against the DAX over
2012–2025 measures the gap between the US and German markets, not the analyst event.

**The tell is direction.** A real signal cannot pay off both ways:

| Exchange | US-domiciled, after upgrades (T+63) | after downgrades (T+63) |
|----------|-------------------------------------|-------------------------|
| XETRA | +1.485% (sig) | +0.649% (sig) |
| TSX | +6.705% (sig) | +5.342% (sig) |

Canada shows it most clearly because it is otherwise the cleanest market: 7.4% of TSX
events are US-domiciled, and that sliver returns +6.71% after upgrades **and** +5.34%
after downgrades against the TSX Composite.

**Domicile-only reruns do not rescue the non-US markets here.** Restricting to German
companies leaves 155 upgrades across 31 names, with Deutsche Bank at 54 of them. FMP
records most German companies' grades against the primary ticker rather than the
Frankfurt line (`SAP` 536 records vs `SAP.DE` 240; `BMW.DE` has 6 across 14 years), so
exchange-screening selects exactly the tickers where domestic coverage is absent.

Run the check yourself:

```bash
python3 analyst-revision/domicile_analysis.py
python3 analyst-revision/backtest.py --preset germany --domicile-filter
```

## Key Findings (2012–2025, next-day-close entry, local benchmarks)

### US (NYSE+NASDAQ+AMEX) — the result that stands

Domestic universe, matched benchmark, 133,995 events.

| Window | Upgrade CAR | Downgrade CAR |
|--------|-------------|---------------|
| T+1    | +0.018%**   | -0.027%**     |
| T+5    | -0.002% (ns)| -0.031% (ns)  |
| T+21   | -0.171%**   | -0.094%**     |
| T+63   | -0.496%**   | -0.660%**     |

n=65,725 upgrades, n=68,270 downgrades. The upgrade move is priced in on announcement
day; entering at the next close underperforms.

Cluster vs single at T+21: **+0.285% vs -0.518%**, a gap of 0.80pp. Clustered upgrades
(2+ firms within 30 days) are the only positive post-announcement signal in the data.
Downgrades are the durable one, still falling at three months.

### Non-US markets — withdrawn as market findings

The figures below are correct as descriptions of the **listed** universe and are kept
for reproducibility. They are **not** findings about German, British or Swiss companies.

| Exchange | Upgrades | Benchmark | T+21 | T+63 |
|----------|----------|-----------|------|------|
| XETRA | 10,844 | DAX | +0.647% | +1.335% |
| LSE | 8,800 | FTSE 100 | +0.527% | +1.127% |
| SIX | 1,945 | SMI | +0.326% | +1.172% |
| TSX | 2,128 | TSX Composite | -0.081% (ns) | +0.840% |

Canada is the one non-US universe that is genuinely domestic (90.5%). On the domestic
subset, Canadian upgrades show -0.350% at T+21 and +0.335% at T+63, neither significant.

`**` = p<0.05, `ns` = not significant.

## Exchange Eligibility

FMP's `stock_grade` coverage concentrates in Western markets. Asian markets carry under
50 events per year for most exchanges, against more than 4,000 per year in the US.

| Exchange | Upgrades | Status |
|----------|----------|--------|
| NYSE+NASDAQ+AMEX (US) | 65,725 | Flagship, blog live |
| XETRA (Germany) | 10,844 | Blog retracted, universe contaminated |
| LSE (UK) | 8,800 | Blog retracted, universe contaminated |
| SIX (Switzerland) | 1,945 | Not reported, contaminated and thin |
| TSX (Canada) | 2,128 | Clean universe, no significant upgrade effect |
| BSE/NSE, JPX, KSC, etc. | <50 | Excluded, insufficient data |

## Academic Basis

- Stickel, S. (1995). "The Anatomy of the Performance of Buy and Sell
  Recommendations." *Financial Analysts Journal*, 51(5), 25–39.
- Womack, K. (1996). "Do Brokerage Analysts' Recommendations Have Investment Value?"
  *Journal of Finance*, 51(1), 137–167.
- Barber, B., Lehavy, R., McNichols, M. & Trueman, B. (2001). "Can Investors Profit
  from the Prophets?" *Journal of Finance*, 56(2), 531–563.

## Usage

```bash
# US market (default)
python3 analyst-revision/backtest.py

# All eligible exchanges, writes per-exchange JSON plus the comparison file
python3 analyst-revision/backtest.py --global \
  --output analyst-revision/results/exchange_comparison.json

# Non-US: see the contamination section above first
python3 analyst-revision/backtest.py --preset germany
python3 analyst-revision/backtest.py --preset germany --domicile-filter

# Domicile decomposition of the last run's event CSVs
python3 analyst-revision/domicile_analysis.py

# Live screens
python3 analyst-revision/screen.py --preset us
python3 analyst-revision/screen.py --clusters

# Charts (--all-exchanges, the default builds US only)
python3 analyst-revision/generate_charts.py --all-exchanges
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Event study across eligible exchanges |
| `domicile_analysis.py` | CAR by company domicile, from the event CSVs |
| `screen.py` | Live screen: recent upgrades and clusters |
| `generate_charts.py` | Charts from the results JSON |
| `results/exchange_comparison.json` | All exchange results |
| `results/analyst_revision_{EXCHANGE}.json` | Per-exchange results |
| `results/analyst_revision_{EXCHANGE}_events.csv` | Event-level returns |
| `results/domicile_decomposition.json` | Per-domicile CAR breakdown |

See `../DATA_QUALITY_ISSUES.md` for the cross-topic write-up of the domicile artifact
and the direction test.
