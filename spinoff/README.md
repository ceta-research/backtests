# Corporate Spinoff Event Study

An event study measuring cumulative abnormal returns (CAR) for both spinoff parents and children at multiple time horizons after the separation.

**Strategy type:** Event study
**Universe:** Curated list of 30 major US corporate spinoffs (2011–2024)
**Benchmark:** SPY
**Event windows:** T+1, T+5, T+21, T+63, T+126, T+252 trading days
**Academic basis:** Cusatis, Miles & Woolridge (1993); McConnell & Ovtchinnikov (2004)

## The Mechanism

When a large company spins off a division, index funds that owned the parent receive shares of the child entity. If the child is too small for the index (e.g., below S&P 500 minimum market cap), passive funds must sell — not because the business is bad, but because their mandate requires it. This forced selling depresses the child's price. Over subsequent months, fundamental investors step in and the price recovers.

The parent also benefits. A focused company trades at a higher multiple than a conglomerate. The "conglomerate discount" disappears once the division is separated.

## Data Note

No dedicated spinoff table exists in the FMP warehouse. This study uses a curated list of confirmed corporate spinoffs compiled from SEC filings, press releases, and public records. The methodology is disclosed in all content.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Event study: fetch prices, compute CAR, save results |
| `screen.py` | Current screen: recent spinoffs and their performance |
| `generate_charts.py` | Generate PNG charts from results |
| `results/summary_metrics.json` | CAR statistics by category and window |
| `results/individual_spinoffs.csv` | Per-event returns at each window |
| `results/parent_vs_child.json` | Parent vs child CAR comparison |

## Usage

```bash
cd backtests

# Run the full event study
python3 spinoff/backtest.py --output spinoff/results/ --verbose

# Screen recent spinoffs
python3 spinoff/screen.py --months 36

# Generate charts (after running backtest)
python3 spinoff/generate_charts.py
```

## Curated Spinoff List (30 events, 2011–2024)

| Parent | Child | Date | Description |
|--------|-------|------|-------------|
| MSI | MMI | 2011-01-04 | Motorola Solutions / Motorola Mobility |
| EXPE | TRIP | 2011-12-21 | Expedia / TripAdvisor |
| COP | PSX | 2012-05-01 | ConocoPhillips / Phillips 66 |
| MPC | MPLX | 2012-10-31 | Marathon Petroleum / MPLX LP |
| ABT | ABBV | 2013-01-02 | Abbott Labs / AbbVie |
| PFE | ZTS | 2013-02-01 | Pfizer / Zoetis |
| GE | SYF | 2014-07-31 | GE Capital / Synchrony Financial |
| ADP | CDK | 2014-10-01 | ADP / CDK Global |
| EBAY | PYPL | 2015-07-20 | eBay / PayPal |
| HPQ | HPE | 2015-11-02 | HP Inc / HP Enterprise |
| YUM | YUMC | 2016-11-01 | Yum Brands / Yum China |
| XRX | CNDT | 2017-01-03 | Xerox / Conduent |
| MET | BHF | 2017-08-07 | MetLife / Brighthouse Financial |
| PNR | NVT | 2018-04-30 | Pentair / nVent Electric |
| TNL | WH | 2018-06-01 | Wyndham Worldwide (now TNL) / Wyndham Hotels |
| HON | GTX | 2018-10-04 | Honeywell / Garrett Motion |
| HON | REZI | 2018-11-01 | Honeywell / Resideo Technologies |
| DD | DOW | 2019-04-01 | DowDuPont / Dow Inc |
| DD | CTVA | 2019-06-03 | DowDuPont / Corteva |
| DHR | NVST | 2019-09-20 | Danaher / Envista |
| RTX | OTIS | 2020-04-03 | Raytheon Technologies / Otis |
| RTX | CARR | 2020-04-03 | Raytheon Technologies / Carrier |
| FTV | VNT | 2020-10-09 | Fortive / Vontier |
| PFE | VTRS | 2020-11-16 | Pfizer / Viatris |
| IBM | KD | 2021-11-04 | IBM / Kyndryl |
| GSK | HLN | 2022-07-18 | GSK / Haleon |
| GE | GEHC | 2023-01-04 | GE / GE Healthcare |
| JNJ | KVUE | 2023-05-04 | J&J / Kenvue |
| MMM | SOLV | 2024-04-01 | 3M / Solventum |
| GE | GEV | 2024-04-02 | GE / GE Vernova |

**Note on deduplication:** RTX appears as parent for both OTIS and CARR on the same date. Parent events are deduplicated by (symbol, date) to avoid double-counting identical return observations.

## Key Limitations

- Small sample (~30 spinoffs, ~55 events) limits statistical power. t-statistics should be interpreted with caution.
- Curated list overrepresents large, well-known spinoffs. Results may not generalize to smaller spinoffs.
- SPY benchmark doesn't control for sector or size effects.
- Period (2011–2024) was predominantly a bull market. Results in bear markets may differ.

## Content

Blog and content are in `ts-content-creator/content/_current/event-05-spinoff/`.
