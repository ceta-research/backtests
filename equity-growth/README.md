# Compounding Equity Screen

Screens for companies that have grown shareholders' equity at 10%+ CAGR over 5 consecutive years, with quality overlays to ensure growth is operationally earned.

## Strategy

A company's book value (shareholders' equity) grows when it retains more earnings than it pays out. Consistent 10%+ equity CAGR over 5 years requires both profitability and capital discipline — the company must be generating returns above its cost of capital and deploying them back into the business.

The signal is simple: find companies that have been compounding their own value for half a decade, then buy the ones doing it fastest.

## Signal

| Filter | Threshold | Purpose |
|--------|-----------|---------|
| 5-yr shareholders' equity CAGR | > 10% | Core compounding signal |
| Return on Equity (TTM) | > 8% | Growth from operations, not share issuances |
| Operating Profit Margin (TTM) | > 8% | Operational quality, pricing power |
| Market Cap | > exchange threshold | Liquidity filter (per-exchange local currency) |

**Ranking:** Top 30 by equity CAGR (highest compounders first), equal weight.

**Cash rule:** Hold cash if fewer than 10 stocks qualify. This preserves capital during periods of tight screening conditions.

## Parameters

- **Rebalancing:** Annual (July). Annual FY filings are available by then with 45-day filing lag.
- **Equity CAGR window:** 3.5 to 7.0 years (target: 5 years, tolerance: ±1.5 years for data availability)
- **Universe:** Full exchange universe (NYSE + NASDAQ + AMEX for US), not index-constrained
- **Transaction costs:** Size-tiered model from `costs.py`
- **Period:** 2000–2025

## Academic Basis

Directly inspired by Warren Buffett's use of book value per share CAGR as a proxy for intrinsic value growth (Berkshire Hathaway annual letters). Related to:

- **Quality Minus Junk** (Asness, Frazzini & Pedersen, 2019): Consistent equity growth is a quality signal
- **Sustainable growth rate** (Gordon Growth Model): g = ROE × (1 − payout ratio). Companies compounding equity at 10%+ either have high ROE or high retention — both are positive quality signals.
- **Asset Growth Anomaly** (Cooper, Gulen & Schill, 2008): Equity growth is the retained-earnings component of the same balance sheet dynamic.

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Full historical backtest (2000-2025, all exchanges) |
| `screen.py` | Current stock screen using latest TTM/FY data |
| `generate_charts.py` | Chart generation from exchange_comparison.json |
| `results/exchange_comparison.json` | Computed results per exchange |
| `results/returns_*.json` | Per-period returns per exchange |

## Usage

```bash
# US backtest (default)
python3 equity-growth/backtest.py

# India
python3 equity-growth/backtest.py --preset india

# All exchanges
python3 equity-growth/backtest.py --global --output equity-growth/results/exchange_comparison.json --verbose

# Current stock screen
python3 equity-growth/screen.py
python3 equity-growth/screen.py --preset india

# Generate charts (run after --global backtest)
python3 equity-growth/generate_charts.py
```

## Data Notes

- `totalStockholdersEquity` from FMP `balance_sheet` table (FY period only)
- 45-day filing lag applied for point-in-time correctness
- CAGR capped at 100% to filter inflation-distorted results (e.g., hyperinflationary markets)
- Exchanges excluded: ASX, SAO (adjClose price artifacts affect return calculation)
- Both equity endpoints must be positive (avoids CAGR on negative or zero equity)

## Exchange Coverage

| Exchange | Preset | Notes |
|----------|--------|-------|
| NYSE + NASDAQ + AMEX | `--preset us` | Primary |
| BSE + NSE | `--preset india` | Strong universe |
| JPX | `--preset japan` | Deep FY history |
| LSE | `--preset uk` | Solid coverage |
| XETRA | `--preset germany` | |
| SHZ + SHH | `--preset china` | |
| TSX | `--preset canada` | |
| HKSE | `--preset hongkong` | |
| TAI + TWO | `--preset taiwan` | |
| STO | `--preset sweden` | |
| SIX | `--preset switzerland` | |
| SET | `--preset thailand` | Smaller universe |
| KSC | `--preset korea` | Smaller universe |
| JNB | `--preset southafrica` | Very small universe |

**Not included:** ASX (adjClose artifacts), SAO/Brazil (adjClose artifacts), IST/Turkey (lira inflation distorts CAGR)
