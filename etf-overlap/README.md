# ETF Overlap Analysis

Measure how much overlap exists between ETFs. Most multi-ETF portfolios share
far more holdings than investors realize, creating false diversification and
wasted fees.

## What This Does

- **Pairwise overlap**: Count-based and weight-based overlap between any two ETFs
- **Portfolio analysis**: Check your multi-ETF portfolio for redundancy
- **Most widely held stocks**: Which stocks appear in the most ETFs
- **Popular combo analysis**: Pre-configured analysis of common portfolio templates

This is a cross-sectional analysis tool, not a return-based backtest. ETF
holdings are current snapshots from FMP.

## Quick Start

```bash
# Check overlap between two ETFs
python3 etf-overlap/screen.py SPY QQQ

# Check your portfolio
python3 etf-overlap/screen.py SPY QQQ VTI VXUS BND --details

# Full analysis with all popular ETFs
python3 etf-overlap/analysis.py --output results/etf_overlap.json --verbose
```

## Key Findings

**Same index, different provider (95-100% overlap):**
- SPY / VOO / IVV all track the S&P 500
- QQQ / QQQM both track the Nasdaq-100
- VTI / ITOT both target total US market

**Overlapping universes (80-99% overlap):**
- SPY + QQQ: 92 shared stocks (81% of QQQ)
- SPY + VTI: 507 shared stocks (99% of SPY is in VTI)

**Genuine diversification (0-3% overlap):**
- SPY + VXUS: near-zero overlap (US vs international)
- SPY + IWM: zero overlap (large-cap vs small-cap)
- SPY + BND: zero overlap (equity vs bonds)

## Data

- **Source**: FMP ETF Holdings via Ceta Research
- **Coverage**: 13,981 ETFs, 5.9M+ holdings, 146K+ unique stocks
- **Table**: `etf_holder` (symbol, asset, weightPercentage)

## Academic References

- Madhavan (2016). "Exchange-Traded Funds and the New Dynamics of Investing."
- Antoniou, Doukas & Subrahmanyam (2023). "ETF Overlap and Price Discovery."
- Statman (1987). "How Many Stocks Make a Diversified Portfolio?"
