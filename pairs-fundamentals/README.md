# Pairs Trading: Fundamentals Backtest

Correlation-based pairs trading backtested across 12 exchanges from 2005-2024. Market-neutral strategy using annual formation periods and z-score entry signals.

## Signal

| Parameter | Value |
|-----------|-------|
| Universe | Top 30 stocks/sector by market cap |
| Pair selection | Same sector, 252-day returns correlation > 0.70, top 20 pairs |
| Entry | \|z-score\| > 1.5 at formation-year end |
| Hedge ratio | OLS from log-prices during formation year |
| Return model | Equal-dollar: `-sign(z) × (Return_A - Return_B) / 2` |
| Rebalancing | Annual (formation year Y-1, trading year Y) |
| Costs | 4 one-way legs per pair (entry + exit × 2 stocks) |
| Cash condition | < 3 active pairs → 100% cash (earn 0%) |

**Reference:** Gatev, Goetzmann & Rouwenhorst (2006), *Review of Financial Studies*

## Results Summary (2005-2024)

| Exchange | CAGR | vs SPY | Sharpe | MaxDD | Cash% |
|----------|------|--------|--------|-------|-------|
| LSE (UK) | 1.83% | -8.18% | -0.201 | -10.86% | 65% |
| JNB (SA) | 1.57% | -8.44% | -0.777 | -30.32% | 15% |
| BSE+NSE (India) | 0.72% | -9.29% | -1.793 | -3.52% | 75% |
| **JPX (Japan)** | **0.61%** | -9.40% | **+0.141** | -10.61% | **5%** |
| **NYSE+NASDAQ+AMEX (US)** | **0.33%** | -9.68% | -0.407 | -12.99% | 25% |
| SHZ+SHH (China) | 0.23% | -9.79% | -0.570 | -14.95% | 40% |
| HKSE (HK) | 0.02% | -9.99% | -0.767 | -14.80% | 40% |
| TSX (Canada) | -0.05% | -10.06% | -0.668 | -11.46% | 25% |
| STO (Sweden) | -0.25% | -10.26% | -0.393 | -16.72% | 40% |
| XETRA (Germany) | -0.83% | -10.84% | -1.184 | -16.14% | 70% |
| KSC (Korea) | -0.90% | -10.91% | -0.976 | -20.83% | 45% |
| TAI+TWO (Taiwan) | -1.85% | -11.87% | -0.490 | -31.22% | 55% |

SPY benchmark: 10.01% CAGR (2005-2024).

Japan is the only exchange with a positive Sharpe ratio (+0.141) and the highest investment rate (95% of years active). The strategy confirms academic findings that pairs trading profitability declined sharply after 2002 (Do & Faff, 2010).

**Excluded from content:**
- SIX (Switzerland): 2007 single-year +56.72% — implausible data artifact
- SET (Thailand): 90% cash — signal almost never fires
- SES (Singapore): 90% cash — insufficient correlated pairs at z-threshold

## Usage

```bash
export CR_API_KEY="your_key_here"

# Screen current correlated pairs (US)
python3 pairs-fundamentals/screen.py

# Screen Japanese pairs
python3 pairs-fundamentals/screen.py --preset japan

# Run full backtest on US
python3 pairs-fundamentals/backtest.py

# Run on all exchanges, save results
python3 pairs-fundamentals/backtest.py --global --output results/exchange_comparison.json

# Regenerate charts from existing results
python3 pairs-fundamentals/generate_charts.py
```

## Files

- `backtest.py` — Full historical backtest. Fetches prices via API, caches in DuckDB, runs correlation + spread computation locally.
- `screen.py` — Current pairs screen. Shows top correlated same-sector pairs for the last 252 trading days.
- `generate_charts.py` — Regenerate all charts from `results/exchange_comparison.json`.
- `results/` — Pre-computed results (exchange_comparison.json, per-exchange returns JSON).
- `charts/` — PNG charts used in blog posts.

## How the backtest works

1. **Fetch** (20-60s): Pull daily prices 2004-present for the top 30 large-cap stocks per sector, plus SPY.
2. **Cache**: All price data loaded into in-memory DuckDB.
3. **Annual loop** (2005-2024):
   - Compute pairwise daily-return correlations across all same-sector pairs over the prior year
   - Select up to 20 pairs with correlation > 0.70 and minimum 200 common trading days
   - For each pair, estimate hedge ratio (OLS beta from log-prices) and spread mean/std
   - Compute z-score at year start. If |z| > 1.5, the pair is active
   - Annual pair return = `-sign(z) × (Return_A - Return_B) / 2` (equal-dollar, market-neutral)
   - Apply transaction costs: 4 one-way legs per pair
4. **Metrics**: Portfolio metrics via shared `metrics.py` vs SPY benchmark.

## Exchange presets

| Preset | Exchanges | MCap Threshold |
|--------|-----------|----------------|
| `--preset us` | NYSE, NASDAQ, AMEX | $1B USD |
| `--preset japan` | JPX | ¥10B |
| `--preset india` | BSE, NSE | ₹20B |
| `--preset uk` | LSE | £500M |
| `--preset germany` | XETRA | €500M |
| `--preset china` | SHZ, SHH | ¥2B CNY |

## Data notes

- **Short-selling**: Results assume short-selling is possible. In practice, China (SHZ+SHH) and India (BSE+NSE) have significant short-selling restrictions. Results there are theoretical.
- **Survivorship bias**: Uses currently active stocks (isActivelyTrading = true). Stocks that went bankrupt or were acquired are excluded, which slightly biases results upward.
- **Annual signal**: Annual z-score check means many opportunities within the year are missed. Higher-frequency implementations would find more pairs but require daily data access.
