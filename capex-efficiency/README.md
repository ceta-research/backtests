# Capital Expenditure Efficiency Strategy

## Overview

The Capital Expenditure Efficiency strategy identifies companies that generate strong returns on invested capital while requiring minimal reinvestment in fixed assets. These businesses convert more revenue into free cash flow, weather downturns more easily, and compound wealth faster over time.

## Strategy Logic

**Signal:**
- Capex-to-Revenue < 8% (asset-light business model)
- Capex-to-Operating Cash Flow < 40% (retains 60%+ of OCF as FCF)
- Return on Invested Capital > 15% (earns well above cost of capital)
- Operating Profit Margin > 15% (pricing power)
- Market Cap > threshold (exchange-specific)

**Portfolio Construction:**
- Top 30 stocks by highest ROIC
- Equal weight
- Cash position if < 10 stocks qualify

**Rebalancing:**
- Annual (July)
- Uses FY data with 45-day filing lag
- 2000-2025 backtest period

## Academic Foundation

1. **Cooper, Gulen & Schill (2008)** - "Asset Growth and the Cross-Section of Stock Returns"
   - Firms in highest decile of asset growth underperformed lowest decile by ~20%/year
   - Effect robust across size groups and international markets
   - Investors overreact to growth narratives accompanying heavy investment

2. **Titman, Wei & Xie (2004)** - "Capital Investments and Stock Returns"
   - Firms with heavy capital spending underperformed
   - Effect strongest among firms with discretionary spending and poor governance
   - Unchecked management tends to overinvest

3. **Novy-Marx (2013)** - "The Other Side of Value: The Gross Profitability Premium"
   - Gross profitability predicted returns as strongly as value metrics
   - Combining profitability with value measures produced best results
   - Low capex without high profitability doesn't create value

## Key Metrics

**Capex-to-Revenue**: Fraction of sales absorbed by capital spending
- Software/services: 2-4%
- Semiconductors: 30-40%
- Lower is better within same industry

**Capex-to-Operating Cash Flow**: How much OCF goes back into assets
- 20% = company keeps 80 cents per dollar of OCF as FCF
- 70% = only 30 cents survives as FCF

**Capex-to-Depreciation**: Growth vs. maintenance spending
- > 1.0 = expanding asset base
- < 1.0 = riding existing equipment harder
- Well below 1.0 for multiple years = potential under-investment

**ROIC**: Profit per dollar of invested capital
- Distinguishes efficient design from just not investing
- 3% capex + 25% ROIC = cash machine
- 3% capex + 4% ROIC = business not worth investing in

## When It Works Best

1. **Mature, stable markets** - Capital-light businesses become safe havens
2. **Rising interest rates** - Less capital needed means less affected by higher financing costs
3. **Late-cycle environments** - Aggressive expanders show diminishing returns
4. **Quality rotations** - Investors shift from speculation to profitability

## When It Struggles

1. **Early-cycle recoveries** - Capital-intensive cyclicals snap back hard
2. **Capex-driven growth phases** - Amazon warehouses, TSMC fabs require massive upfront spending
3. **Commodity booms** - Energy companies with massive capex generate eye-watering cash flows

## Limitations

**Sector bias**: Always tilts toward software, financials, healthcare, services. Systematically underweights industrials, energy, utilities, materials.

**Under-investment risk**: Low capex can signal milking existing assets. Check capex-to-depreciation over multiple years.

**Growth vs. maintenance capex**: Financial statements don't separate these. 15% capex could be 5% maintenance + 10% growth projects with 30% IRR (great) or 15% just to keep lights on (bad).

**ROIC calculation differences**: Multiple definitions exist. FMP's TTM ROIC may differ from manual calculations.

**Snapshot risk**: TTM data captures one year. Company just completing capex cycle looks capital-intensive even if entering harvest phase.

## Usage

**Screen current stocks:**
```bash
python3 capex-efficiency/screen.py
python3 capex-efficiency/screen.py --preset india
python3 capex-efficiency/screen.py --exchange BSE,NSE --limit 50
```

**Run backtest:**
```bash
# US stocks (default)
python3 capex-efficiency/backtest.py

# German stocks
python3 capex-efficiency/backtest.py --preset germany

# All exchanges (loop)
python3 capex-efficiency/backtest.py --global --output results/exchange_comparison.json --verbose

# Without transaction costs (academic baseline)
python3 capex-efficiency/backtest.py --no-costs
```

## Data Source

- FMP financial data warehouse via Ceta Research API
- Tables: `key_metrics` (FY), `financial_ratios` (FY), `stock_eod`, `profile`
- Columns: `capexToRevenue`, `capexToOperatingCashFlow`, `capexToDepreciation`, `returnOnInvestedCapital`, `operatingProfitMargin`, `marketCap`

## References

- Cooper, M., Gulen, H. & Schill, M. (2008). "Asset Growth and the Cross-Section of Stock Returns." *Journal of Finance*, 63(4), 1609-1651.
- Fairfield, P., Whisenant, J. & Yohn, T. (2003). "Accrued Earnings and Growth: Implications for Future Profitability and Market Mispricing." *The Accounting Review*, 78(1), 353-371.
- Novy-Marx, R. (2013). "The Other Side of Value: The Gross Profitability Premium." *Journal of Financial Economics*, 108(1), 1-28.
- Titman, S., Wei, K. & Xie, F. (2004). "Capital Investments and Stock Returns." *Journal of Financial and Quantitative Analysis*, 39(4), 677-700.
