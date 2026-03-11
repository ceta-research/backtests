"""Generate shareable query links for market-share strategy."""
import os
import sys
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

TS_API_KEY = os.environ["TS_API_KEY"]
TS_BASE = "https://tradingstudio.finance/api/v1"

US_SIMPLE = """SELECT
  curr.symbol,
  p.companyName,
  p.sector,
  ROUND((curr.revenue - prev.revenue) / prev.revenue * 100, 1) AS rev_growth_pct,
  ROUND(curr.revenue / 1e9, 2) AS revenue_b
FROM income_statement curr
JOIN income_statement prev
  ON curr.symbol = prev.symbol
  AND CAST(curr.fiscalYear AS INT) = CAST(prev.fiscalYear AS INT) + 1
  AND curr.period = 'FY' AND prev.period = 'FY'
JOIN profile p ON curr.symbol = p.symbol
WHERE prev.revenue > 0
  AND curr.revenue IS NOT NULL
  AND p.exchange IN ('NYSE', 'NASDAQ', 'AMEX')
  AND curr.fiscalYear = (SELECT MAX(fiscalYear) FROM income_statement WHERE period = 'FY')
ORDER BY rev_growth_pct DESC
LIMIT 50"""

US_ADVANCED = """WITH rev_growth AS (
  SELECT curr.symbol, p.sector,
    (curr.revenue - prev.revenue) / prev.revenue AS rev_growth
  FROM income_statement curr
  JOIN income_statement prev
    ON curr.symbol = prev.symbol
    AND CAST(curr.fiscalYear AS INT) = CAST(prev.fiscalYear AS INT) + 1
    AND curr.period = 'FY' AND prev.period = 'FY'
  JOIN profile p ON curr.symbol = p.symbol
  WHERE prev.revenue > 0 AND curr.revenue IS NOT NULL
    AND p.exchange IN ('NYSE', 'NASDAQ', 'AMEX')
    AND curr.fiscalYear = (SELECT MAX(fiscalYear) FROM income_statement WHERE period = 'FY')
),
sector_medians AS (
  SELECT sector,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rev_growth) AS median_growth
  FROM rev_growth GROUP BY sector HAVING COUNT(*) >= 3
)
SELECT g.symbol, p.companyName, g.sector,
  ROUND(g.rev_growth * 100, 1) AS rev_growth_pct,
  ROUND(sm.median_growth * 100, 1) AS sector_median_pct,
  ROUND((g.rev_growth - sm.median_growth) * 100, 1) AS excess_growth_pct,
  ROUND(k.returnOnEquity * 100, 1) AS roe_pct,
  ROUND(f.operatingProfitMargin * 100, 1) AS opm_pct,
  ROUND(k.marketCap / 1e9, 1) AS mktcap_b
FROM rev_growth g
JOIN sector_medians sm ON g.sector = sm.sector
JOIN profile p ON g.symbol = p.symbol
JOIN key_metrics k ON g.symbol = k.symbol AND k.period = 'FY'
JOIN financial_ratios f ON g.symbol = f.symbol AND f.period = 'FY'
WHERE (g.rev_growth - sm.median_growth) >= 0.10
  AND k.returnOnEquity > 0.08
  AND f.operatingProfitMargin > 0.05
  AND k.marketCap > 1e9
ORDER BY excess_growth_pct DESC
LIMIT 30"""

INDIA = """WITH rev_growth AS (
  SELECT curr.symbol, p.sector,
    (curr.revenue - prev.revenue) / prev.revenue AS rev_growth
  FROM income_statement curr
  JOIN income_statement prev
    ON curr.symbol = prev.symbol
    AND CAST(curr.fiscalYear AS INT) = CAST(prev.fiscalYear AS INT) + 1
    AND curr.period = 'FY' AND prev.period = 'FY'
  JOIN profile p ON curr.symbol = p.symbol
  WHERE prev.revenue > 0 AND p.exchange IN ('BSE', 'NSE')
),
sector_medians AS (
  SELECT sector,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rev_growth) AS median_growth
  FROM rev_growth GROUP BY sector HAVING COUNT(*) >= 3
)
SELECT g.symbol, p.companyName,
  ROUND((g.rev_growth - sm.median_growth) * 100, 1) AS excess_growth_pct
FROM rev_growth g
JOIN sector_medians sm ON g.sector = sm.sector
JOIN profile p ON g.symbol = p.symbol
JOIN key_metrics k ON g.symbol = k.symbol AND k.period = 'FY'
JOIN financial_ratios f ON g.symbol = f.symbol AND f.period = 'FY'
WHERE (g.rev_growth - sm.median_growth) >= 0.10
  AND k.returnOnEquity > 0.08
  AND f.operatingProfitMargin > 0.05
  AND k.marketCap > 5e9
ORDER BY excess_growth_pct DESC LIMIT 30"""


def execute_and_share(label, sql):
    print(f"\n{'='*60}")
    print(f"{label}")

    cr = CetaResearch()
    print(f"  Submitting...")
    task_id = cr._submit(sql, timeout=180, limit=10000, memory_mb=8192, threads=4)
    print(f"  task_id: {task_id}")

    print(f"  Polling...")
    result = cr._poll(task_id, timeout=180, verbose=False)
    status = result.get("status")
    if status != "completed":
        print(f"  ERROR: {result.get('error', result)}")
        return None
    print(f"  Completed")

    print(f"  Sharing...")
    resp = requests.post(
        f"{TS_BASE}/data-explorer/share",
        headers={"X-API-Key": TS_API_KEY, "Content-Type": "application/json"},
        json={"query": sql, "taskId": task_id},
    )
    if resp.status_code == 200:
        share_id = resp.json().get("shareId")
        print(f"  shareId: {share_id}")
        print(f"  URL: cetaresearch.com/data-explorer?q={share_id}")
        return share_id
    else:
        print(f"  ERROR [{resp.status_code}]: {resp.text}")
        return None


if __name__ == "__main__":
    us_simple = execute_and_share("US Simple Screen", US_SIMPLE)
    us_advanced = execute_and_share("US Advanced Screen", US_ADVANCED)
    india = execute_and_share("India Screen", INDIA)

    print("\n" + "="*60)
    print("SHAREABLE LINKS:")
    print(f"  US Simple:   {us_simple or 'FAILED'}")
    print(f"  US Advanced: {us_advanced or 'FAILED'}")
    print(f"  India:       {india or 'FAILED'}")
    print()
    if us_simple:
        print(f"Update metadata.yaml:")
        print(f"  us_simple: '{us_simple}'")
        print(f"  us_advanced: '{us_advanced}'")
        print(f"  india: '{india}'")
