"""Generate shareable query links for earnings-yield strategy."""
import os
import sys
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

TS_API_KEY = os.environ["TS_API_KEY"]
TS_BASE = "https://tradingstudio.finance/api/v1"

US_SIMPLE = """SELECT k.symbol, p.companyName, p.sector,
  ROUND(k.earningsYield * 100, 2) AS earnings_yield_pct,
  ROUND(k.marketCap / 1e9, 2) AS mktcap_b
FROM key_metrics k
JOIN profile p ON k.symbol = p.symbol
WHERE k.earningsYield > 0 AND k.period = 'FY'
  AND p.exchange IN ('NYSE', 'NASDAQ', 'AMEX')
  AND k.fiscalYear = (SELECT MAX(fiscalYear) FROM key_metrics WHERE period = 'FY')
ORDER BY k.earningsYield DESC LIMIT 50"""

US_ADVANCED = """SELECT k.symbol, p.companyName, p.sector,
  ROUND(k.earningsYield * 100, 2) AS ey_pct,
  ROUND(k.returnOnEquity * 100, 2) AS roe_pct,
  ROUND(f.debtToEquityRatio, 2) AS de,
  ROUND(f.interestCoverageRatio, 2) AS ic,
  ROUND(k.marketCap / 1e9, 2) AS mktcap_b
FROM key_metrics k
JOIN profile p ON k.symbol = p.symbol
JOIN financial_ratios f ON k.symbol = f.symbol AND f.period = 'FY'
  AND f.fiscalYear = k.fiscalYear
WHERE k.earningsYield > 0 AND k.returnOnEquity > 0.12
  AND f.debtToEquityRatio < 1.5 AND f.interestCoverageRatio > 3.0
  AND k.marketCap > 1e9 AND k.period = 'FY'
  AND p.exchange IN ('NYSE', 'NASDAQ', 'AMEX')
  AND k.fiscalYear = (SELECT MAX(fiscalYear) FROM key_metrics WHERE period = 'FY')
ORDER BY k.earningsYield DESC LIMIT 50"""

INDIA = """SELECT k.symbol, p.companyName,
  ROUND(k.earningsYield * 100, 2) AS ey_pct,
  ROUND(k.returnOnEquity * 100, 2) AS roe_pct,
  ROUND(k.marketCap / 1e9, 2) AS mktcap_b
FROM key_metrics k
JOIN profile p ON k.symbol = p.symbol
JOIN financial_ratios f ON k.symbol = f.symbol AND f.period = 'FY'
  AND f.fiscalYear = k.fiscalYear
WHERE k.earningsYield > 0 AND k.returnOnEquity > 0.12
  AND f.debtToEquityRatio < 1.5 AND f.interestCoverageRatio > 3.0
  AND k.marketCap > 5e9 AND k.period = 'FY'
  AND p.exchange IN ('BSE', 'NSE')
ORDER BY k.earningsYield DESC LIMIT 50"""


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
