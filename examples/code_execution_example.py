#!/usr/bin/env python3
"""Example: Run a QARP screen via the Code Execution API.

This demonstrates submitting a self-contained Python script to Ceta Research's
cloud compute. The script runs on managed infrastructure with no local
dependencies needed.

Usage:
    export CR_API_KEY="your_key"
    python3 examples/code_execution_example.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

# The code to run on the cloud. This is a self-contained QARP screen
# that uses the Ceta Research SQL API directly.
SCREEN_CODE = '''
import os
import requests

API_KEY = os.environ.get("CR_API_KEY", "")
BASE_URL = "https://api.cetaresearch.com/api/v1"

session = requests.Session()
session.headers.update({"X-API-Key": API_KEY, "Content-Type": "application/json"})

sql = """
SELECT
    k.symbol,
    p.companyName,
    p.exchange,
    k.returnOnEquityTTM * 100 as roe_pct,
    s.piotroskiScore,
    f.priceToEarningsRatioTTM as pe_ratio,
    k.marketCap / 1e9 as market_cap_billions
FROM key_metrics_ttm k
JOIN financial_ratios_ttm f ON k.symbol = f.symbol
JOIN scores s ON k.symbol = s.symbol
JOIN profile p ON k.symbol = p.symbol
WHERE
    k.returnOnEquityTTM > 0.15
    AND f.debtToEquityRatioTTM >= 0
    AND f.debtToEquityRatioTTM < 0.5
    AND k.currentRatioTTM > 1.5
    AND k.incomeQualityTTM > 1
    AND s.piotroskiScore >= 7
    AND f.priceToEarningsRatioTTM > 5
    AND f.priceToEarningsRatioTTM < 25
    AND k.marketCap > 1000000000
    AND p.exchange IN ('NYSE', 'NASDAQ', 'AMEX')
ORDER BY s.piotroskiScore DESC, k.returnOnEquityTTM DESC
"""

import time, json

# Submit query
resp = session.post(f"{BASE_URL}/data-explorer/execute", json={
    "query": sql,
    "options": {"timeout": 60, "limit": 100, "format": "json"},
})
task_id = resp.json().get("taskId")

# Poll for completion
for _ in range(30):
    resp = session.get(f"{BASE_URL}/data-explorer/tasks/{task_id}")
    task = resp.json()
    if task["status"] == "completed":
        break
    time.sleep(2)

# Download results
artifact_id = task.get("artifactId")
resp = session.get(f"{BASE_URL}/data-explorer/artifacts/{artifact_id}/download/result.json")
results = resp.json()

print(f"QARP Screen: {len(results)} stocks qualify")
print(f"{'Symbol':<10} {'Company':<30} {'Exchange':<10} {'F-Score':>8} {'ROE%':>8} {'P/E':>8} {'MCap($B)':>10}")
print("-" * 88)
for r in results[:20]:
    print(f"{r['symbol']:<10} {r['companyName'][:28]:<30} {r['exchange']:<10} "
          f"{r['piotroskiScore']:>8} {r['roe_pct']:>7.1f}% {r['pe_ratio']:>8.1f} {r['market_cap_billions']:>9.1f}")
if len(results) > 20:
    print(f"... and {len(results) - 20} more")
'''


def main():
    cr = CetaResearch()

    print("Submitting QARP screen to Ceta Research cloud...")
    print()

    result = cr.execute_code(
        code=SCREEN_CODE,
        language="python",
        dependencies=["requests"],
        timeout_seconds=120,
        verbose=True,
    )

    print()
    if result.get("stdout"):
        print(result["stdout"])
    if result.get("stderr"):
        print("STDERR:", result["stderr"])
    print(f"\nStatus: {result['status']}, Exit code: {result.get('exitCode')}")
    print(f"Execution time: {result.get('executionTimeMs', 0) / 1000:.1f}s")


if __name__ == "__main__":
    main()
