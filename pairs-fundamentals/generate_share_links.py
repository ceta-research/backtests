"""Generate shareable query links for pairs-01-fundamentals backtest.sql."""
import os
import sys
import json
import requests
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

TS_API_KEY = os.environ["TS_API_KEY"]
TS_BASE = "https://tradingstudio.finance/api/v1"

Q3_SQL = """WITH large_caps AS (
    SELECT DISTINCT p.symbol, p.sector
    FROM profile p
    JOIN (
        SELECT symbol, marketCap,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
        FROM key_metrics
        WHERE period = 'FY' AND marketCap IS NOT NULL
    ) km ON p.symbol = km.symbol AND km.rn = 1
    WHERE p.sector = 'Energy'
      AND p.exchange IN ('NYSE', 'NASDAQ', 'AMEX')
      AND p.isActivelyTrading = true
      AND km.marketCap > 1000000000
),
daily_ret AS (
    SELECT
        eod.symbol,
        CAST(eod.date AS DATE) AS trade_date,
        (eod.adjClose - LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date))
            / NULLIF(LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date), 0) AS ret
    FROM stock_eod eod
    JOIN large_caps lc ON eod.symbol = lc.symbol
    WHERE CAST(eod.date AS DATE) >= (CURRENT_DATE - INTERVAL '365 days')
      AND eod.adjClose IS NOT NULL AND eod.adjClose > 0
),
pair_corr AS (
    SELECT
        a.symbol AS symbol_a,
        b.symbol AS symbol_b,
        ROUND(CORR(a.ret, b.ret), 4) AS correlation,
        COUNT(*) AS common_days
    FROM daily_ret a
    JOIN daily_ret b ON a.trade_date = b.trade_date AND a.symbol < b.symbol
    WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL
    GROUP BY a.symbol, b.symbol
    HAVING COUNT(*) >= 200
)
SELECT symbol_a, symbol_b, correlation, common_days
FROM pair_corr
WHERE correlation > 0.60
ORDER BY correlation DESC
LIMIT 50"""

Q4_SQL = """WITH sector_map AS (
    SELECT DISTINCT symbol, sector
    FROM profile
    WHERE sector IS NOT NULL
      AND exchange IN ('NYSE', 'NASDAQ', 'AMEX')
      AND isActivelyTrading = true
),
ranked_by_mktcap AS (
    SELECT sm.symbol, sm.sector, km.marketCap,
           ROW_NUMBER() OVER (PARTITION BY sm.sector ORDER BY km.marketCap DESC) AS sector_rank
    FROM sector_map sm
    JOIN (
        SELECT symbol, marketCap,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
        FROM key_metrics
        WHERE period = 'FY' AND marketCap IS NOT NULL
    ) km ON sm.symbol = km.symbol AND km.rn = 1
    WHERE km.marketCap > 1000000000
),
large_caps AS (
    SELECT symbol, sector FROM ranked_by_mktcap WHERE sector_rank <= 30
),
daily_ret AS (
    SELECT
        eod.symbol,
        CAST(eod.date AS DATE) AS trade_date,
        (eod.adjClose - LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date))
            / NULLIF(LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date), 0) AS ret
    FROM stock_eod eod
    JOIN large_caps lc ON eod.symbol = lc.symbol
    WHERE CAST(eod.date AS DATE) >= (CURRENT_DATE - INTERVAL '365 days')
      AND eod.adjClose IS NOT NULL AND eod.adjClose > 0
),
pair_corr AS (
    SELECT
        a.symbol AS symbol_a,
        b.symbol AS symbol_b,
        la.sector,
        ROUND(CORR(a.ret, b.ret), 4) AS correlation,
        COUNT(*) AS common_days
    FROM daily_ret a
    JOIN daily_ret b ON a.trade_date = b.trade_date AND a.symbol < b.symbol
    JOIN large_caps la ON a.symbol = la.symbol
    JOIN large_caps lb ON b.symbol = lb.symbol
    WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL
      AND la.sector = lb.sector
    GROUP BY a.symbol, b.symbol, la.sector
    HAVING COUNT(*) >= 200
)
SELECT symbol_a, symbol_b, sector, correlation, common_days
FROM pair_corr
WHERE correlation > 0.70
ORDER BY correlation DESC
LIMIT 100"""

Q6_SQL = """WITH sector_map AS (
    SELECT DISTINCT symbol, sector
    FROM profile
    WHERE sector IS NOT NULL
      AND exchange IN ('NYSE', 'NASDAQ', 'AMEX')
      AND isActivelyTrading = true
),
ranked_by_mktcap AS (
    SELECT sm.symbol, sm.sector, km.marketCap,
           ROW_NUMBER() OVER (PARTITION BY sm.sector ORDER BY km.marketCap DESC) AS sector_rank
    FROM sector_map sm
    JOIN (
        SELECT symbol, marketCap,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
        FROM key_metrics
        WHERE period = 'FY' AND marketCap IS NOT NULL
    ) km ON sm.symbol = km.symbol AND km.rn = 1
    WHERE km.marketCap > 1000000000
),
large_caps AS (
    SELECT symbol, sector FROM ranked_by_mktcap WHERE sector_rank <= 30
),
daily_ret AS (
    SELECT
        eod.symbol,
        CAST(eod.date AS DATE) AS trade_date,
        (eod.adjClose - LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date))
            / NULLIF(LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date), 0) AS ret
    FROM stock_eod eod
    JOIN large_caps lc ON eod.symbol = lc.symbol
    WHERE CAST(eod.date AS DATE) >= (CURRENT_DATE - INTERVAL '365 days')
      AND eod.adjClose IS NOT NULL AND eod.adjClose > 0
),
pair_corr AS (
    SELECT
        a.symbol AS symbol_a,
        b.symbol AS symbol_b,
        la.sector,
        ROUND(CORR(a.ret, b.ret), 4) AS correlation,
        COUNT(*) AS common_days
    FROM daily_ret a
    JOIN daily_ret b ON a.trade_date = b.trade_date AND a.symbol < b.symbol
    JOIN large_caps la ON a.symbol = la.symbol
    JOIN large_caps lb ON b.symbol = lb.symbol
    WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL
      AND la.sector = lb.sector
    GROUP BY a.symbol, b.symbol, la.sector
    HAVING COUNT(*) >= 200
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY sector ORDER BY correlation DESC) AS sector_rank
    FROM pair_corr
    WHERE correlation > 0.50
)
SELECT sector, symbol_a, symbol_b, correlation, common_days
FROM ranked
WHERE sector_rank <= 5
ORDER BY sector, correlation DESC"""


def execute_and_share(label, sql):
    print(f"\n{'='*60}")
    print(f"Processing: {label}")

    # Step 1: Submit via api.cetaresearch.com + CR_API_KEY
    cr = CetaResearch()
    print(f"  Submitting query...")
    task_id = cr._submit(sql, timeout=300, limit=10000, memory_mb=16384, threads=6)
    print(f"  task_id: {task_id}")

    # Step 2: Poll until complete
    print(f"  Polling...")
    result = cr._poll(task_id, timeout=300, verbose=True)
    status = result.get("status")
    print(f"  Status: {status}")
    if status != "completed":
        print(f"  ERROR: {result.get('error', result)}")
        return None

    # Step 3: Share via tradingstudio.finance + TS_API_KEY with both query + taskId
    print(f"  Sharing...")
    resp = requests.post(
        f"{TS_BASE}/data-explorer/share",
        headers={"X-API-Key": TS_API_KEY, "Content-Type": "application/json"},
        json={"query": sql, "taskId": task_id},
    )
    print(f"  Share response [{resp.status_code}]: {resp.text[:300]}")
    if resp.status_code == 200:
        share_id = resp.json().get("shareId")
        url = f"cetaresearch.com/data-explorer?q={share_id}"
        print(f"  URL: {url}")
        return share_id
    return None


if __name__ == "__main__":
    q3_id = execute_and_share("Q3 - Energy sector pairs (US)", Q3_SQL)
    q4_id = execute_and_share("Q4 - Top pairs all sectors (US)", Q4_SQL)
    q6_id = execute_and_share("Q6 - Current examples by sector (US)", Q6_SQL)

    print("\n" + "="*60)
    print("RESULTS:")
    print(f"  Q3 (Energy sector pairs):       {q3_id}")
    print(f"  Q4 (Top pairs all sectors):     {q4_id}")
    print(f"  Q6 (Current examples, sectors): {q6_id}")
    print()
    if q3_id:
        print(f"  cetaresearch.com/data-explorer?q={q3_id}")
    if q4_id:
        print(f"  cetaresearch.com/data-explorer?q={q4_id}")
    if q6_id:
        print(f"  cetaresearch.com/data-explorer?q={q6_id}")
