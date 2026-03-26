#!/usr/bin/env python3
"""
Graham Net-Net: Data Inspection Script (corrected)

grahamNetNet in key_metrics IS per-share NCAV (not total NCAV).
Correct net-net filter: adjClose < grahamNetNet (both per-share, same currency).

balance_sheet_statement does NOT exist - must use key_metrics.grahamNetNet + stock_eod prices.

Run:
    python3 graham-net-net/data_inspection.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

client = CetaResearch()

def run(sql, label):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print('='*60)
    result = client.query(sql, format='json', timeout=300, memory_mb=4096, threads=2)
    if result:
        for row in result[:30]:
            print(row)
        print(f"  ({len(result)} rows)")
    else:
        print("  No results or error.")
    return result

# 1. Sanity check: verify grahamNetNet is per-share
run("""
SELECT k.symbol, k.date, k.period, k.marketCap, k.grahamNetNet,
       s.adjClose as price_at_year_start,
       ROUND(s.adjClose / NULLIF(k.grahamNetNet, 0), 3) as price_to_ncav
FROM key_metrics k
JOIN (
    SELECT symbol, LEFT(date,4) as yr, MIN(adjClose) as adjClose
    FROM stock_eod
    WHERE date >= '2006-01-01' AND date <= '2008-12-31'
    GROUP BY symbol, LEFT(date,4)
) s ON k.symbol = s.symbol AND CAST(LEFT(k.date, 4) AS INTEGER) + 1 = CAST(s.yr AS INTEGER)
WHERE k.period = 'FY'
    AND k.symbol = '000020.KS'
    AND k.grahamNetNet > 0
ORDER BY k.date
LIMIT 5
""", "SANITY: 000020.KS price vs grahamNetNet (price_to_ncav < 1 = net-net)")

# 2. Snapshot: count net-nets by exchange (2024 prices vs latest FY)
run("""
SELECT
    p.exchange,
    COUNT(DISTINCT k.symbol) as n_qualifying
FROM (
    SELECT symbol, grahamNetNet, marketCap, currentRatio
    FROM key_metrics
    WHERE period = 'FY'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
) k
JOIN (
    SELECT symbol, adjClose
    FROM stock_eod
    WHERE date >= '2024-01-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
) s ON k.symbol = s.symbol
JOIN profile p ON k.symbol = p.symbol
WHERE k.grahamNetNet > 0
    AND s.adjClose > 0.50
    AND s.adjClose < k.grahamNetNet
    AND k.marketCap > 0
GROUP BY p.exchange
ORDER BY n_qualifying DESC
LIMIT 25
""", "SNAPSHOT (2024): adjClose < grahamNetNet per exchange")

# 3. Annual count for key exchanges
run("""
SELECT
    p.exchange,
    CAST(LEFT(k.date, 4) AS INTEGER) + 1 as screening_year,
    COUNT(DISTINCT k.symbol) as n_qualifying
FROM (
    SELECT symbol, date, grahamNetNet, marketCap
    FROM key_metrics
    WHERE period = 'FY'
        AND grahamNetNet > 0
        AND marketCap > 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, LEFT(date,4) ORDER BY date DESC) = 1
) k
JOIN (
    SELECT symbol, LEFT(date,4) as price_year, MIN(adjClose) as adjClose
    FROM stock_eod
    WHERE date >= '2000-01-01'
    GROUP BY symbol, LEFT(date,4)
) s ON k.symbol = s.symbol
    AND CAST(LEFT(k.date, 4) AS INTEGER) + 1 = CAST(s.price_year AS INTEGER)
JOIN profile p ON k.symbol = p.symbol
WHERE k.grahamNetNet > 0
    AND s.adjClose > 0
    AND s.adjClose < k.grahamNetNet
    AND p.exchange IN ('JPX', 'NYSE', 'NASDAQ', 'AMEX', 'KSC', 'SHH', 'SHZ', 'BSE', 'NSE', 'HKSE', 'LSE', 'XETRA')
    AND CAST(LEFT(k.date, 4) AS INTEGER) BETWEEN 2000 AND 2023
GROUP BY p.exchange, screening_year
ORDER BY p.exchange, screening_year
""", "ANNUAL COUNT: net-nets per exchange per year")

# 4. Summary: avg qualifying per year
run("""
WITH annual AS (
    SELECT
        p.exchange,
        CAST(LEFT(k.date, 4) AS INTEGER) + 1 as screening_year,
        COUNT(DISTINCT k.symbol) as n_qualifying
    FROM (
        SELECT symbol, date, grahamNetNet, marketCap
        FROM key_metrics
        WHERE period = 'FY'
            AND grahamNetNet > 0
            AND marketCap > 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, LEFT(date,4) ORDER BY date DESC) = 1
    ) k
    JOIN (
        SELECT symbol, LEFT(date,4) as price_year, MIN(adjClose) as adjClose
        FROM stock_eod
        WHERE date >= '2000-01-01'
        GROUP BY symbol, LEFT(date,4)
    ) s ON k.symbol = s.symbol
        AND CAST(LEFT(k.date, 4) AS INTEGER) + 1 = CAST(s.price_year AS INTEGER)
    JOIN profile p ON k.symbol = p.symbol
    WHERE k.grahamNetNet > 0
        AND s.adjClose > 0
        AND s.adjClose < k.grahamNetNet
        AND CAST(LEFT(k.date, 4) AS INTEGER) BETWEEN 2000 AND 2023
    GROUP BY p.exchange, screening_year
)
SELECT
    exchange,
    ROUND(AVG(n_qualifying), 1) as avg_per_year,
    MIN(n_qualifying) as min_year,
    MAX(n_qualifying) as max_year,
    COUNT(*) as years_with_data
FROM annual
GROUP BY exchange
ORDER BY avg_per_year DESC
LIMIT 25
""", "SUMMARY: avg qualifying net-nets per year per exchange (2001-2024)")

# 5. Column check
run("""
SELECT symbol, date, period, marketCap, grahamNetNet, currentRatio,
       returnOnEquity
FROM key_metrics
WHERE period = 'FY' AND symbol = 'AAPL'
ORDER BY date DESC
LIMIT 5
""", "COLUMN CHECK: key_metrics for AAPL")

print("\n\nInspection complete.")
