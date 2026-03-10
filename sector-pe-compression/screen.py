#!/usr/bin/env python3
"""
Sector P/E Compression - Current Screen

Shows which S&P 500 sectors currently have compressed P/E ratios
relative to their 5-year historical average.

Usage:
    python3 sector-pe-compression/screen.py
    python3 sector-pe-compression/screen.py --verbose
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch


LOOKBACK_YEARS = 5
Z_THRESHOLD = -1.0

SECTOR_TO_ETF = {
    "Technology": "XLK",
    "Energy": "XLE",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Industrials": "XLI",
    "Basic Materials": "XLB",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}


def run_screen(client, verbose=False):
    """Compute current sector P/E z-scores from S&P 500 constituent data."""
    sql = f"""
        WITH sector_members AS (
            SELECT DISTINCT symbol, sector
            FROM sp500_constituent
            WHERE sector IS NOT NULL AND sector != ''
        ),
        latest_pe AS (
            SELECT r.symbol, r.priceToEarningsRatio AS pe,
                   r.dateEpoch AS filing_epoch,
                   ROW_NUMBER() OVER (PARTITION BY r.symbol ORDER BY r.dateEpoch DESC) AS rn
            FROM financial_ratios r
            JOIN sector_members s ON r.symbol = s.symbol
            WHERE r.period = 'FY'
              AND r.priceToEarningsRatio > 0
              AND r.priceToEarningsRatio < 200
        ),
        latest_mcap AS (
            SELECT m.symbol, m.marketCap,
                   ROW_NUMBER() OVER (PARTITION BY m.symbol ORDER BY m.dateEpoch DESC) AS rn
            FROM key_metrics m
            JOIN sector_members s ON m.symbol = s.symbol
            WHERE m.period = 'FY' AND m.marketCap > 0
        ),
        current_data AS (
            SELECT sm.sector, lp.symbol, lp.pe, lm.marketCap
            FROM sector_members sm
            JOIN latest_pe lp ON sm.symbol = lp.symbol AND lp.rn = 1
            JOIN latest_mcap lm ON sm.symbol = lm.symbol AND lm.rn = 1
            WHERE lp.pe > 0 AND lm.marketCap > 0
        ),
        current_sector_pe AS (
            SELECT sector,
                   COUNT(*) AS n_stocks,
                   SUM(marketCap) / NULLIF(SUM(marketCap / pe), 0) AS current_pe
            FROM current_data
            GROUP BY sector
            HAVING COUNT(*) >= 3
        ),
        historical_annual AS (
            SELECT sm.sector,
                   EXTRACT(YEAR FROM CAST(r.date AS DATE)) AS yr,
                   SUM(m.marketCap) / NULLIF(SUM(m.marketCap / r.priceToEarningsRatio), 0) AS sector_pe
            FROM financial_ratios r
            JOIN sector_members sm ON r.symbol = sm.symbol
            JOIN key_metrics m ON r.symbol = m.symbol
                AND ABS(r.dateEpoch - m.dateEpoch) < 86400 * 60
            WHERE r.period = 'FY'
              AND r.priceToEarningsRatio > 0
              AND r.priceToEarningsRatio < 200
              AND m.marketCap > 0
              AND EXTRACT(YEAR FROM CAST(r.date AS DATE)) >= EXTRACT(YEAR FROM CURRENT_DATE) - {LOOKBACK_YEARS}
              AND EXTRACT(YEAR FROM CAST(r.date AS DATE)) < EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY sm.sector, yr
            HAVING COUNT(DISTINCT r.symbol) >= 3
        ),
        sector_stats AS (
            SELECT sector,
                   AVG(sector_pe) AS avg_pe_5yr,
                   STDDEV(sector_pe) AS std_pe_5yr,
                   COUNT(*) AS n_years
            FROM historical_annual
            GROUP BY sector
            HAVING COUNT(*) >= 3
        )
        SELECT
            c.sector,
            c.n_stocks,
            ROUND(c.current_pe, 1) AS current_pe,
            ROUND(s.avg_pe_5yr, 1) AS avg_pe_5yr,
            ROUND(s.std_pe_5yr, 1) AS std_pe,
            s.n_years,
            ROUND((c.current_pe - s.avg_pe_5yr) / NULLIF(s.std_pe_5yr, 0), 2) AS z_score,
            CASE
                WHEN (c.current_pe - s.avg_pe_5yr) / NULLIF(s.std_pe_5yr, 0) < {Z_THRESHOLD}
                    THEN 'COMPRESSED'
                WHEN (c.current_pe - s.avg_pe_5yr) / NULLIF(s.std_pe_5yr, 0) > 1.0
                    THEN 'STRETCHED'
                ELSE 'NORMAL'
            END AS signal
        FROM current_sector_pe c
        JOIN sector_stats s ON c.sector = s.sector
        ORDER BY z_score ASC
    """
    results = client.query(sql, verbose=verbose)
    return results or []


def main():
    parser = argparse.ArgumentParser(description="Sector P/E Compression live screen")
    parser.add_argument("--api-key", default=os.environ.get("CR_API_KEY") or os.environ.get("TS_API_KEY"))
    parser.add_argument("--base-url", default="https://api.cetaresearch.com/api/v1")
    parser.add_argument("--json", dest="output_json", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    print("Sector P/E Compression Screen | S&P 500 Sectors")
    print(f"Signal: z-score < {Z_THRESHOLD} (sector PE more than 1 std dev below 5yr avg)")
    print("=" * 80)

    results = run_screen(cr, verbose=args.verbose)

    if not results:
        print("No data returned.")
        return

    if args.output_json:
        print(json.dumps(results, indent=2))
        return

    print(f"\n{'Sector':<28} {'ETF':<6} {'Current PE':>11} {'5yr Avg':>8} {'Std':>6} {'Z-Score':>8} {'Signal':<12}")
    print("-" * 80)
    for r in results:
        sector = r.get("sector", "")
        etf = SECTOR_TO_ETF.get(sector, "-")
        signal = r.get("signal", "")
        flag = " <-- BUY" if signal == "COMPRESSED" else ""
        print(f"{sector:<28} {etf:<6} {r.get('current_pe', 'N/A'):>11} "
              f"{r.get('avg_pe_5yr', 'N/A'):>8} {r.get('std_pe', 'N/A'):>6} "
              f"{r.get('z_score', 'N/A'):>8} {signal:<12}{flag}")

    compressed = [r for r in results if r.get("signal") == "COMPRESSED"]
    if compressed:
        print(f"\nCompressed sectors ({len(compressed)}): "
              + ", ".join(f"{r['sector']} ({SECTOR_TO_ETF.get(r['sector'], '?')}, z={r.get('z_score', 'N/A')})"
                          for r in compressed))
    else:
        print("\nNo compressed sectors. Strategy holds SPY.")

    print("\nData: Ceta Research (FMP), S&P 500 constituents, FY filings.")


if __name__ == "__main__":
    main()
