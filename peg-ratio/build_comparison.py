#!/usr/bin/env python3
"""Build exchange_comparison.json from individual returns_*.json files."""
import json
import os
from pathlib import Path

results_dir = Path(__file__).parent / "results"

# Order matches presets in backtest.py main()
PRESETS = [
    ("us", "NYSE_NASDAQ_AMEX"),
    ("india", "NSE"),
    ("china", "SHZ_SHH"),
    ("hongkong", "HKSE"),
    ("taiwan", "TAI"),
    ("japan", "JPX"),
    ("uk", "LSE"),
    ("thailand", "SET"),
    ("germany", "XETRA"),
    ("korea", "KSC"),
    ("canada", "TSX"),
    ("sweden", "STO"),
    ("switzerland", "SIX"),
    ("indonesia", "JKT"),
    ("southafrica", "JNB"),
    ("norway", "OSL"),
    ("singapore", "SES"),
    ("italy", "MIL"),
    ("malaysia", "KLS"),
]

all_results = {}
for preset_name, key in PRESETS:
    path = results_dir / f"returns_{key}.json"
    if path.exists():
        with open(path) as f:
            all_results[key] = json.load(f)
        cagr = all_results[key]["portfolio"]["cagr"]
        print(f"  {key}: CAGR={cagr}%")
    else:
        print(f"  {key}: MISSING ({path})")

out = results_dir / "exchange_comparison.json"
with open(out, "w") as f:
    json.dump(all_results, f, indent=2)
print(f"\nWrote {len(all_results)} exchanges to {out}")
