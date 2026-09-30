"""Copy regenerated chart PNGs into the content repo's blog directories.

WHY THIS EXISTS instead of the shell loop in the bias-fix runbook. That loop
resolves a source by stripping the leading "N_" from the blog's filename:

    bare=$(basename "$blog_png" | sed 's/^[0-9]*_//')
    [ -f "$CHARTS_DIR/$bare" ] && cp ...

Topics disagree about whether charts/ carries the numeric prefix. low-debt
writes 1_china_cumulative_growth.png, so the stripped lookup for
china_cumulative_growth.png misses, and because the guard is `[ -f ] && cp`
the miss is SILENT: nothing is copied, nothing is reported, and the blog keeps
its old chart while the run looks successful.

So: try the exact filename first, then the stripped one, and report every
unmatched blog image loudly.

Dry-run by default. Copy one topic with `<backtest-dir> --apply`; every topic
needs `--apply --all`.
"""
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTENT = "/Users/swas/Desktop/Swas/Kite/ATO_SUITE/ts-content-creator/content/_ready"

# backtest dir -> content dir. The word order often reverses between the two,
# and a naive suffix match silently reports "no content dir" and skips.
TOPIC_DIRS = {
    "small-cap": "growth-04-small-cap",
    "low-debt": "risk-01-low-debt",
    "value-momentum": "factor-03-value-momentum",
    "high-yield": "dividend-01-high-yield",
    "price-momentum": "momentum-01-12-month",
    "small-value": "factor-05-small-value",
    "price-to-sales": "value-09-price-to-sales",
    "ev-ebitda": "value-03-ev-ebitda",
    "equity-growth": "balance-05-equity-growth",
    "working-capital": "balance-03-working-capital",
    "price-to-book": "value-05-price-to-book",
    "altman-z": "quality-02-altman-z",
    "oversold-quality": "reversion-03-oversold-quality",
    "ev-ebitda-relative": "timing-02-ev-ebitda-relative",
    "pe-mean-revert": "timing-01-pe-mean-revert",
    "qarp": "factor-02-qarp",
    "analyst-revision": "momentum-05-analyst-revision",
    # NOTE: two content dirs carry this name. reversion-05 is the one whose
    # regions match this backtest; sector-06-pe-compression has only a `us`
    # blog and belongs to a different study.
    "pe-compression": "reversion-05-pe-compression",
    "margin-expansion": "quality-08-margin-expansion",
    "52-week-low": "reversion-01-52-week-low",
    "sector-rotation": "reversion-04-sector-rotation",
    "revenue-surprise": "momentum-04-revenue-surprise",
    # Topic slug is sector-01-rotation (see sector-momentum/README.md); the
    # content dir on disk is named sector-07-momentum.
    "sector-momentum": "sector-07-momentum",
    "fcf-growth": "growth-03-fcf-growth",
    "rd-efficiency": "growth-05-rd-efficiency",
    "yield-gap": "reversion-06-yield-gap",
    "volume-confirmed-momentum": "momentum-08-volume-confirmed",
    "52-week-high": "momentum-02-52-week-high",
    "asset-growth": "balance-04-asset-growth",
    "asset-light": "quality-07-asset-light",
    "capex-efficiency": "cashflow-03-capex-efficiency",
    "cash-conversion": "quality-04-cash-conversion",
    "cyclical-timing": "sector-05-cyclical-timing",
    "dcf-discount": "value-06-dcf-discount",
    "dcf-threshold": "timing-04-dcf-threshold",
    "defensive-quality": "sector-04-defensive",
    "deleveraging": "risk-03-deleveraging",
    "dividend-coverage": "dividend-03-coverage",
    "dividend-growth": "dividend-02-growth",
    "dividend-sustainability": "dividend-07-sustainability",
    "dogs-of-dow": "dividend-06-dogs-of-dow",
    "earnings-consistency": "growth-02-earnings-consistency",
    "earnings-yield": "value-08-earnings-yield",
    "etf-concentration": "etf-05-concentration",
    "etf-crowding": "etf-02-crowding",
    "etf-rebalancing": "etf-04-rebalancing",
    "etf-underowned": "etf-03-underowned",
    "fcf-compounders": "cashflow-04-compounders",
    "fcf-conversion": "cashflow-01-fcf-yield",
    "fcf-yield": "value-04-fcf-yield",
    "garp": "factor-08-garp",
    # One backtest feeds two topics. ncav's README runs graham-net-net/backtest.py.
    "graham-net-net": ["value-02-graham-net-net", "balance-01-ncav"],
    "graham-number": "value-10-graham-number",
    "graham-timing": "timing-03-graham-timing",
    "income-quality": "quality-06-income-quality",
    "industry-leader": "sector-03-industry-leader",
    "low-pe": "value-01-classic-pe",
    "low-vol-quality": "factor-06-low-vol-quality",
    "magic-formula": "factor-01-magic-formula",
    "market-share": "growth-06-market-share",
    "net-debt-ebitda": "risk-04-net-debt-ebitda",
    "ocf-growth": "cashflow-02-ocf-growth",
    "owner-earnings": "cashflow-05-owner-earnings",
    "pairs-fundamentals": ["pairs-01-fundamentals", "pairs-05-backtest"],
    "piotroski": "quality-01-piotroski",
    "quality-momentum": "factor-04-quality-momentum",
    "relative-strength": "momentum-07-relative-strength",
    "rising-yield": "dividend-05-rising-yield",
    "roe-dupont": "quality-05-roe-dupont",
    "sustained-roic": "quality-03-roic-sustained",
    "tangible-book": "balance-02-tangible-book",
}

# Regions whose blog is not live; copying into them is harmless but noisy.
SKIP_REGIONS = {("value-05-price-to-book", "brazil")}   # unpublished, SAO data corrupt

# Chart files sometimes name a region differently from the blog directory.
REGION_ALIASES = {"us": ["us", "usmajor", "us_major", "nyse_nasdaq_amex"],
                  "hongkong": ["hongkong", "hkse"],
                  "southafrica": ["southafrica", "jse", "jnb"],
                  "korea": ["korea", "ksc"],
                  "uk": ["uk", "lse"],
                  "switzerland": ["switzerland", "six"],
                  "sweden": ["sweden", "sto"],
                  "taiwan": ["taiwan", "tai"],
                  # fcf-growth names its charts by exchange code, not country.
                  "india": ["india", "nse", "bse-nse"],
                  "germany": ["germany", "xetra"],
                  "canada": ["canada", "tsx"],
                  "japan": ["japan", "jpx"],
                  "china": ["china", "shz-shh", "shh-shz"]}


def resolve(charts_dir, blog_png, region):
    """Source path for a blog image, trying every naming convention in use.

    Topics disagree three ways: whether charts/ carries the leading "N_",
    whether the region appears in the filename at all (price-to-sales blogs
    are 1_cumulative_growth.png against canada_cumulative_growth.png), and
    what the region is called (altman-z writes usmajor for the us blog).
    """
    name = os.path.basename(blog_png)
    stripped = name.split("_", 1)[1] if name[:1].isdigit() and "_" in name else name
    prefix = name.split("_", 1)[0] + "_" if name[:1].isdigit() and "_" in name else ""

    cands = [name, stripped]
    for alias in REGION_ALIASES.get(region, [region]):
        # region-qualified: canada_cumulative_growth.png, 1_usmajor_annual_returns.png
        cands.append(f"{alias}_{stripped}")
        cands.append(f"{prefix}{alias}_{stripped}")
        # region swapped for the blog's own region token
        if stripped.startswith(f"{region}_"):
            rest = stripped[len(region) + 1:]
            cands.append(f"{alias}_{rest}")
            cands.append(f"{prefix}{alias}_{rest}")

    seen = set()
    for cand in cands:
        if cand in seen:
            continue
        seen.add(cand)
        p = os.path.join(charts_dir, cand)
        if os.path.exists(p):
            return p, cand
    return None, stripped


def run(apply=False, only=None):
    """Copy charts into the content repo.

    only: optional list of backtest-dir names. Restricts the run to those
    topics, so a single-topic rerun doesn't sweep every other topic's charts
    into the content repo as a side effect.
    """
    copied = skipped = missing = 0
    misses = []
    items = {k: v for k, v in TOPIC_DIRS.items() if not only or k in only}
    if only:
        unknown = sorted(set(only) - set(TOPIC_DIRS))
        if unknown:
            print(f"  WARNING: not in TOPIC_DIRS, ignored: {unknown}")
    pairs = [(t, c) for t, v in sorted(items.items())
             for c in ([v] if isinstance(v, str) else v)]
    for topic, cdir in pairs:
        charts = f"{ROOT}/{topic}/charts"
        blogs = f"{CONTENT}/{cdir}/blogs"
        if not os.path.isdir(charts):
            print(f"  {topic:<20} NO charts/ dir")
            continue
        if not os.path.isdir(blogs):
            print(f"  {topic:<20} NO content dir at {cdir}")
            continue
        c = m = 0
        for region in sorted(os.listdir(blogs)):
            rdir = f"{blogs}/{region}"
            if not os.path.isdir(rdir) or (cdir, region) in SKIP_REGIONS:
                continue
            for png in sorted(f for f in os.listdir(rdir) if f.endswith(".png")):
                dest = f"{rdir}/{png}"
                src, tried = resolve(charts, dest, region)
                if src is None:
                    m += 1
                    misses.append(f"{cdir}/{region}/{png}  (no {tried} in {topic}/charts)")
                    continue
                if apply:
                    shutil.copy2(src, dest)
                c += 1
        copied += c
        missing += m
        flag = "" if not m else f"   <-- {m} UNMATCHED"
        print(f"  {topic:<20} {c:>3} images{flag}")

    print(f"\n{copied} images {'copied' if apply else 'would be copied'}, {missing} unmatched")
    if misses:
        print("\nUNMATCHED (blog keeps its old chart, investigate before publishing):")
        for x in misses:
            print(f"  {x}")
    if not apply:
        print("\nDRY RUN, nothing written")
    return missing


if __name__ == "__main__":
    only = [a for a in sys.argv[1:] if not a.startswith("-")] or None
    # A bare --apply once overwrote 90 PNGs across 16 unrelated topics (2026-09-30).
    if "--apply" in sys.argv and not only and "--all" not in sys.argv:
        sys.exit("refusing --apply without a topic: pass <backtest-dir> ... or --all")
    sys.exit(1 if run(apply="--apply" in sys.argv, only=only) and "--apply" in sys.argv else 0)
