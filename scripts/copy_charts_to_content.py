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

Dry-run by default, --apply to copy.
"""
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTENT = "/Users/swas/Desktop/Swas/Kite/ATO_SUITE/ts-content-creator/content/_ready"

# backtest dir -> content dir. The word order often reverses between the two,
# and a naive suffix match silently reports "no content dir" and skips.
TOPIC_DIRS = {
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
    # NOTE: two content dirs carry this name. reversion-05 is the one whose
    # regions match this backtest; sector-06-pe-compression has only a `us`
    # blog and belongs to a different study.
    "pe-compression": "reversion-05-pe-compression",
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
                  "taiwan": ["taiwan", "tai"]}


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


def run(apply=False):
    copied = skipped = missing = 0
    misses = []
    for topic, cdir in sorted(TOPIC_DIRS.items()):
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
    sys.exit(1 if run(apply="--apply" in sys.argv) and "--apply" in sys.argv else 0)
