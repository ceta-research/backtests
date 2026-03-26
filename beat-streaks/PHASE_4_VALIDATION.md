# Phase 4 Validation: Earnings Beat Streaks Event Study

**Date:** 2026-03-09
**Status:** Complete — 16 exchanges tested, 102,577 total streak events

---

## Executive Summary

Beat streaks show **positive abnormal returns across all tested exchanges** at T+1 (immediate reaction). The signal is strongest in:
- **India**: +2.10% T+21, +5.05% T+63 (t=17.92) — but 95% of data from 2022+
- **Taiwan**: +1.79% T+21 (t=9.43)
- **Brazil**: +1.46% T+21 (t=4.88), +2.49% T+63
- **Canada**: +1.22% T+21 (t=10.15), 25-year coverage
- **Japan**: +1.21% T+21 (t=9.85), strongest T+1 globally (+0.90%)
- **US**: +0.55% T+21 (t=14.83), 73K events, gold standard

**Key pattern:** Streak_2 and Streak_3 drive most of the signal. Streak_5+ shows diminishing returns (market partially prices in long streaks or they reflect expectation management).

**Critical data quality finding:** Most non-US exchanges show 70-95% of events from 2020+, reflecting FMP's analyst coverage expansion into emerging markets post-2019. Results represent recent regimes, not 25-year patterns.

---

## Exchange Results Summary

| Exchange | Events | T+1 CAR | T+21 CAR | T+63 CAR | t(21) | Sig | Eff. Start | Data Quality Flag |
|----------|--------|---------|----------|----------|-------|-----|------------|-------------------|
| **US** | 73,386 | +0.52% | +0.55% | +0.31% | +14.83 | ** | 2000 | ✓ Clean, 25-year depth |
| **Canada** | 5,684 | +0.83% | +1.22% | +1.76% | +10.15 | ** | 2000 | ✓ Clean, 25-year depth |
| **Japan** | 5,461 | +0.90% | +1.21% | +1.47% | +9.85 | ** | 2015 | ✓ Strong signal, 16 years |
| **Taiwan** | 3,721 | +0.49% | +1.79% | +1.20% | +9.43 | ** | 2015 | ✓ Strong T+21 |
| **India** | 3,483 | +0.58% | +2.10% | +5.05% | +12.59 | ** | **2022** | ⚠️ 95% from 2020+ |
| **UK** | 3,200 | +0.44% | +0.69% | +0.53% | +4.07 | ** | 2020 | ⚠️ 93% from 2020+ |
| **China** | 3,019 | +0.75% | +0.43% | +1.28% | +2.01 | * | 2015 | ✓ Weaker but significant |
| **Germany** | 1,514 | +0.31% | +1.00% | +0.68% | +3.88 | ** | 2022 | ⚠️ 96% from 2020+ |
| **Korea** | 1,466 | +0.57% | +0.84% | +0.60% | +2.99 | ** | 2016 | ✓ Good signal |
| **Brazil** | 1,358 | +0.30% | +1.46% | +2.49% | +4.88 | ** | 2015 | ✓ Strong T+63 |
| **Hong Kong** | 1,015 | +0.69% | +1.22% | +2.84% | +3.11 | ** | 2020 | ⚠️ 88% from 2020+ |
| **Thailand** | 1,021 | +0.42% | **-0.15%** | **-1.26%** | -0.50 | NS | 2017 | ⚠️ NEGATIVE T+21/T+63 |
| **Sweden** | 1,575 | +0.30% | +0.20% | +0.22% | +0.94 | NS | 2015 | ✗ Weak, not significant |
| **Australia** | 626 | +0.24% | +0.67% | +0.93% | +1.73 | NS | 2020 | ✗ 98% from 2020+, weak |
| **Norway** | 474 | +0.40% | +0.43% | +1.29% | +1.15 | NS | 2021 | ✗ Low sample, weak |
| **Switzerland** | 599 | +0.11% | +0.08% | +0.63% | +0.22 | NS | 2021 | ✗ No signal |

**Sig legend:** ** = p<0.01, * = p<0.05, NS = not significant

**Event study metrics:** CAR (Cumulative Abnormal Return) = stock return minus benchmark return over window. CAGR/Sharpe/MaxDD not applicable (this is event study, not portfolio strategy).

---

## Data Quality Concerns

### Critical Flags

**1. India (BSE+NSE) — Phenomenal results, but 4-year effective sample**
- T+63 CAR: +5.05% (t=17.92, strongest globally by far)
- **Issue:** 95% of events from 2020-2025 (only 174 events pre-2020)
- Effective coverage: 2022 spike (604 events → 922 in 2023)
- **Interpretation:** Results reflect India's 2022-2025 bull market more than the beat-streak signal itself
- **Recommendation:** Publish with **clear caveat** that effective data is 4 years (2022-2025), not 25 years
- Still publish — the signal is genuine for recent period, and transparency about recency builds trust

**2. Germany (XETRA) — 96% from 2020+**
- Only 61 events pre-2020, effective start 2022
- T+21 CAR +0.997% is significant (t=3.88) but recent-regime specific
- **Recommendation:** Publish with caveat on coverage period

**3. UK (LSE) — 93% from 2020+**
- Only 235 events pre-2020, effective start 2020
- Signal is significant (t=4.07) but also recent-data dominated
- **Recommendation:** Publish with caveat

**4. Australia (ASX) — 98% from 2020+, only 626 events**
- Effectively 5-year sample starting 2020
- Not statistically significant at T+21 (t=1.73)
- **Recommendation:** Comparison only, too short and weak

**5. Thailand (SET) — NEGATIVE T+21 and T+63**
- T+21: -0.15% (NS), T+63: -1.26% (t=-2.51, significant)
- Using SPY benchmark (no regional ETF for Thailand in framework)
- 73% of events from 2020+ when SPY strongly outperformed Asian markets
- **Interpretation:** Could be benchmark mismatch OR genuine finding (beat streaks don't work in Thailand)
- **Recommendation:** Include in comparison with honest framing ("not all markets show the pattern")

### Minor Flags

**6. Switzerland (SIX), Sweden (STO), Norway (OSL)** — Weak signals, comparison only
- None are statistically significant at T+21
- Coverage issues (effective starts 2021, 2015, 2021 respectively)
- **Recommendation:** Comparison charts only, no dedicated blogs

---

## Streak Length Pattern Analysis

**Key finding:** The signal is **non-monotonic** — streak_2 and streak_3 are strongest, streak_5+ is weakest.

| Streak | US T+1 | US T+21 | Japan T+21 | Canada T+21 | India T+21 | Taiwan T+21 |
|--------|--------|---------|------------|-------------|------------|-------------|
| Streak 2 | +0.63% | +0.77% | +1.47% | +1.33% | +2.65% | +1.55% |
| Streak 3 | +0.57% | +0.56% | +1.34% | +1.65% | +1.99% | +2.60% |
| Streak 4 | +0.55% | +0.62% | +0.82% | +1.22% | +1.28% | +1.57% |
| Streak 5+ | +0.42% | +0.42% | +0.92% | +1.21% | +1.66% | +1.53% |

**Pattern:** Streak_2 and Streak_3 show the strongest incremental signal. At Streak_5+, the market has partially priced in the pattern, or very long streaks are driven more by expectation management (sandbagging) than fundamental momentum.

**Exception:** India and Taiwan show less decay at Streak_5+, possibly due to lower market efficiency or different analyst behavior.

---

## Regional Blog Recommendations

### Tier 1: Strong Dedicated Blogs (Clear Signal + Adequate Sample)

**1. US (NYSE+NASDAQ+AMEX)** — Flagship
- **Why:** 73,386 events, 25 years, gold standard
- **Angle:** "Beat streaks produce +0.55% T+21 abnormal return. The 2nd and 3rd consecutive beats are the sweet spot."
- **Sample size:** All streak categories have 5K+ events (streak_5+ has 32K)
- **Narrative:** Diminishing returns at long streaks → market learns or expectation management dominates

**2. Canada (TSX)** — Strongest clean T+21 globally (+1.22%)
- **Why:** 5,684 events, 25 years, t=10.15 (highly significant)
- **Angle:** "Canadian beat streaks outperform US by 2.2x at T+21"
- **Narrative:** All streak categories significant, consistent pattern across 25 years

**3. Japan (JPX)** — Strongest T+1 globally (+0.90%)
- **Why:** 5,461 events, 16 years (2009+), t=9.85
- **Angle:** "Japanese markets show the strongest immediate reaction to beat streaks"
- **Narrative:** T+63 reaches +1.47%, persistent drift. Streak_3 T+63 is +2.35% (exceptional).

**4. Taiwan (TAI+TWO)** — Strongest T+21 proportionally (+1.79%)
- **Why:** 3,721 events, t=9.43, 13-year coverage (2012+)
- **Angle:** "Taiwanese tech dominance: beat streaks show +1.79% T+21 drift"
- **Caveat:** Uses SPY benchmark (no Taiwan regional ETF), effective start 2015

### Tier 2: Publish with Caveats (Interesting Results but Data Quality Flags)

**5. India (BSE+NSE)** — Phenomenal results, but recency caveat
- **Why:** +2.10% T+21, +5.05% T+63 (t=17.92) — strongest globally
- **Caveat:** **95% of data from 2020-2025** (only 174 events pre-2020). Reflects recent bull market.
- **Angle:** "Indian beat streaks: +5% at T+63. But there's a catch." → transparency builds trust
- **Recommendation:** Publish but clearly flag the effective 4-year sample (2022-2025)

**6. UK (LSE)** — Significant but recent-data dominated
- **Why:** 3,200 events, t=4.07, 25-year nominal coverage
- **Caveat:** 93% from 2020+, effective start 2020
- **Recommendation:** Publish with caveat on coverage period

**7. Brazil (SAO)** — Strong T+63 (+2.49%), good narrative
- **Why:** 1,358 events, t=4.88 at T+21, persistent drift to T+63
- **Coverage:** 2015+, adequate for emerging market
- **Recommendation:** Publish (EM beat streaks angle)

**8. Germany (XETRA)** — Borderline
- **Why:** 1,514 events, t=3.88 at T+21
- **Caveat:** 96% from 2020+, effective start 2022
- **Recommendation:** Publish with short coverage caveat, or comparison only

**9. Korea (KSC)** — Borderline
- **Why:** 1,466 events, t=2.99 at T+21
- **Coverage:** Effective from 2016, 68% from 2020+
- **Recommendation:** Comparison only (weaker signal, short history)

**10. Hong Kong (HKSE)** — Borderline
- **Why:** 1,015 events, +1.22% T+21 (t=3.11), +2.84% T+63
- **Caveat:** 88% from 2020+, effective start 2020
- **Recommendation:** Comparison only (sample too short/recent)

### Tier 3: Comparison Only (Weak Signal or Data Issues)

**11. China (SHZ+SHH)** — Weak T+21 (+0.43%, t=2.01*)
- **Reason:** Barely significant, effective from 2015
- **Recommendation:** Comparison only

**12. Thailand (SET)** — NEGATIVE T+21 and T+63
- **Finding:** T+21 -0.15% (NS), T+63 -1.26% (t=-2.51, significant)
- **Caveat:** Uses SPY benchmark (no SET regional ETF). 73% from 2020+.
- **Recommendation:** Include in comparison with honest framing ("counter-evidence")

**13. Sweden (STO)** — No signal (t=0.94, not significant)
- **Recommendation:** Comparison only

**14. Australia (ASX)** — Too short and weak
- **Issue:** 98% from 2020+, only 626 events, t=1.73 (NS)
- **Recommendation:** Comparison only

**15. Norway (OSL)** — Low sample, not significant
- **Issue:** 474 events, t=1.15 (NS)
- **Recommendation:** Comparison only

**16. Switzerland (SIX)** — No signal
- **Issue:** 599 events, t=0.22 (no statistical significance)
- **Recommendation:** Comparison only

---

## Recommended Blog Package

### Dedicated Regional Blogs (6-8 blogs)

**Must Write:**
1. **US** (flagship) — 73K events, clean 25-year data
2. **Canada** — strongest clean T+21 globally, 25-year depth
3. **Japan** — strongest T+1 globally, interesting Streak_3 pattern
4. **Taiwan** — strong T+21, tech-heavy market angle

**Strong Consider:**
5. **India** — phenomenal results (+5% T+63) with **transparency caveat** on 2022-2025 effective coverage
6. **Brazil** — EM angle, good T+63 persistence

**Borderline:**
7. **UK** — significant but 93% recent data
8. **Germany** — significant but 96% recent data

### Comparison Blog (Required)

**Title:** "Beat Streaks Work Globally — With Exceptions"
- Include all 16 exchanges
- Honest about Thailand's negative drift, Switzerland's no-signal, Sweden's weakness
- Highlight top performers (India, Taiwan, Japan, Canada)
- Flag coverage periods clearly ("India 2022+", "Germany 2022+")
- **Content angle:** Transparency about what works where, builds credibility

---

## Data Quality Assessment by Exchange

### Clean Data (Publish Confidently)

| Exchange | Events | Years | Coverage Pattern | Verdict |
|----------|--------|-------|------------------|---------|
| US | 73,386 | 25 | Consistent 1,000-5,000/yr since 2000 | ✓ Gold standard |
| Canada | 5,684 | 25 | Consistent 200-400/yr since 2000 | ✓ Clean |
| Japan | 5,461 | 16 | Ramp from 2015 (440/yr → 560/yr) | ✓ Strong from 2015 |
| Taiwan | 3,721 | 13 | Spike 2015 (31 → 219), stable 2015+ | ✓ Good from 2015 |

### Usable with Caveats

| Exchange | Events | Caveat | Publish Strategy |
|----------|--------|--------|------------------|
| **India** | 3,483 | 95% from 2020+, effective 2022-2025 | Publish with **bold caveat** on coverage |
| **UK** | 3,200 | 93% from 2020+, effective 2020-2022 | Publish with coverage caveat |
| **Germany** | 1,514 | 96% from 2020+, effective 2022 | Borderline — publish or comparison only |
| **Brazil** | 1,358 | Effective 2015+, but consistent | Publish (standard EM coverage) |
| **Hong Kong** | 1,015 | 88% from 2020+, effective 2020 | Comparison only (too short) |
| **Korea** | 1,466 | Effective 2016+, 68% from 2020+ | Comparison only (weaker signal) |
| **China** | 3,019 | Effective 2015+, lower beat rate | Comparison only (barely sig) |

### Weak/Exclude from Dedicated Blogs

| Exchange | Events | Issue | Action |
|----------|--------|-------|--------|
| Thailand | 1,021 | NEGATIVE T+21/T+63, benchmark mismatch possible | Comparison (counter-evidence) |
| Sweden | 1,575 | Not significant (t=0.94) | Comparison only |
| Australia | 626 | 98% from 2020+, not significant | Comparison only |
| Norway | 474 | Low sample, not significant | Comparison only |
| Switzerland | 599 | No signal (t=0.22) | Comparison only |

---

## Final Recommendations

### Blog Count: 6-8 Dedicated + 1 Comparison

**Core 4 (Must Write):**
1. US — flagship, clean data, 25 years
2. Canada — strongest clean T+21 (+1.22%), 25 years
3. Japan — strongest immediate reaction, Streak_3 outperforms Streak_2 at T+63
4. Taiwan — tech-heavy market, strong T+21

**Strong Additions (2-3):**
5. **India** — +5% T+63 with transparency caveat on 2022+ coverage (builds trust)
6. **Brazil** — EM beat streaks angle, persistent drift
7. **UK** — optional (significant but recent-data issue)

**Comparison Blog:**
- Include all 16 exchanges
- Highlight clean winners (Canada, Japan, Taiwan)
- Show India's phenomenal-but-recent results with caveat
- Include Thailand's negative finding (builds credibility)
- Transparent about coverage periods

### Total Content Package

- **6-8 regional blogs** (US, Canada, Japan, Taiwan, India, Brazil, +/- UK, Germany)
- **1 comparison blog** (16 exchanges)
- **15 LinkedIn posts** (5 per blog set: US, best regional, comparison)
- **2 Reddit posts** (US standalone, comparison)
- **5 video scripts** (already complete in _current/)
- **3 charts** per blog (CAR by streak, progression, specific to region)

---

## Next Steps (Phase 5)

Before proceeding to Phase 5 content generation:

**User Decision Required:**
1. **India:** Publish with recency caveat, or exclude due to short effective sample?
2. **Blog count:** Write 6, 7, or 8 dedicated blogs?
3. **Germany/UK:** Include or comparison-only?

**Recommended:** Write **6 dedicated blogs** (US, Canada, Japan, Taiwan, India with caveat, Brazil) + comparison (16 exchanges). This balances:
- Signal strength
- Data quality
- Honest transparency about limitations (India caveat)
- Geographic diversity (US, Canada, Asia x3, LatAm)
- Content volume (achievable without duplication)

**Chart generation next:** Run `generate_charts.py` after confirming blog list.
