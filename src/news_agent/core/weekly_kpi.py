"""Weekly KPI — ONE honest, reproducible measurement of the 4 agreed
metrics, computed the SAME way every week so growth is comparable.

The four axes (manager-agreed):
  1. coverage      — of what the EDITOR published this week, how much did we
                     even collect (matched in our cache)?
  2. found_right   — of what WE accepted (would publish), how much did the
                     editor actually publish (matched in the archive)?
  3. section_right — of the found-right matches, how often is our section the
                     editor's section?
  4. reject_right  — of what WE rejected, how much did the editor NOT publish
                     (proxy for "correctly rejected"; the PRECISE version
                     needs the editor's да/нет labelling — flagged below).

Honesty by construction:
  * ONE matcher used both directions — exact url_key, else brand-gated fuzzy
    title (token_set_ratio >= threshold, min-token guard). This is the SAME
    strict method as miss_funnel.py — NOT the retired "brand somewhere in the
    blob" token match that inflated coverage to 83%.
  * every metric returns its denominator + a `match_method` breakdown so the
    report can show how much rests on fuzzy matches (a large fuzzy share =
    softer number). No metric is reported without its caveat.

Pure logic only (indexing + matching + metric math); the script does the
Sheets/SQLite I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from rapidfuzz import fuzz

from news_agent.core.brand_canonical import canonicalize_brand
from news_agent.core.primary_source import normalise_title
from news_agent.core.published_dedup import url_key

# Same threshold as miss_funnel.py (the validated coverage method) so the
# KPI coverage number IS the funnel's number — one consistent, honest
# measurement, not a third competing one. The min-token guard (below) is what
# kills the short-string trap (token_set_ratio=100 on a 1-token title).
DEFAULT_THRESHOLD = 85.0
MIN_TITLE_TOKENS = 3


@dataclass
class Item:
    """A news item from either side (editor publication or our article)."""
    title: str
    url: str = ""
    section: str = ""
    title_alt: str = ""     # second language title (archive has EN+RU)

    @property
    def brand(self) -> str:
        return canonicalize_brand(f"{self.title} {self.title_alt}")

    def norms(self) -> list[str]:
        out = []
        for t in (self.title, self.title_alt):
            n = normalise_title(t) if t else ""
            if len([x for x in n.split() if x]) >= MIN_TITLE_TOKENS:
                out.append(n)
        return out


@dataclass
class Index:
    url_keys: set = field(default_factory=set)
    # brand -> list of (normalised_title, section)
    by_brand: dict = field(default_factory=dict)


def build_index(items: list[Item]) -> Index:
    idx = Index()
    for it in items:
        if it.url:
            k = url_key(it.url)
            if k:
                idx.url_keys.add(k)
        b = it.brand
        for n in it.norms():
            idx.by_brand.setdefault(b, []).append((n, it.section))
    return idx


def match(item: Item, idx: Index, threshold: float = DEFAULT_THRESHOLD):
    """Return (matched: bool, method: 'url'|'fuzzy'|'none', section: str).
    Exact url_key first (zero false positives), then brand-gated fuzzy title
    (only within the same canonical brand → no cross-brand collisions)."""
    if item.url and url_key(item.url) in idx.url_keys:
        return True, "url", ""        # section unknown via url (archive section
                                      # is looked up by the caller if needed)
    qn = item.norms()
    if not qn:
        return False, "none", ""
    cands = idx.by_brand.get(item.brand, [])
    best = 0.0
    best_sec = ""
    for n, sec in cands:
        s = max(fuzz.token_set_ratio(q, n) for q in qn)
        if s > best:
            best, best_sec = s, sec
    if best >= threshold:
        return True, "fuzzy", best_sec
    return False, "none", ""


def _rate(hit: int, tot: int) -> float:
    return hit / tot if tot else 0.0


def coverage(editor_pubs: list[Item], collection_idx: Index,
             threshold: float = DEFAULT_THRESHOLD) -> dict:
    """Of editor publications, how many we collected."""
    hit = url = fuzzy = 0
    misses = []
    for p in editor_pubs:
        m, method, _ = match(p, collection_idx, threshold)
        if m:
            hit += 1
            url += method == "url"
            fuzzy += method == "fuzzy"
        else:
            misses.append(p.title[:70])
    return {"metric": "coverage", "hit": hit, "total": len(editor_pubs),
            "rate": _rate(hit, len(editor_pubs)),
            "by_url": url, "by_fuzzy": fuzzy, "miss_examples": misses[:8]}


# A real prog run collects hundreds of articles; a day with fewer than this
# means no prog ran (an ops gap), so it must NOT count against coverage.
MIN_PROG_DAY = 20


def active_collection_days(collected_dates: list, min_per_day: int = MIN_PROG_DAY) -> set:
    """The calendar dates on which a prog actually ran, inferred from how
    many articles we collected that day. `collected_dates` is a list of
    date objects (one per collected article, in the window)."""
    from collections import Counter
    counts = Counter(collected_dates)
    return {d for d, n in counts.items() if n >= min_per_day}


def coverage_by_day(dated_pubs: list, active_dates: set, collection_idx: Index,
                    threshold: float = DEFAULT_THRESHOLD) -> list:
    """Per-day coverage rows for display. `dated_pubs` is list[(date, Item)].
    Days with no prog are marked prog=False (excluded from the headline)."""
    from collections import defaultdict
    by_day: dict = defaultdict(list)
    for d, it in dated_pubs:
        by_day[d].append(it)
    rows = []
    for d in sorted(by_day):
        pubs = by_day[d]
        if d in active_dates:
            c = coverage(pubs, collection_idx, threshold)
            rows.append({"date": d.isoformat(), "pubs": len(pubs),
                         "found": c["hit"], "rate": c["rate"], "prog": True})
        else:
            rows.append({"date": d.isoformat(), "pubs": len(pubs),
                         "found": 0, "rate": 0.0, "prog": False})
    return rows


def coverage_day_aligned(dated_pubs: list, active_dates: set,
                         collection_idx: Index,
                         threshold: float = DEFAULT_THRESHOLD) -> dict:
    """Day-aligned coverage: of editor pubs on days a prog ACTUALLY ran, how
    many we collected. Pubs on no-prog days are reported separately
    (excluded_no_prog) — that's an uptime gap, not a coverage failure.

    `dated_pubs` is list[(date, Item)]; `active_dates` the set of prog days."""
    on_active = [it for d, it in dated_pubs if d in active_dates]
    excluded = sum(1 for d, _ in dated_pubs if d not in active_dates)
    cov = coverage(on_active, collection_idx, threshold)
    cov["excluded_no_prog"] = excluded
    cov["active_days"] = len(active_dates)
    return cov


def precision_and_section(accepted: list[Item], archive_idx: Index,
                          archive_section_by_norm: dict,
                          threshold: float = DEFAULT_THRESHOLD) -> dict:
    """found_right: of our accepted items, how many the editor published;
    section_right: of those, how many sections agree."""
    found = sec_ok = sec_tot = url = fuzzy = 0
    for a in accepted:
        m, method, matched_sec = match(a, archive_idx, threshold)
        if not m:
            continue
        found += 1
        url += method == "url"
        fuzzy += method == "fuzzy"
        # editor's section for the matched story
        ed_sec = matched_sec
        if method == "url" and not ed_sec:
            ed_sec = archive_section_by_norm.get(url_key(a.url), "")
        if a.section and ed_sec:
            sec_tot += 1
            sec_ok += _section_eq(a.section, ed_sec)
    return {
        "found_right": {"metric": "found_right", "hit": found,
                        "total": len(accepted), "rate": _rate(found, len(accepted)),
                        "by_url": url, "by_fuzzy": fuzzy},
        "section_right": {"metric": "section_right", "hit": sec_ok,
                          "total": sec_tot, "rate": _rate(sec_ok, sec_tot)},
    }


def reject_right(rejected: list[Item], archive_idx: Index,
                 threshold: float = DEFAULT_THRESHOLD) -> dict:
    """Proxy: of what we rejected, how many the editor did NOT publish.
    NOTE: 'not in archive' ≈ 'correctly rejected' but is a PROXY — the editor
    may simply never have seen it. The precise version needs editor да/нет
    labelling (the rejection-labelling ritual)."""
    correct = false_reject = 0
    fr_examples = []
    for r in rejected:
        m, _method, _ = match(r, archive_idx, threshold)
        if m:
            false_reject += 1            # editor DID publish it → we wrongly rejected
            if len(fr_examples) < 8:
                fr_examples.append(r.title[:70])
        else:
            correct += 1
    tot = len(rejected)
    return {"metric": "reject_right", "hit": correct, "total": tot,
            "rate": _rate(correct, tot), "false_rejects": false_reject,
            "false_reject_examples": fr_examples, "is_proxy": True}


def _section_eq(a: str, b: str) -> bool:
    """Loose section equality — strip case/punct so 'Confirmed' == 'confirmed'
    and 'Dealer news / Promo' == 'Dealer news/promo'."""
    def n(s):
        return "".join(ch for ch in s.lower() if ch.isalnum())
    return n(a) == n(b) if a and b else False
