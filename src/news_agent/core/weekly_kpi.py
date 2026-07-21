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


# Numero-sign forms: the editor writes «№7», our feed writes «N°7» — after
# normalise_title they tokenised differently and a genuinely-covered story
# (DS N°7 Elysee, jul-21 audit) counted as a coverage MISS. Unify to " n "
# before normalisation on BOTH sides.
_NUMERO_RE = None  # compiled lazily (module import stays light)
_DASH_RE = None
_ANCHOR_RE = None


def _fold_numero(t: str) -> str:
    """«№7»/«N°7» → " n7" — one joined token, so it doubles as a model
    anchor; both sides fold identically («№7» editor-side, «N°7» ours)."""
    global _NUMERO_RE
    if _NUMERO_RE is None:
        import re
        _NUMERO_RE = re.compile(r"(?:№|[Nn]°)\s*")
    return _NUMERO_RE.sub(" n", t or "")


def _fold_dashes(t: str) -> str:
    """Fold dash/pipe separators to spaces BEFORE normalise_title: its
    source-suffix peeler treats a trailing «- Word Word» as a site name and
    ate the most distinctive token of the jul-21 audit miss («…electric №7
    SUV - Elysee in France» lost "Elysee"). Metric-side only."""
    global _DASH_RE
    if _DASH_RE is None:
        import re
        _DASH_RE = re.compile(r"\s[—–|-]\s")
    return _DASH_RE.sub(" ", t or "")


def _anchor_tokens(norm: str) -> frozenset:
    """Model-like tokens: contain BOTH a letter and a digit (h10, n7, 07l).
    Pure numbers and years are excluded — they anchor nothing."""
    global _ANCHOR_RE
    if _ANCHOR_RE is None:
        import re
        _ANCHOR_RE = re.compile(r"^(?=.*[a-z])(?=.*\d)[a-z0-9]{2,10}$")
    return frozenset(w for w in (norm or "").split() if _ANCHOR_RE.match(w))


# Corporate FAMILIES for the metric's brand gate only. The jul-21 coverage
# audit found a covered story counted as a miss because the editor titled it
# «Haval … GWM H10» while our source wrote «Great Wall H10» — three names,
# one family, and the brand-gated fuzzy never compared them. Bridging is
# METRIC-ONLY (dedup keeps brands separate on purpose) and the >=85 title
# similarity still applies, so cross-brand collisions stay bounded.
_BRAND_FAMILIES: list[set] = [
    {"haval", "gwm", "great wall", "tank", "wey", "ora"},
]


def _brand_bucket(brand: str) -> str:
    b = (brand or "").strip().lower()
    for fam in _BRAND_FAMILIES:
        if b in fam:
            return "|".join(sorted(fam))
    return b


@dataclass
class Item:
    """A news item from either side (editor publication or our article)."""
    title: str
    url: str = ""
    section: str = ""
    title_alt: str = ""     # second language title (archive has EN+RU)

    @property
    def brand(self) -> str:
        return _brand_bucket(canonicalize_brand(f"{self.title} {self.title_alt}"))

    def norms(self) -> list[str]:
        out = []
        for t in (self.title, self.title_alt):
            t = _fold_dashes(_fold_numero(t))
            n = normalise_title(t) if t else ""
            if len([x for x in n.split() if x]) >= MIN_TITLE_TOKENS:
                out.append(n)
        return out


@dataclass
class Index:
    url_keys: set = field(default_factory=set)
    # brand -> list of (normalised_title, section)
    by_brand: dict = field(default_factory=dict)


# A shared MODEL anchor (letter+digit token: h10, n7) inside the SAME
# non-empty brand bucket is strong evidence two rewordings describe one story
# — the jul-21 audit found two PROVEN-covered stories counted as misses at
# ratio ~52 («Haval … GWM H10» vs «Great Wall H10 …», «DS №7 - Elysee» vs
# «DS N°7 Elysee …»). With an anchor the similarity bar drops to this value;
# without one the strict DEFAULT_THRESHOLD stands. Kept above the 50s the
# audit pairs actually score, documented risk: two DIFFERENT same-model
# stories in one window can now cross-match — bounded by the anchor + bucket
# + ratio, and honest in the direction of fixing PROVEN undercounts.
ANCHORED_THRESHOLD = 50.0


def build_index(items: list[Item]) -> Index:
    idx = Index()
    for it in items:
        if it.url:
            k = url_key(it.url)
            if k:
                idx.url_keys.add(k)
        b = it.brand
        for n in it.norms():
            idx.by_brand.setdefault(b, []).append((n, it.section, _anchor_tokens(n)))
    return idx


def match(item: Item, idx: Index, threshold: float = DEFAULT_THRESHOLD):
    """Return (matched: bool, method: 'url'|'fuzzy'|'none', section: str).
    Exact url_key first (zero false positives), then brand-gated fuzzy title
    (only within the same canonical brand → no cross-brand collisions); a
    shared model anchor lowers the similarity bar (see ANCHORED_THRESHOLD)."""
    if item.url and url_key(item.url) in idx.url_keys:
        return True, "url", ""        # section unknown via url (archive section
                                      # is looked up by the caller if needed)
    qn = item.norms()
    if not qn:
        return False, "none", ""
    cands = idx.by_brand.get(item.brand, [])
    q_anchors = frozenset().union(*(_anchor_tokens(q) for q in qn)) if qn else frozenset()
    best = 0.0
    best_sec = ""
    for n, sec, n_anchors in cands:
        s = max(fuzz.token_set_ratio(q, n) for q in qn)
        # anchored: same non-empty brand bucket + shared model token
        if (item.brand and q_anchors and n_anchors & q_anchors
                and s >= ANCHORED_THRESHOLD):
            s = max(s, threshold)  # promote past the strict bar
        if s > best:
            best, best_sec = s, sec
    if best >= threshold:
        return True, "fuzzy", best_sec
    return False, "none", ""


# ---- coverage-denominator exclusions (jul-21 miss audit, manager-agreed) ---
# Two publication classes are STRUCTURALLY unreachable for the bot, so keeping
# them in the denominator understates честный coverage:
#   * the editor's OWN test-drives («Deepal S07 test-drive») — original
#    content with no source URL to collect;
#   * routine daily market briefs (oil price, ЦБ currency rates) — the bot is
#     explicitly not supposed to collect these (manager decision 21.07).
_ROUTINE_RE = None


def is_editor_own_content(it: "Item") -> bool:
    sec = (it.section or "").lower()
    if "test-drive" in sec or sec == "test drive":
        return True
    t = (it.title or "").lower()
    return (not it.url) and ("test-drive" in t or "тест-драйв" in t
                             or "comparative test" in t)


def is_routine_brief(it: "Item") -> bool:
    global _ROUTINE_RE
    if _ROUTINE_RE is None:
        import re
        _ROUTINE_RE = re.compile(
            r"^\s*(oil prices\b|цены на нефть\b"
            r"|central bank (increased|decreased|raised|lowered)\b"
            r"|цб (повысил|понизил|снизил) (курс|ставку)"
            r"|курс (доллара|евро|юаня)\b)", re.I)
    for t in (it.title, it.title_alt):
        if t and _ROUTINE_RE.search(t):
            return True
    return False


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
