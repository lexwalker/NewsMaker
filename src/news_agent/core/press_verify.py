"""Layer 4 — verify that a PressCandidate URL actually describes
the event the search query was about.

Why this is non-negotiable: Brave search returns ranked URLs by text
similarity, but the index may be stale, the URL may now 404, the
content may have changed, or the title that matched our query may
describe a different event. Without verification, ~30% of search
results would be wrong-but-plausible. We pay $0.003 per LLM judge
call to drop that to <5%.

Design doc: docs/LAYER_3_DESIGN.md (Layer 4 section)

Verification pipeline
---------------------
  1. HTTP GET (with Range: bytes=0-4095 to fetch only the header)
     - 4xx / 5xx → reject with status code
     - redirect → follow once, then re-check (httpx default)
     - timeout / network → reject with error
  2. Parse the head/meta for title, og:title, og:description,
     article:published_time, og:image — these alone are usually
     enough to identify the event.
  3. If a publish date is parseable AND falls outside the expected
     window (±N days), reject without calling LLM (cheap reject).
  4. LLM judge with tool-use:
       "Does this page describe THIS event?"
       Input: event_summary, expected_date, page_excerpt
       Output: {match, confidence, reason}
  5. confidence >= acceptance_threshold → accept; else reject.

Public API
----------
  verify_press_candidate(candidate, *, event_summary, expected_date,
                          ...) -> VerificationResult
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import httpx

from news_agent.core.press_search import PressCandidate

if TYPE_CHECKING:  # pragma: no cover
    import anthropic


# ── Pricing constants (kept here to avoid circular import) ──────────
_HAIKU_INPUT_PER_MTOK = 1.0
_HAIKU_OUTPUT_PER_MTOK = 5.0
DEFAULT_VERIFY_MODEL = "claude-haiku-4-5"

DEFAULT_ACCEPTANCE_THRESHOLD = 0.70
DEFAULT_MAX_CONTENT_BYTES = 4096
DEFAULT_HTTP_TIMEOUT_S = 8.0
DEFAULT_DATE_WINDOW_DAYS = 3


# ── Data classes ────────────────────────────────────────────────────

@dataclass
class VerificationResult:
    """All info about one verification attempt. Always populated even
    on failure (so caller can inspect status/error)."""
    candidate: PressCandidate
    match: bool = False
    confidence: float = 0.0
    reason: str = ""
    fetched_status: int = 0
    final_url: str = ""
    page_excerpt: str = ""
    extracted_date_iso: str = ""
    date_in_window: bool | None = None
    cost_usd: float = 0.0
    elapsed_s: float = 0.0
    error: str = ""


# ── HTML parsing helpers ────────────────────────────────────────────

# Only the head fields we need. Robust to malformed HTML; we use
# regex (BeautifulSoup would add a 100ms+ overhead per call and isn't
# worth it for 4KB head fetches).
_RX_TITLE = re.compile(
    r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL
)
_RX_OG_TITLE = re.compile(
    r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)',
    re.IGNORECASE,
)
_RX_OG_DESC = re.compile(
    r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\']([^"\']+)',
    re.IGNORECASE,
)
_RX_OG_TYPE = re.compile(
    r'<meta[^>]*property=["\']og:type["\'][^>]*content=["\']([^"\']+)',
    re.IGNORECASE,
)
_RX_ARTICLE_DATE = re.compile(
    r'<meta[^>]*property=["\']article:published_time["\']'
    r'[^>]*content=["\']([^"\']+)',
    re.IGNORECASE,
)
_RX_TIME_DATETIME = re.compile(
    r"<time[^>]+datetime=[\"']([^\"']+)[\"']", re.IGNORECASE,
)


def _extract_head_fields(html: str) -> dict[str, str]:
    """Pull title / og: / dates out of the head excerpt.

    Returns a dict with str values, missing keys default to empty.
    Strips HTML entities only minimally (good enough for LLM input).
    """
    def _first(rx: re.Pattern[str]) -> str:
        m = rx.search(html)
        return (m.group(1).strip() if m else "")[:400]

    out = {
        "title": _first(_RX_TITLE),
        "og_title": _first(_RX_OG_TITLE),
        "og_description": _first(_RX_OG_DESC),
        "og_type": _first(_RX_OG_TYPE),
        "article_published_time": _first(_RX_ARTICLE_DATE),
        "time_datetime": _first(_RX_TIME_DATETIME),
    }
    # Tiny entity cleanup
    for k, v in out.items():
        if not v:
            continue
        out[k] = (v.replace("&amp;", "&")
                    .replace("&quot;", '"')
                    .replace("&#39;", "'")
                    .replace("&lt;", "<")
                    .replace("&gt;", ">"))
    return out


def _parse_iso_date(s: str) -> datetime | None:
    """Best-effort ISO 8601 parse, returning None on failure."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _date_in_window(
    extracted: datetime | None,
    expected: datetime,
    window_days: int,
) -> bool | None:
    """True if extracted ± window_days covers expected. None if no
    extracted date (= unknown, don't reject on missing data)."""
    if extracted is None:
        return None
    if extracted.tzinfo is None:
        extracted = extracted.replace(tzinfo=timezone.utc)
    if expected.tzinfo is None:
        expected = expected.replace(tzinfo=timezone.utc)
    return abs((extracted - expected).total_seconds()) \
        <= window_days * 86400


# ── HTTP fetch ──────────────────────────────────────────────────────

def _fetch_head_excerpt(
    url: str,
    *,
    http_client: httpx.Client | None,
    max_bytes: int,
    timeout_s: float,
) -> tuple[str, str, int, str]:
    """Return (text_excerpt, final_url, status_code, error).

    Uses Range: bytes=0-N to only pull the head — typical OEM press
    pages exceed 100KB but their head fits in 4KB easily.
    """
    if http_client is None:
        http_client = httpx.Client(timeout=timeout_s, follow_redirects=True)
        owned = True
    else:
        owned = False
    try:
        try:
            resp = http_client.get(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; NewsMaker-Verify/1.0)"
                    ),
                    "Range": f"bytes=0-{max_bytes - 1}",
                    "Accept": "text/html,application/xhtml+xml",
                },
                timeout=timeout_s,
            )
        except httpx.TimeoutException:
            return "", url, 0, "timeout"
        except httpx.RequestError as e:
            return "", url, 0, f"network: {type(e).__name__}"

        final_url = str(resp.url)
        status = resp.status_code
        if status >= 400:
            return "", final_url, status, f"http_{status}"

        # Read up to max_bytes worth of content
        try:
            text = resp.text
        except (UnicodeDecodeError, httpx.DecodingError):
            return "", final_url, status, "decode_error"
        if not text:
            return "", final_url, status, "empty"

        return text[:max_bytes], final_url, status, ""
    finally:
        if owned:
            try:
                http_client.close()
            except Exception:  # noqa: BLE001
                pass


# ── LLM judge ───────────────────────────────────────────────────────

JUDGE_SYSTEM = """\
Ты — критичный fact-checker. На вход получаешь:
  • описание события (event_summary с датой)
  • выдержку из веб-страницы (page_excerpt)

Твоя задача — ответить ОДИН раз через tool-use: описывает ли страница \
ИМЕННО это событие?

ВАЖНО — что считается «соответствием»:
  • Та же модель, тот же тип события, в пределах нескольких дней от \
expected_date → match=true, confidence высокая.
  • Тот же бренд, ДРУГАЯ модель (BMW M5 vs BMW M3) → match=false.
  • Та же модель, ДРУГОЙ событийный beat (reveal vs recall vs review) \
→ match=false.
  • Тот же бренд+модель, но ОЧЕВИДНО другой год/событие (2023 \
launch vs 2026 facelift) → match=false.
  • Категорийная страница / list page без конкретного события → \
match=false, confidence=0.
  • Paywall / редирект на home / 404 без явного контента → match=false.

Не угадывай. Если выдержка пустая или не даёт чётких признаков события — \
match=false, confidence ≤ 0.3, reason объясни."""

VERIFY_TOOL: dict[str, Any] = {
    "name": "verify_page_matches_event",
    "description": (
        "Return whether the page excerpt describes the given event. "
        "Conservative: prefer match=false when unsure."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "match": {
                "type": "boolean",
                "description": "True iff the page describes THIS event.",
            },
            "confidence": {
                "type": "number",
                "minimum": 0,
                "maximum": 1,
                "description": (
                    "0 = nothing in common, 1 = certain match. "
                    "Caller accepts at >= 0.70 by default."
                ),
            },
            "reason": {
                "type": "string",
                "description": (
                    "One-sentence justification grounded in the excerpt."
                ),
            },
        },
        "required": ["match", "confidence", "reason"],
        "additionalProperties": False,
    },
}


def _llm_judge(
    *,
    event_summary: str,
    expected_date_iso: str,
    page_excerpt: str,
    head_fields: dict[str, str],
    llm_client: "anthropic.Anthropic",
    model: str = DEFAULT_VERIFY_MODEL,
    max_tokens: int = 300,
    timeout_s: float = 30.0,
) -> tuple[bool, float, str, dict]:
    """Returns (match, confidence, reason, usage_dict)."""
    # Build a compact user message — head fields first (highest
    # signal), then raw excerpt as fallback.
    user_parts = [
        f"event_summary: {event_summary}",
        f"expected_date: {expected_date_iso}",
    ]
    if head_fields.get("og_title"):
        user_parts.append(f"page_og_title: {head_fields['og_title']}")
    if head_fields.get("title"):
        user_parts.append(f"page_title: {head_fields['title']}")
    if head_fields.get("og_description"):
        user_parts.append(
            f"page_og_description: {head_fields['og_description']}"
        )
    if head_fields.get("article_published_time"):
        user_parts.append(
            f"article_published_time: "
            f"{head_fields['article_published_time']}"
        )
    # Truncate the raw excerpt — head_fields are usually enough.
    if page_excerpt:
        clean = re.sub(r"<[^>]+>", " ", page_excerpt)
        clean = re.sub(r"\s+", " ", clean).strip()
        user_parts.append(f"raw_excerpt: {clean[:800]}")
    user_msg = "\n".join(user_parts)

    resp = llm_client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=JUDGE_SYSTEM,
        tools=[VERIFY_TOOL],
        tool_choice={"type": "tool",
                      "name": "verify_page_matches_event"},
        messages=[{"role": "user", "content": user_msg}],
        timeout=timeout_s,
    )
    in_tok = resp.usage.input_tokens
    out_tok = resp.usage.output_tokens
    cost = (in_tok * _HAIKU_INPUT_PER_MTOK
            + out_tok * _HAIKU_OUTPUT_PER_MTOK) / 1_000_000
    usage = {"input_tokens": in_tok, "output_tokens": out_tok,
              "cost_usd": cost}
    for block in resp.content:
        if getattr(block, "type", None) == "tool_use":
            inp = block.input or {}
            return (
                bool(inp.get("match", False)),
                float(inp.get("confidence", 0.0)),
                str(inp.get("reason", ""))[:300],
                usage,
            )
    return False, 0.0, "llm_no_tool_use", usage


# ── Public API ──────────────────────────────────────────────────────

def verify_press_candidate(
    candidate: PressCandidate,
    *,
    event_summary: str,
    expected_date_iso: str,
    date_window_days: int = DEFAULT_DATE_WINDOW_DAYS,
    acceptance_threshold: float = DEFAULT_ACCEPTANCE_THRESHOLD,
    max_content_bytes: int = DEFAULT_MAX_CONTENT_BYTES,
    http_client: httpx.Client | None = None,
    llm_client: "anthropic.Anthropic | None" = None,
    model: str = DEFAULT_VERIFY_MODEL,
    http_timeout_s: float = DEFAULT_HTTP_TIMEOUT_S,
) -> VerificationResult:
    """Verify a single candidate. See module docstring for pipeline.

    Returns a populated VerificationResult; never raises. Pipeline
    callers check .match + .confidence to decide accept/reject.
    """
    t0 = time.time()
    result = VerificationResult(candidate=candidate, final_url=candidate.url)

    # 1. Fetch head
    excerpt, final_url, status, fetch_err = _fetch_head_excerpt(
        candidate.url,
        http_client=http_client,
        max_bytes=max_content_bytes,
        timeout_s=http_timeout_s,
    )
    result.final_url = final_url
    result.fetched_status = status
    result.page_excerpt = excerpt
    if fetch_err:
        result.error = fetch_err
        result.reason = f"fetch failed: {fetch_err}"
        result.elapsed_s = time.time() - t0
        return result

    # 2. Parse head fields
    head = _extract_head_fields(excerpt)

    # 3. Date window check (cheap early reject)
    extracted_dt = (_parse_iso_date(head.get("article_published_time", ""))
                    or _parse_iso_date(head.get("time_datetime", "")))
    try:
        expected_dt = datetime.fromisoformat(
            expected_date_iso.replace("Z", "+00:00")
        )
    except (ValueError, TypeError):
        expected_dt = datetime.now(timezone.utc)
    if expected_dt.tzinfo is None:
        expected_dt = expected_dt.replace(tzinfo=timezone.utc)

    in_window = _date_in_window(extracted_dt, expected_dt, date_window_days)
    result.date_in_window = in_window
    if extracted_dt is not None:
        result.extracted_date_iso = extracted_dt.isoformat()
    if in_window is False:
        # We KNOW the date is wrong → reject without LLM call.
        result.match = False
        result.confidence = 0.0
        result.reason = (
            f"date_out_of_window: page={result.extracted_date_iso} "
            f"vs expected={expected_date_iso}"
        )
        result.elapsed_s = time.time() - t0
        return result

    # 4. LLM judge
    if llm_client is None:
        result.error = "no_llm_client"
        result.reason = "llm_client not provided, cannot verify content"
        result.elapsed_s = time.time() - t0
        return result
    try:
        match, conf, reason, usage = _llm_judge(
            event_summary=event_summary,
            expected_date_iso=expected_date_iso,
            page_excerpt=excerpt,
            head_fields=head,
            llm_client=llm_client,
            model=model,
        )
    except Exception as e:  # noqa: BLE001
        result.error = f"llm_error: {type(e).__name__}"
        result.reason = str(e)[:200]
        result.elapsed_s = time.time() - t0
        return result

    result.cost_usd = usage["cost_usd"]
    result.confidence = conf
    result.reason = reason
    # Acceptance: BOTH the LLM said match AND conf >= threshold.
    result.match = bool(match) and conf >= acceptance_threshold
    result.elapsed_s = time.time() - t0
    return result


def verify_candidates(
    candidates: list[PressCandidate],
    *,
    event_summary: str,
    expected_date_iso: str,
    stop_at_first_match: bool = True,
    **verify_kwargs: Any,
) -> tuple[VerificationResult | None, list[VerificationResult]]:
    """Try candidates in order, return (first_accepted_or_None, all_attempts).

    Stops at first match by default to bound LLM cost. When all candidates
    fail, returns (None, all_results) so caller can inspect why.
    """
    attempts: list[VerificationResult] = []
    for cand in candidates:
        res = verify_press_candidate(
            cand,
            event_summary=event_summary,
            expected_date_iso=expected_date_iso,
            **verify_kwargs,
        )
        attempts.append(res)
        if res.match and stop_at_first_match:
            return res, attempts
    return None, attempts
