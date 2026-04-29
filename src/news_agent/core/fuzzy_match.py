"""Aggressive normaliser used ONLY for fuzzy title matching.

The goal is to make different spellings of the same headline collapse to
the same string before rapidfuzz scoring — so:

  «АвтоВАЗ и Промтех заключили соглашение»
  «AvtoVAZ and Promtekh signed agreement»
  «АвтоВАЗ подписал соглашение с Promtech»

…all reduce to a comparable form. Without this, the AvtoVAZ-Promtech
deduplication fails because rapidfuzz sees three completely different
character sequences.

This is NOT for display — the original title is kept everywhere the user
sees it. ``normalise_for_match`` is purely a hashing/comparison aid.
"""

from __future__ import annotations

import re
import unicodedata

# ---------------------------------------------- Cyrillic → Latin transliteration
# Conservative table — chooses the "ASCII-most" form. ``х→kh`` is the
# scholarly standard; ``х→h`` also exists but matches less reliably across
# news outlets. We don't need bidirectional reversibility — both sides of
# the comparison go through this table, so as long as it's deterministic
# the equivalence holds.
_CYR_TABLE = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
    "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i",
    "й": "y", "к": "k", "л": "l", "м": "m", "н": "n",
    "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "kh", "ц": "ts", "ч": "ch",
    "ш": "sh", "щ": "shch", "ъ": "", "ы": "y", "ь": "",
    "э": "e", "ю": "yu", "я": "ya",
}


def _translit_cyr_to_lat(s: str) -> str:
    """Return ``s`` with all Cyrillic characters folded to Latin."""
    out: list[str] = []
    for ch in s:
        low = ch.lower()
        if low in _CYR_TABLE:
            out.append(_CYR_TABLE[low])
        else:
            out.append(ch)
    return "".join(out)


# ---------------------------------------------- Number-word ↔ digit
# Cover one through twelve in EN + RU. Beyond that, headlines almost
# always use digits already.
_NUMBER_WORDS = {
    # english
    "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
    "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
    "eleven": "11", "twelve": "12",
    # russian
    "один": "1", "два": "2", "три": "3", "четыре": "4", "пять": "5",
    "шесть": "6", "семь": "7", "восемь": "8", "девять": "9", "десять": "10",
}
_NUMBER_WORD_RE = re.compile(
    r"\b(" + "|".join(re.escape(w) for w in _NUMBER_WORDS) + r")\b",
    flags=re.IGNORECASE,
)


def _word_numbers_to_digits(s: str) -> str:
    """``five-seat sedan`` → ``5-seat sedan`` (post-lowercase)."""
    return _NUMBER_WORD_RE.sub(lambda m: _NUMBER_WORDS[m.group(1).lower()], s)


# ---------------------------------------------- Language / suffix tags
_LANG_TAG_RE = re.compile(r"\(\s*[a-zа-я]{2,4}\s*\)\s*$", flags=re.IGNORECASE)
_EN_PREFIX_RE = re.compile(r"^\s*en:\s*", flags=re.IGNORECASE)
_RU_PREFIX_RE = re.compile(r"\n\s*ru:\s*", flags=re.IGNORECASE)
_TRAILING_SOURCE_RE = re.compile(
    r"\s*[—\-|]\s*[a-zа-я0-9 \.&]+$",
    flags=re.IGNORECASE,
)
_PUNCT_RE = re.compile(r"[^\w\s]+", flags=re.UNICODE)
_WS_RE = re.compile(r"\s+")


def _strip_diacritics(s: str) -> str:
    """``Škoda`` → ``Skoda``; ``Citroën`` → ``Citroen``."""
    return "".join(
        ch for ch in unicodedata.normalize("NFD", s)
        if unicodedata.category(ch) != "Mn"
    )


# ---------------------------------------------- Public API

def normalise_for_match(title: str) -> str:
    """Return a heavily-normalised form of ``title`` suitable for fuzzy match.

    Pipeline (each step strictly more aggressive than the previous):

    1. Lowercase
    2. Strip ``EN: ...\\nRU: ...`` prefixes
    3. Strip trailing language tag ``(EN)`` / ``(KOR)`` / etc.
    4. Strip trailing source-name suffix ``— CarBuzz``
    5. Fold diacritics (``Škoda`` → ``skoda``)
    6. Transliterate Cyrillic to Latin (``промтех`` → ``promtekh``)
    7. Convert number-words to digits (``five-seat`` → ``5-seat``)
    8. Drop punctuation, collapse whitespace

    The output is intended ONLY as a fuzzy-match key — never displayed
    to users.
    """
    if not title:
        return ""
    t = title.strip().lower()

    # Strip combined "EN: ... \n RU: ..." sheet titles
    t = _EN_PREFIX_RE.sub("", t)
    t = _RU_PREFIX_RE.sub(" | ", t)

    # Strip trailing language tag ((EN), (КОР), etc.)
    t = _LANG_TAG_RE.sub("", t).strip()

    # Strip trailing source name (— Korean Car Blog, | MotorTrend …)
    # Loop a couple of times to peel multi-segment suffixes
    for _ in range(2):
        nt = _TRAILING_SOURCE_RE.sub("", t).strip()
        if nt == t or len(nt) < 20:
            break
        t = nt

    # Diacritics + Cyrillic transliteration
    t = _strip_diacritics(t)
    t = _translit_cyr_to_lat(t)

    # Number words → digits (after translit, so Russian works too)
    t = _word_numbers_to_digits(t)

    # Drop punctuation, collapse whitespace
    t = _PUNCT_RE.sub(" ", t)
    t = _WS_RE.sub(" ", t).strip()
    return t
