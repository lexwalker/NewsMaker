"""Facts that identify an event, and how close two of them are.

The dedup problem this exists for: the editor marks a row «было» when the
outlet already covered that story, and the two texts come from different
sources — often in different languages. Title matching missed those by
construction, and word overlap on the bodies scored a flat zero: a Russian
report and an English one about the same launch share almost no vocabulary.

Numbers do survive the language barrier. «63 900 юаней» and "63,900 yuan"
normalise to the same token; a model code (A05, X5, T7) is written the same
way everywhere; a date is a date. And numbers are what tell two events on the
SAME car apart — "published a price list on the 14th" and "started sales"
carry different figures even though brand and model are identical, which is
exactly where the brand+model+type key scored 9% and was abandoned.

Measured on the operator's colour marks, the pairs this finds are the ones a
human would: Leapmotor A05 matched on {a05, 510, 63900}, and a NAMI engine
story — no brand at all, invisible to every key-based approach — matched on
{299, 414320}.

NOT wired into the pipeline. This is the comparison; whether it is good enough
to divert on is a question for the evaluation against the red/green labels,
and that needs the longer stored text to have accumulated first.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

# A digit group separated by a space, NBSP or comma is one number: Russian
# writes 63 900, English 63,900, and they must land on the same token.
# Matched as a WHOLE grouped number, not separator-by-separator: the loose
# form collapsed any figure followed by a three-digit one, so
# "899 000 rub 181 hp" became one nonsense token 899000181 and BOTH real
# facts were lost. Requiring a 1-3 digit lead group leaves plain long
# numbers alone.
_GROUPED_NUMBER = re.compile(r"\b\d{1,3}(?:[\u00a0\u202f ,]\d{3})+\b")
_SEPARATORS = re.compile(r"[\u00a0\u202f ,]")

# Model codes: a token mixing letters and digits (a05, x5, id4, 414320 is not
# one — it is a plain number). Hyphens are dropped so "CS-55" == "CS55".
_MODEL = re.compile(r"\b(?=[a-zA-Zа-яА-Я]*\d)(?=\d*[a-zA-Zа-яА-Я])[a-zA-Zа-яА-Я0-9-]{2,12}\b")

_NUM = re.compile(r"\b\d[\d.]*\b")
_DMY = re.compile(r"\b(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?\b")
_MONTHS = (
    "январ феврал март апрел ма[йя] июн июл август сентябр октябр ноябр декабр "
    "januar februar march april may june july august september october november december"
).split()
_DAY_MONTH = re.compile(r"\b(\d{1,2})\s+(" + "|".join(_MONTHS) + r")\w*", re.I)

# Years and small integers appear in nearly every automotive story and identify
# nothing. Two digits is almost always a percentage, an age or a count.
_NOISE_YEARS = {str(y) for y in range(2000, 2051)}
MIN_DIGITS = 3


def extract(*texts: str) -> frozenset[str]:
    """Identity-bearing tokens from a title and/or body.

    Prefixed by kind so a model code and a bare number can never collide:
    ``m:`` model, ``n:`` number, ``d:`` date.
    """
    out: set[str] = set()
    for raw in texts:
        if not raw:
            continue
        t = _GROUPED_NUMBER.sub(lambda m: _SEPARATORS.sub("", m.group(0)), raw)
        for m in _MODEL.finditer(t):
            w = m.group(0).lower().replace("-", "")
            if not w.isdigit():
                out.add("m:" + w)
        for m in _NUM.finditer(t):
            n = m.group(0).rstrip(".")
            if len(n) >= MIN_DIGITS and n not in _NOISE_YEARS:
                # Leading zeros are formatting, not identity: 0510 and 510 are
                # the same figure. An all-zero run strips to nothing and is not
                # a fact at all.
                stripped = n.lstrip("0")
                if stripped and stripped not in _NOISE_YEARS:
                    out.add("n:" + stripped)
        for d, mo, _y in _DMY.findall(t):
            if 1 <= int(d) <= 31 and 1 <= int(mo) <= 12:
                out.add(f"d:{int(d):02d}.{int(mo):02d}")
        for d, mo in _DAY_MONTH.findall(t):
            out.add(f"d:{int(d):02d}.{mo[:3].lower()}")
    return frozenset(out)


@dataclass
class FactIndex:
    """Rarity weights over a corpus.

    A fact shared by half the archive ("100", "v8") means nothing; one shared by
    two articles nearly identifies the event. Without this the score is driven
    by whichever common number happens to appear in both — the first cut of
    this scored 1.00 on a shared "15" and called two unrelated stories a match.
    """

    df: dict[str, int] = field(default_factory=dict)
    n_docs: int = 0

    def add(self, facts: frozenset[str]) -> None:
        self.n_docs += 1
        for f in facts:
            self.df[f] = self.df.get(f, 0) + 1

    @classmethod
    def build(cls, corpus) -> "FactIndex":  # noqa: ANN001 — any iterable of frozensets
        idx = cls()
        for facts in corpus:
            idx.add(facts)
        return idx

    def weight(self, fact: str) -> float:
        """Inverse document frequency. Unseen facts are treated as maximally
        rare — a number nobody has written before is strong evidence, and
        pretending otherwise would silently ignore the newest events."""
        d = self.df.get(fact, 0)
        return math.log((self.n_docs + 1) / (d + 1)) if self.n_docs else 0.0

    def score(self, a: frozenset[str], b: frozenset[str]) -> float:
        """Summed rarity of what two articles share.

        Deliberately a SUM, not a ratio. Two articles sharing three rare
        numbers are the same event whatever else each contains; normalising by
        set size would let a story with one fact score a perfect match on a
        single coincidence, which is exactly the failure the first cut had.
        """
        return sum(self.weight(f) for f in (a & b))

    def best_match(self, facts: frozenset[str], candidates, *, min_score: float = 0.0):  # noqa: ANN001
        """(score, key, shared) for the closest candidate, or (0.0, None, empty).

        ``candidates`` is an iterable of (key, facts). Linear on purpose: the
        comparison base is ~1650 delivered rows over 60 days, so a full scan is
        milliseconds and an index would be machinery with nothing to buy.
        """
        best: tuple[float, object, frozenset[str]] = (0.0, None, frozenset())
        for key, other in candidates:
            shared = facts & other
            if not shared:
                continue
            s = self.score(facts, other)
            if s > best[0]:
                best = (s, key, shared)
        return best if best[0] >= min_score else (0.0, None, frozenset())
