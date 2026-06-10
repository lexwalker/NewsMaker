"""Archive-dedup judge — has the editor ALREADY published this story?
(precision complaint #4: "добавляется то, что уже было").

We have no published-portal API, but we DO have the editor's published
archive. A deterministic dedup against it fails (measured: token-fuzzy
catches 14% of real dups; embeddings catch 60% but wrongly kill 54% of
genuine new stories — auto news is semantically repetitive, so cosine
can't tell "republished same event" from "new event about the same
model"). So we use embeddings only to RETRIEVE the few nearest published
stories, then let the LLM JUDGE whether the candidate is the SAME news
event — a distinction it can make ("facelift reveal" ≠ "new generation",
"sales started" ≠ "first teaser") and cosine cannot.

Cheap by design: if no archive entry is within ``min_cos`` of the
candidate, there is nothing to be a duplicate of → return not-dup with NO
LLM call. Only candidates with a near neighbour cost a judge call.

LLM is injected as ``judge_fn(prompt) -> (text, cost_usd)`` so tests run
with a fake and the script wires Anthropic. Embeddings reuse the same
multilingual MiniLM the editorial judge uses.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

DEFAULT_EMBED_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
DEFAULT_MIN_COS = 0.55      # below this, not even a candidate → skip LLM
DEFAULT_K = 8               # neighbours shown to the judge
_HAIKU_IN = 1.0
_HAIKU_OUT = 5.0


@dataclass
class ArchiveEntry:
    title: str
    date: str = ""
    url: str = ""


@dataclass
class Neighbor:
    entry: ArchiveEntry
    cosine: float


@dataclass
class DupVerdict:
    is_duplicate: bool
    matched_title: str = ""
    matched_date: str = ""
    reason: str = ""
    confidence: float = 0.0
    n_candidates: int = 0       # neighbours the judge actually saw
    top_cosine: float = 0.0
    cost_usd: float = 0.0
    error: str = ""


def _query_lines(title: str, body: str = "") -> list[str]:
    """Split an "EN: …\nRU: …" candidate title into clean query strings;
    fall back to the whole title. Body is not embedded (titles match
    better cross-source)."""
    lines = [
        re.sub(r"^(EN|RU|АНГЛ|АНГ)\s*:\s*", "", ln).strip()
        for ln in (title or "").splitlines()
        if ln.strip()
    ]
    return lines or ([title.strip()] if title.strip() else [])


def build_dedup_prompt(
    candidate_title: str, candidate_body: str, neighbors: list[Neighbor]
) -> str:
    """The judge prompt. The discriminator instruction is the whole point:
    same EVENT, not merely same brand/model."""
    pub = "\n".join(
        f"  {i+1}. «{n.entry.title[:90]}»"
        + (f"  ({n.entry.date[:10]})" if n.entry.date else "")
        for i, n in enumerate(neighbors)
    )
    body_hint = f"\nЛид кандидата: {candidate_body[:200]}" if candidate_body else ""
    return (
        "Ты — редактор автомобильного портала. Проверь, не ПУБЛИКОВАЛИ ли "
        "мы УЖЕ это же событие.\n\n"
        "Уже опубликованные похожие новости:\n" + pub + "\n\n"
        f"КАНДИДАТ: «{candidate_title}»{body_hint}\n\n"
        "Считается ДУБЛЕМ, только если кандидат описывает ТО ЖЕ САМОЕ "
        "событие/факт/анонс, что и одна из опубликованных (пусть другими "
        "словами или с другого источника).\n"
        "НЕ дубль, если это РАЗНОЕ событие про ту же марку/модель — "
        "например: рестайлинг vs новое поколение; тизер vs полный показ; "
        "старт продаж vs премьера; другой рынок; новые цифры продаж за "
        "другой период; разные модели.\n\n"
        "Ответь строго JSON: {\"duplicate\": true/false, "
        "\"match\": номер_опубликованной_или_0, "
        "\"confidence\": 0.0-1.0, \"reason\": \"кратко почему\"}"
    )


def parse_verdict(txt: str) -> dict:
    m = re.search(r"\{.*\}", txt or "", re.DOTALL)
    if not m:
        raise ValueError("no json")
    return json.loads(m.group())


class ArchiveDedupJudge:
    """Retrieve nearest published stories, then LLM-judge same-event."""

    def __init__(
        self,
        entries: list[ArchiveEntry],
        *,
        embed_model: Any = None,
        judge_fn: Callable[[str], tuple[str, float]] | None = None,
        min_cos: float = DEFAULT_MIN_COS,
        k: int = DEFAULT_K,
        vectors_cache: Any = None,
    ) -> None:
        self.entries = entries
        self._embed = embed_model
        self._judge_fn = judge_fn
        self.min_cos = min_cos
        self.k = k
        self.vectors_cache = vectors_cache
        self._vecs = None          # (n_vec, d)
        self._vec2entry: list[int] = []   # vector-index → entry-index

    # -- lazy embed model -------------------------------------------
    def _embed_model(self):
        if self._embed is None:
            from sentence_transformers import SentenceTransformer
            self._embed = SentenceTransformer(DEFAULT_EMBED_MODEL)
        return self._embed

    def _encode(self, texts: list[str]):
        return self._embed_model().encode(
            texts, normalize_embeddings=True, show_progress_bar=False,
            batch_size=128)

    def build_index(self) -> None:
        """Embed every archive entry's title line(s). Each entry may emit
        more than one vector (EN + RU); all map back to the same entry.
        Vectors are cached to disk keyed by a hash of the archive titles +
        the vec→entry map, so re-embedding only happens when the archive
        changes (the encode of ~10k titles is the slow part, ~90s)."""
        import hashlib

        import numpy as np

        texts: list[str] = []
        vec2entry: list[int] = []
        for ei, e in enumerate(self.entries):
            for q in _query_lines(e.title):
                texts.append(q)
                vec2entry.append(ei)
        key = hashlib.sha1(
            ("\x00".join(texts)).encode("utf-8", "ignore")).hexdigest()[:16]

        if self.vectors_cache is not None and Path(self.vectors_cache).exists():
            try:
                d = np.load(self.vectors_cache, allow_pickle=False)
                if str(d.get("key")) == key:
                    self._vecs = d["vecs"]
                    self._vec2entry = [int(i) for i in d["v2e"]]
                    return
            except Exception:  # noqa: BLE001
                pass

        self._vec2entry = vec2entry
        self._vecs = (self._encode(texts) if texts
                      else np.zeros((0, 384), dtype="float32"))
        if self.vectors_cache is not None:
            try:
                np.savez_compressed(
                    self.vectors_cache, vecs=self._vecs,
                    v2e=np.array(vec2entry, dtype="int32"),
                    key=np.array(key))
            except Exception:  # noqa: BLE001
                pass

    def retrieve(
        self, candidate_title: str, *, exclude_self_cos: float | None = None
    ) -> list[Neighbor]:
        """Top-k entries by max cosine over the candidate's query lines.

        ``exclude_self_cos``: drop the single best entry if its cosine is
        >= this value (used in offline measurement to remove a candidate's
        OWN archived copy, simulating "not yet in the archive")."""
        import numpy as np

        if self._vecs is None:
            self.build_index()
        if len(self._vecs) == 0:
            return []
        q = _query_lines(candidate_title)
        if not q:
            return []
        Q = self._encode(q)                      # (n_q, d)
        sims = Q @ self._vecs.T                   # (n_q, n_vec)
        per_vec = sims.max(axis=0)                # best query line per vec
        # collapse to best score per entry
        best_per_entry: dict[int, float] = {}
        for vi, score in enumerate(per_vec):
            ei = self._vec2entry[vi]
            s = float(score)
            if s > best_per_entry.get(ei, -1.0):
                best_per_entry[ei] = s
        ranked = sorted(best_per_entry.items(), key=lambda x: -x[1])
        if exclude_self_cos is not None and ranked and \
                ranked[0][1] >= exclude_self_cos:
            ranked = ranked[1:]
        out: list[Neighbor] = []
        for ei, s in ranked[: self.k]:
            if s < self.min_cos:
                break
            out.append(Neighbor(self.entries[ei], s))
        return out

    def _judge(self, prompt: str) -> tuple[str, float]:
        if self._judge_fn is not None:
            return self._judge_fn(prompt)
        raise RuntimeError("no judge_fn injected")

    def is_duplicate(
        self, candidate_title: str, candidate_body: str = "",
        *, exclude_self_cos: float | None = None,
    ) -> DupVerdict:
        try:
            neighbors = self.retrieve(
                candidate_title, exclude_self_cos=exclude_self_cos)
        except Exception as e:  # noqa: BLE001
            return DupVerdict(False, error=f"retrieve:{type(e).__name__}")
        if not neighbors:
            # no near neighbour → nothing to duplicate → cheap not-dup
            return DupVerdict(False, n_candidates=0)
        prompt = build_dedup_prompt(candidate_title, candidate_body, neighbors)
        try:
            txt, cost = self._judge(prompt)
        except Exception as e:  # noqa: BLE001
            return DupVerdict(False, n_candidates=len(neighbors),
                              top_cosine=neighbors[0].cosine,
                              error=f"llm:{type(e).__name__}")
        try:
            j = parse_verdict(txt)
        except Exception:  # noqa: BLE001
            return DupVerdict(False, n_candidates=len(neighbors),
                              top_cosine=neighbors[0].cosine,
                              cost_usd=cost, error="json_parse")
        is_dup = bool(j.get("duplicate"))
        match_no = j.get("match") or 0
        matched = ""
        matched_date = ""
        try:
            mi = int(match_no) - 1
            if is_dup and 0 <= mi < len(neighbors):
                matched = neighbors[mi].entry.title
                matched_date = neighbors[mi].entry.date
        except (TypeError, ValueError):
            pass
        return DupVerdict(
            is_duplicate=is_dup,
            matched_title=matched,
            matched_date=matched_date,
            reason=str(j.get("reason", ""))[:200],
            confidence=float(j.get("confidence", 0.0) or 0.0),
            n_candidates=len(neighbors),
            top_cosine=neighbors[0].cosine,
            cost_usd=cost,
        )


def haiku_cost(input_tokens: int, output_tokens: int) -> float:
    return (input_tokens * _HAIKU_IN + output_tokens * _HAIKU_OUT) / 1_000_000
