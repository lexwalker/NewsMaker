"""Editorial judge — reasons about a candidate by ANALOGY to the
editor's real past decisions, instead of hand-written rules.

The vision (jun-2026): stop patching with rigid blacklist phrases.
Give the LLM the editor's ACTUAL published vs rejected items most
similar to the candidate, and let it judge by precedent. As the
feedback loop accumulates more editor decisions, the judge gets
smarter — with zero new rules.

HONESTY CONTRACT (this is advisory infrastructure, read carefully):
  • This module NEVER changes the pipeline's publish/section decision
    on its own. Callers use it in ADVISORY mode — it produces a
    second opinion + reasoning, logged for comparison against the
    editor's real verdict. Only after it proves >~80% agreement over
    weeks should it be promoted to a deciding role.
  • PoC accuracy was ~62% on a tiny hand-picked set. It is NOT
    production-accurate yet. It is unstable (adding one precedent can
    shift unrelated verdicts) and capped by the editor's own
    inconsistency (~80-85% ceiling).
  • Cost: local MiniLM embeddings ($0) + ~$0.0008/candidate Haiku.

Design:
  EditorialJudge(decisions_path, vectors_cache, llm_client, embed_model)
    .judge(title, body) -> JudgeVerdict
  Decision base = positives (editor-published, with section) +
  negatives (editor-rejected, with the editor's stated reason).
  Embeddings cached to disk keyed by a hash of the decision titles so
  we re-embed only when the base changes.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    import anthropic
    import numpy as np


DEFAULT_MODEL = "claude-haiku-4-5"
DEFAULT_EMBED_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
_HAIKU_IN = 1.0
_HAIKU_OUT = 5.0


@dataclass
class JudgeVerdict:
    """Advisory second-opinion. Never the final decision by itself."""
    advisory_publish: bool | None
    advisory_section: str = ""
    reason: str = ""
    confidence: float = 0.0
    precedents_published: list[str] = field(default_factory=list)
    precedents_rejected: list[str] = field(default_factory=list)
    cost_usd: float = 0.0
    error: str = ""


# ── Decision base ───────────────────────────────────────────────────

def load_decisions(path: Path) -> tuple[list[dict], list[dict]]:
    """Return (positives, negatives) from editor_decisions.json.

    positive: {title, section, decision='ОПУБЛИКОВАЛ'}
    negative: {title, comment, decision='ОТКЛОНИЛ'}
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("positive", []), data.get("negative", [])


def _titles_hash(titles: list[str]) -> str:
    h = hashlib.sha1()
    for t in titles:
        h.update(t.encode("utf-8", "ignore"))
        h.update(b"\x00")
    return h.hexdigest()[:16]


class EditorialJudge:
    """Precedent-based advisory judge. LLM + embed model injectable
    for tests."""

    def __init__(
        self,
        decisions_path: Path,
        *,
        vectors_cache: Path | None = None,
        llm_client: "anthropic.Anthropic | None" = None,
        embed_model: Any = None,
        model: str = DEFAULT_MODEL,
        k_pos: int = 6,
        k_neg: int = 5,
    ) -> None:
        self.decisions_path = decisions_path
        self.vectors_cache = vectors_cache
        self._llm = llm_client
        self._embed = embed_model
        self.model = model
        self.k_pos = k_pos
        self.k_neg = k_neg
        self.pos, self.neg = load_decisions(decisions_path)
        self._pos_vec = None
        self._neg_vec = None

    # -- lazy heavy deps --------------------------------------------
    def _embed_model(self):
        if self._embed is None:
            from sentence_transformers import SentenceTransformer
            self._embed = SentenceTransformer(DEFAULT_EMBED_MODEL)
        return self._embed

    def _llm_client(self):
        if self._llm is None:
            import anthropic
            self._llm = anthropic.Anthropic()
        return self._llm

    def _ensure_vectors(self) -> None:
        if self._pos_vec is not None and self._neg_vec is not None:
            return
        import numpy as np

        pos_titles = [p["title"] for p in self.pos]
        neg_titles = [n["title"] for n in self.neg]
        key = _titles_hash(pos_titles + ["|NEG|"] + neg_titles)

        # try cache
        if self.vectors_cache and self.vectors_cache.exists():
            try:
                d = np.load(self.vectors_cache, allow_pickle=False)
                if str(d.get("key")) == key:
                    self._pos_vec = d["pos"]
                    self._neg_vec = d["neg"]
                    return
            except Exception:  # noqa: BLE001
                pass

        m = self._embed_model()
        self._pos_vec = m.encode(pos_titles, batch_size=64,
                                  normalize_embeddings=True,
                                  show_progress_bar=False)
        self._neg_vec = m.encode(neg_titles, batch_size=64,
                                  normalize_embeddings=True,
                                  show_progress_bar=False) if neg_titles \
            else np.zeros((0, self._pos_vec.shape[1]))
        if self.vectors_cache:
            try:
                np.savez_compressed(
                    self.vectors_cache, pos=self._pos_vec,
                    neg=self._neg_vec, key=np.array(key))
            except Exception:  # noqa: BLE001
                pass

    # -- retrieval --------------------------------------------------
    def _retrieve(self, title: str) -> tuple[list[int], list[int]]:
        import numpy as np

        self._ensure_vectors()
        m = self._embed_model()
        qv = m.encode([title], normalize_embeddings=True)[0]
        pos_idx = (np.argsort(self._pos_vec @ qv)[::-1][:self.k_pos]
                   if len(self._pos_vec) else [])
        neg_idx = (np.argsort(self._neg_vec @ qv)[::-1][:self.k_neg]
                   if len(self._neg_vec) else [])
        return [int(i) for i in pos_idx], [int(i) for i in neg_idx]

    # -- judge ------------------------------------------------------
    def judge(self, title: str, body: str = "") -> JudgeVerdict:
        if not title.strip():
            return JudgeVerdict(advisory_publish=None,
                                reason="empty title")
        try:
            pos_idx, neg_idx = self._retrieve(title)
        except Exception as e:  # noqa: BLE001
            return JudgeVerdict(advisory_publish=None,
                                error=f"retrieve: {type(e).__name__}")

        pub_ex = [f"[{self.pos[i].get('section','')}] "
                  f"{self.pos[i]['title'][:75]}" for i in pos_idx]
        rej_ex = [f"{self.neg[i]['title'][:55]} — "
                  f"«{self.neg[i].get('comment','')[:50]}»"
                  for i in neg_idx]
        prompt = (
            "Учись мыслить как наш редактор по его РЕАЛЬНЫМ решениям "
            "(не по абстрактным правилам).\n\n"
            "ОПУБЛИКОВАЛ похожее:\n"
            + "\n".join(f"  • {e}" for e in pub_ex)
            + "\n\nОТКЛОНИЛ похожее (с причиной):\n"
            + "\n".join(f"  • {e}" for e in rej_ex)
            + f"\n\nКандидат: «{title}»\n\n"
            "По аналогии с реальным поведением редактора реши: "
            "публиковать или нет, и раздел. Ответь строго JSON: "
            '{"publish": true/false, "section": "...", '
            '"confidence": 0.0-1.0, "reason": "краткая аналогия"}'
        )
        try:
            client = self._llm_client()
            resp = client.messages.create(
                model=self.model, max_tokens=220,
                messages=[{"role": "user", "content": prompt}])
        except Exception as e:  # noqa: BLE001
            return JudgeVerdict(advisory_publish=None,
                                error=f"llm: {type(e).__name__}")
        txt = "".join(getattr(b, "text", "") for b in resp.content)
        cost = (resp.usage.input_tokens * _HAIKU_IN
                + resp.usage.output_tokens * _HAIKU_OUT) / 1_000_000
        try:
            j = json.loads(re.search(r"\{.*\}", txt, re.DOTALL).group())
        except Exception:  # noqa: BLE001
            return JudgeVerdict(advisory_publish=None, cost_usd=cost,
                                error="json_parse",
                                reason=txt[:120])
        return JudgeVerdict(
            advisory_publish=bool(j.get("publish")) if j.get("publish")
            is not None else None,
            advisory_section=str(j.get("section", "")),
            reason=str(j.get("reason", ""))[:200],
            confidence=float(j.get("confidence", 0.0) or 0.0),
            precedents_published=pub_ex,
            precedents_rejected=rej_ex,
            cost_usd=cost,
        )
