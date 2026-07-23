"""Event-key token-subset fallback + partnership counterparty synthesis
(jul-23 editor dup report: two pairs reached the feed as separate rows).

Real case 1 — Nissan recall (21.07, same batch): NHTSA keyed
("nissan","armada qx56 qx80","recall"), autonews.ru keyed
("nissan","armada","recall"). Exact key equality failed on the model
wording, RU-vs-EN titles were lexically distant → two clusters. The
cross-run hint (dedup.recent_event_dup_hint) already ships a token-subset
fallback for exactly this; the in-batch clusterer now mirrors it.

Real case 2 — Honda–GAC (21.07, same batch): both copies keyed
("honda","","partnership") — the empty-model guard silenced the key on
every copy, and «продлили партнерство» vs «продолжит сотрудничество»
never fuzz-matched. The partnership counterparty (GAC) is now
synthesised into the model slot from title brand mentions.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import build_news_clusters as bnc  # noqa: E402

_T0 = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)


def _art(norm: str, *, eb: str = "", em: str = "", et: str = "",
         pub: datetime | None = None) -> dict:
    return {
        "normalised": norm,
        "launch_brand_model": "",
        "pub_dt": pub if pub is not None else _T0,
        "primary_url": "",
        "event_brand": eb,
        "event_model": em,
        "event_type": et,
    }


def _clusters(arts: list[dict]) -> list[list[dict]]:
    bnc._BRANDS_LOWER = ["honda", "gac", "nissan", "byd", "toyota"]
    return bnc.cluster_articles(arts)


def test_model_token_subset_clusters_nissan_recall() -> None:
    # "armada" ⊂ "armada qx56 qx80", same brand+type ⇒ one event.
    a = _art("nissan north america recalls 168149 vehicles incorrect gawr "
             "certification label",
             eb="nissan", em="armada qx56 qx80", et="recall")
    b = _art("nissan otzovet 168 tys avtomobiley iz-za nepravilnykh nakleek",
             eb="nissan", em="armada", et="recall",
             pub=_T0 + timedelta(hours=2))
    groups = _clusters([a, b])
    assert len(groups) == 1
    assert len(groups[0]) == 2


def test_model_non_subset_stays_apart() -> None:
    # "seal" vs "sealion" — not a token subset either way ⇒ distinct models.
    # Titles deliberately lexically distant so ONLY the event-key path is
    # exercised (near-identical wording would merge via plain fuzz anyway).
    a = _art("byd seal poyavilsya v prodazhe s novoy batareey blade",
             eb="byd", em="seal", et="launch")
    b = _art("byd sealion arrives at german dealerships this autumn",
             eb="byd", em="sealion", et="launch",
             pub=_T0 + timedelta(hours=2))
    groups = _clusters([a, b])
    assert len(groups) == 2


def test_partnership_counterparty_synthesised_clusters_honda_gac() -> None:
    # Both copies: empty model, type=partnership, GAC named in the title.
    a = _art("honda i gac prodlili partnerstvo na 12 let",
             eb="honda", em="", et="partnership")
    b = _art("honda prodolzhit sotrudnichestvo s gac do 2038 goda nesmotrya "
             "na padenie prodazh",
             eb="honda", em="", et="partnership",
             pub=_T0 + timedelta(hours=1))
    groups = _clusters([a, b])
    assert len(groups) == 1
    assert len(groups[0]) == 2


def test_partnership_different_counterparties_stay_apart() -> None:
    # Honda–Nissan is NOT Honda–GAC even in the same news cycle.
    a = _art("honda i nissan obyavili o novom partnerstve po batareyam",
             eb="honda", em="", et="partnership")
    b = _art("honda i gac prodlili partnerstvo na 12 let",
             eb="honda", em="", et="partnership",
             pub=_T0 + timedelta(hours=1))
    groups = _clusters([a, b])
    assert len(groups) == 2


def test_partnership_without_named_counterparty_keeps_status_quo() -> None:
    # No partner brand in the title ⇒ the key stays disabled (empty-model
    # guard), nothing new merges.
    a = _art("honda rasshiryaet promyshlennoe sotrudnichestvo v kitae",
             eb="honda", em="", et="partnership")
    b = _art("honda podpisala soglashenie o sovmestnom predpriyatii",
             eb="honda", em="", et="partnership",
             pub=_T0 + timedelta(hours=1))
    groups = _clusters([a, b])
    assert len(groups) == 2


def test_event_subset_respects_time_window() -> None:
    # Same subset key months apart = different stories (36h window holds).
    a = _art("nissan north america recalls vehicles incorrect gawr",
             eb="nissan", em="armada qx56 qx80", et="recall")
    b = _art("nissan otzovet avtomobili iz-za nakleek",
             eb="nissan", em="armada", et="recall",
             pub=_T0 + timedelta(hours=90))
    groups = _clusters([a, b])
    assert len(groups) == 2
