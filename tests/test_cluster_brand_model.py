"""brand_model_match clustering signal (may-2026 editor dup report).

Real case: two v39 rows about the SAME Jaguar Type 01 spy-shot story
were NOT clustered and got different sections, because the EN headlines
shared only the token "Jaguar":
  r11 "Jaguar Type 01 appears in new images ahead of imminent debut"
  r20 "Jaguar electric sedan prototype spotted without heavy camouflage"
Both extract launch_brand_model "jaguar type 01" — that now clusters
them, gated on the 36h time window so coverage months apart stays split.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import build_news_clusters as bnc  # noqa: E402

_T0 = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)


def _art(norm: str, *, bm: str = "", pub: datetime | None = None,
         purl: str = "") -> dict:
    return {
        "normalised": norm,
        "launch_brand_model": bm,
        "pub_dt": pub if pub is not None else _T0,
        "primary_url": purl,
    }


def _clusters(arts: list[dict]) -> list[list[dict]]:
    bnc._BRANDS_LOWER = ["jaguar", "toyota", "bmw"]
    return bnc.cluster_articles(arts)


def test_jaguar_weak_titles_same_brand_model_cluster() -> None:
    a = _art("jaguar type 01 appears new images ahead imminent debut",
             bm="Jaguar Type 01")
    b = _art("jaguar electric sedan prototype spotted without heavy camouflage",
             bm="Jaguar Type 01", pub=_T0 + timedelta(hours=6))
    groups = _clusters([a, b])
    # Both must land in ONE cluster.
    assert len(groups) == 1
    assert len(groups[0]) == 2


def test_same_brand_model_outside_time_window_NOT_clustered() -> None:
    """Same model written about > 36h apart = different stories."""
    a = _art("jaguar type 01 appears new images ahead imminent debut",
             bm="Jaguar Type 01")
    b = _art("jaguar electric sedan prototype spotted without camouflage",
             bm="Jaguar Type 01", pub=_T0 + timedelta(hours=72))
    groups = _clusters([a, b])
    assert len(groups) == 2


def test_bare_brand_no_model_does_NOT_force_cluster() -> None:
    """launch_brand_model must be a real "<brand> <model>" pair — a bare
    brand like "jaguar" must NOT glue two unrelated Jaguar stories."""
    a = _art("jaguar quarterly sales dropped in europe", bm="Jaguar")
    b = _art("jaguar opens new design studio in london", bm="Jaguar")
    groups = _clusters([a, b])
    assert len(groups) == 2


def test_different_models_same_brand_NOT_clustered() -> None:
    a = _art("toyota launched new camry sedan", bm="Toyota Camry")
    b = _art("toyota unveiled corolla cross suv", bm="Toyota Corolla Cross")
    groups = _clusters([a, b])
    assert len(groups) == 2


def test_brand_model_match_still_unions_when_one_title_empty() -> None:
    a = _art("", bm="BMW iX3", purl="")
    b = _art("bmw ix3 new electric crossover details",
             bm="BMW iX3", pub=_T0 + timedelta(hours=2))
    groups = _clusters([a, b])
    assert len(groups) == 1


def test_outside_time_window_helper() -> None:
    assert bnc._outside_time_window(_T0, _T0 + timedelta(hours=72)) is True
    assert bnc._outside_time_window(_T0, _T0 + timedelta(hours=6)) is False
    # missing timestamps → not "outside" (don't block on absent data)
    assert bnc._outside_time_window(None, _T0) is False
    assert bnc._outside_time_window(_T0, None) is False


# ---- _url_model_key: slug-derived brand+model (the real fix) -------------

def test_url_model_key_jaguar_real_urls() -> None:
    bnc._BRANDS_LOWER = ["jaguar", "toyota", "bmw"]
    assert bnc._url_model_key(
        "https://www.kolesa.ru/news/jaguar-type-01-pokazalsia-na-snimkax"
    ) == "jaguar type 01"
    assert bnc._url_model_key(
        "https://www.motor1.com/news/796122/jaguar-type-01-new-images/"
    ) == "jaguar type 01"


def test_url_model_key_no_brand_returns_empty() -> None:
    bnc._BRANDS_LOWER = ["jaguar", "toyota"]
    assert bnc._url_model_key(
        "https://example.com/news/some-generic-market-report"
    ) == ""
    assert bnc._url_model_key("") == ""


def test_url_model_key_brand_then_long_word_no_model() -> None:
    """Brand followed only by non-digit words → no key. A model id is a
    digit-bearing code; verb/noun phrases (incl. RU translit) are not."""
    bnc._BRANDS_LOWER = ["jaguar"]
    assert bnc._url_model_key(
        "https://kolesa.ru/news/jaguar-nazval-imia-pervoi-modeli"
    ) == ""


def test_url_model_key_no_collision_on_pressroom_slugs() -> None:
    """Audit regression: geely-motors.com/about-geely/news/<slug> used to
    yield "geely news" for EVERY press release (catastrophic over-merge);
    cnevpost xpeng-to-<verb> yielded "xpeng to". Both must be ''."""
    bnc._BRANDS_LOWER = ["geely", "xpeng", "jaguar"]
    assert bnc._url_model_key(
        "https://www.geely-motors.com/about-geely/news/"
        "geely-unveiled-an-innovative-v6-engine-for"
    ) == ""
    assert bnc._url_model_key(
        "https://www.geely-motors.com/about-geely/news/"
        "geely-reports-record-revenue-and-sales-2025"
    ) == ""
    assert bnc._url_model_key(
        "https://www.geely-motors.com/about-geely/news/"
        "geely-introduces-new-geely-coolray-in-russia"
    ) == ""  # Coolray has no digit code → no key (falls back to title)
    assert bnc._url_model_key(
        "https://cnevpost.com/2026/05/13/xpeng-to-report-q1-earnings-may-28/"
    ) == ""
    assert bnc._url_model_key(
        "https://cnevpost.com/2026/05/11/xpeng-to-launch-gx-suv-may-20/"
    ) == ""


def test_url_model_key_digit_code_models_extracted() -> None:
    """Digit-bearing model codes ARE extracted (the real dup signal)."""
    bnc._BRANDS_LOWER = ["jaguar", "bmw", "kia"]
    assert bnc._url_model_key(
        "https://media.jaguar.com/news/2026/05/jaguar-type-01-name-new-era"
    ) == "jaguar type 01"
    assert bnc._url_model_key(
        "https://example.com/news/bmw-ix3-long-wheelbase-revealed"
    ) == "bmw ix3"
    assert bnc._url_model_key(
        "https://example.com/news/new-kia-ev9-gt-breaks-cover"
    ) == "kia ev9"


def test_url_slug_merges_divergent_headlines() -> None:
    """End-to-end: the exact editor case. Two empty-launch_brand_model
    articles with only 'jaguar' shared in titles but the same URL slug
    must land in one cluster."""
    bnc._BRANDS_LOWER = ["jaguar"]
    a = {
        "normalised": "jaguar type 01 appears new images ahead imminent debut",
        "launch_brand_model": "",
        "pub_dt": _T0,
        "primary_url": "",
        "url": "https://www.kolesa.ru/news/jaguar-type-01-pokazalsia-na-snimkax",
    }
    b = {
        "normalised": "jaguar electric sedan prototype spotted without camouflage",
        "launch_brand_model": "",
        "pub_dt": _T0 + timedelta(hours=5),
        "primary_url": "",
        "url": "https://www.motor1.com/news/796122/jaguar-type-01-new-images/",
    }
    groups = bnc.cluster_articles([a, b])
    assert len(groups) == 1
    assert len(groups[0]) == 2
