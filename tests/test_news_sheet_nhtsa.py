"""A NHTSA recall cluster must never collapse INTO a secondary-source feed
row. _is_nhtsa_cluster gates the fuzzy anti-dup bypass in build_news_sheet."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import build_news_sheet as bns  # noqa: E402


def test_detects_nhtsa_by_domain() -> None:
    assert bns._is_nhtsa_cluster(
        {"primary_domain": "nhtsa.gov", "primary_url": "", "canonical_url": ""})


def test_detects_nhtsa_by_url() -> None:
    assert bns._is_nhtsa_cluster({
        "primary_domain": "",
        "primary_url": "https://www.nhtsa.gov/recalls?nhtsaId=26V400000",
        "canonical_url": "",
    })


def test_non_nhtsa_is_false() -> None:
    assert not bns._is_nhtsa_cluster({
        "primary_domain": "motor1.com",
        "primary_url": "https://motor1.com/news/x",
        "canonical_url": "https://motor1.com/news/x",
    })


def test_missing_fields_safe() -> None:
    assert not bns._is_nhtsa_cluster({})
