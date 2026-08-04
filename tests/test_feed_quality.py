"""The delivered-quality verdict: is a pushed row useful to the editor?

This is the metric we now steer by (aug-04), so its precedence rules need
pinning. The editor's own answer always outranks the archive lookup: he
sometimes comments «дубль» on a row whose URL also appears in the archive
(he ran ONE of the copies), and counting that as useful would flatter every
duplicate we ship.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import feed_quality as fq  # noqa: E402

KEY = "example.com/a"


def test_editor_approval_is_useful() -> None:
    labels = {KEY: {"label_publish": True}}
    assert fq.verdict(KEY, labels, set()) == fq.USEFUL


def test_editor_rejection_is_junk() -> None:
    labels = {KEY: {"label_publish": False}}
    assert fq.verdict(KEY, labels, set()) == fq.JUNK


def test_dup_label_is_junk_even_when_published() -> None:
    # He ran one copy and flagged the other — the row we shipped is still junk.
    labels = {KEY: {"label_dup_cross_run": True, "label_publish": True}}
    assert fq.verdict(KEY, labels, {KEY}) == fq.JUNK


def test_within_batch_dup_is_junk() -> None:
    labels = {KEY: {"label_dup_within": True}}
    assert fq.verdict(KEY, labels, set()) == fq.JUNK


def test_archive_hit_counts_when_no_comment() -> None:
    # Silence + he published it = useful. He passes clean rows without a word.
    assert fq.verdict(KEY, {}, {KEY}) == fq.USEFUL


def test_no_signal_is_unknown() -> None:
    # Not reviewed yet — must NOT be scored as junk, or every fresh run reads 0%.
    assert fq.verdict(KEY, {}, set()) == fq.UNKNOWN


def test_comment_without_verdict_is_unknown() -> None:
    labels = {KEY: {"editor_comment": "хм", "label_publish": None}}
    assert fq.verdict(KEY, labels, set()) == fq.UNKNOWN
