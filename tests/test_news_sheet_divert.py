"""The clean feed's last line of defence: a row whose OWN llm_reason
self-rejects must go to the review tab, not to the editor's working list.

The LLM sometimes publishes a row while explaining, in the same breath, that
it is off-topic or a suspected duplicate. _llm_flag reads that rationale back
and diverts. The filter had no tests, and it drifted twice: «linkedin/sharing»
class aside, «не автомобильн» failed to match «БЕЗ АВТОМОБИЛЬНОГО угла», so
five macro-economics rows reached the clean feed in the two weeks to jul-30
(Fed rate hold ×2, RF GDP ×2, inflation expectations) and the editor rejected
the two he got to: «нет», «я экономику не ищу для дайджеста».
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import build_news_sheet as bns  # noqa: E402


def _flag(reason: str) -> str:
    return bns._llm_flag({"llm_reason": reason})


# --- the jul-30 leak ------------------------------------------------------

def test_macro_without_auto_angle_is_diverted() -> None:
    assert _flag("Макроэкономика без автомобильного угла — раздел «Экономика».") == "junk"
    assert _flag("Макроэкономическая статистика ВВП без автомобильного угла.") == "junk"
    assert _flag("Макроэкономика без автомобильного угла — чистая Economics.") == "junk"


def test_macro_mention_in_a_live_auto_story_is_kept() -> None:
    """The anchor is the self-rejecting phrase, not the word «макроэкономика» —
    a real story may reason about macro conditions and must still publish."""
    assert _flag(
        "Продажи Lada в июне выросли на 12% несмотря на макроэкономическое "
        "давление и ставку ЦБ — рыночная статистика РФ.") == ""
    assert _flag(
        "ЦБ ужесточил макропруденциальные лимиты по автокредитам — "
        "регуляторное решение с прямым эффектом на авторынок РФ.") == ""


# --- the classes that were already guarded --------------------------------

def test_off_topic_vocabulary_is_diverted() -> None:
    for reason in (
        "Это не новость, а подборка советов",
        "Не наша тема — категория исключена",
        "Инфраструктурная новость, не автопром",
        "Лайфстайл-листикл от агрегатора",
        "Не реальное событие, социальный контент",
    ):
        assert _flag(reason) == "junk", reason


def test_suspected_dup_is_diverted_as_dup() -> None:
    assert _flag("возможно дубль: писали об этом 5 дней назад") == "dup"


def test_junk_wins_over_dup() -> None:
    """Self-rejection is the more confident signal of the two."""
    assert _flag("возможно дубль, и вообще не новость") == "junk"


def test_clean_reason_is_not_flagged() -> None:
    assert _flag("Официальный дебют Lada Azimut с ценами — реальное событие") == ""
    assert _flag("") == ""
