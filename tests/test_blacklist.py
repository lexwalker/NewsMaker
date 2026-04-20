"""Blacklist hard-reject logic.

Rule: a phrase in the TITLE rejects the article; a phrase in the body
(even many times) does NOT — legitimate auto-market news frequently
mentions «автобус» or «tractor» in a single paragraph.
"""

from news_agent.core.config_loader import Blacklist, BrandDomainEntry
from news_agent.core.heuristic_relevance import blacklist_hit
from news_agent.core.models import RawArticle

BRANDS = [
    BrandDomainEntry(brand="BMW", domains=["bmw.com"]),
    BrandDomainEntry(brand="Volvo", aliases=["Вольво"], domains=["volvocars.com"]),
    BrandDomainEntry(brand="Tesla", aliases=["Тесла"], domains=["tesla.com"]),
]


def _art(title: str = "", body: str = "", url: str = "https://example.com/") -> RawArticle:
    return RawArticle(
        url=url,
        title=title,
        body=body,
        source_name="s",
        source_url="https://example.com/",
    )


BL = Blacklist(
    topic_phrases_ru=["автобус", "трактор", "цены на литий"],
    topic_phrases_en=["bus demand", "tractor"],
    domains=["benchmarkminerals.com"],
)


def test_title_phrase_rejects() -> None:
    v = blacklist_hit(_art(title="Record Q1 for zero emission bus demand"), BL)
    assert v.hit
    assert "bus demand" in v.reason


def test_russian_title_phrase_rejects() -> None:
    assert blacklist_hit(_art(title="Продажи автобусов выросли на 30%"), BL).hit


def test_body_mention_does_NOT_reject() -> None:
    # Auto-market review that mentions buses in passing — must pass.
    raw = _art(
        title="Рынок легковых автомобилей в РФ за март: рост на 12%",
        body=(
            "По данным АЕБ, продажи легковых автомобилей в марте 2026 года выросли "
            "на 12%. Отдельно отмечается, что сегмент автобусов снизился на 5%, "
            "а LCV остался на уровне прошлого года. Лидером по продажам остаётся Lada."
        ),
    )
    v = blacklist_hit(raw, BL)
    assert not v.hit, v.reason


def test_body_only_tractor_mention_does_NOT_reject() -> None:
    raw = _art(
        title="Чери открыл новый завод в Узбекистане",
        body="Завод будет выпускать седаны и кроссоверы. На той же площадке ранее "
        "производили тракторы, но линии перепрофилированы под легковые автомобили.",
    )
    assert not blacklist_hit(raw, BL).hit


def test_domain_block_still_applies() -> None:
    raw = _art(
        title="Battery Market Intelligence",
        url="https://benchmarkminerals.com/?camefrom=rhomotion.com",
    )
    v = blacklist_hit(raw, BL)
    assert v.hit
    assert "benchmarkminerals" in v.reason


def test_empty_blacklist_never_hits() -> None:
    empty = Blacklist()
    assert not blacklist_hit(_art(title="Новый электробус"), empty).hit


# --- Auto-signal override ----------------------------------------------------
def test_brand_in_title_overrides_blacklist() -> None:
    # «BMW представила электробус» — там и «электробус», и «BMW».
    # LLM должен сам решить, что это: Other news или что-то ещё.
    raw = _art(title="BMW представила новый электробус для европейского рынка")
    assert not blacklist_hit(raw, BL, brands=BRANDS).hit


def test_auto_marker_in_title_overrides_blacklist() -> None:
    # «Автомобильный рынок: легковые, LCV и автобусы за Q1» — есть автомаркер
    raw = _art(title="Авторынок Q1: легковые, автобусы и LCV — рост на 12%")
    assert not blacklist_hit(raw, BL, brands=BRANDS).hit


def test_blacklist_phrase_without_any_auto_signal_rejects() -> None:
    # «Продажи автобусов выросли на 30%» — нет ни бренда, ни авто-маркера.
    raw = _art(title="Продажи автобусов в Москве выросли на 30%")
    assert blacklist_hit(raw, BL, brands=BRANDS).hit


def test_short_brand_alias_does_not_cause_false_override() -> None:
    # У KIA / BMW / GAZ есть короткие алиасы — они не должны случайно срабатывать
    # в словах вроде "Aristocrat BUS" (где нет реального бренда).
    raw = _art(title="Экскаватор нового поколения: рекорд по скорости")
    # Тут blacklist-слово «экскаватор» есть, бренда нет — отклоняем.
    BL_EX = Blacklist(topic_phrases_ru=["экскаватор"])
    assert blacklist_hit(raw, BL_EX, brands=BRANDS).hit
