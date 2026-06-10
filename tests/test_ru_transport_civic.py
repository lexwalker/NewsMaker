"""Coverage for is_ru_transport_civic — the RU transport-civic
recognizer that force-accepts Russian transport-CIVIC news (traffic
law, roads, taxi/carsharing, ОСАГО, утильсбор, car surveys) to Local
specifics, rescuing it from the LLM's product-only rejection.

Built from the jun-2026 "Опубликованные 3" recall audit. Positive
cases = editor-published transport-civic the bot was missing.
Negative cases = military/banking/utility noise that must NOT pass.
"""

from news_agent.core.heuristic_relevance import is_ru_transport_civic


# ── POSITIVE: editor publishes these (transport-civic) ──────────────

def test_traffic_law_accepted() -> None:
    cases = [
        "Упростили процедуру проверки на опьянение в России",
        "Что грозит водителю за сбитого голубя: какой штраф за нарушение ПДД",
        "Изменили срок действия водительского удостоверения иностранцам",
        "В Москве число камер для фиксации нарушений на самокатах выросло",
    ]
    for t in cases:
        assert is_ru_transport_civic(t), f"should accept traffic-law: {t!r}"


def test_road_infrastructure_accepted() -> None:
    cases = [
        "Участки трассы М-2 Крым расширили до 4 полос",
        "Автодор продлит беспилотный коридор на М-12 до 2030 года",
        "Росавтодор раскритиковал строительство цементных дорог",
        "Тариф на платных парковках у поликлиник изменится",
        "Эвакуаторы переместили на спецстоянки 20 автомобилей",
    ]
    for t in cases:
        assert is_ru_transport_civic(t), f"should accept road-infra: {t!r}"


def test_taxi_carsharing_accepted() -> None:
    cases = [
        "Каршеринг предлагают ввести в правовое поле России",
        "Таксопарк России по маркам на 1 апреля 2026",
        "Каршеринг Яндекс Драйв добавил в автопарк электромобили",
        "Почти 400 камер в Москве научились штрафовать самокатчиков",
        "Кикшеринг Юрент рассказал о пользователях электросамокатов",
    ]
    for t in cases:
        assert is_ru_transport_civic(t), f"should accept taxi/sharing: {t!r}"


def test_ownership_civics_accepted() -> None:
    cases = [
        "На сайте ФТС можно проверить утильсбор по VIN автомобиля",
        "Новый порядок проверки КБМ и наличия полиса ОСАГО",
        "На 8% вырос российский рынок лизинга легковых автомобилей",
        "ТОП-5 моделей, ввезённых в Россию по льготному утильсбору",
    ]
    for t in cases:
        assert is_ru_transport_civic(t), f"should accept ownership: {t!r}"


def test_car_surveys_accepted() -> None:
    cases = [
        "По каким критериям автовладельцы выбирают антифриз?",
        "Где владельцы автомобилей покупают антифриз в России",
        "Около 50% владельцев электромобилей в России имеют вторую машину",
    ]
    for t in cases:
        assert is_ru_transport_civic(t), f"should accept car-survey: {t!r}"


def test_test_drive_accepted() -> None:
    cases = [
        "Москвич М70 тест-драйв",
        "Tank 400 test-drive",
        "Nordcross 001 test-drive",
    ]
    for t in cases:
        assert is_ru_transport_civic(t), f"should accept test-drive: {t!r}"


# ── NEGATIVE: must NOT pass (noise) ─────────────────────────────────

def test_military_drones_rejected() -> None:
    cases = [
        "Средства ПВО уничтожили за сутки 418 беспилотников ВСУ",
        "Минобороны заявило об ударах по транспортной инфраструктуре",
        "Губернатор Самарской области сообщил об атаке ВСУ",
        "Силы ПВО сбили 203 украинских беспилотника над регионами",
    ]
    for t in cases:
        assert not is_ru_transport_civic(t), f"should reject military: {t!r}"


def test_banking_consumer_finance_rejected() -> None:
    cases = [
        "Посмотреть уставный капитал организации по ИНН бесплатно",
        "С 1 мая переводы через СБП для бизнеса станут платными",
        "Сколько платили за ОСАГО в декабре 2023: исследование Банки.ру",
        "Каждый пятый россиянин планирует увеличить свой доход",
        "Россияне купили рекордное число роутеров",
        "Россиянам досрочно перечислят детские пособия",
    ]
    for t in cases:
        assert not is_ru_transport_civic(t), f"should reject finance: {t!r}"


def test_utility_networks_rejected() -> None:
    """«штрафы за повреждение теплосетей» — fines but NOT transport."""
    cases = [
        "Штрафы за повреждение тепловых и электросетей предложили повысить",
        "Энергосети региона модернизируют к зиме",
    ]
    for t in cases:
        assert not is_ru_transport_civic(t), f"should reject utility: {t!r}"


def test_napi_taxi_fleet_published_still_passes() -> None:
    """The NAPI taxi-fleet structure the editor publishes — taxopark
    is a CORE transport term so it passes (good)."""
    assert is_ru_transport_civic(
        "НАПИ опубликовало структуру таксопарка России на 1 апреля"
    )


# ── survey-needs-object guard ───────────────────────────────────────

def test_survey_without_auto_object_rejected() -> None:
    """Bare 'россияне выбрали X' without an auto-object must NOT pass."""
    cases = [
        "Россияне выбрали лучшие подписки 2025 года",
        "Каждый пятый россиянин планирует сменить работу",
        "Россияне назвали любимые сериалы мая",
    ]
    for t in cases:
        assert not is_ru_transport_civic(t), \
            f"survey w/o auto-object should NOT pass: {t!r}"


def test_survey_with_auto_object_passes() -> None:
    assert is_ru_transport_civic(
        "Россияне назвали главные критерии при выборе автомобиля"
    )


def test_v47_military_personnel_rejected() -> None:
    """v47 audit: «проверки военных на опьянение» matched опьянени but
    is about servicemen, not drivers — must be gated out."""
    cases = [
        "Порядок проверки военных на опьянение изменят",
        "Призывников проверят на опьянение перед службой",
        "Военнослужащих обяжут проходить медосмотр",
    ]
    for t in cases:
        assert not is_ru_transport_civic(t), f"military: {t!r}"


def test_v47_license_howto_REJECTED() -> None:
    """CORRECTION (jun-2026 editor feedback on v47): licence how-to
    guides are JUNK, not Local. Editor: «категория D как получить — не
    нужно ни в один раздел». My earlier assumption was WRONG."""
    cases = [
        "Права категории С: как получить, инструкция",
        "Что обозначает категория D в водительских правах",
        "Как восстановить водительское удостоверение: пошаговая инструкция",
    ]
    for t in cases:
        assert not is_ru_transport_civic(t), \
            f"licence how-to should be REJECTED: {t!r}"


def test_v47_ownership_blog_REJECTED() -> None:
    """Ownership-experience blogs are NOT test-drives (editor: «не
    нужно»). Real test-drives still pass."""
    assert not is_ru_transport_civic("Опыт владения Evolute i-Pro")
    assert is_ru_transport_civic("Тест-драйв Voyah Free Sport+")


def test_v47_section_routing() -> None:
    """v47 section-loss bug: rescued transport-civic was dumped into
    Other news. heuristic_section must now route test-drives→Test-drive
    and transport-civic→Local so the accept-path agrees with rescue."""
    from news_agent.core.heuristic_relevance import heuristic_section

    def sec(title: str) -> str:
        r = heuristic_section(title=title, body_excerpt="")
        return r.section if r else "None"

    assert sec("Тест-драйв Voyah Free Sport+") == "Test-drive"
    # ownership blog + licence how-to are JUNK (editor feedback) — they
    # must NOT get a section (defer to LLM/reject, not force-publish)
    assert sec("Опыт владения EVOLUTE") != "Test-drive"
    assert sec("Что обозначает категория D в водительских правах") \
        != "Local specifics"
    assert sec("Водителей могут освободить от платы парковки") \
        == "Local specifics"
    # launches/LCV still win their sections (must not be clobbered)
    assert sec("Volkswagen представил пикап Tukan") == "LCV news"


def test_empty_safe() -> None:
    assert not is_ru_transport_civic("")
    assert not is_ru_transport_civic("   ")


def test_global_transport_not_russian_context() -> None:
    """A pure global product story shouldn't be mistaken for civic —
    no transport-civic CORE term present."""
    assert not is_ru_transport_civic(
        "BMW unveiled the new X5 with redesigned interior"
    )
