import pytest

from src.models import Vacancy


pytestmark = [pytest.mark.asyncio, pytest.mark.live_api]

TEST_CHAT_ID = "live_test_chat"

# Обезличенный аналог реального профиля — по объёму и «весу» как в проде
# (executive + AI, масштаб в десятки млн USD). Это принципиально: на коротком
# профиле разница между промптами не видна, а на развёрнутом старый промпт
# начинает мерить престиж работодателя и резать всё подряд.
TEST_PROFILE = (
    "Коммерческий руководитель и AI-трансформатор. CEO / CCO / COO / Head of AI / "
    "AI Transformation Lead. 18+ лет опыта.\n"
    "- Двойной фокус: (1) executive-роли в коммерции и операциях, (2) внедрение "
    "AI-агентов и автоматизации в бизнес-процессы с измеримым эффектом на P&L.\n"
    "- Отрасли: B2B-дистрибуция (текстиль, бытовая техника), производство, опт "
    "и ритейл. Беларусь, Россия, Польша.\n"
    "- Масштаб: команды до 150 человек, оборот под управлением до 60 млн USD.\n"
    "- AI и автоматизация: 10+ AI-агентов в продакшене (коммерция, поддержка, "
    "контроль качества, HR, финансы); 20% критических процессов автоматизированы "
    "с Human-in-the-Loop. Собрал CustDev-агента на 500+ клиентских интервью с "
    "транскрибацией и разметкой инсайтов.\n"
    "- Коммерция: построение и перестройка отделов продаж, дистрибуционных сетей "
    "и категорийного менеджмента; вывод новых товарных групп; управление "
    "маржинальностью и оборотным капиталом; переговоры с поставщиками из Азии "
    "и Европы; годовое и квартальное планирование, KPI и система мотивации.\n"
    "- Операции: складская логистика, закупки, ценообразование, внедрение CRM "
    "и BI-отчётности, оптимизация сквозных процессов от закупки до отгрузки.\n"
    "- Управление: найм и развитие руководителей среднего звена, постановка "
    "регулярного менеджмента, работа с собственниками и советом директоров.\n"
    "- Образование: экономика и управление предприятием; программы по "
    "стратегическому менеджменту и цифровой трансформации.\n"
    "- Ищет: executive-роль в коммерции или операциях, либо роль лидера "
    "AI-трансформации в среднем и крупном бизнесе."
)


@pytest.fixture
def stub_profile(monkeypatch):
    """Профиль кандидата без похода в БД — API остаётся живым."""
    from src import ai_filter

    async def fake_info(chat_id):
        return "Тестовый Кандидат", TEST_PROFILE

    monkeypatch.setattr(ai_filter, "get_candidate_info", fake_info)


EXECUTIVE_VACANCY = Vacancy(
    external_id="live_exec_1",
    url="https://example.com/live/exec",
    title="Commercial Director / COO",
    company="B2B Distribution Group",
    salary="6000 USD",
    city="Minsk",
    description=(
        "We need an executive leader for a wholesale distribution business. "
        "Responsibilities include P&L ownership, sales management, KPI design, "
        "category management, CRM rollout, supplier negotiations, and process optimization "
        "for a 40-person team."
    ),
)

AI_TRANSFORMATION_VACANCY = Vacancy(
    external_id="live_exec_2",
    url="https://example.com/live/ai",
    title="Director of Business Development and AI Transformation",
    company="Digital Transformation Studio",
    salary="7000 USD",
    city="Minsk",
    description=(
        "Lead commercial growth, client strategy, and AI-driven process redesign for "
        "mid-market companies. Role includes executive stakeholder management, "
        "automation roadmap, delivery team leadership, and packaging AI products "
        "for business use cases."
    ),
)

FRONTEND_VACANCY = Vacancy(
    external_id="live_low_1",
    url="https://example.com/live/frontend",
    title="Frontend Engineer (React/TypeScript)",
    company="Product Engineering Team",
    salary="3500 USD",
    city="Minsk",
    description=(
        "Build UI features with React, TypeScript, GraphQL, design systems, and "
        "automated tests. Individual contributor role focused on frontend architecture, "
        "component development, and close collaboration with designers."
    ),
)


def _assert_valid_live_result(result: dict):
    assert isinstance(result, dict)
    assert isinstance(result.get("score"), int)
    assert 0 <= result["score"] <= 100

    reason = result.get("reason")
    assert isinstance(reason, str)
    assert reason.strip()
    assert not reason.startswith("AI error:")


async def test_live_relevance_scores_rank_executive_roles_above_frontend(
    live_anthropic_api_key, stub_profile,
):
    from src.ai_filter import evaluate_and_cover

    executive = await evaluate_and_cover(EXECUTIVE_VACANCY, TEST_CHAT_ID, min_score=60)
    ai_transformation = await evaluate_and_cover(
        AI_TRANSFORMATION_VACANCY, TEST_CHAT_ID, min_score=60
    )
    frontend = await evaluate_and_cover(FRONTEND_VACANCY, TEST_CHAT_ID, min_score=60)

    _assert_valid_live_result(executive)
    _assert_valid_live_result(ai_transformation)
    _assert_valid_live_result(frontend)

    assert executive["score"] > frontend["score"]
    assert ai_transformation["score"] > frontend["score"]

    # cover letter генерируется для релевантных в том же вызове
    assert executive.get("cover_letter")
    assert isinstance(executive["cover_letter"], str)
    assert len(executive["cover_letter"].strip()) >= 80


# Точный состав прод-батча за 05.09.2026 — «слабый день»: executive-вакансий нет,
# лучшее, что есть, — руководители отделов. Порядок, компании и зарплаты взяты
# как есть: старый промпт калибровал шкалу внутри батча и учитывал масштаб
# работодателя (здесь это мелкие ООО с 2500-3500 Br), поэтому выдавал максимум 32
# при пороге 40 — три дня подряд «подходящих не найдено». Скрининг должен мерить
# соответствие должности профилю, абсолютной шкалой, а не престиж вакансии.
# kind: rel — обязан пройти порог, noise — обязан не пройти, grey — не проверяем.
SCREENING_BATCH = [
    ("Арт-менеджер Ресторан-клуба «Танцы»", "Арена-пицца", None, "noise"),
    ("Менеджер по продажам Яндекс Директ", "ООО Таргенетика",
     "85 000 – 135 000 ₽ за месяц, до вычета налогов", "noise"),
    ("Начальник отдела организации продаж (микро и малый бизнес)",
     "ОАО Сбер Банк", None, "rel"),
    ("Заместитель генерального директора по строительству", "ЗАО Агрокомбинат Заря",
     "2 500 – 3 500 Br за месяц, до вычета налогов", "noise"),
    ("Заместитель директора по организации розничной торговли",
     "ООО АгроФудЛидер", None, "rel"),
    ("Начальник коммерческого отдела", "Государственное предприятие ЦСК",
     "3 000 – 3 500 Br за месяц, до вычета налогов", "rel"),
    ("NKAM / Национальный менеджер по работе с ключевыми клиентами",
     "МОСТРА-ГРУПП", None, "rel"),
    ("Управляющий туристическим комплексом", "ООО Минерал Гудс Трейд", None, "grey"),
    ("Специалист по развитию оптовой сети продаж собственной торговой марки",
     "ООО АквилонАвто", "от 3 500 Br за месяц, на руки", "grey"),
    ("Менеджер по продажам IT-решений / Sales (B2B)", "ООО Софтомнител",
     "2 500 – 6 000 Br за месяц, на руки", "grey"),
    ("Senior Full Stack разработчик", "AccentNetwork",
     "6 000 – 8 000 ₽ за месяц, на руки", "noise"),
    ("Главный бухгалтер", "ООО Автопромсервис", None, "noise"),
    ("Бизнес-ассистент / помощник основателя в стартапе", "ИП (частное лицо)",
     "2 000 – 4 000 Br за месяц, на руки", "grey"),
    ("Специалист по работе с клиентами", "ООО БелПроектКонсалтинг",
     "2 200 – 3 500 Br за месяц, до вычета налогов", "grey"),
    ("Начальник склада", "Мебель-Неман, СООО", "от 2 000 Br за месяц, на руки", "noise"),
    ("Офис-менеджер / операционный администратор (1С)", "ООО Робокоп",
     "1 800 – 2 200 Br за месяц, на руки", "noise"),
]


def _vacancy(idx: int, title: str, company: str, salary: str | None) -> Vacancy:
    return Vacancy(
        external_id=f"screen_{idx}",
        url=f"https://example.com/live/screen/{idx}",
        title=title,
        company=company,
        salary=salary,
        city="Минск",
        description="",
    )


async def test_live_batch_screening_separates_relevant_from_noise(
    live_anthropic_api_key, stub_profile,
):
    """Быстрая оценка по названиям пропускает профильные роли и режет чужие.

    Защита от регрессии калибровки: шкала в BATCH_EVALUATE_PROMPT должна быть
    абсолютной, иначе «слабый» день снова обнулит выдачу.
    """
    from src.ai_filter import batch_evaluate_titles
    from src.pipeline import BATCH_THRESHOLD

    vacancies = [
        _vacancy(i, t, c, sal) for i, (t, c, sal, _) in enumerate(SCREENING_BATCH)
    ]
    scores = await batch_evaluate_titles(vacancies, TEST_CHAT_ID)
    assert len(scores) == len(vacancies)

    relevant = [scores[i] for i, row in enumerate(SCREENING_BATCH) if row[3] == "rel"]
    noise = [scores[i] for i, row in enumerate(SCREENING_BATCH) if row[3] == "noise"]

    passed = [s for s in relevant if s >= BATCH_THRESHOLD]
    assert len(passed) >= 3, f"профильные роли зарезаны: {relevant}"
    assert max(noise) < BATCH_THRESHOLD, f"чужие роли прошли порог: {noise}"
    assert min(relevant) > max(noise), f"шкала не разделяет: {relevant} vs {noise}"
