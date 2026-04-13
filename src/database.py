import aiosqlite
from typing import List, Optional

from src.config import settings, log
from src.models import Vacancy, Message, Conversation

_db_path = settings.db_path


async def init():
    async with aiosqlite.connect(_db_path) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT UNIQUE NOT NULL,
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                company TEXT,
                salary TEXT,
                city TEXT,
                description TEXT,
                relevance_score INTEGER DEFAULT 0,
                relevance_reason TEXT,
                cover_letter TEXT,
                status TEXT DEFAULT 'new'
                    CHECK(status IN ('new','filtered','sent_to_tg','applied','skipped','error')),
                error_message TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                applied_at TEXT,
                apply_state TEXT DEFAULT 'idle',
                cover_letter_version INTEGER DEFAULT 0,
                employer_requirements TEXT,
                require_cover_letter INTEGER DEFAULT 0,
                negotiation_id TEXT
            );

            CREATE TABLE IF NOT EXISTS search_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_query TEXT,
                total_found INTEGER,
                new_found INTEGER,
                relevant_found INTEGER,
                executed_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE NOT NULL,
                vacancy_id INTEGER REFERENCES vacancies(id),
                vacancy_title TEXT,
                company TEXT,
                conversation_id TEXT,
                direction TEXT CHECK(direction IN ('incoming','outgoing')),
                sender TEXT,
                text TEXT NOT NULL,
                is_read INTEGER DEFAULT 0,
                replied INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                replied_at TEXT
            );

            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT UNIQUE NOT NULL,
                vacancy_id INTEGER REFERENCES vacancies(id),
                vacancy_title TEXT,
                company TEXT,
                last_message_at TEXT,
                status TEXT DEFAULT 'active'
                    CHECK(status IN ('active','waiting_reply','replied','closed')),
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        await db.commit()

        # Миграция: добавить новые поля если их нет (для существующих БД)
        cursor = await db.execute("PRAGMA table_info(vacancies)")
        existing_cols = {row[1] for row in await cursor.fetchall()}
        migrations = [
            ("apply_state", "TEXT DEFAULT 'idle'"),
            ("cover_letter_version", "INTEGER DEFAULT 0"),
            ("employer_requirements", "TEXT"),
            ("require_cover_letter", "INTEGER DEFAULT 0"),
            ("negotiation_id", "TEXT"),
        ]
        for col_name, col_def in migrations:
            if col_name not in existing_cols:
                await db.execute(f"ALTER TABLE vacancies ADD COLUMN {col_name} {col_def}")
                log.info(f"Migrated: added column vacancies.{col_name}")
        await db.commit()
    log.info("Database initialized")


# ─── Vacancies ────────────────────────────────────

async def save_vacancy(v: Vacancy) -> int:
    async with aiosqlite.connect(_db_path) as db:
        cursor = await db.execute(
            """INSERT INTO vacancies
               (external_id, url, title, company, salary, city, description,
                relevance_score, relevance_reason, cover_letter, status,
                apply_state, cover_letter_version, employer_requirements,
                require_cover_letter, negotiation_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(external_id) DO UPDATE SET
                relevance_score=excluded.relevance_score,
                relevance_reason=excluded.relevance_reason,
                cover_letter=excluded.cover_letter,
                status=excluded.status,
                description=excluded.description,
                employer_requirements=excluded.employer_requirements,
                require_cover_letter=excluded.require_cover_letter
            """,
            (v.external_id, v.url, v.title, v.company, v.salary, v.city,
             v.description, v.relevance_score, v.relevance_reason,
             v.cover_letter, v.status, v.apply_state, v.cover_letter_version,
             v.employer_requirements, v.require_cover_letter, v.negotiation_id),
        )
        await db.commit()
        return cursor.lastrowid


async def filter_new(vacancies: List[Vacancy]) -> List[Vacancy]:
    if not vacancies:
        return []
    async with aiosqlite.connect(_db_path) as db:
        placeholders = ",".join("?" for _ in vacancies)
        ids = [v.external_id for v in vacancies]
        cursor = await db.execute(
            f"SELECT external_id FROM vacancies WHERE external_id IN ({placeholders})",
            ids,
        )
        existing = {row[0] for row in await cursor.fetchall()}
    return [v for v in vacancies if v.external_id not in existing]


async def update_status(vacancy_id: int, status: str, error_message: str = None):
    async with aiosqlite.connect(_db_path) as db:
        if status == "applied":
            await db.execute(
                "UPDATE vacancies SET status=?, applied_at=datetime('now') WHERE id=?",
                (status, vacancy_id),
            )
        elif error_message:
            await db.execute(
                "UPDATE vacancies SET status=?, error_message=? WHERE id=?",
                (status, error_message, vacancy_id),
            )
        else:
            await db.execute(
                "UPDATE vacancies SET status=? WHERE id=?",
                (status, vacancy_id),
            )
        await db.commit()


def _row_to_vacancy(row) -> Vacancy:
    return Vacancy(
        id=row["id"],
        external_id=row["external_id"],
        url=row["url"],
        title=row["title"],
        company=row["company"],
        salary=row["salary"],
        city=row["city"],
        description=row["description"],
        relevance_score=row["relevance_score"],
        relevance_reason=row["relevance_reason"],
        cover_letter=row["cover_letter"],
        status=row["status"],
        apply_state=row["apply_state"] or "idle",
        cover_letter_version=row["cover_letter_version"] or 0,
        employer_requirements=row["employer_requirements"],
        require_cover_letter=bool(row["require_cover_letter"]),
        negotiation_id=row["negotiation_id"],
    )


async def get_vacancy(vacancy_id: int) -> Optional[Vacancy]:
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM vacancies WHERE id=?", (vacancy_id,))
        row = await cursor.fetchone()
        if not row:
            return None
        return _row_to_vacancy(row)


async def get_last_vacancies(limit: int = 5) -> List[Vacancy]:
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM vacancies ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
    return [_row_to_vacancy(r) for r in rows]


async def get_stats() -> dict:
    async with aiosqlite.connect(_db_path) as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM vacancies")).fetchone())[0]
        sent = (await (await db.execute(
            "SELECT COUNT(*) FROM vacancies WHERE status='sent_to_tg'"
        )).fetchone())[0]
        applied = (await (await db.execute(
            "SELECT COUNT(*) FROM vacancies WHERE status='applied'"
        )).fetchone())[0]
        today_applied = (await (await db.execute(
            "SELECT COUNT(*) FROM vacancies WHERE status='applied' AND date(applied_at)=date('now')"
        )).fetchone())[0]
    return {
        "total": total, "sent_to_tg": sent,
        "applied": applied, "today_applied": today_applied,
    }


async def save_search_log(query: str = "", total: int = 0, new: int = 0, relevant: int = 0):
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            "INSERT INTO search_log (search_query, total_found, new_found, relevant_found) VALUES (?,?,?,?)",
            (query, total, new, relevant),
        )
        await db.commit()


async def update_apply_state(vacancy_id: int, state: str, **kwargs):
    async with aiosqlite.connect(_db_path) as db:
        sets = ["apply_state=?"]
        vals = [state]
        for key, val in kwargs.items():
            sets.append(f"{key}=?")
            vals.append(val)
        vals.append(vacancy_id)
        await db.execute(
            f"UPDATE vacancies SET {', '.join(sets)} WHERE id=?", vals
        )
        await db.commit()


async def increment_cover_letter_version(vacancy_id: int) -> int:
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            "UPDATE vacancies SET cover_letter_version = cover_letter_version + 1 WHERE id=?",
            (vacancy_id,),
        )
        await db.commit()
        row = await (await db.execute(
            "SELECT cover_letter_version FROM vacancies WHERE id=?", (vacancy_id,)
        )).fetchone()
        return row[0] if row else 0


async def count_today_applies() -> int:
    async with aiosqlite.connect(_db_path) as db:
        row = await (await db.execute(
            "SELECT COUNT(*) FROM vacancies WHERE status='applied' AND date(applied_at)=date('now')"
        )).fetchone()
    return row[0]


# ─── Messages / Conversations ────────────────────

async def save_conversation(c: Conversation) -> int:
    async with aiosqlite.connect(_db_path) as db:
        cursor = await db.execute(
            """INSERT INTO conversations
               (conversation_id, vacancy_id, vacancy_title, company, last_message_at, status)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(conversation_id) DO UPDATE SET
                last_message_at=excluded.last_message_at,
                status=excluded.status
            """,
            (c.conversation_id, c.vacancy_id, c.vacancy_title,
             c.company, c.last_message_at, c.status),
        )
        await db.commit()
        return cursor.lastrowid


async def save_incoming_message(msg: Message) -> Optional[int]:
    async with aiosqlite.connect(_db_path) as db:
        try:
            cursor = await db.execute(
                """INSERT INTO messages
                   (message_id, vacancy_id, vacancy_title, company, conversation_id,
                    direction, sender, text)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (msg.message_id, msg.vacancy_id, msg.vacancy_title, msg.company,
                 msg.conversation_id, msg.direction, msg.sender, msg.text),
            )
            await db.commit()
            return cursor.lastrowid
        except aiosqlite.IntegrityError:
            return None


async def save_outgoing_message(conversation_id: str, text: str):
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            """INSERT INTO messages
               (message_id, conversation_id, direction, text)
               VALUES (?, ?, 'outgoing', ?)""",
            (f"out_{conversation_id}_{int(__import__('time').time())}", conversation_id, text),
        )
        await db.execute(
            "UPDATE conversations SET status='replied', last_message_at=datetime('now') WHERE conversation_id=?",
            (conversation_id,),
        )
        await db.commit()


async def get_conversation_history(conversation_id: str) -> List[Message]:
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM messages WHERE conversation_id=? ORDER BY created_at",
            (conversation_id,),
        )
        rows = await cursor.fetchall()
    return [
        Message(
            id=r["id"], message_id=r["message_id"], text=r["text"],
            direction=r["direction"], sender=r["sender"],
            vacancy_title=r["vacancy_title"], company=r["company"],
            conversation_id=r["conversation_id"],
        )
        for r in rows
    ]


async def get_conversation(conversation_id: str) -> Optional[Conversation]:
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM conversations WHERE conversation_id=?",
            (conversation_id,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return Conversation(
            id=row["id"], conversation_id=row["conversation_id"],
            vacancy_id=row["vacancy_id"], vacancy_title=row["vacancy_title"],
            company=row["company"], status=row["status"],
        )


async def get_vacancy_by_conversation(conversation_id: str) -> Optional[Vacancy]:
    conv = await get_conversation(conversation_id)
    if conv and conv.vacancy_id:
        return await get_vacancy(conv.vacancy_id)
    return None


async def find_vacancy_by_company_title(company: str, title: str) -> Optional[Vacancy]:
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM vacancies WHERE company=? AND title=? LIMIT 1",
            (company, title),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return _row_to_vacancy(row)


async def get_unread_messages() -> List[Message]:
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM messages WHERE direction='incoming' AND is_read=0 ORDER BY created_at DESC"
        )
        rows = await cursor.fetchall()
    return [
        Message(
            id=r["id"], message_id=r["message_id"], text=r["text"],
            direction=r["direction"], sender=r["sender"],
            vacancy_title=r["vacancy_title"], company=r["company"],
            conversation_id=r["conversation_id"],
        )
        for r in rows
    ]


async def get_active_conversations() -> List[Conversation]:
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM conversations WHERE status IN ('active','waiting_reply') ORDER BY last_message_at DESC"
        )
        rows = await cursor.fetchall()
    return [
        Conversation(
            id=r["id"], conversation_id=r["conversation_id"],
            vacancy_title=r["vacancy_title"], company=r["company"],
            status=r["status"], last_message_at=r["last_message_at"],
        )
        for r in rows
    ]
