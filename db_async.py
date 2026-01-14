"""Асинхронные функции для работы с БД через aiosqlite"""
import aiosqlite
from datetime import datetime
import logging
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)

DB_NAME = "tasks.db"


# --- ДОБАВЛЕНИЕ ЗАДАЧИ ---
async def add_task(chat_id, user_id, username, text, message_id=None):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "INSERT INTO tasks (chat_id, user_id, username, text, status, created_at, message_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (chat_id, user_id, username, text, 'new', datetime.now().isoformat(), message_id)
        )
        await db.commit()
        return cursor.lastrowid


# --- ОБНОВЛЕНИЕ MESSAGE_ID ЗАДАЧИ ---
async def update_task_message_id(task_id, message_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE tasks SET message_id=? WHERE id=?", (message_id, task_id))
        await db.commit()


# --- ПОЛУЧИТЬ MESSAGE_ID ЗАДАЧИ ---
async def get_task_message_id(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT message_id FROM tasks WHERE id=?", (task_id,)) as cursor:
            row = await cursor.fetchone()
    return row[0] if row else None


# --- ОБНОВЛЕНИЕ TOPIC_ID ЗАДАЧИ ---
async def update_task_topic_id(task_id, topic_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE tasks SET topic_id=? WHERE id=?", (topic_id, task_id))
        await db.commit()


async def get_task_topic_id(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT topic_id FROM tasks WHERE id=?", (task_id,)) as cursor:
            row = await cursor.fetchone()
    return row[0] if row else None


# --- ЗАКРЫТИЕ ЗАДАЧИ ---
async def close_task(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE tasks SET status='closed', closed_at=? WHERE id=?",
            (datetime.now().isoformat(), task_id)
        )
        await db.commit()


async def reopen_task(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE tasks SET status='open', closed_at=NULL WHERE id=?",
            (task_id,)
        )
        await db.commit()


# --- УСТАНОВИТЬ СТАТУС ЗАДАЧИ ---
async def set_task_status(task_id, status):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE tasks SET status=? WHERE id=?", (status, task_id))
        await db.commit()


async def get_task_status(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT status FROM tasks WHERE id=?", (task_id,)) as cursor:
            row = await cursor.fetchone()
    return row[0] if row else None


# --- ПОЛУЧИТЬ СТАТИСТИКУ ---
async def get_stats(chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM tasks WHERE chat_id=? AND status='open'", (chat_id,)) as cursor:
            open_tasks = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM tasks WHERE chat_id=? AND status='closed'", (chat_id,)) as cursor:
            closed_tasks = (await cursor.fetchone())[0]
        async with db.execute("SELECT id, username, text, message_id FROM tasks WHERE chat_id=? AND status='open' ORDER BY id ASC", (chat_id,)) as cursor:
            open_list = await cursor.fetchall()
    return open_tasks, closed_tasks, open_list


# --- ПОЛУЧИТЬ PIN_MESSAGE_ID ИЗ БД ---
async def get_pin_message_id(chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT pin_message_id FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            result = await cursor.fetchone()
    return result[0] if result else None


# --- СОХРАНИТЬ PIN_MESSAGE_ID В БД ---
async def save_pin_message_id(chat_id, message_id):
    async with aiosqlite.connect(DB_NAME) as db:
        # Сохраняем/обновляем только pin_message_id, не теряя mode
        async with db.execute("SELECT mode FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            await db.execute(
                "INSERT INTO chats (chat_id, pin_message_id, mode) VALUES (?, ?, ?)",
                (chat_id, message_id, 'manual')
            )
        else:
            await db.execute(
                "UPDATE chats SET pin_message_id=? WHERE chat_id=?",
                (message_id, chat_id)
            )
        await db.commit()


# --- РЕЖИМЫ ЧАТА ---
async def get_chat_mode(chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT mode FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            row = await cursor.fetchone()
    return row[0] if row and row[0] else 'manual'


async def set_chat_mode(chat_id, mode):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT chat_id FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            exists = await cursor.fetchone() is not None
        if exists:
            await db.execute("UPDATE chats SET mode=? WHERE chat_id=?", (mode, chat_id))
        else:
            await db.execute("INSERT INTO chats (chat_id, pin_message_id, mode) VALUES (?, ?, ?)", (chat_id, None, mode))
        await db.commit()


# --- ТОГГЛ РЕЖИМА ТЕМ ---
async def get_topic_enabled(chat_id) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT topic_enabled FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            row = await cursor.fetchone()
    return bool(row[0]) if row else False


async def set_topic_enabled(chat_id, enabled: bool):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT chat_id FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            exists = await cursor.fetchone() is not None
        val = 1 if enabled else 0
        if exists:
            await db.execute("UPDATE chats SET topic_enabled=? WHERE chat_id=?", (val, chat_id))
        else:
            await db.execute("INSERT INTO chats (chat_id, pin_message_id, mode, topic_enabled) VALUES (?, ?, ?, ?)", (chat_id, None, 'manual', val))
        await db.commit()


async def upsert_chat_user(chat_id: int, user_id: int, username: Optional[str], full_name: Optional[str]):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            INSERT INTO chat_users (chat_id, user_id, username, full_name, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                last_seen=excluded.last_seen
            """,
            (chat_id, user_id, username, full_name, datetime.now().isoformat())
        )
        await db.commit()


async def get_chat_users(chat_id: int) -> List[Tuple[int, Optional[str], Optional[str]]]:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT user_id, username, full_name FROM chat_users WHERE chat_id=? ORDER BY last_seen DESC",
            (chat_id,)
        ) as cursor:
            return await cursor.fetchall()


async def get_all_chat_ids() -> List[int]:
    async with aiosqlite.connect(DB_NAME) as db:
        chat_ids = set()
        async with db.execute("SELECT DISTINCT chat_id FROM chat_users") as cursor:
            for row in await cursor.fetchall():
                chat_ids.add(row[0])
        async with db.execute("SELECT DISTINCT chat_id FROM tasks") as cursor:
            for row in await cursor.fetchall():
                chat_ids.add(row[0])
        async with db.execute("SELECT DISTINCT chat_id FROM chats") as cursor:
            for row in await cursor.fetchall():
                chat_ids.add(row[0])
        return list(chat_ids)


async def get_chat_info_text(chat_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT info_text FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            row = await cursor.fetchone()
    return row[0] if row and row[0] else None


async def set_chat_info_text(chat_id: int, text: str):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT chat_id FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            exists = await cursor.fetchone() is not None
        if exists:
            await db.execute("UPDATE chats SET info_text=? WHERE chat_id=?", (text, chat_id))
        else:
            await db.execute(
                "INSERT INTO chats (chat_id, pin_message_id, mode, topic_enabled, info_text) VALUES (?, ?, ?, ?, ?)",
                (chat_id, None, 'manual', 0, text)
            )
        await db.commit()


async def get_chat_current_info_text(chat_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT current_info_text FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            row = await cursor.fetchone()
    return row[0] if row and row[0] else None


async def set_chat_current_info_text(chat_id: int, text: str):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT chat_id FROM chats WHERE chat_id=?", (chat_id,)) as cursor:
            exists = await cursor.fetchone() is not None
        if exists:
            await db.execute("UPDATE chats SET current_info_text=? WHERE chat_id=?", (text, chat_id))
        else:
            await db.execute(
                "INSERT INTO chats (chat_id, pin_message_id, mode, topic_enabled, current_info_text) VALUES (?, ?, ?, ?, ?)",
                (chat_id, None, 'manual', 0, text)
            )
        await db.commit()


async def get_period_stats(chat_id: int, start_iso: str, end_iso: str):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM tasks WHERE chat_id=? AND created_at>=? AND created_at<=?",
            (chat_id, start_iso, end_iso)
        ) as cursor:
            created_cnt = (await cursor.fetchone())[0]
        async with db.execute(
            "SELECT COUNT(*) FROM tasks WHERE chat_id=? AND closed_at IS NOT NULL AND closed_at>=? AND closed_at<=?",
            (chat_id, start_iso, end_iso)
        ) as cursor:
            closed_cnt = (await cursor.fetchone())[0]
        async with db.execute(
            "SELECT COUNT(*) FROM tasks WHERE chat_id=? AND status='open'",
            (chat_id,)
        ) as cursor:
            open_now = (await cursor.fetchone())[0]
    return created_cnt, closed_cnt, open_now
