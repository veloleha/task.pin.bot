import asyncio
import logging
import sqlite3
from datetime import datetime
import os
from dotenv import load_dotenv
import html

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Загрузка токена из .env
load_dotenv()
API_TOKEN = os.getenv("BOT_TOKEN")

if not API_TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден в .env файле!")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

DB_NAME = "tasks.db"


# --- ИНИЦИАЛИЗАЦИЯ БАЗЫ ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Таблица задач
    c.execute('''CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        user_id INTEGER,
        username TEXT,
        text TEXT,
        status TEXT DEFAULT 'open',
        created_at TEXT,
        message_id INTEGER
    )''')
    
    # Таблица для хранения pin_message_id для каждого чата
    c.execute('''CREATE TABLE IF NOT EXISTS chats (
        chat_id INTEGER PRIMARY KEY,
        pin_message_id INTEGER
    )''')
    # Добавляем колонку режима при необходимости
    try:
        c.execute("PRAGMA table_info(chats)")
        columns = [col[1] for col in c.fetchall()]
        if 'mode' not in columns:
            c.execute("ALTER TABLE chats ADD COLUMN mode TEXT DEFAULT 'manual'")
        if 'topic_enabled' not in columns:
            c.execute("ALTER TABLE chats ADD COLUMN topic_enabled INTEGER DEFAULT 0")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось добавить колонку mode: {e}")
    
    # Добавляем колонку topic_id для задач при необходимости
    try:
        c.execute("PRAGMA table_info(tasks)")
        task_columns = [col[1] for col in c.fetchall()]
        if 'topic_id' not in task_columns:
            c.execute("ALTER TABLE tasks ADD COLUMN topic_id INTEGER")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось добавить колонку topic_id: {e}")
    
    conn.commit()
    conn.close()
    logger.info("✅ База данных инициализирована")


# --- ДОБАВЛЕНИЕ ЗАДАЧИ ---
def add_task(chat_id, user_id, username, text, message_id=None):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "INSERT INTO tasks (chat_id, user_id, username, text, status, created_at, message_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (chat_id, user_id, username, text, 'new', datetime.now().isoformat(), message_id)
    )
    conn.commit()
    task_id = c.lastrowid
    conn.close()
    return task_id


# --- ОБНОВЛЕНИЕ MESSAGE_ID ЗАДАЧИ ---
def update_task_message_id(task_id, message_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE tasks SET message_id=? WHERE id=?", (message_id, task_id))
    conn.commit()
    conn.close()


# --- ПОЛУЧИТЬ MESSAGE_ID ЗАДАЧИ ---
def get_task_message_id(task_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT message_id FROM tasks WHERE id=?", (task_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


# --- ОБНОВЛЕНИЕ TOPIC_ID ЗАДАЧИ ---
def update_task_topic_id(task_id, topic_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE tasks SET topic_id=? WHERE id=?", (topic_id, task_id))
    conn.commit()
    conn.close()


def get_task_topic_id(task_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT topic_id FROM tasks WHERE id=?", (task_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


# --- ЗАКРЫТИЕ ЗАДАЧИ ---
def close_task(task_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE tasks SET status='closed' WHERE id=?", (task_id,))
    conn.commit()
    conn.close()

# --- УСТАНОВИТЬ СТАТУС ЗАДАЧИ ---
def set_task_status(task_id, status):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE tasks SET status=? WHERE id=?", (status, task_id))
    conn.commit()
    conn.close()


# --- ПОЛУЧИТЬ СТАТИСТИКУ ---
def get_stats(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM tasks WHERE chat_id=? AND status='open'", (chat_id,))
    open_tasks = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM tasks WHERE chat_id=? AND status='closed'", (chat_id,))
    closed_tasks = c.fetchone()[0]
    c.execute("SELECT id, username, text, message_id FROM tasks WHERE chat_id=? AND status='open' ORDER BY id ASC", (chat_id,))
    open_list = c.fetchall()
    conn.close()
    return open_tasks, closed_tasks, open_list


# --- ПОЛУЧИТЬ PIN_MESSAGE_ID ИЗ БД ---
def get_pin_message_id(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT pin_message_id FROM chats WHERE chat_id=?", (chat_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None


# --- СОХРАНИТЬ PIN_MESSAGE_ID В БД ---
def save_pin_message_id(chat_id, message_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # Сохраняем/обновляем только pin_message_id, не теряя mode
    c.execute("SELECT mode FROM chats WHERE chat_id=?", (chat_id,))
    row = c.fetchone()
    if row is None:
        c.execute(
            "INSERT INTO chats (chat_id, pin_message_id, mode) VALUES (?, ?, ?)",
            (chat_id, message_id, 'manual')
        )
    else:
        c.execute(
            "UPDATE chats SET pin_message_id=? WHERE chat_id=?",
            (message_id, chat_id)
        )
    conn.commit()
    conn.close()


# --- РЕЖИМЫ ЧАТА ---
def get_chat_mode(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("SELECT mode FROM chats WHERE chat_id=?", (chat_id,))
        row = c.fetchone()
        return row[0] if row and row[0] else 'manual'
    finally:
        conn.close()


def set_chat_mode(chat_id, mode):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("SELECT chat_id FROM chats WHERE chat_id=?", (chat_id,))
        exists = c.fetchone() is not None
        if exists:
            c.execute("UPDATE chats SET mode=? WHERE chat_id=?", (mode, chat_id))
        else:
            c.execute("INSERT INTO chats (chat_id, pin_message_id, mode) VALUES (?, ?, ?)", (chat_id, None, mode))
        conn.commit()
    finally:
        conn.close()


# --- ТОГГЛ РЕЖИМА ТЕМ ---
def get_topic_enabled(chat_id) -> bool:
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("SELECT topic_enabled FROM chats WHERE chat_id=?", (chat_id,))
        row = c.fetchone()
        return bool(row[0]) if row else False
    finally:
        conn.close()


def set_topic_enabled(chat_id, enabled: bool):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("SELECT chat_id FROM chats WHERE chat_id=?", (chat_id,))
        exists = c.fetchone() is not None
        val = 1 if enabled else 0
        if exists:
            c.execute("UPDATE chats SET topic_enabled=? WHERE chat_id=?", (val, chat_id))
        else:
            c.execute("INSERT INTO chats (chat_id, pin_message_id, mode, topic_enabled) VALUES (?, ?, ?, ?)", (chat_id, None, 'manual', val))
        conn.commit()
    finally:
        conn.close()


# --- СОЗДАНИЕ ССЫЛКИ НА СООБЩЕНИЕ ---
def create_message_link(chat_id, message_id):
    # Для приватных/супергрупп chat_id имеет вид -100XXXXXXXXXX
    if str(chat_id).startswith('-100'):
        chat_id_clean = str(chat_id)[4:]
    else:
        chat_id_clean = str(chat_id).lstrip('-')
    return f"https://t.me/c/{chat_id_clean}/{message_id}"


# --- СОЗДАНИЕ ТЕМЫ ДЛЯ ЗАДАЧИ И ПУБЛИКАЦИЯ СООБЩЕНИЯ ---
async def create_task_topic_and_post(chat_id: int, task_id: int, source_message_id: int):
    try:
        topic_name = f"Задача #{task_id}"
        topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
        topic_id = getattr(topic, "message_thread_id", None)
        if not topic_id:
            logger.warning(f"⚠️ Не удалось получить message_thread_id для темы задачи #{task_id}")
            return
        update_task_topic_id(task_id, topic_id)

        # Копируем исходное сообщение в тему (клавиатура копируется вместе)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✅ Закрыть задачу", callback_data=f"close_{task_id}")]]
        )
        await bot.copy_message(
            chat_id=chat_id,
            from_chat_id=chat_id,
            message_id=source_message_id,
            message_thread_id=topic_id,
            reply_markup=kb
        )
        logger.info(f"🧵 Создана тема (thread_id={topic_id}) и опубликовано сообщение для задачи #{task_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка при создании темы для задачи #{task_id}: {e}")


# --- РЕГИСТРАЦИЯ КОМАНД БОТА ---
async def setup_bot_commands():
    commands = [
        types.BotCommand(command="start", description="Запуск бота"),
        types.BotCommand(command="refresh", description="Обновить закреп"),
        types.BotCommand(command="mode_manual", description="Режим: вручную"),
        types.BotCommand(command="mode_auto", description="Режим: авто"),
        types.BotCommand(command="mode_topic", description="Режим: темы"),
        types.BotCommand(command="topic_on", description="Включить режим тем"),
        types.BotCommand(command="topic_off", description="Выключить режим тем"),
    ]
    try:
        await bot.set_my_commands(commands)
        await bot.set_my_commands(commands, scope=types.BotCommandScopeAllPrivateChats())
        await bot.set_my_commands(commands, scope=types.BotCommandScopeAllGroupChats())
        await bot.set_my_commands(commands, scope=types.BotCommandScopeAllChatAdministrators())
    except Exception as e:
        logger.warning(f"⚠️ Не удалось установить список команд бота: {e}")

# --- ПРОВЕРКА ПРАВ БОТА ---
async def check_bot_permissions(chat_id):
    try:
        member = await bot.get_chat_member(chat_id, bot.id)
        status = getattr(member, "status", "")
        if status in ("administrator", "creator"):
            can_delete = getattr(member, "can_delete_messages", False)
            can_pin = getattr(member, "can_pin_messages", False)
            if can_delete and can_pin:
                return True
            logger.warning(f"⛔ Недостаточно прав: delete={can_delete}, pin={can_pin} в чате {chat_id}")
            return False
        logger.warning(f"⛔ Бот не администратор в чате {chat_id} (status={status})")
        return False
    except Exception as e:
        logger.warning(f"⚠️ Не удалось проверить права бота в чате {chat_id}: {e}")
        return False


# --- ОБНОВЛЕНИЕ ЗАКРЕПЛЕННОГО СООБЩЕНИЯ ---
async def update_pinned_message(chat_id):
    # Проверяем права бота перед обновлением закрепа
    if not await check_bot_permissions(chat_id):
        logger.warning(f"⛔ Не могу обновить закреп в чате {chat_id} - недостаточно прав")
        return
    
    pin_message_id = get_pin_message_id(chat_id)
    open_tasks, closed_tasks, open_list = get_stats(chat_id)

    # Формирование текста в HTML с экранированием пользовательских данных
    text_lines = [
        "<b>📋 Статистика задач</b>",
        "",
        f"🔴 Открыто: {html.escape(str(open_tasks))} | ✅ Закрыто: {html.escape(str(closed_tasks))}",
        "",
    ]

    if open_tasks:
        text_lines.append("<b>🧾 Открытые задачи:</b>")
        text_lines.append("")
        for idx, t in enumerate(open_list, 1):
            task_id = t[0]
            username = html.escape(t[1] if t[1] else "Аноним")
            text_preview = html.escape((t[2] or "(пусто)")[:60])
            message_id = t[3]

            if message_id:
                link = create_message_link(chat_id, message_id)
                text_lines.append(f"• {idx}. <a href=\"{link}\"><i>{text_preview}</i></a> — @{username}")
            else:
                text_lines.append(f"• {idx}. <i>{text_preview}</i> — @{username}")

    new_text = "\n".join(text_lines)
    logger.debug(f"Generated pin text (HTML):\n{new_text}")

    try:
        if pin_message_id:
            # Пытаемся отредактировать существующее закрепленное сообщение
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=pin_message_id,
                    text=new_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
                logger.info(f"✅ Обновлено закрепленное сообщение {pin_message_id}")
                return
            except Exception as e:
                error_msg = str(e).lower()
                # Сообщение не изменилось — редактирование не требуется, ничего не создаем
                if "message is not modified" in error_msg:
                    logger.info("ℹ️ Текст закрепленного сообщения не изменился — редактирование не требуется")
                    return
                # Сообщение отсутствует/нельзя редактировать — создадим новое
                if (
                    "message to edit not found" in error_msg
                    or "message not found" in error_msg
                    or "message can't be edited" in error_msg
                ):
                    logger.warning(
                        f"⚠️ Закрепленное сообщение {pin_message_id} недоступно для редактирования ({e}), создаю новое"
                    )
                    pin_message_id = None
                else:
                    # Прочие ошибки при редактировании — не создаем новое, чтобы избежать дублей
                    logger.warning(
                        f"⚠️ Не удалось отредактировать закреп {pin_message_id}: {e}. Новое сообщение НЕ будет создано"
                    )
                    return

        if not pin_message_id:
            # Создаем новое закрепленное сообщение
            msg = await bot.send_message(
                chat_id, new_text, parse_mode="HTML", disable_web_page_preview=True
            )
            await bot.pin_chat_message(chat_id, msg.message_id, disable_notification=True)
            save_pin_message_id(chat_id, msg.message_id)
            logger.info(f"📌 Создано и закреплено новое сообщение {msg.message_id}")

    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении закрепа: {e}")


# --- КОМАНДА /start ---
@dp.message(CommandStart())
async def start_cmd(message: types.Message):
    await message.answer("✅ TaskPinBot запущен!\nПросто напишите сообщение — я добавлю кнопку для создания задачи.\n\n📌 Команды:\n/refresh - Обновить закрепленное сообщение")


# --- КОМАНДА /refresh ---
@dp.message(Command("refresh"))
async def refresh_cmd(message: types.Message):
    """Принудительное обновление закрепленного сообщения"""
    try:
        chat_id = message.chat.id
        logger.info(f"🔄 Получена команда /refresh от @{message.from_user.username} в чате {chat_id}")
        
        # Обновляем закреп (сначала попытка редактирования существующего; при неудаче — создание нового)
        await update_pinned_message(chat_id)
        
        # Удаляем команду пользователя
        try:
            await bot.delete_message(chat_id, message.message_id)
        except:
            pass
        
        logger.info(f"✅ Закрепленное сообщение обновлено в чате {chat_id}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при выполнении /refresh: {e}")
        await message.answer(f"❌ Ошибка: {e}")


# --- ПЕРЕКЛЮЧЕНИЕ РЕЖИМА: РУЧНОЙ ---
@dp.message(Command("mode_manual"))
async def mode_manual_cmd(message: types.Message):
    chat_id = message.chat.id
    set_chat_mode(chat_id, 'manual')
    await message.answer("🛠️ Режим установлен: ручной. Задачи открываются по кнопке \"Создать задачу\".")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


# --- ПЕРЕКЛЮЧЕНИЕ РЕЖИМА: ТЕМЫ ---
@dp.message(Command("mode_topic"))
async def mode_topic_cmd(message: types.Message):
    chat_id = message.chat.id
    set_topic_enabled(chat_id, True)
    await message.answer("🧵 Режим тем включен. Для каждой задачи создаётся отдельная тема с копией сообщения.")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


# --- ПЕРЕКЛЮЧЕНИЕ РЕЖИМА: АВТО ---
@dp.message(Command("mode_auto"))
async def mode_auto_cmd(message: types.Message):
    chat_id = message.chat.id
    set_chat_mode(chat_id, 'auto')
    await message.answer("⚡ Режим установлен: авто. Новые сообщения сразу создают открытую задачу с кнопкой \"Закрыть задачу\".")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


# --- ВКЛ/ВЫКЛ РЕЖИМА ТЕМ ---
@dp.message(Command("topic_on"))
async def topic_on_cmd(message: types.Message):
    chat_id = message.chat.id
    set_topic_enabled(chat_id, True)
    await message.answer("🧵 Режим тем: включен")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


@dp.message(Command("topic_off"))
async def topic_off_cmd(message: types.Message):
    chat_id = message.chat.id
    set_topic_enabled(chat_id, False)
    await message.answer("🧵 Режим тем: выключен")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


# --- ОБРАБОТКА НОВЫХ СООБЩЕНИЙ ---
@dp.message()
async def handle_message(message: types.Message):
    # Игнорируем сообщения от ботов
    if message.from_user.is_bot:
        return
    # Игнорируем сообщения внутри тем (обсуждение задач)
    if getattr(message, "message_thread_id", None):
        return

    chat_id = message.chat.id
    username = message.from_user.username or message.from_user.full_name or "Аноним"
    text = message.text or message.caption or "(медиа без текста)"

    # Сохранить в базу (сначала без message_id)
    task_id = add_task(chat_id, message.from_user.id, username, text)
    logger.info(f"📝 Создана задача #{task_id} от @{username} в чате {chat_id}")

    # Определяем режим и формируем клавиатуру
    mode = get_chat_mode(chat_id)
    is_auto = (mode == 'auto')
    topics = get_topic_enabled(chat_id)
    if is_auto:
        set_task_status(task_id, 'open')
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✅ Закрыть задачу", callback_data=f"close_{task_id}")]]
        )
    else:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="📝 Создать задачу", callback_data=f"create_{task_id}")]]
        )

    source_message_id = None
    try:
        if getattr(message, "photo", None) or getattr(message, "video", None) or getattr(message, "document", None) or getattr(message, "animation", None) or getattr(message, "voice", None) or getattr(message, "audio", None) or getattr(message, "sticker", None) or getattr(message, "video_note", None):
            copied = await bot.copy_message(
                chat_id=chat_id,
                from_chat_id=chat_id,
                message_id=message.message_id,
                reply_markup=kb
            )
            new_message_id = getattr(copied, "message_id", None)
            if new_message_id:
                update_task_message_id(task_id, new_message_id)
                logger.debug(f"✉️ Скопировано медиа-сообщение {new_message_id} с кнопкой для задачи #{task_id}")
                source_message_id = new_message_id
            else:
                logger.warning("⚠️ Не удалось получить message_id скопированного сообщения")
        else:
            display_username = html.escape(username)
            display_text = html.escape(text)
            sent_msg = await bot.send_message(
                chat_id=chat_id,
                text=f"👤 <b>Сообщение от</b> @{display_username}:\n\n{display_text}",
                parse_mode="HTML",
                reply_markup=kb
            )
            update_task_message_id(task_id, sent_msg.message_id)
            logger.debug(f"✉️ Отправлено сообщение {sent_msg.message_id} с кнопкой для задачи #{task_id}")
            source_message_id = sent_msg.message_id
    except Exception as e:
        logger.error(f"❌ Ошибка отправки сообщения для задачи #{task_id}: {e}")
    
    # В авто-режиме сразу обновляем закреп
    if is_auto:
        try:
            await update_pinned_message(chat_id)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось обновить закреп в авто-режиме: {e}")

    # Если включены темы — создаём тему в авто-режиме сразу
    if topics and is_auto and source_message_id:
        await create_task_topic_and_post(chat_id, task_id, source_message_id)
    
    # Удалить оригинальное сообщение (требуются права администратора)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message.message_id)
        logger.debug(f"🗑️ Удалено сообщение {message.message_id}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось удалить сообщение {message.message_id}: {e}")
        logger.warning("💡 Убедитесь, что бот является администратором чата с правами удаления сообщений")


# --- НАЖАТИЕ КНОПКИ "СОЗДАТЬ ЗАДАЧУ" ---
@dp.callback_query(F.data.startswith("create_"))
async def create_task_callback(callback: types.CallbackQuery):
    try:
        task_id = int(callback.data.split("_")[1])
        # Отмечаем задачу как открытую по нажатию кнопки
        set_task_status(task_id, 'open')
        
        # Меняем кнопку на "Закрыть задачу"
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Закрыть задачу", callback_data=f"close_{task_id}")]
            ]
        )
        await callback.message.edit_reply_markup(reply_markup=kb)
        await callback.answer("Задача создана ✅")
        logger.info(f"✅ Задача #{task_id} принята в работу пользователем @{callback.from_user.username}")
        
        # Обновляем закрепленное сообщение
        await update_pinned_message(callback.message.chat.id)
        
        # Если включены темы — создаём тему и публикуем сообщение
        if get_topic_enabled(callback.message.chat.id):
            await create_task_topic_and_post(callback.message.chat.id, task_id, callback.message.message_id)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании задачи: {e}")
        await callback.answer("❌ Ошибка при создании задачи", show_alert=True)


# --- НАЖАТИЕ КНОПКИ "ЗАКРЫТЬ ЗАДАЧУ" ---
@dp.callback_query(F.data.startswith("close_"))
async def close_task_callback(callback: types.CallbackQuery):
    try:
        task_id = int(callback.data.split("_")[1])
        chat_id = callback.message.chat.id
        in_topic = bool(getattr(callback.message, "message_thread_id", None))

        # 1) Закрываем задачу в БД
        close_task(task_id)

        # 2) Меняем кнопку на исходном сообщении в общем потоке
        kb_reopen = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="♻️ Переоткрыть", callback_data=f"reopen_{task_id}")]]
        )
        try:
            orig_msg_id = get_task_message_id(task_id)
            if orig_msg_id:
                await bot.edit_message_reply_markup(chat_id=chat_id, message_id=orig_msg_id, reply_markup=kb_reopen)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось обновить кнопки исходного сообщения задачи #{task_id}: {e}")

        # 3) Если клик был НЕ в теме (в исходном сообщении) — обновим и текущую кнопку
        if not in_topic:
            try:
                await callback.message.edit_reply_markup(reply_markup=kb_reopen)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось обновить кнопки текущего сообщения: {e}")

        # 4) Удаляем тему (если есть)
        topic_id = get_task_topic_id(task_id)
        if topic_id:
            try:
                await bot.delete_forum_topic(chat_id, message_thread_id=topic_id)
                update_task_topic_id(task_id, None)
                logger.info(f"🧹 Удалена тема задачи #{task_id} (thread_id={topic_id})")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось удалить тему задачи #{task_id}: {e}")

        # 5) Ответ пользователю и обновление закрепа (всегда, даже если часть шагов не удалась)
        try:
            await callback.answer("Задача закрыта ✅")
        except:
            pass
        try:
            await update_pinned_message(chat_id)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось обновить закреп: {e}")

        logger.info(f"🔒 Задача #{task_id} закрыта пользователем @{callback.from_user.username}")

    except Exception as e:
        logger.error(f"❌ Ошибка при закрытии задачи: {e}")
        try:
            await callback.answer("❌ Ошибка при закрытии задачи", show_alert=True)
        except:
            pass


# --- ЗАГЛУШКА ДЛЯ ЗАКРЫТЫХ ЗАДАЧ ---
@dp.callback_query(F.data == "none")
async def none_callback(callback: types.CallbackQuery):
    await callback.answer("Эта задача уже закрыта")


# --- НАЖАТИЕ КНОПКИ "ПЕРЕОТКРЫТЬ" ---
@dp.callback_query(F.data.startswith("reopen_"))
async def reopen_task_callback(callback: types.CallbackQuery):
    try:
        task_id = int(callback.data.split("_")[1])
        chat_id = callback.message.chat.id
        set_task_status(task_id, 'open')

        # Кнопка "Закрыть" для исходного сообщения
        kb_close = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✅ Закрыть задачу", callback_data=f"close_{task_id}")]]
        )

        # Обновляем кнопки на исходном сообщении
        try:
            orig_msg_id = get_task_message_id(task_id)
            if orig_msg_id:
                await bot.edit_message_reply_markup(chat_id=chat_id, message_id=orig_msg_id, reply_markup=kb_close)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось обновить кнопки исходного сообщения при переоткрытии задачи #{task_id}: {e}")

        # Ответ пользователю и обновление закрепа
        try:
            await callback.answer("Задача переоткрыта ✅")
        except:
            pass
        try:
            await update_pinned_message(chat_id)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось обновить закреп: {e}")

        # Если включены темы — создаём новую тему и публикуем копию сообщения
        if get_topic_enabled(chat_id):
            # Используем исходное сообщение (в общем потоке) как источник
            try:
                source_msg_id = get_task_message_id(task_id)
                if source_msg_id:
                    await create_task_topic_and_post(chat_id, task_id, source_msg_id)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось создать тему при переоткрытии задачи #{task_id}: {e}")

        logger.info(f"🔓 Задача #{task_id} переоткрыта пользователем @{callback.from_user.username}")

    except Exception as e:
        logger.error(f"❌ Ошибка при переоткрытии задачи: {e}")
        try:
            await callback.answer("❌ Ошибка при переоткрытии задачи", show_alert=True)
        except:
            pass


# --- ИНИЦИАЛИЗАЦИЯ ЗАКРЕПОВ ДЛЯ ВСЕХ ЧАТОВ ---
async def init_pins_for_all_chats():
    """Создает закрепленные сообщения для всех чатов с открытыми задачами"""
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        # Получаем все уникальные chat_id с открытыми задачами
        c.execute("SELECT DISTINCT chat_id FROM tasks WHERE status='open'")
        chats_with_tasks = [row[0] for row in c.fetchall()]
        
        conn.close()
        
        if chats_with_tasks:
            logger.info(f"📌 Найдено {len(chats_with_tasks)} чатов с открытыми задачами")
            for chat_id in chats_with_tasks:
                pin_id = get_pin_message_id(chat_id)
                if not pin_id:
                    logger.info(f"🔄 Создаю закрепленное сообщение для чата {chat_id}")
                    await update_pinned_message(chat_id)
                else:
                    logger.info(f"✅ Закреп уже существует для чата {chat_id} (message_id: {pin_id})")
        else:
            logger.info("ℹ️ Нет чатов с открытыми задачами")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при инициализации закрепов: {e}")


# --- ЗАПУСК ---
async def main():
    try:
        init_db()
        logger.info("=" * 50)
        logger.info("🚀 TaskPinBot запущен!")
        logger.info("=" * 50)
        logger.info("📝 Функции:")
        logger.info("  • Автоматическая замена сообщений пользователей на сообщения бота")
        logger.info("  • Кнопки создания и закрытия задач")
        logger.info("  • Автоматическое обновление закрепленного сообщения со статистикой")
        logger.info("=" * 50)
        
        # Регистрируем команды, чтобы при вводе '/' клиенты показывали список
        await setup_bot_commands()
        
        # Инициализируем закрепленные сообщения для всех чатов
        await init_pins_for_all_chats()
        
        await dp.start_polling(bot, skip_updates=True)
        
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка при запуске бота: {e}")
        raise
    finally:
        logger.info("🛑 TaskPinBot остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Бот остановлен пользователем (Ctrl+C)")
