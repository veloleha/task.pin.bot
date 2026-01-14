import asyncio
import logging
import aiosqlite
import sqlite3  # Только для init_db
from datetime import datetime
import os
from dotenv import load_dotenv
import html

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Импортируем асинхронные функции БД
from db_async import (
    add_task, update_task_message_id, get_task_message_id,
    update_task_topic_id, get_task_topic_id, close_task,
    set_task_status, get_task_status, get_stats,
    get_pin_message_id, save_pin_message_id,
    get_chat_mode, set_chat_mode,
    get_topic_enabled, set_topic_enabled
)

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

# Anti-spam throttling state
LAST_MSG_TS = {}
LAST_CB_TS = {}

PIN_UPDATE_TASKS = {}
TASK_LOCKS = {}
CHAT_LOCKS = {}


def _throttled(store, key, min_interval: float) -> bool:
    now = asyncio.get_event_loop().time()
    last = store.get(key, 0.0)
    if now - last < min_interval:
        return True
    store[key] = now
    return False


def get_task_lock(task_id):
    """Получить lock для задачи (защита от параллельных операций)"""
    if task_id not in TASK_LOCKS:
        TASK_LOCKS[task_id] = asyncio.Lock()
    return TASK_LOCKS[task_id]


def get_chat_lock(chat_id):
    """Получить lock для чата (защита обновления закрепа)"""
    if chat_id not in CHAT_LOCKS:
        CHAT_LOCKS[chat_id] = asyncio.Lock()
    return CHAT_LOCKS[chat_id]

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
        await update_task_topic_id(task_id, topic_id)

        # Копируем исходное сообщение в тему (клавиатура копируется вместе)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✅ Закрыть задачу", callback_data=f"close_{task_id}")]]
        )
        # Получаем автора и текст для подписи
        try:
            import aiosqlite
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT username, text FROM tasks WHERE id=?", (task_id,)) as cursor:
                    row = await cursor.fetchone()
            username = row[0] if row and row[0] else None
            full_text = row[1] if row and row[1] else ""
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить данные задачи #{task_id} для подписи: {e}")
            username, full_text = None, ""

        author_label = (f"@{html.escape(username)}" if username else "Аноним")
        caption_html = f"👤 <b>Сообщение от</b> {author_label}:\n\n{html.escape(full_text)}"

        # Пытаемся скопировать с подписью, при неудаче — без подписи
        try:
            await bot.copy_message(
                chat_id=chat_id,
                from_chat_id=chat_id,
                message_id=source_message_id,
                message_thread_id=topic_id,
                reply_markup=kb,
                caption=caption_html,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"ℹ️ Не удалось добавить подпись при копировании в тему для задачи #{task_id}: {e}. Копирую без подписи")
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
        types.BotCommand(command="reset", description="Сброс БД и закрепа (с подтверждением)"),
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

async def delete_message_safe(chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.debug(f"🗑️ Удалено сообщение {message_id}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось удалить сообщение {message_id}: {e}")


# --- ОБНОВЛЕНИЕ ЗАКРЕПЛЕННОГО СООБЩЕНИЯ ---
async def update_pinned_message(chat_id):
    # Проверяем права бота перед обновлением закрепа
    if not await check_bot_permissions(chat_id):
        logger.warning(f"⛔ Не могу обновить закреп в чате {chat_id} - недостаточно прав")
        return
    
    pin_message_id = await get_pin_message_id(chat_id)
    open_tasks, closed_tasks, open_list = await get_stats(chat_id)

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
            await save_pin_message_id(chat_id, msg.message_id)
            logger.info(f"📌 Создано и закреплено новое сообщение {msg.message_id}")

    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении закрепа: {e}")

async def schedule_update_pinned_message(chat_id: int, delay: float = 0.7):
    """Debounce обновления закрепа: отменяет предыдущую задачу и планирует новую"""
    # Отменяем предыдущую задачу обновления для этого чата
    existing = PIN_UPDATE_TASKS.get(chat_id)
    if existing and not existing.done():
        existing.cancel()
    
    async def _delayed_pin():
        try:
            await asyncio.sleep(delay)
            # Обновляем закреп с защитой через chat_lock
            async with get_chat_lock(chat_id):
                await update_pinned_message(chat_id)
        except asyncio.CancelledError:
            pass  # Задача отменена — это нормально
        except Exception as e:
            logger.warning(f"⚠️ Ошибка отложенного обновления закрепа для чата {chat_id}: {e}")
    
    PIN_UPDATE_TASKS[chat_id] = asyncio.create_task(_delayed_pin())


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
    await set_chat_mode(chat_id, 'manual')
    await message.answer("🛠️ Режим установлен: ручной. Задачи открываются по кнопке \"Создать задачу\".")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


# --- ПЕРЕКЛЮЧЕНИЕ РЕЖИМА: ТЕМЫ ---
@dp.message(Command("mode_topic"))
async def mode_topic_cmd(message: types.Message):
    chat_id = message.chat.id
    await set_topic_enabled(chat_id, True)
    await message.answer("🧵 Режим тем включен. Для каждой задачи создаётся отдельная тема с копией сообщения.")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


# --- ПЕРЕКЛЮЧЕНИЕ РЕЖИМА: АВТО ---
@dp.message(Command("mode_auto"))
async def mode_auto_cmd(message: types.Message):
    chat_id = message.chat.id
    await set_chat_mode(chat_id, 'auto')
    await message.answer("⚡ Режим установлен: авто. Новые сообщения сразу создают открытую задачу с кнопкой \"Закрыть задачу\".")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


# --- ВКЛ/ВЫКЛ РЕЖИМА ТЕМ ---
@dp.message(Command("topic_on"))
async def topic_on_cmd(message: types.Message):
    chat_id = message.chat.id
    await set_topic_enabled(chat_id, True)
    await message.answer("🧵 Режим тем: включен")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


@dp.message(Command("topic_off"))
async def topic_off_cmd(message: types.Message):
    chat_id = message.chat.id
    await set_topic_enabled(chat_id, False)
    await message.answer("🧵 Режим тем: выключен")
    try:
        await bot.delete_message(chat_id, message.message_id)
    except:
        pass


# --- СБРОС БД И ЗАКРЕПА ---
RESET_CONFIRMATIONS = {}

@dp.message(Command("reset"))
async def reset_cmd(message: types.Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем, есть ли уже запрос на подтверждение
    if RESET_CONFIRMATIONS.get((chat_id, user_id)):
        # Подтверждение получено — выполняем сброс
        try:
            # Удаляем закреп
            pin_id = get_pin_message_id(chat_id)
            if pin_id:
                try:
                    await bot.unpin_chat_message(chat_id, pin_id)
                    await bot.delete_message(chat_id, pin_id)
                    logger.info(f"🗑️ Удален закреп {pin_id} в чате {chat_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось удалить закреп: {e}")
            
            # Очищаем БД для этого чата
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("DELETE FROM tasks WHERE chat_id=?", (chat_id,))
            c.execute("DELETE FROM chats WHERE chat_id=?", (chat_id,))
            conn.commit()
            deleted_tasks = c.rowcount
            conn.close()
            
            RESET_CONFIRMATIONS.pop((chat_id, user_id), None)
            
            await message.answer(f"✅ Сброс выполнен!\n🗑️ Удалено задач: {deleted_tasks}\n📌 Закреп удален")
            logger.info(f"🔄 Сброс БД и закрепа в чате {chat_id} пользователем @{message.from_user.username}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при сбросе: {e}")
            await message.answer(f"❌ Ошибка при сбросе: {e}")
        
        try:
            await bot.delete_message(chat_id, message.message_id)
        except:
            pass
    else:
        # Первый вызов — запрашиваем подтверждение
        RESET_CONFIRMATIONS[(chat_id, user_id)] = True
        await message.answer(
            "⚠️ <b>ВНИМАНИЕ!</b>\n\n"
            "Это действие удалит:\n"
            "• Все задачи в этом чате\n"
            "• Закрепленное сообщение\n"
            "• Настройки чата\n\n"
            "Для подтверждения отправьте /reset еще раз в течение 30 секунд.",
            parse_mode="HTML"
        )
        
        # Автоматически сбрасываем подтверждение через 30 секунд
        async def _clear_confirmation():
            await asyncio.sleep(30)
            RESET_CONFIRMATIONS.pop((chat_id, user_id), None)
        
        asyncio.create_task(_clear_confirmation())
        
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
    # Throttle message spam per chat+user
    if _throttled(LAST_MSG_TS, (chat_id, message.from_user.id), 0.8):
        return
    username = message.from_user.username or message.from_user.full_name or "Аноним"
    text = message.text or message.caption or "(медиа без текста)"
    # Формируем подпись автора: @username если есть, иначе имя без @
    author_label = (
        f"@{html.escape(message.from_user.username)}" if message.from_user.username else html.escape(message.from_user.full_name or "Аноним")
    )
    display_username = html.escape(username)
    display_text = html.escape(text)

    # Сохранить в базу (сначала без message_id)
    task_id = await add_task(chat_id, message.from_user.id, username, text)
    logger.info(f"📝 Создана задача #{task_id} от @{username} в чате {chat_id}")

    # Определяем режим и формируем клавиатуру
    mode = await get_chat_mode(chat_id)
    is_auto = (mode == 'auto')
    topics = await get_topic_enabled(chat_id)
    if is_auto:
        await set_task_status(task_id, 'open')
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✅ Закрыть задачу", callback_data=f"close_{task_id}")]]
        )
    else:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="📝 Создать задачу", callback_data=f"create_{task_id}")]]
        )

    source_message_id = None
    try:
        has_media = (
            getattr(message, "photo", None)
            or getattr(message, "video", None)
            or getattr(message, "document", None)
            or getattr(message, "animation", None)
            or getattr(message, "voice", None)
            or getattr(message, "audio", None)
            or getattr(message, "sticker", None)
            or getattr(message, "video_note", None)
        )
        if has_media:
            sent_msg = None
            # Отправляем медиа c явным caption, чтобы гарантировать подпись автора
            if getattr(message, "photo", None):
                file_id = message.photo[-1].file_id
                sent_msg = await bot.send_photo(chat_id=chat_id, photo=file_id, caption=f"👤 <b>Сообщение от</b> {author_label}:\n\n{display_text}", parse_mode="HTML", reply_markup=kb)
            elif getattr(message, "video", None):
                file_id = message.video.file_id
                sent_msg = await bot.send_video(chat_id=chat_id, video=file_id, caption=f"👤 <b>Сообщение от</b> {author_label}:\n\n{display_text}", parse_mode="HTML", reply_markup=kb)
            elif getattr(message, "document", None):
                file_id = message.document.file_id
                sent_msg = await bot.send_document(chat_id=chat_id, document=file_id, caption=f"👤 <b>Сообщение от</b> {author_label}:\n\n{display_text}", parse_mode="HTML", reply_markup=kb)
            elif getattr(message, "animation", None):
                file_id = message.animation.file_id
                sent_msg = await bot.send_animation(chat_id=chat_id, animation=file_id, caption=f"👤 <b>Сообщение от</b> {author_label}:\n\n{display_text}", parse_mode="HTML", reply_markup=kb)
            elif getattr(message, "audio", None):
                file_id = message.audio.file_id
                sent_msg = await bot.send_audio(chat_id=chat_id, audio=file_id, caption=f"👤 <b>Сообщение от</b> {author_label}:\n\n{display_text}", parse_mode="HTML", reply_markup=kb)
            elif getattr(message, "voice", None):
                file_id = message.voice.file_id
                sent_msg = await bot.send_voice(chat_id=chat_id, voice=file_id, caption=f"👤 <b>Сообщение от</b> {author_label}:\n\n{display_text}", parse_mode="HTML", reply_markup=kb)
            else:
                # Типы без caption (sticker/video_note) — отправим как есть + отдельным сообщением подпись
                copied = await bot.copy_message(chat_id=chat_id, from_chat_id=chat_id, message_id=message.message_id, reply_markup=kb)
                new_message_id = getattr(copied, "message_id", None)
                if new_message_id:
                    await update_task_message_id(task_id, new_message_id)
                    source_message_id = new_message_id
                # Дополнительная подпись отдельным сообщением
                await bot.send_message(chat_id=chat_id, text=f"👤 <b>Сообщение от</b> {author_label}:\n\n{display_text}", parse_mode="HTML")

            if sent_msg:
                await update_task_message_id(task_id, sent_msg.message_id)
                logger.debug(f"✉️ Отправлено медиа {sent_msg.message_id} с подписью автора для задачи #{task_id}")
                source_message_id = sent_msg.message_id
        else:
            sent_msg = await bot.send_message(
                chat_id=chat_id,
                text=f"👤 <b>Сообщение от</b> {author_label}:\n\n{display_text}",
                parse_mode="HTML",
                reply_markup=kb
            )
            await update_task_message_id(task_id, sent_msg.message_id)
            logger.debug(f"✉️ Отправлено сообщение {sent_msg.message_id} с кнопкой для задачи #{task_id}")
            source_message_id = sent_msg.message_id
    except Exception as e:
        logger.error(f"❌ Ошибка отправки сообщения для задачи #{task_id}: {e}")
    
    # В авто-режиме сразу обновляем закреп
    if is_auto:
        try:
            await schedule_update_pinned_message(chat_id)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось обновить закреп в авто-режиме: {e}")

    # Если включены темы — создаём тему в авто-режиме сразу
    if topics and is_auto and source_message_id:
        await create_task_topic_and_post(chat_id, task_id, source_message_id)
    
    # Удалить оригинальное сообщение (требуются права администратора) — не блокируем обработчик
    asyncio.create_task(delete_message_safe(chat_id, message.message_id))


# --- НАЖАТИЕ КНОПКИ "СОЗДАТЬ ЗАДАЧУ" ---
@dp.callback_query(F.data.startswith("create_"))
async def create_task_callback(callback: types.CallbackQuery):
    # Мгновенный ответ для снятия "часиков"
    await callback.answer("⏳")
    
    try:
        chat_id = callback.message.chat.id
        # Throttle callback spam per chat+user
        if _throttled(LAST_CB_TS, (chat_id, callback.from_user.id), 0.5):
            try:
                await callback.answer("Слишком часто. Подождите...", show_alert=True)
            except:
                pass
            return
        task_id = int(callback.data.split("_")[1])
        
        # Lock для защиты от параллельных операций
        lock = get_task_lock(task_id)
        async with lock:
            # Idempotency: ignore if already open/closed
            status = await get_task_status(task_id)
            if status == 'open':
                try:
                    await callback.answer("Задача уже создана", show_alert=True)
                except:
                    pass
                return
            if status == 'closed':
                try:
                    await callback.answer("Задача уже закрыта", show_alert=True)
                except:
                    pass
                return
            await set_task_status(task_id, 'open')
            
            # Меняем кнопку на "Закрыть задачу"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Закрыть задачу", callback_data=f"close_{task_id}")]
                ]
            )
            await callback.message.edit_reply_markup(reply_markup=kb)
            await callback.answer("Задача создана ✅", show_alert=False)
            logger.info(f"✅ Задача #{task_id} принята в работу пользователем @{callback.from_user.username}")
            
            # Обновляем закрепленное сообщение
            await schedule_update_pinned_message(callback.message.chat.id)
        
        # Если включены темы — создаём тему и публикуем сообщение (ВНЕ lock!)
        if await get_topic_enabled(callback.message.chat.id):
            await create_task_topic_and_post(callback.message.chat.id, task_id, callback.message.message_id)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании задачи: {e}")
        await callback.answer("❌ Ошибка при создании задачи", show_alert=True)


# --- НАЖАТИЕ КНОПКИ "ЗАКРЫТЬ ЗАДАЧУ" ---
@dp.callback_query(F.data.startswith("close_"))
async def close_task_callback(callback: types.CallbackQuery):
    # Мгновенный ответ для снятия "часиков"
    await callback.answer("⏳")
    
    try:
        chat_id = callback.message.chat.id
        # Throttle callback spam per chat+user
        if _throttled(LAST_CB_TS, (chat_id, callback.from_user.id), 0.5):
            try:
                await callback.answer("Слишком часто. Подождите...", show_alert=True)
            except:
                pass
            return
        task_id = int(callback.data.split("_")[1])
        
        # Lock для защиты от параллельных операций
        lock = get_task_lock(task_id)
        async with lock:
            chat_id = callback.message.chat.id
            in_topic = bool(getattr(callback.message, "message_thread_id", None))
            # Idempotency: ignore if already closed
            if await get_task_status(task_id) == 'closed':
                try:
                    await callback.answer("Уже закрыта", show_alert=True)
                except:
                    pass
                return

            # 1) Закрываем задачу в БД
            await close_task(task_id)

            # 2) Меняем кнопку на "Переоткрыть"
            kb_reopen = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="♻️ Переоткрыть", callback_data=f"reopen_{task_id}")]]
            )
            
            # Обновляем кнопку на текущем сообщении (где был клик)
            try:
                await callback.message.edit_reply_markup(reply_markup=kb_reopen)
                logger.debug(f"✅ Обновлена кнопка на текущем сообщении задачи #{task_id}")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось обновить кнопку текущего сообщения: {e}")
            
            # Если клик был в теме, обновим также исходное сообщение в общем потоке
            if in_topic:
                try:
                    orig_msg_id = await get_task_message_id(task_id)
                    if orig_msg_id:
                        await bot.edit_message_reply_markup(chat_id=chat_id, message_id=orig_msg_id, reply_markup=kb_reopen)
                        logger.debug(f"✅ Обновлена кнопка на исходном сообщении задачи #{task_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось обновить кнопки исходного сообщения задачи #{task_id}: {e}")

            # 4) Удаляем тему (если есть)
            topic_id = await get_task_topic_id(task_id)
            if topic_id:
                try:
                    await bot.delete_forum_topic(chat_id, message_thread_id=topic_id)
                    await update_task_topic_id(task_id, None)
                    logger.info(f"🧹 Удалена тема задачи #{task_id} (thread_id={topic_id})")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось удалить тему задачи #{task_id}: {e}")

            # 5) Ответ пользователю и обновление закрепа (всегда, даже если часть шагов не удалась)
            try:
                await callback.answer("Задача закрыта ✅", show_alert=False)
            except:
                pass
            try:
                await schedule_update_pinned_message(chat_id)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось обновить закреп: {e}")

            logger.info(f"🔒 Задача #{task_id} закрыта пользователем @{callback.from_user.username}")
        
        # Удаление темы вне lock (если нужно)
        # (уже выполнено внутри lock, оставляем как есть)

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
    # Мгновенный ответ для снятия "часиков"
    await callback.answer("⏳")
    
    try:
        chat_id = callback.message.chat.id
        # Throttle callback spam per chat+user
        if _throttled(LAST_CB_TS, (chat_id, callback.from_user.id), 0.5):
            try:
                await callback.answer("Слишком часто. Подождите...", show_alert=True)
            except:
                pass
            return
        task_id = int(callback.data.split("_")[1])
        
        # Lock для защиты от параллельных операций
        lock = get_task_lock(task_id)
        async with lock:
            chat_id = callback.message.chat.id
            # Idempotency: ignore if already open
            if await get_task_status(task_id) == 'open':
                try:
                    await callback.answer("Уже открыта", show_alert=True)
                except:
                    pass
                return
            await set_task_status(task_id, 'open')

            # Кнопка "Закрыть" для исходного сообщения
            kb_close = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="✅ Закрыть задачу", callback_data=f"close_{task_id}")]]
            )

            # Обновляем кнопки на исходном сообщении
            try:
                orig_msg_id = await get_task_message_id(task_id)
                if orig_msg_id:
                    await bot.edit_message_reply_markup(chat_id=chat_id, message_id=orig_msg_id, reply_markup=kb_close)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось обновить кнопки исходного сообщения при переоткрытии задачи #{task_id}: {e}")

            # Ответ пользователю и обновление закрепа
            try:
                await callback.answer("Задача переоткрыта ✅", show_alert=False)
            except:
                pass
            try:
                await schedule_update_pinned_message(chat_id)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось обновить закреп: {e}")

            logger.info(f"🔓 Задача #{task_id} переоткрыта пользователем @{callback.from_user.username}")
        
        # Если включены темы — создаём новую тему и публикуем копию сообщения (ВНЕ lock!)
        if await get_topic_enabled(chat_id):
            # Используем исходное сообщение (в общем потоке) как источник
            try:
                source_msg_id = await get_task_message_id(task_id)
                if source_msg_id:
                    await create_task_topic_and_post(chat_id, task_id, source_msg_id)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось создать тему при переоткрытии задачи #{task_id}: {e}")

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
        import aiosqlite
        async with aiosqlite.connect(DB_NAME) as db:
            # Получаем все уникальные chat_id с открытыми задачами
            async with db.execute("SELECT DISTINCT chat_id FROM tasks WHERE status='open'") as cursor:
                chats_with_tasks = [row[0] for row in await cursor.fetchall()]
        
        if chats_with_tasks:
            logger.info(f"📌 Найдено {len(chats_with_tasks)} чатов с открытыми задачами")
            for chat_id in chats_with_tasks:
                pin_id = await get_pin_message_id(chat_id)
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
