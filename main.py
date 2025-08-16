import os
import re
import time
import asyncio
import tempfile
import sqlite3
import math
from typing import Dict, Any

import io
import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    LabeledPrice, FSInputFile
)
from yandex_music import ClientAsync
from mutagen.id3 import ID3, TPE1, TIT2, APIC
from PIL import Image

# === КОНФИГУРАЦИЯ ===
invoices = {}   
try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    YM_TOKEN = os.getenv("YM_TOKEN")
except ImportError:
    raise SystemExit(1)

# Параметры подписки и БД
SUBSCRIPTIONS_DB = "subscriptions.db"
SUBSCRIBE_PRICE_STARS = 50
SUBSCRIBE_DURATION_DAYS = 30

# === ИНИЦИАЛИЗАЦИЯ ===
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

ym_client = None

# Добавлено: сохранение id бота для проверки reply_to_message
BOT_ID: int | None = None

user_states: Dict[int, Dict[str, Any]] = {}

# === УПРАВЛЕНИЕ ЗАГРУЗКАМИ И ПОДПИСЧИКАМИ ===
download_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
MAX_CONCURRENT_DOWNLOADS = 10


def init_db():
    conn = sqlite3.connect(SUBSCRIPTIONS_DB)
    try:
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS subscriptions
               (
                   user_id
                   INTEGER
                   PRIMARY
                   KEY,
                   expires_at
                   INTEGER
               )"""
        )
        conn.commit()
    finally:
        conn.close()


def add_subscription(user_id: int, days: int = SUBSCRIBE_DURATION_DAYS) -> None:
    now = int(time.time())
    conn = sqlite3.connect(SUBSCRIPTIONS_DB)
    try:
        c = conn.cursor()
        c.execute("SELECT expires_at FROM subscriptions WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if row and int(row[0]) > now:
            # продлеваем от текущего expires_at
            current_expires = int(row[0])
            new_expires = current_expires + days * 86400
            c.execute("UPDATE subscriptions SET expires_at = ? WHERE user_id = ?", (new_expires, user_id))
        else:
            # новая подписка от текущего момента
            new_expires = now + days * 86400
            c.execute("INSERT OR REPLACE INTO subscriptions (user_id, expires_at) VALUES (?, ?)",
                      (user_id, new_expires))
        conn.commit()
    finally:
        conn.close()


def get_subscription_days_left(user_id: int) -> int:
    conn = sqlite3.connect(SUBSCRIPTIONS_DB)
    try:
        c = conn.cursor()
        c.execute("SELECT expires_at FROM subscriptions WHERE user_id = ?", (user_id,))
        row = c.fetchone()
    finally:
        conn.close()

    if not row:
        return 0
    expires_at = int(row[0])
    now = int(time.time())
    if expires_at <= now:
        return 0
    seconds_left = expires_at - now
    days_left = math.ceil(seconds_left / 86400)
    return days_left


def is_subscribed(user_id: int) -> bool:
    return get_subscription_days_left(user_id) > 0


# === ОСНОВНЫЕ ФУНКЦИИ БОТА ===
def sanitize_filename(filename: str) -> str:
    return re.sub(r'[\/*?:"<>|]', "", filename)


async def edit_progress_message(chat_id: int, message_id: int, text: str) -> None:
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
    except:
        pass


async def add_action_buttons(chat_id: int, message_id: int, title: str) -> None:
    markup = InlineKeyboardMarkup(inline_keyboard=[
         [InlineKeyboardButton(text="Удалить", callback_data=f"delete_{message_id}")]
    ])
    try:
        await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=markup)
    except:
        pass


def add_tags_to_audio_blocking(filename: str, title: str, artists: str, cover_data: bytes) -> None:
    audio = ID3()
    audio.add(TPE1(encoding=3, text=artists))
    audio.add(TIT2(encoding=3, text=title))
    audio.add(APIC(encoding=3, mime="image/jpeg", type=3, desc="Cover", data=cover_data))
    audio.save(filename)


async def add_tags_to_audio(filename: str, title: str, artists: str, cover_data: bytes) -> None:
    await asyncio.to_thread(add_tags_to_audio_blocking, filename, title, artists, cover_data)


async def download_file_aio(url: str, filename: str, chat_id: int, progress_msg_id: int) -> None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                total_size = int(resp.headers.get('Content-Length', 0) or 0)
                downloaded = 0
                start_time = time.time()
                last_update = 0.0
                with open(filename, 'wb') as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            current_time = time.time()

                            if total_size > 0 and (current_time - last_update >= 1 or downloaded == total_size):
                                last_update = current_time
                                progress = int(downloaded / total_size * 100) if total_size > 0 else 0
                                elapsed = current_time - start_time
                                speed = (downloaded / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                                progress_text = (
                                    f"Загрузка... {progress}%\n"
                                    f"Скачано: {downloaded / (1024 * 1024):.2f}MB / {(total_size / (1024 * 1024)) if total_size > 0 else 0:.2f}MB\n"
                                    f"Скорость: {speed:.2f} MB/s"
                                )
                                await edit_progress_message(chat_id, progress_msg_id, progress_text)
    except:
        await edit_progress_message(chat_id, progress_msg_id, f"Ошибка при загрузке файла")


# === Новая функция: сохранение превью для Telegram ===
def save_jpeg_thumb(cover_data: bytes) -> str:
    fd, path = tempfile.mkstemp(suffix=".jpg", prefix="thumb_")
    os.close(fd)
    try:
        img = Image.open(io.BytesIO(cover_data))
        img = img.convert("RGB")
        img.thumbnail((320, 320), Image.LANCZOS)

        for quality in (95, 85, 75, 65, 50):
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=True)
            size = buf.tell()
            if size <= 200 * 1024 or quality == 50:
                with open(path, "wb") as f:
                    f.write(buf.getvalue())
                return path
    except:
        try:
            if os.path.exists(path):
                os.remove(path)
        except:
            pass
        raise


async def download_and_send_track(chat_id: int, track_id: int, progress_msg_id: int) -> None:
    temp_file = None
    temp_thumb = None
    try:
        track_info = (await ym_client.tracks(track_id))[0]
        artists = ", ".join(artist.name for artist in track_info.artists)
        title = track_info.title

        await edit_progress_message(chat_id, progress_msg_id, "Получение информации о треке...")

        cover_url = f"https://{track_info.cover_uri.replace('%%', '400x400')}"

        async with aiohttp.ClientSession() as session:
            async with session.get(cover_url) as resp:
                resp.raise_for_status()
                cover_data = await resp.read()

        try:
            temp_thumb = save_jpeg_thumb(cover_data)
        except:
            temp_thumb = None

        if hasattr(track_info, 'get_download_info_async'):
            download_info = await track_info.get_download_info_async(get_direct_links=True)
        else:
            download_info = await asyncio.to_thread(lambda: track_info.get_download_info(get_direct_links=True))

        if not download_info:
            pass
        mp3_infos = [di for di in download_info if di.codec == 'mp3' and di.direct_link]
        if not mp3_infos:
            await edit_progress_message(chat_id, progress_msg_id, "MP3 формат недоступен для этого трека. Попробуйте другой трек.")
            return

        mp3_infos.sort(key=lambda x: x.bitrate_in_kbps, reverse=True)
        direct_link = mp3_infos[0].direct_link

        fd, temp_path = tempfile.mkstemp(suffix=".mp3", prefix=f"ym_{chat_id}_")
        os.close(fd)
        temp_file = temp_path

        await download_file_aio(direct_link, temp_path, chat_id, progress_msg_id)

        file_size = os.path.getsize(temp_path)
        if file_size > 50 * 1024 * 1024:
            await edit_progress_message(chat_id, progress_msg_id, "Файл слишком большой для отправки как аудио (>50MB).")
            return

        await add_tags_to_audio(temp_path, title, artists, cover_data)

        await edit_progress_message(chat_id, progress_msg_id, "Отправка трека...")

        try:
            if temp_thumb:
                sent_audio = await bot.send_audio(
                    chat_id=chat_id,
                    audio=FSInputFile(temp_path),
                    title=title,
                    performer=artists,
                    thumbnail=FSInputFile(temp_thumb)
                )
            else:
                sent_audio = await bot.send_audio(
                    chat_id=chat_id,
                    audio=FSInputFile(temp_path),
                    title=title,
                    performer=artists
                )

            await add_action_buttons(chat_id, sent_audio.message_id, title)
        except:
            await edit_progress_message(chat_id, progress_msg_id, f"Ошибка при отправке трека")
            return

        try:
            await bot.delete_message(chat_id, progress_msg_id)
        except:
            pass

    except:
        await edit_progress_message(chat_id, progress_msg_id, f"Общая ошибка")
    finally:
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        if temp_thumb and os.path.exists(temp_thumb):
            try:
                os.remove(temp_thumb)
            except:
                pass


async def download_worker():
    while True:
        priority, task = await download_queue.get()
        try:
            await download_and_send_track(*task)
        except:
            pass
        finally:
            download_queue.task_done()


# === ОБРАБОТЧИКИ КОМАНД И СООБЩЕНИЙ AIОGRAM ===
@dp.message(Command("start"))
async def send_welcome(message: Message):
    await message.answer(
        "Отправьте мне название песни или строчку из неё, и я найду этот трек!\n\n"
        "Пример: `Rammstein - Deutschland`\n\n"
        "/subscribe - оформить подписку для приоритетной загрузки\n\n"
        "Добавьте меня в чат и ищите песни вместе с друзьями с помощью команды /search __название__",
        parse_mode="Markdown"
    )


# === Команда статуса ===
@dp.message(Command("status"))
async def status_handler(message: Message):
    chat_id = message.chat.id
    days_left = get_subscription_days_left(chat_id)
    await message.answer(f"Ваш user_id: {chat_id}\nОсталось дней подписки: {days_left} дней")


# === БЛОК ОПЛАТЫ И ПОДПИСКИ ===
@dp.message(Command("subscribe"))
async def subscribe_handler(message: Message):
    chat_id = message.chat.id
    days_left = get_subscription_days_left(chat_id)
    if days_left > 0:
        await message.answer(
            f"У вас уже есть подписка.\n\nОсталось: {days_left} дней.\n\nПосле оплаты к текущей подписке добавится ещё {SUBSCRIBE_DURATION_DAYS} дней.")

    try:
        amount = SUBSCRIBE_PRICE_STARS
        prices = [LabeledPrice(label="Подписка", amount=amount)]

        invoice_msg = await bot.send_invoice(
            chat_id=chat_id,
            title=f"Подписка на {SUBSCRIBE_DURATION_DAYS} дней",
            description=f"Оплата подписки на {SUBSCRIBE_DURATION_DAYS} дней с приоритетной загрузкой треков.",
            payload="subscribe_30d",
            provider_token="",
            currency="XTR",
            prices=prices,
            start_parameter="subscribe"
        )

        invoices[chat_id] = invoice_msg.message_id

    except:
        await message.answer("Не удалось создать счёт для оплаты. Пожалуйста, попробуйте позже.")


@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query):
    await pre_checkout_query.answer(True)


@dp.message(lambda m: getattr(m, "successful_payment", None) is not None)
async def successful_payment_handler(message: Message):
    chat_id = message.chat.id
    if chat_id in invoices:
        try:
            await bot.delete_message(chat_id, invoices[chat_id])
        except:
            pass
        finally:
            invoices.pop(chat_id, None)

    if message.successful_payment and message.successful_payment.invoice_payload == "subscribe_30d":
        add_subscription(chat_id, days=SUBSCRIBE_DURATION_DAYS)
        days_left = get_subscription_days_left(chat_id)
        await message.answer(
            f"Спасибо за оплату! Ваша подписка оформлена.\n\nОсталось {days_left} дней.")
    else:
        await message.answer(
            "Оплата прошла, но не удалось активировать подписку из-за технической ошибки. Пожалуйста, обратитесь в поддержку")


# === ОСНОВНЫЕ ФУНКЦИИ ПОИСКА (вынесены для переиспользования) ===
async def perform_search_and_show(message: Message, query: str):
    """
    Вынесенная логика поиска/показа результатов.
    Используется и для приватных сообщений (plain text), и для /search в группах.
    """
    chat_id = message.chat.id

    try:
        # удаляем старое сообщение с выбором, если было
        if chat_id in user_states and "select_msg" in user_states[chat_id]:
            try:
                await bot.delete_message(chat_id, user_states[chat_id]["select_msg"].message_id)
            except:
                pass

        # используем query вместо message.text
        search_result = await ym_client.search(query, type_="track")

        if not getattr(search_result, 'tracks', None) or not getattr(search_result.tracks, 'results', None):
            await message.answer("Ничего не найдено. Попробуйте изменить запрос.")
            return

        tracks = search_result.tracks.results[:5]
        inline_keyboard = []
        for track in tracks:
            if not getattr(track, 'available', True):
                continue
            title = f"{track.title} - {', '.join(artist.name for artist in track.artists)}"
            callback_data = f"download_{track.id}"
            inline_keyboard.append([InlineKeyboardButton(text=title, callback_data=callback_data)])

        if inline_keyboard:
            markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)
            select_msg = await message.answer("Выберите трек для загрузки:", reply_markup=markup)
            user_states[chat_id] = {"select_msg": select_msg}
        else:
            await message.answer("Найденные треки недоступны для загрузки.")

    except:
        await message.answer("Произошла ошибка при поиске. Попробуйте позже.")


# --- Новый обработчик команды /search (для групп и приватных) ---
@dp.message(Command("search"))
async def search_command_handler(message: Message):
    """
    Обработчик /search название
    В группах обязателен, в приватных можно использовать тоже.
    """
    # Получаем аргументы команды
    query = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            query = parts[1].strip()

    if not query:
        await message.answer("Использование: /search название песни (например: /search Rammstein - Deutschland)")
        return

    await perform_search_and_show(message, query)


# --- Изменённый универсальный обработчик сообщений ---
@dp.message()
async def search_track_handler(message: Message):
    """
    Теперь этот хендлер принимает ПОЛНЫЕ текстовые сообщения ТОЛЬКО в приватных чатах.
    В группах обычные сообщения игнорируются (и предлагается использовать /search).
    Также предотвращаем обработку сообщений, начинающихся с '/' (команды).
    """
    # Если это команда — пропускаем (команды обрабатываются командным хендлером выше)
    if message.text and message.text.startswith("/"):
        return

    # Разрешаем "просто название" только в приватных чатах
    # message.chat.type может быть 'private', 'group', 'supergroup', 'channel'
    if message.chat.type != "private":
        # НЕ реагируем на сообщения от ботов
        if getattr(message.from_user, "is_bot", False):
            return

        # НЕ реагируем, если это ответ на сообщение бота (fix)
        reply = getattr(message, "reply_to_message", None)
        if reply and getattr(reply, "from_user", None) and getattr(reply.from_user, "id", None) == BOT_ID:
            return

        # По умолчанию — подсказка использовать /search в публичных чатах
        await message.answer("В публичных чатах используйте команду /search название.")
        return

    # В приватных — используем присланный текст как запрос
    query = message.text.strip() if message.text else ""
    if not query:
        await message.answer("Отправьте название трека (например: Rammstein - Deutschland) или используйте /search.")
        return

    await perform_search_and_show(message, query)


@dp.callback_query(lambda c: c.data and c.data.startswith("download_"))
async def download_callback_handler(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    try:
        track_id = int(callback.data.split("_")[1])

        priority = 0 if is_subscribed(chat_id) else 1

        try:
            await bot.delete_message(chat_id, callback.message.message_id)
        except:
            pass

        progress_msg = await bot.send_message(chat_id, "Ваш запрос добавлен в очередь...")

        await download_queue.put((priority, (chat_id, track_id, progress_msg.message_id)))

        if priority == 0:
            await callback.answer("Приоритетная загрузка началась!")
        else:
            await callback.answer("Загрузка началась...")

    except:
        await callback.answer("Ошибка при добавлении в очередь загрузки.")


@dp.callback_query(lambda c: c.data and c.data.startswith("delete_"))
async def delete_track_handler(callback: CallbackQuery):
    try:
        message_id_to_delete = int(callback.data.split("_")[1])
        await bot.delete_message(callback.message.chat.id, message_id_to_delete)
        await callback.answer("Трек удалён.")
    except:
        await callback.answer("Не удалось удалить трек (возможно, он уже удалён).")


# === ЗАПУСК БОТА ===
async def main():
    global ym_client, BOT_ID
    init_db()
    ym_client = ClientAsync(YM_TOKEN)
    await ym_client.init()
    # Инициализируем информацию о боте (чтобы знать его id и не отвечать на собственные сообщения)
    me = await bot.get_me()
    BOT_ID = me.id
    print("Running!")
    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        asyncio.create_task(download_worker())
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
