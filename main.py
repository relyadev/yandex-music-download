import os
import re
import time
import asyncio
import tempfile
import sqlite3
import math
from typing import Dict, Any

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    LabeledPrice, FSInputFile
)
from yandex_music import ClientAsync
from mutagen.id3 import ID3, TPE1, TIT2, APIC

# === КОНФИГУРАЦИЯ ===
invoices = {}
try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    YM_TOKEN = os.getenv("YM_TOKEN")
except ImportError:
    raise SystemExit(1)

# Параметры подписки и БД
SUBSCRIPTIONS_DB = "subscriptions.db"
SUBSCRIBE_PRICE_STARS = 1
SUBSCRIBE_DURATION_DAYS = 30

# === ИНИЦИАЛИЗАЦИЯ ===
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

ym_client = None

user_states: Dict[int, Dict[str, Any]] = {}

# === УПРАВЛЕНИЕ ЗАГРУЗКАМИ И ПОДПИСЧИКАМИ ===
download_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
MAX_CONCURRENT_DOWNLOADS = 10


def init_db():
    """Создаёт БД и таблицу подписок, если их нет."""
    conn = sqlite3.connect(SUBSCRIPTIONS_DB)
    try:
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS subscriptions (
                   user_id INTEGER PRIMARY KEY,
                   expires_at INTEGER
               )"""
        )
        conn.commit()
    finally:
        conn.close()


def add_subscription(user_id: int, days: int = SUBSCRIBE_DURATION_DAYS) -> None:
    """Добавляет подписку: если подписка уже есть и ещё не истекла — продлевает её на days.
    Иначе создаёт новую подписку на days от текущего момента.
    """
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
            c.execute("INSERT OR REPLACE INTO subscriptions (user_id, expires_at) VALUES (?, ?)", (user_id, new_expires))
        conn.commit()
    finally:
        conn.close()


def get_subscription_days_left(user_id: int) -> int:
    """Возвращает количество оставшихся дней подписки (целое, округление вверх).
    Если подписки нет или она просрочена — возвращает 0.
    """
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
        [InlineKeyboardButton(text="Поделиться", url=f"https://t.me/share/url?url={title}&text=Слушай: {title}"),
         InlineKeyboardButton(text="Удалить", callback_data=f"delete_{message_id}")]
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
                                    f"Загрузка... {progress}%"
                                    f"Скачано: {downloaded / (1024 * 1024):.2f}MB / {(total_size / (1024 * 1024)) if total_size > 0 else 0:.2f}MB"
                                    f"Скорость: {speed:.2f} MB/s"
                                )
                                await edit_progress_message(chat_id, progress_msg_id, progress_text)
    except:
        pass


async def download_and_send_track(chat_id: int, track_id: int, progress_msg_id: int) -> None:
    temp_file = None
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

        if hasattr(track_info, 'get_download_info_async'):
            download_info = await track_info.get_download_info_async(get_direct_links=True)
        else:
            download_info = await asyncio.to_thread(lambda: track_info.get_download_info(get_direct_links=True))

        if not download_info:
            pass
        direct_link = download_info[0].direct_link

        fd, temp_path = tempfile.mkstemp(suffix=".mp3", prefix=f"ym_{chat_id}_")
        os.close(fd)
        temp_file = temp_path

        await download_file_aio(direct_link, temp_path, chat_id, progress_msg_id)

        await add_tags_to_audio(temp_path, title, artists, cover_data)

        await edit_progress_message(chat_id, progress_msg_id, "Отправка трека...")

        sent_audio = await bot.send_audio(chat_id=chat_id, audio=FSInputFile(temp_path), title=title, performer=artists)

        await add_action_buttons(chat_id, sent_audio.message_id, title)
        try:
            await bot.delete_message(chat_id, progress_msg_id)
        except Exception:
            pass

    except:
        pass
    finally:
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception:
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
        "Отправьте мне название песни, и я найду этот трек!\n\n"
        "Пример: `Rammstein - Deutschland`\n\n"
        "/subscribe - оформить подписку для приоритетной загрузки",
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
        # Если подписка уже есть — разрешаем оплату и информируем пользователя
        await message.answer(f"У вас уже есть подписка.\n\nОсталось: {days_left} дней.\n\nПосле оплаты к текущей подписке добавится ещё {SUBSCRIBE_DURATION_DAYS} дней.")

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

    except Exception as e:
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


# === ОСНОВНОЙ ФУНКЦИОНАЛ ПОИСКА И ЗАГРУЗКИ ===
@dp.message()
async def search_track_handler(message: Message):
    chat_id = message.chat.id
    try:
        if chat_id in user_states and "select_msg" in user_states[chat_id]:
            try:
                await bot.delete_message(chat_id, user_states[chat_id]["select_msg"].message_id)
            except:
                pass

        search_result = await ym_client.search(message.text, type_="track")

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


@dp.callback_query(lambda c: c.data and c.data.startswith("download_"))
async def download_callback_handler(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    try:
        track_id = int(callback.data.split("_")[1])

        priority = 0 if is_subscribed(chat_id) else 1

        try:
            await bot.delete_message(chat_id, callback.message.message_id)
        except Exception:
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
    global ym_client
    init_db()
    ym_client = ClientAsync(YM_TOKEN)
    await ym_client.init()
    print("Running!")
    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        asyncio.create_task(download_worker())
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
