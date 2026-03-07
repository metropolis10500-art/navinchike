import asyncio
import logging
import sqlite3
import os
import time
import random
from datetime import datetime
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton, 
    LabeledPrice, PreCheckoutQuery, ContentType, InputMediaPhoto
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

# --- 1. НАСТРОЙКИ ---
load_dotenv()

API_TOKEN = os.getenv("BOT_TOKEN")
PAYMENT_TOKEN = os.getenv("PAYMENT_TOKEN")
ADMIN_ID_RAW = os.getenv("ADMIN_ID")

if not API_TOKEN:
    print("❌ ОШИБКА: Нет BOT_TOKEN в файле .env")
    exit()

try:
    ADMIN_ID = int(ADMIN_ID_RAW)
except:
    ADMIN_ID = 0

DB_FILE = 'vinchik_pro.db'
DAILY_LIMIT = 15    
BOOST_COST = 50     
BOOST_TIME = 1800   

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# --- 2. ТОВАРЫ ---
PRODUCTS = {
    "week": {"type": "vip", "title": "🥂 VIP Неделя", "price": 199, "days": 7, "payload": "pay_week"},
    "month": {"type": "vip", "title": "🍷 VIP Месяц", "price": 590, "days": 30, "payload": "pay_month"},
    "forever": {"type": "vip", "title": "🏰 VIP Навсегда", "price": 4990, "days": 36500, "payload": "pay_forever"},
    "coins_50": {"type": "coins", "title": "💰 50 Монет", "price": 149, "amount": 50, "payload": "pay_c50"},
    "coins_200": {"type": "coins", "title": "💰 200 Монет", "price": 490, "amount": 200, "payload": "pay_c200"},
    "coins_500": {"type": "coins", "title": "💰 500 Монет", "price": 990, "amount": 500, "payload": "pay_c500"},
}

# --- 3. БАЗА ДАННЫХ ---
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, name TEXT, age INTEGER, city TEXT, bio TEXT, 
            photo_id TEXT, gender TEXT, target_gender TEXT, vip_until INTEGER DEFAULT 0, 
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            coins INTEGER DEFAULT 10, boost_until INTEGER DEFAULT 0
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS votes (
            from_user_id INTEGER, to_user_id INTEGER, action TEXT, timestamp INTEGER DEFAULT 0, 
            PRIMARY KEY (from_user_id, to_user_id)
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS matches (
            user1_id INTEGER, user2_id INTEGER, timestamp INTEGER DEFAULT 0, 
            PRIMARY KEY (user1_id, user2_id)
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount INTEGER, item TEXT, date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        try: cursor.execute("ALTER TABLE users ADD COLUMN coins INTEGER DEFAULT 10")
        except: pass
        try: cursor.execute("ALTER TABLE users ADD COLUMN boost_until INTEGER DEFAULT 0")
        except: pass
        conn.commit()

def get_user(user_id):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cursor.fetchone()

def update_coins(user_id, amount):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (amount, user_id))

def activate_boost(user_id):
    until = int(time.time()) + BOOST_TIME
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("UPDATE users SET boost_until = ? WHERE user_id = ?", (until, user_id))

def is_vip(user_id):
    user = get_user(user_id)
    return user and user[9] > int(time.time())

def check_limits(user_id):
    if user_id == ADMIN_ID or is_vip(user_id): return True
    day_ago = int(time.time()) - 86400
    with sqlite3.connect(DB_FILE) as conn:
        count = conn.execute('SELECT COUNT(*) FROM votes WHERE from_user_id = ? AND timestamp > ?', (user_id, day_ago)).fetchone()[0]
    return count < DAILY_LIMIT

# --- 4. ОФОРМЛЕНИЕ ---
def format_caption(user_data):
    u_id, u_name, u_age, u_city, u_bio = user_data[0], user_data[2], user_data[3], user_data[4], user_data[5]
    u_vip, u_boost = user_data[9], user_data[12]
    
    if u_id == ADMIN_ID:
        return (f"🌟 <b>ОФИЦИАЛЬНЫЙ ПРОФИЛЬ</b> 🌟\n\n👑 <b>{u_name}, {u_age}</b> ☑️\n"
                f"🛡 <b>Должность:</b> Владелец (CEO)\n📍 <b>Локация:</b> {u_city}\n\n📜 <b>О себе:</b>\n<i>{u_bio}</i>\n\n⚡️ <i>CEO Bot</i>")
    
    status = ""
    if u_boost > int(time.time()): status += "🚀 <b>BOOST (В ТОПЕ)</b>\n"
    if u_vip > int(time.time()): status += "💎 <b>VIP STATUS</b>\n"
    if not status: status = "🌑 Статус: <b>Гость</b>"
    
    return (f"{status}\n<b>{u_name}, {u_age}</b>\n📍 {u_city}\n\n{u_bio}")

# --- 5. FSM ---
class RegState(StatesGroup):
    name = State(); age = State(); city = State(); gender = State(); target = State(); bio = State(); photo = State()

# --- 6. КЛАВИАТУРЫ ---
def kb_main(user_id):
    kb = ReplyKeyboardBuilder()
    kb.button(text="🚀 Смотреть анкеты"); kb.button(text="💌 Мои пары")
    kb.button(text="👀 Гости"); kb.button(text="👤 Мой профиль")
    kb.button(text="💎 Магазин")
    if user_id == ADMIN_ID: kb.button(text="📊 Админ-панель")
    kb.adjust(2, 2, 1, 1); return kb.as_markup(resize_keyboard=True)

def kb_shop_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="💎 Купить VIP", callback_data="shop_vip")
    kb.button(text="💰 Купить Монеты", callback_data="shop_coins")
    kb.button(text=f"🚀 БУСТ АНКЕТЫ ({BOOST_COST}🍷)", callback_data="buy_boost")
    kb.adjust(1); return kb.as_markup()

def kb_buy_vip():
    kb = InlineKeyboardBuilder()
    kb.button(text="🥂 Неделя (199₽)", callback_data="buy_week")
    kb.button(text="🍷 Месяц (590₽)", callback_data="buy_month")
    kb.button(text="🏰 Навсегда (4990₽)", callback_data="buy_forever")
    kb.button(text="🔙 Назад", callback_data="back_shop")
    kb.adjust(1); return kb.as_markup()

def kb_buy_coins():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 50 монет (149₽)", callback_data="buy_coins_50")
    kb.button(text="💰 200 монет (490₽)", callback_data="buy_coins_200")
    kb.button(text="💰 500 монет (990₽)", callback_data="buy_coins_500")
    kb.button(text="🔙 Назад", callback_data="back_shop")
    kb.adjust(1); return kb.as_markup()

def kb_gender():
    kb = ReplyKeyboardBuilder(); kb.button(text="Я Парень 🤵"); kb.button(text="Я Девушка 💃"); kb.adjust(2); return kb.as_markup(resize_keyboard=True)
def kb_target():
    kb = ReplyKeyboardBuilder(); kb.button(text="Парней"); kb.button(text="Девушек"); kb.button(text="Всех"); kb.adjust(2); return kb.as_markup(resize_keyboard=True)

# --- 7. РЕГИСТРАЦИЯ ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user = get_user(message.from_user.id)
    if user: await message.answer(f"🍷 С возвращением!", reply_markup=kb_main(message.from_user.id))
    else: await message.answer("👋 <b>Добро пожаловать!</b>\nКак тебя зовут?", parse_mode="HTML"); await state.set_state(RegState.name)

@dp.message(RegState.name)
async def process_name(msg: types.Message, state: FSMContext): await state.update_data(name=msg.text); await msg.answer("Возраст?"); await state.set_state(RegState.age)
@dp.message(RegState.age)
async def process_age(msg: types.Message, state: FSMContext): 
    if not msg.text.isdigit(): return
    await state.update_data(age=int(msg.text)); await msg.answer("Город?"); await state.set_state(RegState.city)
@dp.message(RegState.city)
async def process_city(msg: types.Message, state: FSMContext): await state.update_data(city=msg.text); await msg.answer("Пол?", reply_markup=kb_gender()); await state.set_state(RegState.gender)
@dp.message(RegState.gender)
async def process_gen(msg: types.Message, state: FSMContext): 
    g = "M" if "Парень" in msg.text else "F"; await state.update_data(gender=g); await msg.answer("Кого искать?", reply_markup=kb_target()); await state.set_state(RegState.target)
@dp.message(RegState.target)
async def process_targ(msg: types.Message, state: FSMContext): 
    t = {"Парней":"M","Девушек":"F","Всех":"ALL"}.get(msg.text, "ALL"); await state.update_data(target=t); await msg.answer("О себе:", reply_markup=ReplyKeyboardRemove()); await state.set_state(RegState.bio)
@dp.message(RegState.bio)
async def process_bio(msg: types.Message, state: FSMContext): await state.update_data(bio=msg.text); await msg.answer("Фото 📸"); await state.set_state(RegState.photo)
@dp.message(RegState.photo, F.photo)
async def process_photo(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id; d = await state.get_data()
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("INSERT OR REPLACE INTO users (user_id, username, name, age, city, bio, photo_id, gender, target_gender, coins) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 10)", 
                     (message.from_user.id, message.from_user.username, d['name'], d['age'], d['city'], d['bio'], photo_id, d['gender'], d['target']))
    await state.clear(); await message.answer("✅ <b>Анкета создана!</b>\n🎁 +10 монет в подарок!", parse_mode="HTML", reply_markup=kb_main(message.from_user.id))

# --- 8. ПОИСК ---
@dp.message(F.text == "🚀 Смотреть анкеты")
async def search_profiles(message: types.Message):
    user = get_user(message.from_user.id)
    if not user: await message.answer("Жми /start"); return
    if not check_limits(message.from_user.id): 
        await message.answer("💔 Лимит исчерпан. Жди 24ч или купи VIP.", reply_markup=kb_shop_menu()); return
    
    target = user[8]
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        query = "SELECT * FROM users WHERE user_id != ? AND user_id NOT IN (SELECT to_user_id FROM votes WHERE from_user_id = ?)"
        params = [user[0], user[0]]
        if target != "ALL": query += " AND gender = ?"; params.append(target)
        now = int(time.time())
        query += f" ORDER BY (user_id = {ADMIN_ID}) DESC, (boost_until > {now}) DESC, (vip_until > {now}) DESC, reg_date DESC LIMIT 1"
        cursor.execute(query, tuple(params)); candidate = cursor.fetchone()
        
    if not candidate: await message.answer("😴 Анкеты закончились."); return
    caption = format_caption(candidate)
    
    kb = InlineKeyboardBuilder()
    if candidate[0] == ADMIN_ID: kb.button(text="🔥 ЛАЙК АДМИНУ", callback_data=f"like_{candidate[0]}")
    else: kb.button(text="❤️", callback_data=f"like_{candidate[0]}")
    kb.button(text="👎", callback_data=f"dislike_{candidate[0]}"); kb.button(text="💤", callback_data="stop_search"); kb.adjust(2, 1)
    
    try: await message.answer_photo(candidate[6], caption=caption, parse_mode="HTML", reply_markup=kb.as_markup())
    except: await search_profiles(message)

# --- 9. ЛАЙКИ ---
@dp.callback_query(F.data.startswith("like_"))
async def on_like(cb: types.CallbackQuery):
    if not check_limits(cb.from_user.id): await cb.answer("Лимит!", show_alert=True); return
    tid = int(cb.data.split("_")[1]); mid = cb.from_user.id
    
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("INSERT OR IGNORE INTO votes (from_user_id, to_user_id, action, timestamp) VALUES (?, ?, 'like', ?)", (mid, tid, int(time.time())))
        if conn.execute("SELECT * FROM votes WHERE from_user_id = ? AND to_user_id = ? AND action = 'like'", (tid, mid)).fetchone():
            conn.execute("INSERT OR IGNORE INTO matches (user1_id, user2_id, timestamp) VALUES (?, ?, ?)", (min(mid, tid), max(mid, tid), int(time.time())))
            u1=get_user(tid); u2=get_user(mid)
            tips = ["Спроси про любимое вино 🍷", "Отправь смешной стикер", "Спроси, как прошел день?"]
            tip = random.choice(tips)
            kb1 = InlineKeyboardBuilder(); kb1.button(text="💬 Написать", url=f"tg://user?id={u1[0]}"); kb1.adjust(1)
            kb2 = InlineKeyboardBuilder(); kb2.button(text="💬 Написать", url=f"tg://user?id={u2[0]}"); kb2.adjust(1)
            await bot.send_message(mid, f"🔥 <b>МЭТЧ!</b>\n👉 {tip}", parse_mode="HTML", reply_markup=kb1.as_markup())
            try: await bot.send_message(tid, f"🔥 <b>МЭТЧ!</b>\n👉 {tip}", parse_mode="HTML", reply_markup=kb2.as_markup())
            except: pass
        elif is_vip(tid) or tid == ADMIN_ID:
            try: await bot.send_message(tid, "👀 <b>Вас лайкнули!</b>", parse_mode="HTML")
            except: pass
    await cb.message.delete(); await search_profiles(cb.message)

@dp.callback_query(F.data.startswith("dislike_"))
async def on_dislike(cb: types.CallbackQuery):
    if not check_limits(cb.from_user.id): await cb.answer("Лимит!", show_alert=True); return
    tid = int(cb.data.split("_")[1])
    with sqlite3.connect(DB_FILE) as conn: conn.execute("INSERT OR IGNORE INTO votes (from_user_id, to_user_id, action, timestamp) VALUES (?, ?, 'dislike', ?)", (cb.from_user.id, tid, int(time.time())))
    await cb.message.delete(); await search_profiles(cb.message)

@dp.callback_query(F.data == "stop_search")
async def stop_search_cb(cb: types.CallbackQuery): await cb.message.delete(); await cb.message.answer("Стоп.", reply_markup=kb_main(cb.from_user.id))

# --- 10. ГАЛЕРЕЯ МЭТЧЕЙ ---
async def show_match_card(message: types.Message, match_index: int):
    uid = message.chat.id
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute('SELECT CASE WHEN user1_id = ? THEN user2_id ELSE user1_id END FROM matches WHERE user1_id = ? OR user2_id = ? ORDER BY timestamp DESC', (uid, uid, uid)).fetchall()
    
    if not rows:
        if isinstance(message, types.CallbackQuery): await message.answer("Пар нет.", show_alert=True)
        else: await message.answer("💔 <b>У вас нет пар.</b>", parse_mode="HTML")
        return

    if match_index >= len(rows): match_index = 0
    if match_index < 0: match_index = len(rows) - 1
    
    partner_id = rows[match_index][0]; partner = get_user(partner_id)
    if not partner:
        with sqlite3.connect(DB_FILE) as conn: conn.execute("DELETE FROM matches WHERE (user1_id=? AND user2_id=?) OR (user1_id=? AND user2_id=?)", (uid, partner_id, partner_id, uid))
        await show_match_card(message, 0); return

    caption = f"💘 <b>Мэтч #{match_index+1}</b>\n\n" + format_caption(partner)
    kb = InlineKeyboardBuilder()
    link = f"https://t.me/{partner[1]}" if partner[1] else f"tg://user?id={partner[0]}"
    kb.button(text="💬 НАПИСАТЬ", url=link)
    kb.button(text="⬅️", callback_data=f"match_prev_{match_index}"); kb.button(text="🗑", callback_data=f"match_del_{partner_id}"); kb.button(text="➡️", callback_data=f"match_next_{match_index}")
    kb.adjust(1, 3)

    try:
        if isinstance(message, types.CallbackQuery):
            media = InputMediaPhoto(media=partner[6], caption=caption, parse_mode="HTML")
            await message.message.edit_media(media, reply_markup=kb.as_markup())
        else:
            await bot.send_photo(uid, partner[6], caption=caption, parse_mode="HTML", reply_markup=kb.as_markup())
    except:
        await bot.send_photo(uid, partner[6], caption=caption, parse_mode="HTML", reply_markup=kb.as_markup())

@dp.message(F.text == "💌 Мои пары")
async def my_matches(message: types.Message): await show_match_card(message, 0)

@dp.callback_query(F.data.startswith("match_next_"))
async def match_next(cb: types.CallbackQuery): await show_match_card(cb, int(cb.data.split("_")[2]) + 1); await cb.answer()
@dp.callback_query(F.data.startswith("match_prev_"))
async def match_prev(cb: types.CallbackQuery): await show_match_card(cb, int(cb.data.split("_")[2]) - 1); await cb.answer()
@dp.callback_query(F.data.startswith("match_del_"))
async def match_del(cb: types.CallbackQuery):
    pid = int(cb.data.split("_")[2]); uid = cb.from_user.id
    with sqlite3.connect(DB_FILE) as conn: conn.execute("DELETE FROM matches WHERE (user1_id=? AND user2_id=?) OR (user1_id=? AND user2_id=?)", (uid, pid, pid, uid))
    await cb.answer("💔 Пара удалена", show_alert=True); await show_match_card(cb, 0)

# --- 11. МАГАЗИН И ОПЛАТА (КРАСИВЫЙ) ---
@dp.message(F.text == "💎 Магазин")
async def open_shop(message: types.Message):
    user = get_user(message.from_user.id)
    balance = user[11]
    vip_status = "✅ <b>АКТИВЕН</b>" if is_vip(user[0]) else "❌ <b>НЕ АКТИВЕН</b>"
    
    text = (
        f"🛍 <b>МАГАЗИН ПРИВИЛЕГИЙ</b>\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"💳 Ваш баланс: <b>{balance} 🍷</b>\n"
        f"💎 VIP Статус: {vip_status}\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"🚀 <b>TURBO БУСТ</b> (Цена: {BOOST_COST} 🍷)\n"
        f"🔥 <i>Твоя анкета взлетит на <b>1-е место</b> в поиске!\n"
        f"⚡️ Тебя увидят в <b>10 раз больше</b> людей за 30 минут.</i>\n\n"
        f"👑 <b>VIP ПОДПИСКА</b>\n"
        f"• 👀 <b>Инсайт:</b> Смотри, кто тебя лайкнул.\n"
        f"• ♾ <b>Свобода:</b> Никаких лимитов на лайки.\n"
        f"• ✨ <b>Статус:</b> Золотая корона в профиле.\n\n"
        f"👇 <b>ВЫБЕРИТЕ УСЛУГУ:</b>"
    )
    
    await message.answer_photo(
        photo="https://cdn-icons-png.flaticon.com/512/2424/2424464.png", 
        caption=text, parse_mode="HTML", reply_markup=kb_shop_menu()
    )

@dp.callback_query(F.data == "shop_vip")
async def sv(cb): await cb.message.edit_reply_markup(reply_markup=kb_buy_vip())
@dp.callback_query(F.data == "shop_coins")
async def sc(cb): await cb.message.edit_reply_markup(reply_markup=kb_buy_coins())
@dp.callback_query(F.data == "back_shop")
async def bs(cb): await cb.message.edit_reply_markup(reply_markup=kb_shop_menu())

@dp.callback_query(F.data == "buy_boost")
async def buy_boost(cb: types.CallbackQuery):
    user = get_user(cb.from_user.id)
    if user[11] < BOOST_COST: await cb.answer("Мало монет!", show_alert=True); return
    update_coins(cb.from_user.id, -BOOST_COST); activate_boost(cb.from_user.id)
    await cb.message.edit_text("🚀 <b>БУСТ АКТИВИРОВАН!</b>\n\nВаша анкета в самом верху поиска на 30 минут.\nЖдите поток лайков!", parse_mode="HTML")

@dp.callback_query(F.data.startswith("buy_"))
async def send_invoice(cb: types.CallbackQuery):
    key = cb.data.split("_", 1)[1]; item = PRODUCTS.get(key)
    if not item: return
    img = "https://cdn-icons-png.flaticon.com/512/2583/2583319.png" if item['type'] == 'vip' else "https://cdn-icons-png.flaticon.com/512/9696/9696752.png"
    await bot.send_invoice(cb.message.chat.id, title=item['title'], description=f"Цена: {item['price']}₽", payload=item['payload'], provider_token=PAYMENT_TOKEN, currency="RUB", prices=[LabeledPrice(label=item['title'], amount=item['price']*100)], start_parameter="buy", photo_url=img, photo_height=512, photo_width=512, photo_size=512); await cb.answer()

@dp.pre_checkout_query(lambda q: True)
async def checkout(q: PreCheckoutQuery): await bot.answer_pre_checkout_query(q.id, ok=True)

@dp.message(F.content_type == ContentType.SUCCESSFUL_PAYMENT)
async def got_payment(msg: types.Message):
    payload = msg.successful_payment.invoice_payload; amt = msg.successful_payment.total_amount // 100
    item = next((v for v in PRODUCTS.values() if v['payload'] == payload), None)
    if item:
        with sqlite3.connect(DB_FILE) as conn:
            if item['type'] == 'vip':
                until = int(time.time()) + (item['days'] * 86400)
                conn.execute("UPDATE users SET vip_until = ? WHERE user_id = ?", (until, msg.from_user.id))
                text = "🎉 <b>VIP Активирован!</b>"
            elif item['type'] == 'coins':
                update_coins(msg.from_user.id, item['amount'])
                text = f"💰 <b>Начислено {item['amount']} монет!</b>"
            conn.execute("INSERT INTO payments (user_id, amount, item) VALUES (?, ?, ?)", (msg.from_user.id, amt, item['title']))
        await msg.answer(text, parse_mode="HTML"); await bot.send_message(ADMIN_ID, f"💰 <b>+{amt}₽</b>")

# --- СТАНДАРТНОЕ ---
@dp.message(F.text == "👀 Гости")
async def my_guests(message: types.Message):
    if not is_vip(message.from_user.id) and message.from_user.id != ADMIN_ID:
        await message.answer("🔒 <b>Нужен VIP, чтобы видеть гостей.</b>", parse_mode="HTML", reply_markup=kb_shop_menu()); return
    with sqlite3.connect(DB_FILE) as conn:
        guests = conn.execute("SELECT u.* FROM users u JOIN votes v ON v.from_user_id = u.user_id WHERE v.to_user_id = ? AND v.action = 'like' AND u.user_id NOT IN (SELECT to_user_id FROM votes WHERE from_user_id = ?)", (message.from_user.id, message.from_user.id)).fetchall()
    if not guests: await message.answer("Гостей нет."); return
    g = guests[0]; caption = format_caption(g); kb = InlineKeyboardBuilder()
    kb.button(text="❤️ Лайк", callback_data=f"like_{g[0]}"); kb.button(text="👎 Скрыть", callback_data=f"dislike_{g[0]}")
    await message.answer_photo(g[6], caption=f"👀 <b>Вас лайкнул:</b>\n{caption}", parse_mode="HTML", reply_markup=kb.as_markup())

@dp.message(F.text == "👤 Мой профиль")
async def mp(msg: types.Message): u=get_user(msg.from_user.id); await msg.answer_photo(u[6], caption=format_caption(u), parse_mode="HTML")
@dp.message(F.text == "📊 Админ-панель")
async def admin_stats(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    with sqlite3.connect(DB_FILE) as conn:
        u = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        m = conn.execute("SELECT SUM(amount) FROM payments").fetchone()[0] or 0
    await msg.answer(f"👑 <b>ВЛАДЕЛЕЦ</b>\n👥 Люди: {u}\n💰 Касса: {m} RUB", parse_mode="HTML")

async def main():
    init_db(); print("✅ БОТ ВИНЧИК ЗАПУЩЕН!")
    await bot.delete_webhook(drop_pending_updates=True); await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())