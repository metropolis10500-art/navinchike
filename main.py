import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from dotenv import load_dotenv
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Boolean,
    DateTime,
    Text,
    Enum as SQLEnum,
    ForeignKey,
    select,
    update,
    and_,
    or_,
    func,
    text as sql_text,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BOT_NAME = "🍷 Знакомства на Винчике"


@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///dating_bot.db")
    FREE_DAILY_LIKES: int = 30
    FREE_DAILY_MESSAGES: int = 20


config = Config()


class Gender(str, Enum):
    MALE = "male"
    FEMALE = "female"


class LookingFor(str, Enum):
    MALE = "male"
    FEMALE = "female"
    BOTH = "both"


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False, index=True)
    username = Column(String(255), nullable=True)
    name = Column(String(100), nullable=True)
    age = Column(Integer, nullable=True)
    gender = Column(SQLEnum(Gender), nullable=True)
    city = Column(String(100), nullable=True)
    bio = Column(Text, nullable=True)
    looking_for = Column(SQLEnum(LookingFor), default=LookingFor.BOTH)
    photos = Column(Text, default="")
    main_photo = Column(String(255), nullable=True)
    is_profile_complete = Column(Boolean, default=False)
    is_banned = Column(Boolean, default=False)
    daily_likes_remaining = Column(Integer, default=30)
    daily_messages_remaining = Column(Integer, default=20)
    last_limits_reset = Column(DateTime, nullable=True)
    views_count = Column(Integer, default=0)
    likes_received_count = Column(Integer, default=0)
    matches_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_active_at = Column(DateTime, default=datetime.utcnow)


class Like(Base):
    __tablename__ = "likes"

    id = Column(Integer, primary_key=True)
    from_user_id = Column(Integer, ForeignKey("users.id"))
    to_user_id = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.utcnow)


class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True)
    user1_id = Column(Integer, ForeignKey("users.id"))
    user2_id = Column(Integer, ForeignKey("users.id"))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_message_at = Column(DateTime, nullable=True)


class ChatMessage(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey("matches.id"))
    sender_id = Column(Integer, ForeignKey("users.id"))
    text = Column(Text)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


engine = create_async_engine(config.DATABASE_URL, echo=False)
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB initialized")


async def migrate_sqlite():
    if "sqlite" not in config.DATABASE_URL:
        return

    async with engine.begin() as conn:
        result = await conn.execute(sql_text("PRAGMA table_info(users)"))
        cols = {row[1] for row in result.fetchall()}

        additions = [
            ("daily_messages_remaining", "ALTER TABLE users ADD COLUMN daily_messages_remaining INTEGER DEFAULT 20"),
            ("last_limits_reset", "ALTER TABLE users ADD COLUMN last_limits_reset DATETIME"),
            ("views_count", "ALTER TABLE users ADD COLUMN views_count INTEGER DEFAULT 0"),
            ("likes_received_count", "ALTER TABLE users ADD COLUMN likes_received_count INTEGER DEFAULT 0"),
            ("matches_count", "ALTER TABLE users ADD COLUMN matches_count INTEGER DEFAULT 0"),
            ("photos", "ALTER TABLE users ADD COLUMN photos TEXT DEFAULT ''"),
            ("main_photo", "ALTER TABLE users ADD COLUMN main_photo VARCHAR(255)"),
            ("is_banned", "ALTER TABLE users ADD COLUMN is_banned BOOLEAN DEFAULT 0"),
        ]

        for name, sql in additions:
            if name not in cols:
                await conn.execute(sql_text(sql))

    logger.info("SQLite migration checked")


class RegStates(StatesGroup):
    name = State()
    age = State()
    gender = State()
    city = State()
    photo = State()
    bio = State()
    looking_for = State()


class ChatStates(StatesGroup):
    chatting = State()


class DBService:
    @staticmethod
    def to_dict(u: User) -> Dict:
        return {
            "id": u.id,
            "telegram_id": u.telegram_id,
            "username": u.username,
            "name": u.name,
            "age": u.age,
            "gender": u.gender.value if u.gender else None,
            "city": u.city,
            "bio": u.bio,
            "looking_for": u.looking_for.value if u.looking_for else "both",
            "photos": u.photos or "",
            "main_photo": u.main_photo,
            "is_profile_complete": u.is_profile_complete,
            "is_banned": u.is_banned,
            "daily_likes_remaining": u.daily_likes_remaining or config.FREE_DAILY_LIKES,
            "daily_messages_remaining": u.daily_messages_remaining or config.FREE_DAILY_MESSAGES,
            "last_limits_reset": u.last_limits_reset,
            "views_count": u.views_count or 0,
            "likes_received_count": u.likes_received_count or 0,
            "matches_count": u.matches_count or 0,
        }

    @staticmethod
    async def get_user(tg_id: int) -> Optional[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(User).where(User.telegram_id == tg_id))
            u = r.scalar_one_or_none()
            return DBService.to_dict(u) if u else None

    @staticmethod
    async def get_user_by_id(uid: int) -> Optional[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(User).where(User.id == uid))
            u = r.scalar_one_or_none()
            return DBService.to_dict(u) if u else None

    @staticmethod
    async def create_user(tg_id: int, username: Optional[str]) -> Dict:
        async with async_session_maker() as s:
            u = User(
                telegram_id=tg_id,
                username=username,
                last_limits_reset=datetime.utcnow()
            )
            s.add(u)
            await s.commit()
            await s.refresh(u)
            return DBService.to_dict(u)

    @staticmethod
    async def update_user(tg_id: int, **kwargs) -> Optional[Dict]:
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(**kwargs))
            await s.commit()
            r = await s.execute(select(User).where(User.telegram_id == tg_id))
            u = r.scalar_one_or_none()
            return DBService.to_dict(u) if u else None

    @staticmethod
    async def reset_limits_if_needed(user: Dict) -> Dict:
        now = datetime.utcnow()
        last_reset = user.get("last_limits_reset")
        if last_reset is None or last_reset.date() < now.date():
            return await DBService.update_user(
                user["telegram_id"],
                daily_likes_remaining=config.FREE_DAILY_LIKES,
                daily_messages_remaining=config.FREE_DAILY_MESSAGES,
                last_limits_reset=now,
                last_active_at=now
            )
        await DBService.update_user(user["telegram_id"], last_active_at=now)
        return user

    @staticmethod
    async def search_profiles(user: Dict, limit: int = 1) -> List[Dict]:
        async with async_session_maker() as s:
            liked = await s.execute(select(Like.to_user_id).where(Like.from_user_id == user["id"]))
            excluded = [r[0] for r in liked.fetchall()] + [user["id"]]

            q = select(User).where(and_(
                User.is_profile_complete == True,
                User.is_banned == False,
                User.id.not_in(excluded)
            ))

            looking_for = user.get("looking_for", "both")
            if looking_for == "male":
                q = q.where(User.gender == Gender.MALE)
            elif looking_for == "female":
                q = q.where(User.gender == Gender.FEMALE)

            r = await s.execute(q.limit(limit))
            return [DBService.to_dict(x) for x in r.scalars().all()]

    @staticmethod
    async def add_like(from_id: int, to_id: int) -> bool:
        async with async_session_maker() as s:
            existing = await s.execute(select(Like).where(and_(
                Like.from_user_id == from_id,
                Like.to_user_id == to_id
            )))
            if existing.scalar_one_or_none():
                return False

            s.add(Like(from_user_id=from_id, to_user_id=to_id))

            reverse = await s.execute(select(Like).where(and_(
                Like.from_user_id == to_id,
                Like.to_user_id == from_id
            )))
            is_match = reverse.scalar_one_or_none() is not None

            if is_match:
                s.add(Match(user1_id=from_id, user2_id=to_id))
                await s.execute(update(User).where(User.id.in_([from_id, to_id])).values(
                    matches_count=User.matches_count + 1
                ))

            await s.commit()
            return is_match

    @staticmethod
    async def get_matches(uid: int) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(
                or_(Match.user1_id == uid, Match.user2_id == uid),
                Match.is_active == True
            )))
            out = []
            for m in r.scalars().all():
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                p = await DBService.get_user_by_id(pid)
                if p:
                    out.append({
                        "match_id": m.id,
                        "user_id": p["id"],
                        "name": p["name"],
                        "age": p["age"],
                    })
            return out

    @staticmethod
    async def get_match_between(u1: int, u2: int) -> Optional[int]:
        async with async_session_maker() as s:
            r = await s.execute(select(Match.id).where(or_(
                and_(Match.user1_id == u1, Match.user2_id == u2),
                and_(Match.user1_id == u2, Match.user2_id == u1),
            )))
            row = r.first()
            return row[0] if row else None

    @staticmethod
    async def send_msg(mid: int, sid: int, text_: str):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=text_))
            await s.execute(update(Match).where(Match.id == mid).values(last_message_at=datetime.utcnow()))
            await s.commit()

    @staticmethod
    async def get_msgs(mid: int, limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(ChatMessage)
                .where(ChatMessage.match_id == mid)
                .order_by(ChatMessage.created_at.desc())
                .limit(limit)
            )
            return [{"sender_id": x.sender_id, "text": x.text} for x in reversed(r.scalars().all())]

    @staticmethod
    async def get_unread(uid: int) -> int:
        async with async_session_maker() as s:
            ms = await s.execute(select(Match.id).where(or_(
                Match.user1_id == uid,
                Match.user2_id == uid
            )))
            mids = [m[0] for m in ms.fetchall()]
            if not mids:
                return 0
            r = await s.execute(select(func.count(ChatMessage.id)).where(and_(
                ChatMessage.match_id.in_(mids),
                ChatMessage.sender_id != uid,
                ChatMessage.is_read == False
            )))
            return r.scalar() or 0


class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        tg = event.from_user if isinstance(event, (Message, CallbackQuery)) else None
        user = None
        if tg:
            user = await DBService.get_user(tg.id)
            if user:
                user = await DBService.reset_limits_if_needed(user)
                if user.get("is_banned"):
                    if isinstance(event, Message):
                        await event.answer("🚫 Аккаунт заблокирован.")
                    return
        data["user"] = user
        return await handler(event, data)


class KB:
    @staticmethod
    def main():
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🍷 Анкеты"), KeyboardButton(text="💕 Симпатии")],
                [KeyboardButton(text="💬 Чаты"), KeyboardButton(text="👤 Профиль")],
                [KeyboardButton(text="❓ FAQ")],
            ],
            resize_keyboard=True
        )

    @staticmethod
    def gender():
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="👨 Мужской", callback_data="g:male"),
            InlineKeyboardButton(text="👩 Женский", callback_data="g:female"),
        ]])

    @staticmethod
    def looking():
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="👨 Мужчин", callback_data="l:male"),
                InlineKeyboardButton(text="👩 Женщин", callback_data="l:female"),
            ],
            [InlineKeyboardButton(text="💞 Всех", callback_data="l:both")]
        ])

    @staticmethod
    def skip():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip")]
        ])

    @staticmethod
    def search(uid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💖 Лайк", callback_data=f"lk:{uid}"),
                InlineKeyboardButton(text="👎 Пропустить", callback_data=f"dl:{uid}"),
            ]
        ])

    @staticmethod
    def matches(items: List[Dict]):
        rows = [[InlineKeyboardButton(text=f"💕 {x['name']}, {x['age']}", callback_data=f"ch:{x['user_id']}")] for x in items]
        rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=rows)


rt = Router()


async def show_card(message: Message, p: Dict):
    txt = (
        f"*{p['name']}*, {p['age']}\n"
        f"📍 {p['city']}\n\n"
        f"{p['bio'] or '_Без описания_'}"
    )
    if p.get("main_photo"):
        await message.answer_photo(
            photo=p["main_photo"],
            caption=txt,
            reply_markup=KB.search(p["id"]),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await message.answer(
            txt,
            reply_markup=KB.search(p["id"]),
            parse_mode=ParseMode.MARKDOWN
        )


@rt.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()

    if user and user.get("is_profile_complete"):
        unread = await DBService.get_unread(user["id"])
        await message.answer(
            f"🍷 *С возвращением, {user['name']}!*\n\n"
            f"💬 Новых сообщений: *{unread}*",
            reply_markup=KB.main(),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if not user:
        await DBService.create_user(message.from_user.id, message.from_user.username)

    await message.answer(
        f"🍷 *Добро пожаловать в {BOT_NAME}!*\n\n"
        f"Давай создадим твою анкету.",
        parse_mode=ParseMode.MARKDOWN
    )
    await message.answer("Как тебя зовут?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(RegStates.name)


@rt.message(RegStates.name)
async def reg_name(message: Message, state: FSMContext):
    name = normalize_text(message.text)
    if len(name) < 2 or len(name) > 50:
        await message.answer("Имя должно быть от 2 до 50 символов.")
        return

    await state.update_data(name=name)
    await message.answer("Сколько тебе лет?")
    await state.set_state(RegStates.age)


@rt.message(RegStates.age)
async def reg_age(message: Message, state: FSMContext):
    try:
        age = int(normalize_text(message.text))
        if not 18 <= age <= 99:
            raise ValueError
    except Exception:
        await message.answer("Возраст должен быть от 18 до 99.")
        return

    await state.update_data(age=age)
    await message.answer("Твой пол:", reply_markup=KB.gender())
    await state.set_state(RegStates.gender)


@rt.callback_query(RegStates.gender, F.data.startswith("g:"))
async def reg_gender(callback: CallbackQuery, state: FSMContext):
    await state.update_data(gender=callback.data[2:])
    await callback.message.edit_text("Из какого ты города?")
    await state.set_state(RegStates.city)
    await callback.answer()


@rt.message(RegStates.city)
async def reg_city(message: Message, state: FSMContext):
    city = normalize_text(message.text).title()
    if len(city) < 2:
        await message.answer("Укажи город.")
        return

    await state.update_data(city=city)
    await message.answer("Отправь фото или нажми «Пропустить»:", reply_markup=KB.skip())
    await state.set_state(RegStates.photo)


@rt.message(RegStates.photo, F.photo)
async def reg_photo(message: Message, state: FSMContext):
    await state.update_data(photo=message.photo[-1].file_id)
    await message.answer("Расскажи немного о себе.")
    await state.set_state(RegStates.bio)


@rt.callback_query(RegStates.photo, F.data == "skip")
async def reg_skip_photo(callback: CallbackQuery, state: FSMContext):
    await state.update_data(photo=None)
    await callback.message.edit_text("Расскажи немного о себе.")
    await state.set_state(RegStates.bio)
    await callback.answer()


@rt.message(RegStates.bio)
async def reg_bio(message: Message, state: FSMContext):
    await state.update_data(bio=normalize_text(message.text)[:500])
    await message.answer("Кого ты ищешь?", reply_markup=KB.looking())
    await state.set_state(RegStates.looking_for)


@rt.callback_query(RegStates.looking_for, F.data.startswith("l:"))
async def reg_looking(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()

    values = {
        "name": data["name"],
        "age": data["age"],
        "gender": Gender(data["gender"]),
        "city": data["city"],
        "bio": data["bio"],
        "looking_for": LookingFor(callback.data[2:]),
        "is_profile_complete": True
    }

    if data.get("photo"):
        values["photos"] = data["photo"]
        values["main_photo"] = data["photo"]

    await DBService.update_user(callback.from_user.id, **values)
    await state.clear()

    await callback.message.edit_text(
        "🥂 *Анкета готова!*\n\nТеперь можно знакомиться.",
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.message.answer("Главное меню:", reply_markup=KB.main())
    await callback.answer()


@rt.message(F.text == "🍷 Анкеты")
async def menu_profiles(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer("Сначала заполни профиль: /start")
        return

    profiles = await DBService.search_profiles(user, 1)
    if not profiles:
        await message.answer("Анкет пока нет.")
        return

    await show_card(message, profiles[0])


@rt.callback_query(F.data.startswith("lk:"))
async def handle_like(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    tid = int(callback.data[3:])
    is_match = await DBService.add_like(user["id"], tid)

    if is_match:
        target = await DBService.get_user_by_id(tid)
        try:
            await callback.message.edit_caption(
                f"💘 *У вас мэтч с {target['name'] if target else 'кем-то'}!*",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            await callback.message.edit_text(
                f"💘 *У вас мэтч с {target['name'] if target else 'кем-то'}!*",
                parse_mode=ParseMode.MARKDOWN
            )
    else:
        await callback.answer("Лайк отправлен 💖")

    fresh = await DBService.get_user(callback.from_user.id)
    profiles = await DBService.search_profiles(fresh, 1)
    if profiles:
        await show_card(callback.message, profiles[0])


@rt.callback_query(F.data.startswith("dl:"))
async def handle_dislike(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    profiles = await DBService.search_profiles(user, 1)
    if profiles:
        await show_card(callback.message, profiles[0])
    else:
        await callback.message.answer("Анкет пока нет.")
    await callback.answer()


@rt.message(F.text == "💕 Симпатии")
async def menu_likes(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer("Сначала заполни профиль: /start")
        return

    matches = await DBService.get_matches(user["id"])
    if matches:
        await message.answer("💕 *Твои мэтчи:*", reply_markup=KB.matches(matches), parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer("Пока нет симпатий 💕")


@rt.callback_query(F.data.startswith("ch:"))
async def open_chat(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    pid = int(callback.data[3:])
    partner = await DBService.get_user_by_id(pid)
    if not partner:
        await callback.answer("Пользователь не найден")
        return

    mid = await DBService.get_match_between(user["id"], pid)
    if not mid:
        await callback.answer("Нет мэтча")
        return

    messages = await DBService.get_msgs(mid, 10)

    text = f"💬 *Чат с {partner['name']}*\n\n"
    if messages:
        for msg in messages:
            sender = "Ты" if msg["sender_id"] == user["id"] else partner["name"]
            text += f"*{sender}:* {msg['text']}\n"
    else:
        text += "_Напиши первым!_"

    await state.update_data(cp=pid, mi=mid)
    await state.set_state(ChatStates.chatting)
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.message(ChatStates.chatting)
async def send_chat_msg(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    data = await state.get_data()
    mid = data.get("mi")
    pid = data.get("cp")

    if not mid:
        await state.clear()
        await message.answer("Чат закрыт", reply_markup=KB.main())
        return

    text_ = normalize_text(message.text)
    if not text_:
        return

    await DBService.send_msg(mid, user["id"], text_)

    partner = await DBService.get_user_by_id(pid)
    if partner:
        try:
            await message.bot.send_message(
                partner["telegram_id"],
                f"💬 *{user['name']}:* {text_}",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

    await message.answer("Отправлено ✨")


@rt.message(F.text == "💬 Чаты")
async def menu_chats(message: Message, user: Optional[Dict]):
    await menu_likes(message, user)


@rt.message(F.text == "👤 Профиль")
async def menu_profile(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer("Сначала заполни профиль: /start")
        return

    text = (
        f"👤 *Мой профиль*\n\n"
        f"*{user['name']}*, {user['age']}\n"
        f"📍 {user['city']}\n\n"
        f"{user['bio'] or '_Не указано_'}"
    )

    if user.get("main_photo"):
        await message.answer_photo(
            photo=user["main_photo"],
            caption=text,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@rt.message(F.text == "❓ FAQ")
async def menu_faq(message: Message):
    await message.answer(
        "❓ *FAQ*\n\n"
        "1. Заполни анкету\n"
        "2. Листай анкеты\n"
        "3. Ставь лайки\n"
        "4. Общайся после мэтча",
        parse_mode=ParseMode.MARKDOWN
    )


@rt.callback_query(F.data == "mn")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Главное меню", reply_markup=KB.main())
    await callback.answer()


async def main():
    if not config.BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not found in .env")

    await init_db()
    await migrate_sqlite()

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())
    dp.include_router(rt)

    logger.info("%s starting...", BOT_NAME)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
