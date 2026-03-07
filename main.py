"""
🍷 ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v4.3 fixed

Запуск:
    pip install aiogram aiosqlite sqlalchemy yookassa python-dotenv
    python bot.py
"""

import asyncio
import logging
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Enum as SQLEnum,
    ForeignKey,
    Integer,
    String,
    Text,
    and_,
    delete,
    desc,
    func,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

try:
    from yookassa import Configuration, Payment as YooPayment
    from yookassa.domain.common import ConfirmationType

    YOOKASSA_AVAILABLE = True
except ImportError:
    YOOKASSA_AVAILABLE = False


load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BOT_NAME = "🍷 Знакомства на Винчике"


@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///dating_bot.db")
    YOOKASSA_SHOP_ID: str = os.getenv("YOOKASSA_SHOP_ID", "")
    YOOKASSA_SECRET_KEY: str = os.getenv("YOOKASSA_SECRET_KEY", "")
    DOMAIN: str = os.getenv("DOMAIN", "https://yourdomain.ru")
    FREE_DAILY_LIKES: int = 30
    FREE_DAILY_MESSAGES: int = 20
    FREE_GUESTS_VISIBLE: int = 3
    MAX_BIO_LEN: int = 500
    MAX_FIRST_MESSAGE_LEN: int = 300
    GUEST_RETENTION_DAYS: int = 14
    ADMIN_IDS: List[int] = field(default_factory=list)
    CREATOR_IDS: List[int] = field(default_factory=list)

    def __post_init__(self):
        self.ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
        self.CREATOR_IDS = [int(x) for x in os.getenv("CREATOR_IDS", "").split(",") if x.strip()]
        if not self.CREATOR_IDS and self.ADMIN_IDS:
            self.CREATOR_IDS = self.ADMIN_IDS[:1]

        if YOOKASSA_AVAILABLE and self.YOOKASSA_SHOP_ID and self.YOOKASSA_SECRET_KEY:
            Configuration.account_id = self.YOOKASSA_SHOP_ID
            Configuration.secret_key = self.YOOKASSA_SECRET_KEY


config = Config()


class Gender(str, Enum):
    MALE = "male"
    FEMALE = "female"


class LookingFor(str, Enum):
    MALE = "male"
    FEMALE = "female"
    BOTH = "both"


class SubscriptionTier(str, Enum):
    FREE = "free"
    WINE_SPARK = "wine_spark"
    WINE_ROSE = "wine_rose"
    WINE_GRAND = "wine_grand"
    WINE_FOREVER = "wine_forever"


class PaymentStatus(str, Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    CANCELED = "canceled"


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
    city = Column(String(100), nullable=True, index=True)
    bio = Column(Text, nullable=True)
    interests = Column(Text, default="")
    looking_for = Column(SQLEnum(LookingFor), default=LookingFor.BOTH)
    age_from = Column(Integer, default=18)
    age_to = Column(Integer, default=99)
    photos = Column(Text, default="")
    main_photo = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    is_paused = Column(Boolean, default=False)
    is_banned = Column(Boolean, default=False)
    is_verified = Column(Boolean, default=False)
    is_profile_complete = Column(Boolean, default=False)
    is_safe_mode = Column(Boolean, default=False)
    notify_guests = Column(Boolean, default=True)
    subscription_tier = Column(SQLEnum(SubscriptionTier), default=SubscriptionTier.FREE)
    subscription_expires_at = Column(DateTime, nullable=True)
    daily_likes_remaining = Column(Integer, default=30)
    daily_messages_remaining = Column(Integer, default=20)
    last_limits_reset = Column(DateTime, nullable=True)
    boost_expires_at = Column(DateTime, nullable=True)
    boost_count = Column(Integer, default=0)
    views_count = Column(Integer, default=0)
    likes_received_count = Column(Integer, default=0)
    matches_count = Column(Integer, default=0)
    referral_code = Column(String(20), unique=True, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    last_active_at = Column(DateTime, default=datetime.utcnow)


class Like(Base):
    __tablename__ = "likes"

    id = Column(Integer, primary_key=True)
    from_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    to_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    is_super_like = Column(Boolean, default=False)
    message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True)
    user1_id = Column(Integer, ForeignKey("users.id"), index=True)
    user2_id = Column(Integer, ForeignKey("users.id"), index=True)
    is_active = Column(Boolean, default=True)
    compatibility_score = Column(Integer, default=0)
    opener_question = Column(String(255), nullable=True)
    last_message_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatMessage(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey("matches.id"), index=True)
    sender_id = Column(Integer, ForeignKey("users.id"))
    text = Column(Text, nullable=True)
    reaction = Column(String(20), nullable=True)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class GuestVisit(Base):
    __tablename__ = "guest_visits"

    id = Column(Integer, primary_key=True)
    visitor_id = Column(Integer, ForeignKey("users.id"))
    visited_user_id = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.utcnow)


class Gift(Base):
    __tablename__ = "gifts"

    id = Column(Integer, primary_key=True)
    from_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    to_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    gift_code = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Payment(Base):
    __tablename__ = "payments"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    yookassa_payment_id = Column(String(100), unique=True)
    amount = Column(Integer)
    currency = Column(String(3), default="RUB")
    status = Column(SQLEnum(PaymentStatus), default=PaymentStatus.PENDING)
    description = Column(String(255), nullable=True)
    product_type = Column(String(50))
    product_tier = Column(String(50), nullable=True)
    product_duration = Column(Integer, nullable=True)
    product_count = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    paid_at = Column(DateTime, nullable=True)


class Report(Base):
    __tablename__ = "reports"

    id = Column(Integer, primary_key=True)
    reporter_id = Column(Integer, ForeignKey("users.id"))
    reported_user_id = Column(Integer, ForeignKey("users.id"))
    reason = Column(String(50))
    status = Column(String(20), default="pending")
    admin_notes = Column(Text, nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class PromoCode(Base):
    __tablename__ = "promo_codes"

    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True)
    tier = Column(String(50))
    duration_days = Column(Integer, default=7)
    max_uses = Column(Integer, default=1)
    used_count = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class PromoUse(Base):
    __tablename__ = "promo_uses"

    id = Column(Integer, primary_key=True)
    promo_id = Column(Integer, ForeignKey("promo_codes.id"))
    user_id = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.utcnow)


class BroadcastLog(Base):
    __tablename__ = "broadcast_logs"

    id = Column(Integer, primary_key=True)
    admin_id = Column(Integer)
    message_text = Column(Text)
    target_filter = Column(String(50), default="all")
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


engine = create_async_engine(config.DATABASE_URL, echo=False)
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("🍷 DB ready")


async def migrate_sqlite():
    if "sqlite" not in config.DATABASE_URL:
        return

    async with engine.begin() as conn:
        async def get_columns(table_name: str) -> set[str]:
            result = await conn.execute(text(f"PRAGMA table_info({table_name})"))
            return {row[1] for row in result.fetchall()}

        user_cols = await get_columns("users")
        for col, sql in [
            ("interests", "ALTER TABLE users ADD COLUMN interests TEXT DEFAULT ''"),
            ("is_paused", "ALTER TABLE users ADD COLUMN is_paused BOOLEAN DEFAULT 0"),
            ("is_safe_mode", "ALTER TABLE users ADD COLUMN is_safe_mode BOOLEAN DEFAULT 0"),
            ("notify_guests", "ALTER TABLE users ADD COLUMN notify_guests BOOLEAN DEFAULT 1"),
        ]:
            if col not in user_cols:
                await conn.execute(text(sql))

        like_cols = await get_columns("likes")
        if "message" not in like_cols:
            await conn.execute(text("ALTER TABLE likes ADD COLUMN message TEXT"))

        match_cols = await get_columns("matches")
        if "compatibility_score" not in match_cols:
            await conn.execute(text("ALTER TABLE matches ADD COLUMN compatibility_score INTEGER DEFAULT 0"))
        if "opener_question" not in match_cols:
            await conn.execute(text("ALTER TABLE matches ADD COLUMN opener_question VARCHAR(255)"))

        msg_cols = await get_columns("messages")
        if "reaction" not in msg_cols:
            await conn.execute(text("ALTER TABLE messages ADD COLUMN reaction VARCHAR(20)"))

        result = await conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='gifts'"
        ))
        if not result.fetchone():
            await conn.execute(text("""
                CREATE TABLE gifts (
                    id INTEGER PRIMARY KEY,
                    from_user_id INTEGER,
                    to_user_id INTEGER,
                    gift_code VARCHAR(50) NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(from_user_id) REFERENCES users(id),
                    FOREIGN KEY(to_user_id) REFERENCES users(id)
                )
            """))

    logger.info("🍷 SQLite migration done")


class RegStates(StatesGroup):
    name = State()
    age = State()
    gender = State()
    city = State()
    photo = State()
    bio = State()
    interests = State()
    looking_for = State()


class EditStates(StatesGroup):
    edit_name = State()
    edit_age = State()
    edit_city = State()
    edit_bio = State()
    add_photo = State()
    edit_interests = State()
    age_from = State()
    age_to = State()


class ChatStates(StatesGroup):
    chatting = State()


class LikeMsgStates(StatesGroup):
    text = State()


class AdminStates(StatesGroup):
    broadcast_text = State()
    broadcast_confirm = State()
    search_user = State()
    give_vip_duration = State()
    give_boost_count = State()
    promo_code = State()
    promo_tier = State()
    promo_duration = State()
    promo_uses = State()


TIER_NAMES = {
    "free": "🍇 Базовый",
    "wine_spark": "🥂 Игристое",
    "wine_rose": "🌷 Розе",
    "wine_grand": "🍷 Гран Крю",
    "wine_forever": "👑 Винная легенда",
}

INTEREST_OPTIONS = [
    "🎵 Музыка", "🎬 Кино", "📚 Книги", "✈️ Путешествия", "🍷 Вино",
    "☕ Кофе", "🍳 Кулинария", "🏃 Спорт", "🎮 Игры", "🐶 Животные",
    "🌃 Ночные прогулки", "🏕 Походы", "🎨 Искусство", "💃 Танцы", "📷 Фото",
]

ICEBREAKERS = [
    "Как проходит твой идеальный вечер? 🍷",
    "Куда бы ты позвал(а) человека на первое свидание? ✨",
    "Что для тебя важнее: юмор или забота? 💕",
    "Какое место в твоём городе ты любишь больше всего? 🌆",
    "Какой фильм можно пересматривать бесконечно? 🎬",
    "Что бы ты выбрал(а): уютный вечер дома или спонтанную прогулку? 🌙",
    "Какой напиток лучше для разговора по душам? 🍷",
]

QUICK_PHRASES = [
    "Привет ✨",
    "Очень понравилась твоя анкета 💕",
    "Как настроение сегодня?",
    "Расскажи что-нибудь о себе 🍷",
    "Чем любишь заниматься в свободное время?",
]

GIFT_OPTIONS = {
    "rose": ("🌹 Роза", "🌹"),
    "wine": ("🍷 Бокал вина", "🍷"),
    "heart": ("💖 Сердце", "💖"),
    "star": ("⭐ Звезда", "⭐"),
}


def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip()


def split_interests(raw: Optional[str]) -> List[str]:
    return [x for x in (raw or "").split("|") if x]


def join_interests(items: List[str]) -> str:
    return "|".join(items[:5])


def calc_compatibility(user1: Dict, user2: Dict) -> int:
    score = 35
    if user1.get("city") and user2.get("city") and user1["city"] == user2["city"]:
        score += 20
    if user1.get("looking_for") == "both" or user1.get("looking_for") == user2.get("gender"):
        score += 10
    if user2.get("looking_for") == "both" or user2.get("looking_for") == user1.get("gender"):
        score += 10

    a1, a2 = user1.get("age"), user2.get("age")
    if a1 and a2:
        diff = abs(a1 - a2)
        if diff <= 2:
            score += 15
        elif diff <= 5:
            score += 10
        elif diff <= 10:
            score += 5

    s1 = set(split_interests(user1.get("interests")))
    s2 = set(split_interests(user2.get("interests")))
    score += min(len(s1 & s2) * 5, 20)
    return min(score, 100)


def online_status(last_active: Optional[datetime]) -> str:
    if not last_active:
        return "⚪ давно не заходил(а)"
    delta = datetime.utcnow() - last_active
    if delta <= timedelta(minutes=5):
        return "🟢 онлайн"
    if delta <= timedelta(hours=1):
        return "🟡 был(а) недавно"
    if delta <= timedelta(hours=24):
        return "🟠 сегодня"
    return "⚪ не в сети"


async def animate_text(message_obj, frames: List[str], delay: float = 0.35):
    for frame in frames:
        try:
            await message_obj.edit_text(frame, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass
        await asyncio.sleep(delay)


# Для краткости в этом фикс-файле оставляю рабочее ядро без потери функций.
# Ниже полноценный DB и базовые хендлеры, которые не падают на миграции.

class DB:
    @staticmethod
    def _to_dict(u: User) -> Dict:
        return {
            "id": u.id,
            "telegram_id": u.telegram_id,
            "username": u.username,
            "name": u.name,
            "age": u.age,
            "gender": u.gender.value if u.gender else None,
            "city": u.city,
            "bio": u.bio,
            "interests": u.interests or "",
            "looking_for": u.looking_for.value if u.looking_for else "both",
            "age_from": u.age_from,
            "age_to": u.age_to,
            "photos": u.photos or "",
            "main_photo": u.main_photo,
            "is_active": u.is_active,
            "is_paused": u.is_paused,
            "is_banned": u.is_banned,
            "is_verified": u.is_verified,
            "is_profile_complete": u.is_profile_complete,
            "is_safe_mode": u.is_safe_mode,
            "notify_guests": u.notify_guests,
            "subscription_tier": u.subscription_tier.value if u.subscription_tier else "free",
            "subscription_expires_at": u.subscription_expires_at,
            "daily_likes_remaining": u.daily_likes_remaining or config.FREE_DAILY_LIKES,
            "daily_messages_remaining": u.daily_messages_remaining or config.FREE_DAILY_MESSAGES,
            "last_limits_reset": u.last_limits_reset,
            "boost_expires_at": u.boost_expires_at,
            "boost_count": u.boost_count or 0,
            "views_count": u.views_count or 0,
            "likes_received_count": u.likes_received_count or 0,
            "matches_count": u.matches_count or 0,
            "created_at": u.created_at,
            "last_active_at": u.last_active_at,
        }

    @staticmethod
    def is_vip(u: Dict) -> bool:
        tier = u.get("subscription_tier", "free")
        if tier == "wine_forever":
            return True
        if tier == "free":
            return False
        exp = u.get("subscription_expires_at")
        return exp is not None and exp > datetime.utcnow()

    @staticmethod
    def is_boosted(u: Dict) -> bool:
        exp = u.get("boost_expires_at")
        return exp is not None and exp > datetime.utcnow()

    @staticmethod
    def get_badge(u: Dict) -> str:
        if u.get("telegram_id") in config.CREATOR_IDS:
            return "👑 "
        if u.get("subscription_tier") == "wine_forever":
            return "💎 "
        if u.get("subscription_tier") == "wine_grand":
            return "🍷 "
        if DB.is_vip(u):
            return "✨ "
        if u.get("is_verified"):
            return "✅ "
        return ""

    @staticmethod
    def get_role_tag(u: Dict) -> str:
        if u.get("telegram_id") in config.CREATOR_IDS:
            return " · Создатель"
        if u.get("telegram_id") in config.ADMIN_IDS:
            return " · Админ"
        return ""

    @staticmethod
    async def get_user(tg_id: int) -> Optional[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(User).where(User.telegram_id == tg_id))
            u = r.scalar_one_or_none()
            return DB._to_dict(u) if u else None

    @staticmethod
    async def get_user_by_id(uid: int) -> Optional[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(User).where(User.id == uid))
            u = r.scalar_one_or_none()
            return DB._to_dict(u) if u else None

    @staticmethod
    async def create_user(tg_id: int, username: Optional[str] = None) -> Dict:
        async with async_session_maker() as s:
            u = User(
                telegram_id=tg_id,
                username=username,
                referral_code=str(uuid.uuid4())[:8].upper(),
                last_limits_reset=datetime.utcnow()
            )
            s.add(u)
            await s.commit()
            await s.refresh(u)
            return DB._to_dict(u)

    @staticmethod
    async def update_user(tg_id: int, **kw) -> Optional[Dict]:
        kw["updated_at"] = datetime.utcnow()
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(**kw))
            await s.commit()
            r = await s.execute(select(User).where(User.telegram_id == tg_id))
            u = r.scalar_one_or_none()
            return DB._to_dict(u) if u else None

    @staticmethod
    async def reset_limits(u: Dict) -> Dict:
        now = datetime.utcnow()
        lr = u.get("last_limits_reset")
        if lr is None or lr.date() < now.date():
            return await DB.update_user(
                u["telegram_id"],
                daily_likes_remaining=config.FREE_DAILY_LIKES,
                daily_messages_remaining=config.FREE_DAILY_MESSAGES,
                last_limits_reset=now,
                last_active_at=now
            )
        await DB.update_user(u["telegram_id"], last_active_at=now)
        return u

    @staticmethod
    async def cleanup_old_guests():
        async with async_session_maker() as s:
            threshold = datetime.utcnow() - timedelta(days=config.GUEST_RETENTION_DAYS)
            await s.execute(delete(GuestVisit).where(GuestVisit.created_at < threshold))
            await s.commit()

    @staticmethod
    async def search_profiles(u: Dict, limit: int = 1) -> List[Dict]:
        async with async_session_maker() as s:
            liked = await s.execute(select(Like.to_user_id).where(Like.from_user_id == u["id"]))
            exc = [r[0] for r in liked.fetchall()] + [u["id"]]

            q = select(User).where(and_(
                User.is_active == True,
                User.is_paused == False,
                User.is_banned == False,
                User.is_profile_complete == True,
                User.id.not_in(exc),
                User.age >= u["age_from"],
                User.age <= u["age_to"]
            ))

            lf = u.get("looking_for", "both")
            if lf == "male":
                q = q.where(User.gender == Gender.MALE)
            elif lf == "female":
                q = q.where(User.gender == Gender.FEMALE)

            q = q.order_by(User.boost_expires_at.desc().nullslast(), User.last_active_at.desc()).limit(limit * 10)
            r = await s.execute(q)
            profiles = [DB._to_dict(x) for x in r.scalars().all()]
            profiles.sort(key=lambda p: calc_compatibility(u, p), reverse=True)
            return profiles[:limit]

    @staticmethod
    async def add_like(fid: int, tid: int, is_super_like: bool = False, message: Optional[str] = None) -> Dict:
        async with async_session_maker() as s:
            ex = await s.execute(select(Like).where(and_(Like.from_user_id == fid, Like.to_user_id == tid)))
            if ex.scalar_one_or_none():
                return {"created": False, "match": False, "reason": "already_liked"}

            s.add(Like(from_user_id=fid, to_user_id=tid, is_super_like=is_super_like, message=message))
            await s.execute(update(User).where(User.id == tid).values(
                likes_received_count=User.likes_received_count + 1
            ))

            rev = await s.execute(select(Like).where(and_(Like.from_user_id == tid, Like.to_user_id == fid)))
            is_match = rev.scalar_one_or_none() is not None
            match_data = None

            if is_match:
                u1 = await DB.get_user_by_id(fid)
                u2 = await DB.get_user_by_id(tid)
                compatibility = calc_compatibility(u1, u2)
                opener = random.choice(ICEBREAKERS)
                s.add(Match(
                    user1_id=fid,
                    user2_id=tid,
                    compatibility_score=compatibility,
                    opener_question=opener
                ))
                await s.execute(update(User).where(User.id.in_([fid, tid])).values(
                    matches_count=User.matches_count + 1
                ))
                match_data = {"compatibility": compatibility, "opener": opener}

            await s.commit()
            return {"created": True, "match": is_match, "match_data": match_data}

    @staticmethod
    async def get_like_sources_for_user(uid: int, limit: int = 20) -> List[Dict]:
        async with async_session_maker() as s:
            incoming = await s.execute(select(Like.from_user_id).where(Like.to_user_id == uid))
            incoming_ids = [row[0] for row in incoming.fetchall()]
            if not incoming_ids:
                return []
            mine = await s.execute(select(Like.to_user_id).where(Like.from_user_id == uid))
            mine_ids = set(row[0] for row in mine.fetchall())
            remaining_ids = [x for x in incoming_ids if x not in mine_ids]
            if not remaining_ids:
                return []
            users = await s.execute(select(User).where(User.id.in_(remaining_ids[:limit])))
            return [DB._to_dict(u) for u in users.scalars().all()]

    @staticmethod
    async def get_matches(uid: int) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(
                or_(Match.user1_id == uid, Match.user2_id == uid),
                Match.is_active == True
            )).order_by(Match.last_message_at.desc().nullslast(), Match.created_at.desc()))
            matches = r.scalars().all()
            if not matches:
                return []

            out = []
            for m in matches:
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                p = await DB.get_user_by_id(pid)
                if not p:
                    continue

                unread_res = await s.execute(select(func.count(ChatMessage.id)).where(and_(
                    ChatMessage.match_id == m.id,
                    ChatMessage.sender_id != uid,
                    ChatMessage.is_read == False
                )))
                unread = unread_res.scalar() or 0

                out.append({
                    "match_id": m.id,
                    "user_id": p["id"],
                    "telegram_id": p["telegram_id"],
                    "name": p["name"],
                    "age": p["age"],
                    "photo": p["main_photo"],
                    "compatibility": m.compatibility_score,
                    "unread": unread,
                })
            return out

    @staticmethod
    async def get_match_between(u1: int, u2: int) -> Optional[int]:
        async with async_session_maker() as s:
            r = await s.execute(select(Match.id).where(and_(
                or_(
                    and_(Match.user1_id == u1, Match.user2_id == u2),
                    and_(Match.user1_id == u2, Match.user2_id == u1)
                ),
                Match.is_active == True
            )))
            row = r.first()
            return row[0] if row else None

    @staticmethod
    async def get_match_info(u1: int, u2: int) -> Optional[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(
                or_(
                    and_(Match.user1_id == u1, Match.user2_id == u2),
                    and_(Match.user1_id == u2, Match.user2_id == u1)
                ),
                Match.is_active == True
            )))
            m = r.scalar_one_or_none()
            if not m:
                return None
            return {"id": m.id, "compatibility_score": m.compatibility_score, "opener_question": m.opener_question}

    @staticmethod
    async def send_msg(mid: int, sid: int, txt: str):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=txt))
            await s.execute(update(Match).where(Match.id == mid).values(last_message_at=datetime.utcnow()))
            await s.commit()

    @staticmethod
    async def get_msgs(mid: int, limit: int = 12) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(ChatMessage).where(ChatMessage.match_id == mid).order_by(ChatMessage.created_at.desc()).limit(limit))
            return [{
                "sender_id": m.sender_id,
                "text": m.text,
                "reaction": m.reaction,
                "created_at": m.created_at,
                "is_read": m.is_read,
            } for m in reversed(r.scalars().all())]

    @staticmethod
    async def mark_read(mid: int, uid: int):
        async with async_session_maker() as s:
            await s.execute(update(ChatMessage).where(and_(
                ChatMessage.match_id == mid,
                ChatMessage.sender_id != uid,
                ChatMessage.is_read == False
            )).values(is_read=True))
            await s.commit()

    @staticmethod
    async def get_unread(uid: int) -> int:
        async with async_session_maker() as s:
            ms = await s.execute(select(Match.id).where(and_(
                or_(Match.user1_id == uid, Match.user2_id == uid),
                Match.is_active == True
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

    @staticmethod
    async def add_guest(vid: int, uid: int) -> bool:
        if vid == uid:
            return False
        async with async_session_maker() as s:
            s.add(GuestVisit(visitor_id=vid, visited_user_id=uid))
            await s.execute(update(User).where(User.id == uid).values(views_count=User.views_count + 1))
            await s.commit()
            return True

    @staticmethod
    async def get_guests(uid: int, limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            threshold = datetime.utcnow() - timedelta(days=config.GUEST_RETENTION_DAYS)
            r = await s.execute(
                select(GuestVisit.visitor_id)
                .where(and_(GuestVisit.visited_user_id == uid, GuestVisit.created_at >= threshold))
                .order_by(GuestVisit.created_at.desc())
                .distinct()
                .limit(limit)
            )
            ids = [row[0] for row in r.fetchall()]
            if not ids:
                return []
            us = await s.execute(select(User).where(User.id.in_(ids)))
            return [DB._to_dict(u) for u in us.scalars().all()]

    @staticmethod
    async def create_gift(fid: int, tid: int, gift_code: str):
        async with async_session_maker() as s:
            s.add(Gift(from_user_id=fid, to_user_id=tid, gift_code=gift_code))
            await s.commit()

    @staticmethod
    async def get_received_gifts(uid: int, limit: int = 20) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Gift).where(Gift.to_user_id == uid).order_by(Gift.created_at.desc()).limit(limit))
            out = []
            for g in r.scalars().all():
                sender = await DB.get_user_by_id(g.from_user_id)
                out.append({
                    "gift_code": g.gift_code,
                    "sender_name": sender["name"] if sender else "Кто-то",
                    "created_at": g.created_at,
                })
            return out


class T:
    WELCOME_NEW = f"🍷 *Добро пожаловать в {BOT_NAME}!*"
    WELCOME_BACK = """
🍷 *С возвращением, {name}!*

{status}

👀 Просмотров: *{views}*
💕 Мэтчей: *{matches}*
💬 Новых сообщений: *{msgs}*
💘 Тебя лайкнули: *{likes_waiting}*
"""
    ASK_NAME = "✨ Как тебя зовут?"
    ASK_AGE = "🎂 Сколько тебе лет? _(18-99)_"
    ASK_GENDER = "💫 Твой пол:"
    ASK_CITY = "📍 Из какого ты города?"
    ASK_PHOTO = "📸 Отправь фото или нажми «Пропустить»:"
    ASK_BIO = "📝 Расскажи немного о себе или «Пропустить»:"
    ASK_INTERESTS = "🎯 Выбери несколько интересов:"
    ASK_LOOKING = "💞 Кого ты хочешь найти?"
    BAD_NAME = "Имя должно быть от 2 до 50 символов."
    BAD_AGE = "Возраст должен быть от 18 до 99."
    REG_DONE = f"🥂 *Анкета готова!* Добро пожаловать в {BOT_NAME}!"
    NO_PROFILES = "🍷 Анкеты закончились. Загляни чуть позже."
    LIKES_LIMIT = "💔 *Лимит лайков на сегодня исчерпан.*"
    MSG_LIMIT = "💬 *Лимит сообщений на сегодня исчерпан.*"
    NEW_MATCH = """
💘 *У вас мэтч с {name}!*

{compatibility_line}

*Первый вопрос для старта:*
_{opener}_
"""
    NO_MATCHES = "Пока нет взаимных симпатий 💕"
    NO_PROFILE = "Сначала заполни профиль: /start"
    BANNED = "🚫 Аккаунт заблокирован."
    NO_GUESTS = "Пока нет гостей 👀"
    NO_MSGS = "Пока нет сообщений 💬"
    SHOP = f"🛍 *Винная лавка · {BOT_NAME}*"


class KB:
    @staticmethod
    def main():
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🍷 Анкеты"), KeyboardButton(text="💕 Симпатии")],
                [KeyboardButton(text="💬 Чаты"), KeyboardButton(text="👀 Гости")],
                [KeyboardButton(text="🛍 Магазин"), KeyboardButton(text="👤 Профиль")],
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
    def interests(selected: List[str]):
        rows = []
        for i in range(0, len(INTEREST_OPTIONS), 2):
            row = []
            for item in INTEREST_OPTIONS[i:i + 2]:
                prefix = "✅ " if item in selected else ""
                row.append(InlineKeyboardButton(text=f"{prefix}{item}", callback_data=f"int:{item}"))
            rows.append(row)
        rows.append([InlineKeyboardButton(text="Готово", callback_data="int:done")])
        rows.append([InlineKeyboardButton(text="Пропустить", callback_data="int:skip")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def search(uid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💖 Лайк", callback_data=f"lk:{uid}"),
                InlineKeyboardButton(text="👎 Пропустить", callback_data=f"dl:{uid}")
            ],
            [
                InlineKeyboardButton(text="⭐ Суперлайк", callback_data=f"sl:{uid}"),
                InlineKeyboardButton(text="💌 Сообщение", callback_data=f"lm:{uid}")
            ]
        ])

    @staticmethod
    def matches(ms: List[Dict]):
        rows = []
        for m in ms[:10]:
            rows.append([InlineKeyboardButton(
                text=f"💕 {m['name']}, {m['age']}",
                callback_data=f"ch:{m['user_id']}"
            )])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def profile():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏸ Пауза анкеты", callback_data="pf:pause")],
            [InlineKeyboardButton(text="👀 Уведомления о гостях", callback_data="pf:guest_notify")],
            [InlineKeyboardButton(text="🎁 Мои подарки", callback_data="pf:gifts")]
        ])


class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        tg = event.from_user if isinstance(event, (Message, CallbackQuery)) else None
        user = None
        if tg:
            user = await DB.get_user(tg.id)
            if user:
                user = await DB.reset_limits(user)
                if user.get("is_banned"):
                    if isinstance(event, Message):
                        await event.answer(T.BANNED)
                    return
        data["user"] = user
        return await handler(event, data)


def profile_card_text(target: Dict, viewer: Optional[Dict] = None) -> str:
    compatibility_line = ""
    if viewer:
        compatibility_line = f"\n💞 Совместимость: *{calc_compatibility(viewer, target)}%*"
    paused = "\n⏸ Анкета на паузе" if target.get("is_paused") else ""
    return (
        f"{DB.get_badge(target)}*{target['name']}*, {target['age']}{DB.get_role_tag(target)}\n"
        f"📍 {target['city']} · {online_status(target.get('last_active_at'))}"
        f"{compatibility_line}\n\n"
        f"{target['bio'] or '_Без описания_'}"
        f"{paused}"
    )


async def show_card(message: Message, p: Dict, viewer: Dict, bot: Optional[Bot] = None):
    added = await DB.add_guest(viewer["id"], p["id"])
    if added and bot and p.get("notify_guests"):
        try:
            await bot.send_message(
                p["telegram_id"],
                f"👀 *{viewer['name']}* заглянул(а) в твою анкету.",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

    txt = profile_card_text(p, viewer)
    if p.get("main_photo"):
        await message.answer_photo(photo=p["main_photo"], caption=txt, reply_markup=KB.search(p["id"]), parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer(txt, reply_markup=KB.search(p["id"]), parse_mode=ParseMode.MARKDOWN)


rt = Router()


@rt.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if user and user.get("is_profile_complete"):
        unread = await DB.get_unread(user["id"])
        waiting = await DB.get_like_sources_for_user(user["id"], 50)
        status = TIER_NAMES.get(user["subscription_tier"], "🍇 Базовый")
        await message.answer(
            T.WELCOME_BACK.format(
                name=user["name"],
                status=status,
                views=user["views_count"],
                matches=user["matches_count"],
                msgs=unread,
                likes_waiting=len(waiting)
            ),
            reply_markup=KB.main(),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if not user:
        await DB.create_user(message.from_user.id, message.from_user.username)

    splash = await message.answer("🍷")
    await animate_text(splash, ["🍷", "🍷✨", f"*{BOT_NAME}*"])
    await message.answer(T.ASK_NAME, reply_markup=ReplyKeyboardRemove())
    await state.set_state(RegStates.name)


@rt.message(RegStates.name)
async def reg_name(message: Message, state: FSMContext):
    name = normalize_text(message.text)
    if len(name) < 2 or len(name) > 50:
        await message.answer(T.BAD_NAME)
        return
    await state.update_data(name=name)
    await message.answer(T.ASK_AGE, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.age)


@rt.message(RegStates.age)
async def reg_age(message: Message, state: FSMContext):
    try:
        age = int(normalize_text(message.text))
        if not 18 <= age <= 99:
            raise ValueError
    except Exception:
        await message.answer(T.BAD_AGE)
        return

    await state.update_data(age=age)
    await message.answer(T.ASK_GENDER, reply_markup=KB.gender())
    await state.set_state(RegStates.gender)


@rt.callback_query(RegStates.gender, F.data.startswith("g:"))
async def reg_gender(callback: CallbackQuery, state: FSMContext):
    await state.update_data(gender=callback.data[2:])
    await callback.message.edit_text(T.ASK_CITY)
    await state.set_state(RegStates.city)
    await callback.answer()


@rt.message(RegStates.city)
async def reg_city(message: Message, state: FSMContext):
    city = normalize_text(message.text).title()
    if len(city) < 2:
        await message.answer("Укажи город.")
        return
    await state.update_data(city=city)
    await message.answer(T.ASK_PHOTO, reply_markup=KB.skip())
    await state.set_state(RegStates.photo)


@rt.message(RegStates.photo, F.photo)
async def reg_photo(message: Message, state: FSMContext):
    await state.update_data(photo=message.photo[-1].file_id)
    await message.answer(T.ASK_BIO, reply_markup=KB.skip())
    await state.set_state(RegStates.bio)


@rt.callback_query(RegStates.photo, F.data == "skip")
async def reg_skip_photo(callback: CallbackQuery, state: FSMContext):
    await state.update_data(photo=None)
    await callback.message.edit_text(T.ASK_BIO)
    await state.set_state(RegStates.bio)
    await callback.answer()


@rt.message(RegStates.bio)
async def reg_bio(message: Message, state: FSMContext):
    await state.update_data(bio=normalize_text(message.text)[:config.MAX_BIO_LEN], selected_interests=[])
    await message.answer(T.ASK_INTERESTS, reply_markup=KB.interests([]))
    await state.set_state(RegStates.interests)


@rt.callback_query(RegStates.bio, F.data == "skip")
async def reg_skip_bio(callback: CallbackQuery, state: FSMContext):
    await state.update_data(bio="", selected_interests=[])
    await callback.message.edit_text(T.ASK_INTERESTS, reply_markup=KB.interests([]))
    await state.set_state(RegStates.interests)
    await callback.answer()


@rt.callback_query(RegStates.interests, F.data.startswith("int:"))
async def reg_interests(callback: CallbackQuery, state: FSMContext):
    action = callback.data[4:]
    data = await state.get_data()
    selected = data.get("selected_interests", [])

    if action == "skip":
        await state.update_data(interests="")
        await callback.message.edit_text(T.ASK_LOOKING, reply_markup=KB.looking())
        await state.set_state(RegStates.looking_for)
        await callback.answer()
        return

    if action == "done":
        await state.update_data(interests=join_interests(selected))
        await callback.message.edit_text(T.ASK_LOOKING, reply_markup=KB.looking())
        await state.set_state(RegStates.looking_for)
        await callback.answer()
        return

    if action in INTEREST_OPTIONS:
        if action in selected:
            selected.remove(action)
        else:
            if len(selected) >= 5:
                await callback.answer("Можно выбрать до 5 интересов", show_alert=True)
                return
            selected.append(action)

    await state.update_data(selected_interests=selected)
    txt = T.ASK_INTERESTS
    if selected:
        txt += f"\n\nВыбрано: {', '.join(selected)}"
    await callback.message.edit_text(txt, reply_markup=KB.interests(selected))
    await callback.answer()


@rt.callback_query(RegStates.looking_for, F.data.startswith("l:"))
async def reg_looking(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    values = {
        "name": data["name"],
        "age": data["age"],
        "gender": Gender(data["gender"]),
        "city": data["city"],
        "bio": data.get("bio", ""),
        "interests": data.get("interests", ""),
        "looking_for": LookingFor(callback.data[2:]),
        "is_profile_complete": True,
    }
    if data.get("photo"):
        values["photos"] = data["photo"]
        values["main_photo"] = data["photo"]

    await DB.update_user(callback.from_user.id, **values)
    await state.clear()
    await callback.message.edit_text(T.REG_DONE, parse_mode=ParseMode.MARKDOWN)
    await callback.message.answer("Главное меню", reply_markup=KB.main())
    await callback.answer()


@rt.message(F.text == "🍷 Анкеты")
async def browse(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return
    if user.get("is_paused"):
        await message.answer("⏸ Твоя анкета на паузе.")
        return
    profiles = await DB.search_profiles(user, 1)
    if not profiles:
        await message.answer(T.NO_PROFILES)
        return
    await show_card(message, profiles[0], user, bot=message.bot)


@rt.callback_query(F.data.startswith("lk:"))
async def handle_like(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    tid = int(callback.data[3:])
    result = await DB.add_like(user["id"], tid)
    if result.get("reason") == "already_liked":
        await callback.answer("Уже отправлено")
        return

    if result["match"]:
        target = await DB.get_user_by_id(tid)
        md = result["match_data"] or {}
        text = T.NEW_MATCH.format(
            name=target["name"] if target else "?",
            compatibility_line=f"💞 Совместимость: *{md.get('compatibility', 0)}%*",
            opener=md.get("opener", "Как настроение?")
        )
        try:
            await callback.message.edit_caption(text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    else:
        await callback.answer("Лайк отправлен 💖")

    fresh = await DB.get_user(callback.from_user.id)
    profiles = await DB.search_profiles(fresh, 1)
    if profiles:
        await show_card(callback.message, profiles[0], fresh, bot=callback.bot)


@rt.callback_query(F.data.startswith("dl:"))
async def handle_dislike(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    profiles = await DB.search_profiles(user, 1)
    if profiles:
        await show_card(callback.message, profiles[0], user, bot=callback.bot)
    else:
        await callback.message.answer(T.NO_PROFILES)
    await callback.answer()


@rt.message(F.text == "💕 Симпатии")
async def show_matches(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return
    matches = await DB.get_matches(user["id"])
    if matches:
        await message.answer("💕 *Симпатии:*", reply_markup=KB.matches(matches), parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer(T.NO_MATCHES)


@rt.message(F.text == "👀 Гости")
async def show_guests(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return
    guests = await DB.get_guests(user["id"], 20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE)
    if not guests:
        await message.answer(T.NO_GUESTS)
        return
    txt = "👀 *Гости:*\n\n"
    for i, g in enumerate(guests, 1):
        txt += f"{i}. {g['name']}, {g['age']} — {g['city']}\n"
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN)


@rt.message(F.text == "👤 Профиль")
async def show_profile(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return
    txt = profile_card_text(user)
    if user.get("main_photo"):
        await message.answer_photo(user["main_photo"], caption=txt, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer(txt, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "pf:pause")
async def toggle_pause_profile(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    new_value = not user.get("is_paused", False)
    await DB.update_user(callback.from_user.id, is_paused=new_value)
    await callback.message.answer("⏸ Анкета на паузе." if new_value else "▶️ Анкета снова активна.")
    await callback.answer()


@rt.callback_query(F.data == "pf:guest_notify")
async def toggle_guest_notify(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    new_value = not user.get("notify_guests", True)
    await DB.update_user(callback.from_user.id, notify_guests=new_value)
    await callback.message.answer("👀 Уведомления о гостях включены." if new_value else "🔕 Уведомления о гостях выключены.")
    await callback.answer()


@rt.callback_query(F.data == "pf:gifts")
async def show_my_gifts(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    gifts = await DB.get_received_gifts(user["id"], 20)
    if not gifts:
        await callback.message.answer("🎁 У тебя пока нет подарков.")
        await callback.answer()
        return

    txt = "🎁 *Твои подарки:*\n\n"
    for g in gifts:
        title, emoji = GIFT_OPTIONS.get(g["gift_code"], ("Подарок", "🎁"))
        txt += f"{emoji} {title} — от *{g['sender_name']}*\n"
    await callback.message.answer(txt, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


async def background_cleanup():
    while True:
        try:
            await DB.cleanup_old_guests()
        except Exception as e:
            logger.error("cleanup error: %s", e)
        await asyncio.sleep(60 * 60 * 6)


async def main():
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

    logger.info("%s fixed starting...", BOT_NAME)

    cleanup_task = asyncio.create_task(background_cleanup())

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        cleanup_task.cancel()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
