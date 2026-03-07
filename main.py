"""
🍷 ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v4.3

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
    def is_creator(u: Dict) -> bool:
        return u.get("telegram_id") in config.CREATOR_IDS

    @staticmethod
    def is_admin(u: Dict) -> bool:
        return u.get("telegram_id") in config.ADMIN_IDS

    @staticmethod
    def get_badge(u: Dict) -> str:
        if DB.is_creator(u):
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
        if DB.is_creator(u):
            return " · Создатель"
        if DB.is_admin(u):
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

            local_q = q.where(User.city == u["city"]).order_by(
                User.boost_expires_at.desc().nullslast(),
                User.last_active_at.desc()
            ).limit(limit * 10)
            local_r = await s.execute(local_q)
            local_profiles = [DB._to_dict(x) for x in local_r.scalars().all()]
            if local_profiles:
                local_profiles.sort(key=lambda p: calc_compatibility(u, p), reverse=True)
                return local_profiles[:limit]

            global_q = q.order_by(
                User.boost_expires_at.desc().nullslast(),
                User.last_active_at.desc()
            ).limit(limit * 14)
            global_r = await s.execute(global_q)
            global_profiles = [DB._to_dict(x) for x in global_r.scalars().all()]
            global_profiles.sort(key=lambda p: calc_compatibility(u, p), reverse=True)
            return global_profiles[:limit]

    @staticmethod
    async def get_best_matches_of_day(uid: int, limit: int = 5) -> List[Dict]:
        user = await DB.get_user_by_id(uid)
        if not user:
            return []
        profiles = await DB.search_profiles(user, limit * 3)
        profiles.sort(key=lambda p: calc_compatibility(user, p), reverse=True)
        return profiles[:limit]

    @staticmethod
    async def add_like(fid: int, tid: int, is_super_like: bool = False, message: Optional[str] = None) -> Dict:
        async with async_session_maker() as s:
            ex = await s.execute(select(Like).where(and_(
                Like.from_user_id == fid,
                Like.to_user_id == tid
            )))
            if ex.scalar_one_or_none():
                return {"created": False, "match": False, "reason": "already_liked"}

            s.add(Like(
                from_user_id=fid,
                to_user_id=tid,
                is_super_like=is_super_like,
                message=message
            ))

            await s.execute(update(User).where(User.id == tid).values(
                likes_received_count=User.likes_received_count + 1
            ))

            rev = await s.execute(select(Like).where(and_(
                Like.from_user_id == tid,
                Like.to_user_id == fid
            )))
            is_match = rev.scalar_one_or_none() is not None
            match_data = None

            if is_match:
                u1 = await DB.get_user_by_id(fid)
                u2 = await DB.get_user_by_id(tid)
                compatibility = calc_compatibility(u1, u2)
                opener = random.choice(ICEBREAKERS)

                already_match = await s.execute(select(Match).where(and_(
                    or_(
                        and_(Match.user1_id == fid, Match.user2_id == tid),
                        and_(Match.user1_id == tid, Match.user2_id == fid)
                    ),
                    Match.is_active == True
                )))
                if already_match.scalar_one_or_none() is None:
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

            partner_ids = [m.user2_id if m.user1_id == uid else m.user1_id for m in matches]
            users_res = await s.execute(select(User).where(User.id.in_(partner_ids)))
            users_map = {u.id: u for u in users_res.scalars().all()}

            out = []
            for m in matches:
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                p = users_map.get(pid)
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
                    "user_id": p.id,
                    "telegram_id": p.telegram_id,
                    "name": p.name,
                    "age": p.age,
                    "photo": p.main_photo,
                    "compatibility": m.compatibility_score,
                    "unread": unread,
                    "opener": m.opener_question,
                    "online": online_status(p.last_active_at),
                    "safe": p.is_safe_mode,
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
            return {
                "id": m.id,
                "compatibility_score": m.compatibility_score,
                "opener_question": m.opener_question,
            }

    @staticmethod
    async def unmatch(u1: int, u2: int) -> bool:
        async with async_session_maker() as s:
            res = await s.execute(select(Match).where(and_(
                or_(
                    and_(Match.user1_id == u1, Match.user2_id == u2),
                    and_(Match.user1_id == u2, Match.user2_id == u1)
                ),
                Match.is_active == True
            )))
            m = res.scalar_one_or_none()
            if not m:
                return False

            await s.execute(update(Match).where(Match.id == m.id).values(is_active=False))
            await s.commit()
            return True

    @staticmethod
    async def send_msg(mid: int, sid: int, txt: str):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=txt))
            await s.execute(update(Match).where(Match.id == mid).values(last_message_at=datetime.utcnow()))
            await s.commit()

    @staticmethod
    async def send_reaction(mid: int, sid: int, reaction: str):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=None, reaction=reaction))
            await s.execute(update(Match).where(Match.id == mid).values(last_message_at=datetime.utcnow()))
            await s.commit()

    @staticmethod
    async def get_msgs(mid: int, limit: int = 12) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(ChatMessage)
                .where(ChatMessage.match_id == mid)
                .order_by(ChatMessage.created_at.desc())
                .limit(limit)
            )
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
            await s.execute(update(User).where(User.id == uid).values(
                views_count=User.views_count + 1
            ))
            await s.commit()
            return True

    @staticmethod
    async def get_guests(uid: int, limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            threshold = datetime.utcnow() - timedelta(days=config.GUEST_RETENTION_DAYS)
            r = await s.execute(
                select(GuestVisit.visitor_id)
                .where(and_(
                    GuestVisit.visited_user_id == uid,
                    GuestVisit.created_at >= threshold
                ))
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

    @staticmethod
    async def dec_likes(tg_id: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(
                daily_likes_remaining=User.daily_likes_remaining - 1
            ))
            await s.commit()

    @staticmethod
    async def dec_messages(tg_id: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(
                daily_messages_remaining=User.daily_messages_remaining - 1
            ))
            await s.commit()

    @staticmethod
    async def use_boost(uid: int) -> bool:
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u or (u.boost_count or 0) <= 0:
                return False

            now = datetime.utcnow()
            new_exp = (
                u.boost_expires_at + timedelta(hours=24)
                if u.boost_expires_at and u.boost_expires_at > now
                else now + timedelta(hours=24)
            )

            await s.execute(update(User).where(User.id == uid).values(
                boost_count=User.boost_count - 1,
                boost_expires_at=new_exp
            ))
            await s.commit()
            return True

    @staticmethod
    async def add_boosts(uid: int, count: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.id == uid).values(
                boost_count=User.boost_count + count
            ))
            await s.commit()

    @staticmethod
    async def create_report(rid: int, ruid: int, reason: str):
        async with async_session_maker() as s:
            s.add(Report(reporter_id=rid, reported_user_id=ruid, reason=reason))
            await s.commit()

    @staticmethod
    async def get_stats() -> Dict:
        async with async_session_maker() as s:
            total = (await s.execute(select(func.count(User.id)))).scalar() or 0
            complete = (await s.execute(select(func.count(User.id)).where(User.is_profile_complete == True))).scalar() or 0
            now = datetime.utcnow()
            day_ago = now - timedelta(days=1)
            week_ago = now - timedelta(days=7)
            month_ago = now - timedelta(days=30)

            dau = (await s.execute(select(func.count(User.id)).where(User.last_active_at > day_ago))).scalar() or 0
            wau = (await s.execute(select(func.count(User.id)).where(User.last_active_at > week_ago))).scalar() or 0
            mau = (await s.execute(select(func.count(User.id)).where(User.last_active_at > month_ago))).scalar() or 0
            vip = (await s.execute(select(func.count(User.id)).where(User.subscription_tier != SubscriptionTier.FREE))).scalar() or 0
            banned = (await s.execute(select(func.count(User.id)).where(User.is_banned == True))).scalar() or 0
            today_reg = (await s.execute(select(func.count(User.id)).where(User.created_at > day_ago))).scalar() or 0
            total_matches = (await s.execute(select(func.count(Match.id)))).scalar() or 0
            total_msgs = (await s.execute(select(func.count(ChatMessage.id)))).scalar() or 0
            total_likes = (await s.execute(select(func.count(Like.id)))).scalar() or 0
            revenue = (await s.execute(select(func.sum(Payment.amount)).where(Payment.status == PaymentStatus.SUCCEEDED))).scalar() or 0
            month_rev = (await s.execute(select(func.sum(Payment.amount)).where(and_(
                Payment.status == PaymentStatus.SUCCEEDED,
                Payment.paid_at > month_ago
            )))).scalar() or 0
            pending_reports = (await s.execute(select(func.count(Report.id)).where(Report.status == "pending"))).scalar() or 0

            return {
                "total": total,
                "complete": complete,
                "dau": dau,
                "wau": wau,
                "mau": mau,
                "vip": vip,
                "banned": banned,
                "today_reg": today_reg,
                "matches": total_matches,
                "messages": total_msgs,
                "likes": total_likes,
                "revenue": revenue / 100,
                "month_revenue": month_rev / 100,
                "pending_reports": pending_reports,
                "conversion": (vip / complete * 100) if complete > 0 else 0,
            }

    @staticmethod
    async def search_users(query: str) -> List[Dict]:
        async with async_session_maker() as s:
            if query.isdigit():
                r = await s.execute(select(User).where(or_(
                    User.id == int(query),
                    User.telegram_id == int(query)
                )))
            else:
                q = query.replace("@", "")
                r = await s.execute(select(User).where(or_(
                    User.username.ilike(f"%{q}%"),
                    User.name.ilike(f"%{query}%")
                )).limit(10))
            return [DB._to_dict(u) for u in r.scalars().all()]

    @staticmethod
    async def get_all_user_ids(filter_type: str = "all") -> List[int]:
        async with async_session_maker() as s:
            q = select(User.telegram_id).where(and_(User.is_active == True, User.is_banned == False))
            if filter_type == "complete":
                q = q.where(User.is_profile_complete == True)
            elif filter_type == "vip":
                q = q.where(User.subscription_tier != SubscriptionTier.FREE)
            elif filter_type == "free":
                q = q.where(User.subscription_tier == SubscriptionTier.FREE)
            r = await s.execute(q)
            return [row[0] for row in r.fetchall()]

    @staticmethod
    async def get_pending_reports(limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Report).where(Report.status == "pending").order_by(Report.created_at.desc()).limit(limit))
            out = []
            for rep in r.scalars().all():
                reporter = await DB.get_user_by_id(rep.reporter_id)
                reported = await DB.get_user_by_id(rep.reported_user_id)
                out.append({
                    "id": rep.id,
                    "reason": rep.reason,
                    "created_at": rep.created_at,
                    "reporter": reporter,
                    "reported": reported,
                })
            return out

    @staticmethod
    async def resolve_report(report_id: int, action: str, notes: str = ""):
        async with async_session_maker() as s:
            await s.execute(update(Report).where(Report.id == report_id).values(
                status=action,
                admin_notes=notes,
                resolved_at=datetime.utcnow()
            ))
            await s.commit()

    @staticmethod
    async def get_recent_payments(limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Payment).order_by(Payment.created_at.desc()).limit(limit))
            out = []
            for p in r.scalars().all():
                u = await DB.get_user_by_id(p.user_id)
                out.append({
                    "id": p.id,
                    "amount": p.amount / 100,
                    "status": p.status.value,
                    "description": p.description,
                    "created_at": p.created_at,
                    "user_name": u["name"] if u else "?",
                    "user_tg": u["telegram_id"] if u else 0,
                })
            return out

    @staticmethod
    async def create_promo(code: str, tier: str, days: int, max_uses: int):
        async with async_session_maker() as s:
            s.add(PromoCode(code=code.upper(), tier=tier, duration_days=days, max_uses=max_uses))
            await s.commit()

    @staticmethod
    async def use_promo(user_id: int, code: str) -> Dict:
        async with async_session_maker() as s:
            r = await s.execute(select(PromoCode).where(and_(
                PromoCode.code == code.upper(),
                PromoCode.is_active == True
            )))
            promo = r.scalar_one_or_none()
            if not promo:
                return {"error": "Промокод не найден"}
            if promo.used_count >= promo.max_uses:
                return {"error": "Промокод исчерпан"}

            used = await s.execute(select(PromoUse).where(and_(
                PromoUse.promo_id == promo.id,
                PromoUse.user_id == user_id
            )))
            if used.scalar_one_or_none():
                return {"error": "Ты уже использовал этот промокод"}

            s.add(PromoUse(promo_id=promo.id, user_id=user_id))
            await s.execute(update(PromoCode).where(PromoCode.id == promo.id).values(
                used_count=PromoCode.used_count + 1
            ))
            await s.commit()
            await DB.activate_subscription_by_id(user_id, promo.tier, promo.duration_days)
            return {"success": True, "tier": promo.tier, "days": promo.duration_days}

    @staticmethod
    async def activate_subscription_by_id(uid: int, tier: str, days: int):
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u:
                return

            te = SubscriptionTier(tier)
            now = datetime.utcnow()
            if te == SubscriptionTier.WINE_FOREVER:
                exp = None
            elif u.subscription_expires_at and u.subscription_expires_at > now:
                exp = u.subscription_expires_at + timedelta(days=days)
            else:
                exp = now + timedelta(days=days)

            await s.execute(update(User).where(User.id == uid).values(
                subscription_tier=te,
                subscription_expires_at=exp
            ))
            await s.commit()

    @staticmethod
    async def create_payment(uid, yid, amount, desc, ptype, ptier=None, pdur=None, pcount=None) -> int:
        async with async_session_maker() as s:
            p = Payment(
                user_id=uid,
                yookassa_payment_id=yid,
                amount=amount,
                description=desc,
                product_type=ptype,
                product_tier=ptier,
                product_duration=pdur,
                product_count=pcount
            )
            s.add(p)
            await s.commit()
            await s.refresh(p)
            return p.id

    @staticmethod
    async def get_payment(pid: int) -> Optional[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Payment).where(Payment.id == pid))
            p = r.scalar_one_or_none()
            if p:
                return {
                    "id": p.id,
                    "user_id": p.user_id,
                    "yookassa_payment_id": p.yookassa_payment_id,
                    "status": p.status.value,
                    "product_type": p.product_type,
                    "product_tier": p.product_tier,
                    "product_duration": p.product_duration,
                    "product_count": p.product_count,
                }
            return None

    @staticmethod
    async def update_payment_status(pid: int, st: PaymentStatus):
        async with async_session_maker() as s:
            values = {"status": st}
            if st == PaymentStatus.SUCCEEDED:
                values["paid_at"] = datetime.utcnow()
            await s.execute(update(Payment).where(Payment.id == pid).values(**values))
            await s.commit()

    @staticmethod
    async def log_broadcast(admin_id, text, target, sent, failed):
        async with async_session_maker() as s:
            s.add(BroadcastLog(
                admin_id=admin_id,
                message_text=text,
                target_filter=target,
                sent_count=sent,
                failed_count=failed
            ))
            await s.commit()


class T:
    WELCOME_NEW = f"""
🍷 *Добро пожаловать в {BOT_NAME}!*

Здесь знакомятся легко, красиво и с искрой ✨

💕 Листай анкеты
🥂 Лови взаимные симпатии
💬 Общайся после мэтча
🎁 Дарите друг другу знаки внимания
🌟 Находи лучших мэтчей дня

Давай создадим твою анкету и начнём знакомство.
"""

    WELCOME_BACK = """
🍷 *С возвращением, {name}!*

{status}

👀 Просмотров: *{views}*
💕 Мэтчей: *{matches}*
💬 Новых сообщений: *{msgs}*
💘 Тебя лайкнули: *{likes_waiting}*

Готов продолжить знакомство? ✨
"""

    ASK_NAME = "✨ Как тебя зовут?"
    ASK_AGE = "🎂 Сколько тебе лет? _(18-99)_"
    ASK_GENDER = "💫 Твой пол:"
    ASK_CITY = "📍 Из какого ты города?"
    ASK_PHOTO = "📸 Отправь фото или нажми «Пропустить»:"
    ASK_BIO = "📝 Расскажи немного о себе _(до 500 символов)_ или «Пропустить»:"
    ASK_INTERESTS = "🎯 Выбери несколько интересов:"
    ASK_LOOKING = "💞 Кого ты хочешь найти?"
    BAD_NAME = "Имя должно быть от 2 до 50 символов."
    BAD_AGE = "Возраст должен быть от 18 до 99."
    REG_DONE = f"""
🥂 *Анкета готова!*

Добро пожаловать в {BOT_NAME} ✨
Теперь можно смотреть анкеты и знакомиться.
"""
    NO_PROFILES = "🍷 Анкеты закончились. Загляни чуть позже."
    LIKES_LIMIT = """
💔 *Лимит лайков на сегодня исчерпан.*

Переходи на винные тарифы и знакомься без ограничений ✨
"""
    MSG_LIMIT = """
💬 *Лимит сообщений на сегодня исчерпан.*

Открой один из винных тарифов, чтобы общаться без ограничений.
"""
    NEW_MATCH = """
💘 *У вас мэтч с {name}!*

{compatibility_line}

🥂 Похоже, между вами есть что-то общее.

*Первый вопрос для старта:*
_{opener}_

Напиши первым(ой) ✨
"""
    NO_MATCHES = "Пока нет взаимных симпатий 💕"
    NO_PROFILE = "Сначала заполни профиль: /start"
    BANNED = "🚫 Аккаунт заблокирован."
    NO_GUESTS = "Пока нет гостей 👀"
    NO_MSGS = "Пока нет сообщений 💬"

    SHOP = f"""
🛍 *Винная лавка · {BOT_NAME}*

🥂 Подписки — больше возможностей
🚀 Буст — подними анкету выше
🍷 Сравнение тарифов — выбери свой вкус
🎁 Промокод — активируй бонус
"""

    FAQ = f"""
❓ *FAQ · {BOT_NAME}*

*Как работают симпатии?*
Ставь 💖 лайк. Если симпатия взаимная — у вас мэтч и можно общаться.

*Что такое суперлайк?*
Это яркий знак внимания ⭐

*Что такое буст?*
Анкета поднимается выше в выдаче на 24 часа.

*Что дают подписки?*
Больше лайков, больше гостей, больше шансов на знакомство.

*Что такое безопасный режим?*
В профиле появится отметка осторожного общения.

*Что такое лучшие мэтчи дня?*
Бот подбирает анкеты с наибольшей совместимостью для тебя.

*Что такое пауза анкеты?*
Ты временно скрываешься из выдачи, но сохраняешь профиль.
"""

    BOOST_INFO = """
🚀 *Буст анкеты*

Поднимает профиль в топ на 24 часа.
+ просмотры
+ шанс на мэтчи

{status}
"""

    COMPARE = """
🍷 *Тарифы Знакомств на Винчике*

*🍇 Базовый*
30 лайков в день
20 сообщений в день
3 последних гостя

*🥂 Игристое*
100 лайков в день
Безлимит сообщений
10 гостей
Без рекламы

*🌷 Розе*
Безлимит лайков
Все гости
Приоритет в показах
1 буст в день

*🍷 Гран Крю*
Всё из Розе
3 буста
Суперлайки
Премиум-значок

*👑 Винная легенда*
Все возможности навсегда
Особый статус
Будущие обновления бесплатно
"""

    SPARK = """
🥂 *Игристое*

100 лайков в день
Безлимит сообщений
10 гостей
Без рекламы

• 299₽ / неделя
• 799₽ / месяц
"""

    ROSE = """
🌷 *Розе*

Безлимит лайков
Все гости
Приоритет в показах
1 буст в день

• 499₽ / месяц
• 1199₽ / 3 месяца
"""

    GRAND = """
🍷 *Гран Крю*

Всё из Розе
3 буста
Суперлайки
Премиум-значок

• 799₽ / месяц
• 1999₽ / 3 месяца
• 3499₽ / 6 месяцев
"""

    FOREVER = """
👑 *Винная легенда*

Все возможности навсегда
Особый статус
Будущие обновления бесплатно

• 9999₽ один раз
"""

    FILTERS = """
🎯 *Фильтры поиска*

Твой диапазон: *{age_from}-{age_to}*
Город: *{city}*
Кого ищешь: *{looking_for}*

Выбери, что изменить.
"""

    BEST_MATCHES_TITLE = "🌟 *Лучшие мэтчи дня*"

    ADMIN_MAIN = """
🛠 *Админ-панель · {bot_name}*

{admin_name} {role}
"""

    ADMIN_STATS = """
📊 *Статистика · {bot_name}*

Всего пользователей: {total}
С анкетой: {complete}
DAU: {dau} · WAU: {wau} · MAU: {mau}
VIP: {vip} ({conversion:.1f}%)
Забанено: {banned}
Сегодня: {today_reg}

Лайков: {likes}
Мэтчей: {matches}
Сообщений: {messages}

Выручка: {revenue:.0f}₽
За месяц: {month_revenue:.0f}₽
Жалоб: {pending_reports}
"""

    ADMIN_USER_CARD = """
👤 *Карточка пользователя*

ID: `{id}` · TG: `{telegram_id}`
@{username}

{badge}{name}, {age}
{city}
_{bio}_

Статус: {tier}
👀 {views} · 💖 {likes} · 💕 {matches}
Бустов: {boosts}
Регистрация: {created}
Активность: {active}
Бан: {banned}
"""

    BROADCAST_CONFIRM = """
📢 *Рассылка*

{text}

Аудитория: *{target}*
Получателей: *{count}*

Отправить?
"""


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
            ],
            [InlineKeyboardButton(text="🚨 Жалоба", callback_data=f"rp:{uid}")]
        ])

    @staticmethod
    def likes_waiting(users: List[Dict]):
        rows = []
        for u in users[:10]:
            rows.append([InlineKeyboardButton(
                text=f"💘 {u['name']}, {u['age']} — {u['city']}",
                callback_data=f"lw:{u['id']}"
            )])
        rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def matches(ms: List[Dict]):
        rows = []
        for m in ms[:10]:
            unread = f" 💬{m['unread']}" if m.get("unread", 0) > 0 else ""
            compat = f" · {m.get('compatibility', 0)}%" if m.get("compatibility") is not None else ""
            rows.append([InlineKeyboardButton(
                text=f"💕 {m['name']}, {m['age']}{compat}{unread}",
                callback_data=f"ch:{m['user_id']}"
            )])
        rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def back_matches():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Симпатии", callback_data="bm")]
        ])

    @staticmethod
    def chat_actions(pid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="👤 Профиль", callback_data=f"vp:{pid}"),
                InlineKeyboardButton(text="🎁 Подарок", callback_data=f"gift:{pid}")
            ],
            [
                InlineKeyboardButton(text="⚡ Быстрые фразы", callback_data="chat:quick"),
                InlineKeyboardButton(text="🙂 Реакции", callback_data="chat:react")
            ],
            [
                InlineKeyboardButton(text="💔 Удалить мэтч", callback_data=f"um:{pid}"),
                InlineKeyboardButton(text="⬅️ К симпатиям", callback_data="bm")
            ]
        ])

    @staticmethod
    def quick_phrases():
        rows = [[InlineKeyboardButton(text=phrase, callback_data=f"qp:{idx}")] for idx, phrase in enumerate(QUICK_PHRASES)]
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="chat:back")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def reactions():
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="❤️", callback_data="react:❤️"),
                InlineKeyboardButton(text="😍", callback_data="react:😍"),
                InlineKeyboardButton(text="🔥", callback_data="react:🔥"),
                InlineKeyboardButton(text="🥂", callback_data="react:🥂"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="chat:back")]
        ])

    @staticmethod
    def gifts(pid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌹 Роза", callback_data=f"gift_send:{pid}:rose")],
            [InlineKeyboardButton(text="🍷 Бокал вина", callback_data=f"gift_send:{pid}:wine")],
            [InlineKeyboardButton(text="💖 Сердце", callback_data=f"gift_send:{pid}:heart")],
            [InlineKeyboardButton(text="⭐ Звезда", callback_data=f"gift_send:{pid}:star")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ch:{pid}")]
        ])

    @staticmethod
    def profile():
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✏️ Редактировать", callback_data="pe"),
                InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")
            ],
            [
                InlineKeyboardButton(text="🎯 Интересы", callback_data="ed:interests"),
                InlineKeyboardButton(text="🚀 Буст", callback_data="sh:boost")
            ],
            [
                InlineKeyboardButton(text="🔎 Фильтры поиска", callback_data="pf:filters"),
                InlineKeyboardButton(text="🌟 Лучшие мэтчи", callback_data="pf:best")
            ],
            [
                InlineKeyboardButton(text="🛡 Безопасный режим", callback_data="pf:safe"),
                InlineKeyboardButton(text="⏸ Пауза анкеты", callback_data="pf:pause")
            ],
            [
                InlineKeyboardButton(text="👀 Уведомления о гостях", callback_data="pf:guest_notify"),
                InlineKeyboardButton(text="🎁 Мои подарки", callback_data="pf:gifts")
            ]
        ])

    @staticmethod
    def edit():
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Имя", callback_data="ed:name"),
                InlineKeyboardButton(text="Возраст", callback_data="ed:age"),
            ],
            [
                InlineKeyboardButton(text="Город", callback_data="ed:city"),
                InlineKeyboardButton(text="О себе", callback_data="ed:bio"),
            ],
            [
                InlineKeyboardButton(text="Фото", callback_data="ed:photo"),
                InlineKeyboardButton(text="Интересы", callback_data="ed:interests")
            ],
            [InlineKeyboardButton(text="⬅️ Профиль", callback_data="pv")]
        ])

    @staticmethod
    def filters():
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Возраст от", callback_data="flt:age_from"),
                InlineKeyboardButton(text="Возраст до", callback_data="flt:age_to")
            ],
            [InlineKeyboardButton(text="⬅️ Профиль", callback_data="pv")]
        ])

    @staticmethod
    def report_reasons():
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Спам", callback_data="rr:spam"),
                InlineKeyboardButton(text="Фейк", callback_data="rr:fake"),
            ],
            [
                InlineKeyboardButton(text="18+", callback_data="rr:nsfw"),
                InlineKeyboardButton(text="Оскорбления", callback_data="rr:harass"),
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="mn")],
        ])

    @staticmethod
    def shop():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Подписки", callback_data="sh:subs")],
            [InlineKeyboardButton(text="🚀 Буст анкеты", callback_data="sh:boost")],
            [InlineKeyboardButton(text="🍷 Сравнить тарифы", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="sh:promo")],
            [InlineKeyboardButton(text="⬅️ Меню", callback_data="mn")],
        ])

    @staticmethod
    def subs():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Игристое", callback_data="tf:wine_spark")],
            [InlineKeyboardButton(text="🌷 Розе", callback_data="tf:wine_rose")],
            [InlineKeyboardButton(text="🍷 Гран Крю", callback_data="tf:wine_grand")],
            [InlineKeyboardButton(text="👑 Винная легенда", callback_data="tf:wine_forever")],
            [InlineKeyboardButton(text="🍷 Сравнить тарифы", callback_data="sh:compare")],
            [InlineKeyboardButton(text="⬅️ Магазин", callback_data="sh:mn")],
        ])

    @staticmethod
    def buy_spark():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="299₽ / неделя", callback_data="by:wine_spark:7:29900")],
            [InlineKeyboardButton(text="799₽ / месяц", callback_data="by:wine_spark:30:79900")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sh:subs")],
        ])

    @staticmethod
    def buy_rose():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="499₽ / месяц", callback_data="by:wine_rose:30:49900")],
            [InlineKeyboardButton(text="1199₽ / 3 месяца", callback_data="by:wine_rose:90:119900")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sh:subs")],
        ])

    @staticmethod
    def buy_grand():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="799₽ / месяц", callback_data="by:wine_grand:30:79900")],
            [InlineKeyboardButton(text="1999₽ / 3 месяца", callback_data="by:wine_grand:90:199900")],
            [InlineKeyboardButton(text="3499₽ / 6 месяцев", callback_data="by:wine_grand:180:349900")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sh:subs")],
        ])

    @staticmethod
    def buy_forever():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="9999₽ навсегда", callback_data="by:wine_forever:0:999900")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sh:subs")],
        ])

    @staticmethod
    def boost_menu(has_boosts: bool):
        buttons = []
        if has_boosts:
            buttons.append([InlineKeyboardButton(text="🚀 Активировать буст", callback_data="bo:act")])
        buttons += [
            [InlineKeyboardButton(text="1 шт — 99₽", callback_data="by:boost:1:9900")],
            [InlineKeyboardButton(text="5 шт — 399₽", callback_data="by:boost:5:39900")],
            [InlineKeyboardButton(text="10 шт — 699₽", callback_data="by:boost:10:69900")],
            [InlineKeyboardButton(text="⬅️ Магазин", callback_data="sh:mn")],
        ]
        return InlineKeyboardMarkup(inline_keyboard=buttons)

    @staticmethod
    def pay(url: str, pid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить", url=url)],
            [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"ck:{pid}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="sh:mn")],
        ])

    @staticmethod
    def admin():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Статистика", callback_data="adm:stats")],
            [InlineKeyboardButton(text="Найти пользователя", callback_data="adm:search")],
            [InlineKeyboardButton(text="Рассылка", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="Жалобы", callback_data="adm:reports")],
            [InlineKeyboardButton(text="Платежи", callback_data="adm:payments")],
            [InlineKeyboardButton(text="Создать промокод", callback_data="adm:promo")],
            [InlineKeyboardButton(text="Топ пользователей", callback_data="adm:top")],
            [InlineKeyboardButton(text="Закрыть", callback_data="mn")],
        ])

    @staticmethod
    def admin_user(uid: int, is_banned: bool):
        ban_btn = (
            InlineKeyboardButton(text="Разбанить", callback_data=f"au:unban:{uid}")
            if is_banned else
            InlineKeyboardButton(text="Забанить", callback_data=f"au:ban:{uid}")
        )
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Выдать VIP", callback_data=f"au:givevip:{uid}"),
                InlineKeyboardButton(text="Бусты", callback_data=f"au:giveboost:{uid}"),
            ],
            [
                ban_btn,
                InlineKeyboardButton(text="Удалить фото", callback_data=f"au:delphotos:{uid}")
            ],
            [InlineKeyboardButton(text="Верифицировать", callback_data=f"au:verify:{uid}")],
            [InlineKeyboardButton(text="Админка", callback_data="adm:main")],
        ])

    @staticmethod
    def admin_report(rid: int, ruid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Забанить", callback_data=f"ar:ban:{rid}:{ruid}"),
                InlineKeyboardButton(text="Предупредить", callback_data=f"ar:warn:{rid}:{ruid}"),
            ],
            [InlineKeyboardButton(text="Отклонить", callback_data=f"ar:dismiss:{rid}:{ruid}")],
            [InlineKeyboardButton(text="Следующая", callback_data="adm:reports")],
        ])

    @staticmethod
    def broadcast_targets():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Все", callback_data="bc:all")],
            [InlineKeyboardButton(text="С анкетой", callback_data="bc:complete")],
            [InlineKeyboardButton(text="VIP", callback_data="bc:vip")],
            [InlineKeyboardButton(text="Бесплатные", callback_data="bc:free")],
            [InlineKeyboardButton(text="Отмена", callback_data="adm:main")],
        ])

    @staticmethod
    def broadcast_confirm():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отправить", callback_data="bc:send")],
            [InlineKeyboardButton(text="Отмена", callback_data="adm:main")],
        ])

    @staticmethod
    def give_vip_tiers():
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Игристое", callback_data="gv:wine_spark"),
                InlineKeyboardButton(text="Розе", callback_data="gv:wine_rose"),
            ],
            [
                InlineKeyboardButton(text="Гран Крю", callback_data="gv:wine_grand"),
                InlineKeyboardButton(text="Винная легенда", callback_data="gv:wine_forever"),
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="adm:main")],
        ])

    @staticmethod
    def back_admin():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Админка", callback_data="adm:main")]
        ])


class Pay:
    @staticmethod
    async def create(user: Dict, ptype: str, tier: str = None, dur: int = None, count: int = None, amount: int = 0):
        if not YOOKASSA_AVAILABLE or not config.YOOKASSA_SHOP_ID:
            return {"error": "ЮKassa не настроена"}

        desc = f"Подписка {TIER_NAMES.get(tier, '')}" if ptype == "subscription" else f"Буст ({count} шт)"
        try:
            p = YooPayment.create({
                "amount": {"value": f"{amount / 100:.2f}", "currency": "RUB"},
                "confirmation": {"type": ConfirmationType.REDIRECT, "return_url": f"{config.DOMAIN}/ok"},
                "capture": True,
                "description": desc,
                "metadata": {"user_id": user["id"], "type": ptype, "tier": tier, "dur": dur, "count": count}
            }, str(uuid.uuid4()))

            pid = await DB.create_payment(user["id"], p.id, amount, desc, ptype, tier, dur, count)
            return {"pid": pid, "url": p.confirmation.confirmation_url}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    async def check(pid: int):
        p = await DB.get_payment(pid)
        if not p:
            return {"status": "not_found"}

        try:
            y = YooPayment.find_one(p["yookassa_payment_id"])
            if y.status == "succeeded" and p["status"] != "succeeded":
                await DB.update_payment_status(pid, PaymentStatus.SUCCEEDED)

                if p["product_type"] == "subscription":
                    await DB.activate_subscription_by_id(p["user_id"], p["product_tier"], p["product_duration"] or 30)
                    return {"status": "succeeded", "type": "subscription"}

                if p["product_type"] == "boost":
                    await DB.add_boosts(p["user_id"], p.get("product_count") or 1)
                    return {"status": "succeeded", "type": "boost", "count": p.get("product_count", 1)}

            return {"status": y.status}
        except Exception:
            return {"status": "error"}


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


rt = Router()


def is_admin(user: Optional[Dict]) -> bool:
    return user is not None and user.get("telegram_id") in config.ADMIN_IDS


def looking_label(value: str) -> str:
    return {"male": "👨 Мужчин", "female": "👩 Женщин", "both": "💞 Всех"}.get(value, "💞 Всех")


def profile_card_text(target: Dict, viewer: Optional[Dict] = None) -> str:
    badge = DB.get_badge(target)
    role = DB.get_role_tag(target)
    boost = " 🚀" if DB.is_boosted(target) else ""
    paused = "\n⏸ Анкета на паузе" if target.get("is_paused") else ""
    safe = "\n🛡 Осторожный формат общения" if target.get("is_safe_mode") else ""
    interests = split_interests(target.get("interests"))
    interests_text = f"\n🎯 Интересы: {', '.join(interests)}" if interests else ""
    online = online_status(target.get("last_active_at"))

    compatibility_line = ""
    if viewer:
        compatibility = calc_compatibility(viewer, target)
        compatibility_line = f"\n💞 Совместимость: *{compatibility}%*"

    return (
        f"{badge}*{target['name']}*{boost}, {target['age']}{role}\n"
        f"📍 {target['city']} · {online}"
        f"{compatibility_line}\n\n"
        f"{target['bio'] or '_Без описания_'}"
        f"{interests_text}"
        f"{safe}"
        f"{paused}\n\n"
        f"Ищет: {looking_label(target.get('looking_for', 'both'))}"
    )


async def show_card(message: Message, p: Dict, viewer: Dict, bot: Optional[Bot] = None):
    added = await DB.add_guest(viewer["id"], p["id"])
    if added and bot:
        if p.get("notify_guests"):
            try:
                await bot.send_message(
                    p["telegram_id"],
                    f"👀 *{viewer['name']}* заглянул(а) в твою анкету.",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass

    text = profile_card_text(p, viewer=viewer)
    kb = KB.search(p["id"])

    if p.get("main_photo"):
        await message.answer_photo(
            photo=p["main_photo"],
            caption=text,
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await message.answer(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


@rt.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()

    if user and user.get("is_profile_complete"):
        unread = await DB.get_unread(user["id"])
        waiting = await DB.get_like_sources_for_user(user["id"], 50)
        status = TIER_NAMES.get(user["subscription_tier"], "🍇 Базовый")
        if DB.is_boosted(user):
            status += " · 🚀 Буст"
        status += DB.get_role_tag(user)

        await message.answer(
            T.WELCOME_BACK.format(
                name=user["name"],
                status=status,
                views=user["views_count"],
                matches=user["matches_count"],
                msgs=unread,
                likes_waiting=len(waiting),
            ),
            reply_markup=KB.main(),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if not user:
        await DB.create_user(message.from_user.id, message.from_user.username)

    splash = await message.answer("🍷")
    await animate_text(splash, ["🍷", "🍷✨", "🍷✨💕", f"*{BOT_NAME}*"])
    await message.answer(T.WELCOME_NEW, parse_mode=ParseMode.MARKDOWN)
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
    await message.answer(T.ASK_BIO, reply_markup=KB.skip(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.bio)


@rt.callback_query(RegStates.photo, F.data == "skip")
async def reg_skip_photo(callback: CallbackQuery, state: FSMContext):
    await state.update_data(photo=None)
    await callback.message.edit_text(T.ASK_BIO, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.bio)
    await callback.answer()


@rt.message(RegStates.bio)
async def reg_bio(message: Message, state: FSMContext):
    await state.update_data(bio=normalize_text(message.text)[:config.MAX_BIO_LEN], selected_interests=[])
    await message.answer(T.ASK_INTERESTS, reply_markup=KB.interests([]), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.interests)


@rt.callback_query(RegStates.bio, F.data == "skip")
async def reg_skip_bio(callback: CallbackQuery, state: FSMContext):
    await state.update_data(bio="", selected_interests=[])
    await callback.message.edit_text(T.ASK_INTERESTS, reply_markup=KB.interests([]), parse_mode=ParseMode.MARKDOWN)
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
    text = T.ASK_INTERESTS
    if selected:
        text += f"\n\nВыбрано: {', '.join(selected)}"
    await callback.message.edit_text(text, reply_markup=KB.interests(selected), parse_mode=ParseMode.MARKDOWN)
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

    await animate_text(callback.message, ["✨", "✨🍷", "✨🍷🥂", T.REG_DONE])
    await callback.message.answer("Главное меню открыто 💕", reply_markup=KB.main())
    await callback.answer()


@rt.message(F.text == "🍷 Анкеты")
async def browse(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    if user.get("is_paused"):
        await message.answer("⏸ Твоя анкета на паузе. Сними паузу в профиле, чтобы знакомиться.")
        return

    profiles = await DB.search_profiles(user, 1)
    if not profiles:
        await message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)
        return

    await show_card(message, profiles[0], user, bot=message.bot)


@rt.callback_query(F.data.startswith("lk:"))
async def handle_like(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        await callback.answer("Ошибка")
        return

    if not DB.is_vip(user) and user.get("daily_likes_remaining", 0) <= 0:
        try:
            await callback.message.edit_caption(caption=T.LIKES_LIMIT, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)
        except Exception:
            await callback.message.edit_text(T.LIKES_LIMIT, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)
        return

    tid = int(callback.data[3:])
    result = await DB.add_like(user["id"], tid)

    if result.get("reason") == "already_liked":
        await callback.answer("Ты уже поставил(а) лайк")
        return

    if not DB.is_vip(user):
        await DB.dec_likes(user["telegram_id"])

    if result["match"]:
        target = await DB.get_user_by_id(tid)
        md = result["match_data"] or {}
        compat = md.get("compatibility", 0)
        opener = md.get("opener", "Как настроение?")

        text = T.NEW_MATCH.format(
            name=target["name"] if target else "?",
            compatibility_line=f"💞 Совместимость: *{compat}%*",
            opener=opener,
        )
        try:
            await callback.message.edit_caption(text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)

        if target:
            try:
                await callback.bot.send_message(
                    target["telegram_id"],
                    T.NEW_MATCH.format(
                        name=user["name"],
                        compatibility_line=f"💞 Совместимость: *{compat}%*",
                        opener=opener,
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass
    else:
        await callback.answer("Лайк отправлен 💖")

    fresh_user = await DB.get_user(callback.from_user.id)
    profiles = await DB.search_profiles(fresh_user, 1)
    if profiles:
        await show_card(callback.message, profiles[0], fresh_user, bot=callback.bot)
    else:
        await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("sl:"))
async def handle_super_like(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    if not DB.is_vip(user):
        await callback.answer("Суперлайк доступен в премиум-тарифах", show_alert=True)
        return

    tid = int(callback.data[3:])
    result = await DB.add_like(user["id"], tid, is_super_like=True)
    if result.get("reason") == "already_liked":
        await callback.answer("Уже отправлено")
        return

    target = await DB.get_user_by_id(tid)
    if target:
        try:
            await callback.bot.send_message(
                target["telegram_id"],
                f"⭐ *{user['name']}* отправил(а) тебе суперлайк!",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

    if result["match"]:
        md = result["match_data"] or {}
        compat = md.get("compatibility", 0)
        opener = md.get("opener", "Как настроение?")
        text = T.NEW_MATCH.format(
            name=target["name"] if target else "?",
            compatibility_line=f"💞 Совместимость: *{compat}%*",
            opener=opener,
        )
        try:
            await callback.message.edit_caption(text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    else:
        await callback.answer("Суперлайк отправлен ⭐")

    fresh_user = await DB.get_user(callback.from_user.id)
    profiles = await DB.search_profiles(fresh_user, 1)
    if profiles:
        await show_card(callback.message, profiles[0], fresh_user, bot=callback.bot)
    else:
        await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("lm:"))
async def like_with_message_start(callback: CallbackQuery, state: FSMContext):
    tid = int(callback.data[3:])
    await state.update_data(like_target_id=tid)
    await state.set_state(LikeMsgStates.text)
    await callback.message.answer("💌 Напиши сообщение, которое уйдёт вместе с лайком:")
    await callback.answer()


@rt.message(LikeMsgStates.text)
async def like_with_message_save(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        await state.clear()
        return

    if not DB.is_vip(user) and user.get("daily_likes_remaining", 0) <= 0:
        await state.clear()
        await message.answer(T.LIKES_LIMIT, reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)
        return

    data = await state.get_data()
    tid = data.get("like_target_id")
    text = normalize_text(message.text)[:config.MAX_FIRST_MESSAGE_LEN]
    await state.clear()

    if not tid:
        await message.answer("Ошибка")
        return

    result = await DB.add_like(user["id"], tid, message=text)
    if result.get("reason") == "already_liked":
        await message.answer("Ты уже ставил(а) лайк.", reply_markup=KB.main())
        return

    if not DB.is_vip(user):
        await DB.dec_likes(user["telegram_id"])

    target = await DB.get_user_by_id(tid)
    if target:
        try:
            await message.bot.send_message(
                target["telegram_id"],
                f"💌 *{user['name']}* поставил(а) тебе лайк с сообщением:\n\n_{text}_",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

    if result["match"]:
        md = result["match_data"] or {}
        compat = md.get("compatibility", 0)
        opener = md.get("opener", "Как настроение?")
        await message.answer(
            T.NEW_MATCH.format(
                name=target["name"] if target else "?",
                compatibility_line=f"💞 Совместимость: *{compat}%*",
                opener=opener,
            ),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=KB.main()
        )
    else:
        await message.answer("Лайк с сообщением отправлен 💌", reply_markup=KB.main())


@rt.callback_query(F.data.startswith("dl:"))
async def handle_dislike(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    profiles = await DB.search_profiles(user, 1)
    if profiles:
        await show_card(callback.message, profiles[0], user, bot=callback.bot)
    else:
        try:
            await callback.message.edit_caption(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.message(F.text == "💕 Симпатии")
async def show_matches(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    waiting = await DB.get_like_sources_for_user(user["id"], 20)
    matches = await DB.get_matches(user["id"])

    if waiting:
        if DB.is_vip(user):
            await message.answer(
                f"💘 *Тебя лайкнули ({len(waiting)}):*",
                reply_markup=KB.likes_waiting(waiting),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer(
                f"💘 *Тебя лайкнули: {len(waiting)}*\n\nС VIP можно увидеть, кто именно оставил симпатию.",
                parse_mode=ParseMode.MARKDOWN
            )

    if matches:
        await message.answer(
            f"💕 *Симпатии ({len(matches)}):*",
            reply_markup=KB.matches(matches),
            parse_mode=ParseMode.MARKDOWN
        )
    elif not waiting:
        await message.answer(T.NO_MATCHES, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("lw:"))
async def show_like_waiting_profile(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    if not DB.is_vip(user):
        await callback.answer("Просмотр доступен с VIP", show_alert=True)
        return

    uid = int(callback.data[3:])
    target = await DB.get_user_by_id(uid)
    if not target:
        await callback.answer("Не найдено")
        return

    text = profile_card_text(target, viewer=user)
    kb = KB.search(uid)

    if target.get("main_photo"):
        await callback.message.answer_photo(
            photo=target["main_photo"],
            caption=text,
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await callback.message.answer(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.callback_query(F.data.startswith("ch:"))
async def start_chat(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    pid = int(callback.data[3:])
    partner = await DB.get_user_by_id(pid)
    if not partner:
        await callback.answer("Не найден")
        return

    mid = await DB.get_match_between(user["id"], pid)
    if not mid:
        await callback.answer("Нет мэтча")
        return

    await DB.mark_read(mid, user["id"])
    match_info = await DB.get_match_info(user["id"], pid)
    msgs = await DB.get_msgs(mid, 12)

    text = f"💬 *Чат с {partner['name']}*\n"
    text += f"{online_status(partner.get('last_active_at'))}\n"
    if match_info:
        text += f"💞 Совместимость: *{match_info['compatibility_score']}%*\n"
    if partner.get("is_safe_mode"):
        text += "🛡 Осторожный формат общения\n"
    text += "\n"

    if msgs:
        for m in msgs:
            sender = "Ты" if m["sender_id"] == user["id"] else partner["name"]
            time_part = m["created_at"].strftime("%H:%M") if m.get("created_at") else ""
            body = m["reaction"] if m.get("reaction") else m.get("text", "")
            text += f"*{sender}:* {body} _{time_part}_\n"
    else:
        opener = match_info["opener_question"] if match_info else random.choice(ICEBREAKERS)
        text += f"_Начни разговор первым(ой)!_\n\n💡 *Подсказка:* {opener}"

    await state.update_data(cp=pid, mi=mid)
    await state.set_state(ChatStates.chatting)
    await callback.message.edit_text(
        text,
        reply_markup=KB.chat_actions(pid),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "chat:quick")
async def chat_quick(callback: CallbackQuery):
    await callback.message.answer("⚡ Быстрые фразы:", reply_markup=KB.quick_phrases())
    await callback.answer()


@rt.callback_query(F.data == "chat:react")
async def chat_react(callback: CallbackQuery):
    await callback.message.answer("🙂 Выбери реакцию:", reply_markup=KB.reactions())
    await callback.answer()


@rt.callback_query(F.data == "chat:back")
async def chat_back(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    data = await state.get_data()
    pid = data.get("cp")
    if not pid:
        return
    await start_chat(callback, state, user)


@rt.callback_query(F.data.startswith("qp:"))
async def quick_phrase_send(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    idx = int(callback.data[3:])
    if idx < 0 or idx >= len(QUICK_PHRASES):
        return

    data = await state.get_data()
    mid = data.get("mi")
    pid = data.get("cp")
    if not mid or not pid:
        await callback.answer("Чат не открыт")
        return

    text = QUICK_PHRASES[idx]
    await DB.send_msg(mid, user["id"], text)
    partner = await DB.get_user_by_id(pid)
    if partner:
        try:
            await callback.bot.send_message(
                partner["telegram_id"],
                f"💬 *{user['name']}:* {text}",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass
    await callback.answer("Фраза отправлена ✨")


@rt.callback_query(F.data.startswith("react:"))
async def reaction_send(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    reaction = callback.data.split(":", 1)[1]
    data = await state.get_data()
    mid = data.get("mi")
    pid = data.get("cp")
    if not mid or not pid:
        await callback.answer("Чат не открыт")
        return

    await DB.send_reaction(mid, user["id"], reaction)
    partner = await DB.get_user_by_id(pid)
    if partner:
        try:
            await callback.bot.send_message(
                partner["telegram_id"],
                f"🙂 *{user['name']}* отправил(а) реакцию: {reaction}",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass
    await callback.answer("Реакция отправлена")


@rt.callback_query(F.data.startswith("gift:"))
async def gift_menu(callback: CallbackQuery):
    pid = int(callback.data[5:])
    await callback.message.answer("🎁 Выбери подарок:", reply_markup=KB.gifts(pid))
    await callback.answer()


@rt.callback_query(F.data.startswith("gift_send:"))
async def gift_send(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    _, _, pid_str, gift_code = callback.data.split(":")
    pid = int(pid_str)
    if gift_code not in GIFT_OPTIONS:
        await callback.answer("Неизвестный подарок")
        return

    target = await DB.get_user_by_id(pid)
    if not target:
        await callback.answer("Пользователь не найден")
        return

    title, emoji = GIFT_OPTIONS[gift_code]
    await DB.create_gift(user["id"], pid, gift_code)

    try:
        await callback.bot.send_message(
            target["telegram_id"],
            f"🎁 *{user['name']}* отправил(а) тебе подарок: {emoji} *{title}*",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        pass

    await callback.answer("Подарок отправлен 🎁")


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

    if not DB.is_vip(user) and user.get("daily_messages_remaining", 0) <= 0:
        await message.answer(T.MSG_LIMIT, parse_mode=ParseMode.MARKDOWN)
        return

    text = normalize_text(message.text)
    if not text:
        return

    await DB.send_msg(mid, user["id"], text)

    if not DB.is_vip(user):
        await DB.dec_messages(user["telegram_id"])

    partner = await DB.get_user_by_id(pid)
    if partner:
        try:
            await message.bot.send_message(
                partner["telegram_id"],
                f"💬 *{user['name']}:* {text}",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

    await message.answer("Отправлено ✨", reply_markup=KB.quick_phrases())


@rt.callback_query(F.data == "bm")
async def back_to_matches(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user:
        return

    matches = await DB.get_matches(user["id"])
    if matches:
        await callback.message.edit_text(
            f"💕 *Симпатии ({len(matches)}):*",
            reply_markup=KB.matches(matches),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await callback.message.edit_text(T.NO_MATCHES, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("vp:"))
async def view_partner_profile(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    pid = int(callback.data[3:])
    partner = await DB.get_user_by_id(pid)
    if not partner:
        await callback.answer("Не найден")
        return

    text = profile_card_text(partner, viewer=user)
    back = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад к чату", callback_data=f"ch:{pid}")]
    ])

    if partner.get("main_photo"):
        await callback.message.answer_photo(
            photo=partner["main_photo"],
            caption=text,
            reply_markup=back,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await callback.message.answer(text, reply_markup=back, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.callback_query(F.data.startswith("um:"))
async def unmatch_partner(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    pid = int(callback.data[3:])
    partner = await DB.get_user_by_id(pid)
    ok = await DB.unmatch(user["id"], pid)
    if ok:
        await callback.message.edit_text(
            f"💔 Мэтч с *{partner['name'] if partner else 'пользователем'}* удалён.",
            reply_markup=KB.back_matches(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await callback.answer("Не удалось удалить мэтч", show_alert=True)


@rt.message(F.text == "💬 Чаты")
async def show_chats(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    matches = await DB.get_matches(user["id"])
    if matches:
        await message.answer("💬 *Диалоги:*", reply_markup=KB.matches(matches), parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer(T.NO_MSGS, parse_mode=ParseMode.MARKDOWN)


@rt.message(F.text == "👀 Гости")
async def show_guests(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    limit = 20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE
    guests = await DB.get_guests(user["id"], limit)
    if not guests:
        await message.answer(T.NO_GUESTS)
        return

    text = "👀 *Гости за последние дни:*\n\n"
    for i, g in enumerate(guests, 1):
        text += f"{i}. {g['name']}, {g['age']} — {g['city']} · {online_status(g.get('last_active_at'))}\n"

    if not DB.is_vip(user):
        text += "\n_С VIP видно всех гостей_"

    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@rt.message(F.text == "👤 Профиль")
async def show_profile(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    badge = DB.get_badge(user)
    role = DB.get_role_tag(user)
    sub = TIER_NAMES.get(user["subscription_tier"], "🍇 Базовый")
    if user.get("subscription_expires_at") and user["subscription_tier"] not in ("free", "wine_forever"):
        sub += f" (до {user['subscription_expires_at'].strftime('%d.%m.%Y')})"

    safe = "\n🛡 Безопасный режим включён" if user.get("is_safe_mode") else ""
    paused = "\n⏸ Анкета на паузе" if user.get("is_paused") else ""
    guest_notify = "\n👀 Уведомления о гостях включены" if user.get("notify_guests") else "\n🔕 Уведомления о гостях выключены"

    extra = ""
    if DB.is_boosted(user):
        extra += f"\n🚀 Буст до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if user.get("boost_count", 0) > 0:
        extra += f"\n🎯 Запас бустов: {user['boost_count']}"

    interests = split_interests(user.get("interests"))
    interests_text = f"\n🎯 Интересы: {', '.join(interests)}" if interests else ""
    filters_text = f"\n🔎 Диапазон: {user['age_from']}-{user['age_to']}"

    text = (
        f"👤 *Мой профиль*\n\n"
        f"{badge}*{user['name']}*, {user['age']}{role}\n"
        f"📍 {user['city']}\n\n"
        f"{user['bio'] or '_Не указано_'}"
        f"{interests_text}"
        f"{safe}"
        f"{paused}"
        f"{guest_notify}"
        f"{filters_text}\n\n"
        f"👀 {user['views_count']} · 💖 {user['likes_received_count']} · 💕 {user['matches_count']}\n"
        f"{sub}{extra}"
    )

    if user.get("main_photo"):
        await message.answer_photo(
            photo=user["main_photo"],
            caption=text,
            reply_markup=KB.profile(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await message.answer(text, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "pv")
async def profile_back(callback: CallbackQuery, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        return
    await callback.answer()
    text = (
        f"👤 *Мой профиль*\n\n"
        f"{DB.get_badge(user)}*{user['name']}*, {user['age']}{DB.get_role_tag(user)}\n"
        f"📍 {user['city']}\n\n"
        f"{user['bio'] or '_Не указано_'}\n\n"
        f"👀 {user['views_count']} · 💖 {user['likes_received_count']} · 💕 {user['matches_count']}\n"
        f"{TIER_NAMES.get(user['subscription_tier'], '🍇 Базовый')}"
    )
    try:
        await callback.message.edit_caption(caption=text, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await callback.message.edit_text(text, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "pf:safe")
async def toggle_safe_mode(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    new_value = not user.get("is_safe_mode", False)
    await DB.update_user(callback.from_user.id, is_safe_mode=new_value)
    await callback.answer("Режим обновлён")


@rt.callback_query(F.data == "pf:pause")
async def toggle_pause_profile(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    new_value = not user.get("is_paused", False)
    await DB.update_user(callback.from_user.id, is_paused=new_value)
    text = "⏸ Анкета поставлена на паузу." if new_value else "▶️ Анкета снова показывается в поиске."
    await callback.message.answer(text)
    await callback.answer("Обновлено")


@rt.callback_query(F.data == "pf:guest_notify")
async def toggle_guest_notify(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    new_value = not user.get("notify_guests", True)
    await DB.update_user(callback.from_user.id, notify_guests=new_value)
    text = "👀 Уведомления о гостях включены." if new_value else "🔕 Уведомления о гостях выключены."
    await callback.message.answer(text)
    await callback.answer("Обновлено")


@rt.callback_query(F.data == "pf:gifts")
async def show_my_gifts(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    gifts = await DB.get_received_gifts(user["id"], 20)
    if not gifts:
        await callback.message.answer("🎁 У тебя пока нет подарков.")
        await callback.answer()
        return

    text = "🎁 *Твои подарки:*\n\n"
    for g in gifts:
        title, emoji = GIFT_OPTIONS.get(g["gift_code"], ("Подарок", "🎁"))
        text += f"{emoji} {title} — от *{g['sender_name']}*\n"
    await callback.message.answer(text, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.callback_query(F.data == "pf:filters")
async def open_filters(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    text = T.FILTERS.format(
        age_from=user["age_from"],
        age_to=user["age_to"],
        city=user["city"],
        looking_for=looking_label(user["looking_for"])
    )
    await callback.message.answer(text, reply_markup=KB.filters(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.callback_query(F.data == "flt:age_from")
async def set_age_from_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите минимальный возраст:")
    await state.set_state(EditStates.age_from)
    await callback.answer()


@rt.message(EditStates.age_from)
async def set_age_from_save(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    try:
        value = int(normalize_text(message.text))
        if not 18 <= value <= 99:
            raise ValueError
    except Exception:
        await message.answer("Нужно число от 18 до 99")
        return

    if value > user["age_to"]:
        await message.answer("Минимальный возраст не может быть больше максимального")
        return

    await DB.update_user(message.from_user.id, age_from=value)
    await state.clear()
    await message.answer("Фильтр обновлён ✨", reply_markup=KB.main())


@rt.callback_query(F.data == "flt:age_to")
async def set_age_to_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите максимальный возраст:")
    await state.set_state(EditStates.age_to)
    await callback.answer()


@rt.message(EditStates.age_to)
async def set_age_to_save(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    try:
        value = int(normalize_text(message.text))
        if not 18 <= value <= 99:
            raise ValueError
    except Exception:
        await message.answer("Нужно число от 18 до 99")
        return

    if value < user["age_from"]:
        await message.answer("Максимальный возраст не может быть меньше минимального")
        return

    await DB.update_user(message.from_user.id, age_to=value)
    await state.clear()
    await message.answer("Фильтр обновлён ✨", reply_markup=KB.main())


@rt.callback_query(F.data == "pf:best")
async def show_best_matches(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    best = await DB.get_best_matches_of_day(user["id"], 5)
    if not best:
        await callback.answer("Пока нет подходящих анкет", show_alert=True)
        return

    text = f"{T.BEST_MATCHES_TITLE}\n\n"
    for i, p in enumerate(best, 1):
        compat = calc_compatibility(user, p)
        text += f"{i}. *{p['name']}*, {p['age']} — {p['city']} · {compat}%\n"
    await callback.message.answer(text, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.callback_query(F.data == "pe")
async def profile_edit_menu(callback: CallbackQuery):
    try:
        await callback.message.edit_caption(
            caption="✏️ *Редактировать профиль:*",
            reply_markup=KB.edit(),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        await callback.message.edit_text("✏️ *Редактировать профиль:*", reply_markup=KB.edit(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "ed:name")
async def edit_name(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Новое имя:")
    await state.set_state(EditStates.edit_name)
    await callback.answer()


@rt.message(EditStates.edit_name)
async def save_name(message: Message, state: FSMContext):
    name = normalize_text(message.text)
    if len(name) < 2 or len(name) > 50:
        await message.answer(T.BAD_NAME)
        return
    await DB.update_user(message.from_user.id, name=name)
    await state.clear()
    await message.answer("Имя обновлено ✨", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:age")
async def edit_age(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Новый возраст:")
    await state.set_state(EditStates.edit_age)
    await callback.answer()


@rt.message(EditStates.edit_age)
async def save_age(message: Message, state: FSMContext):
    try:
        age = int(normalize_text(message.text))
        if not 18 <= age <= 99:
            raise ValueError
    except Exception:
        await message.answer(T.BAD_AGE)
        return
    await DB.update_user(message.from_user.id, age=age)
    await state.clear()
    await message.answer("Возраст обновлён ✨", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:city")
async def edit_city(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Новый город:")
    await state.set_state(EditStates.edit_city)
    await callback.answer()


@rt.message(EditStates.edit_city)
async def save_city(message: Message, state: FSMContext):
    await DB.update_user(message.from_user.id, city=normalize_text(message.text).title())
    await state.clear()
    await message.answer("Город обновлён ✨", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:bio")
async def edit_bio(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Новое описание:")
    await state.set_state(EditStates.edit_bio)
    await callback.answer()


@rt.message(EditStates.edit_bio)
async def save_bio(message: Message, state: FSMContext):
    await DB.update_user(message.from_user.id, bio=normalize_text(message.text)[:config.MAX_BIO_LEN])
    await state.clear()
    await message.answer("Описание обновлено ✨", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:photo")
async def edit_photo(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Отправь новое фото:")
    await state.set_state(EditStates.add_photo)
    await callback.answer()


@rt.message(EditStates.add_photo, F.photo)
async def save_photo(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    pid = message.photo[-1].file_id
    photos = user.get("photos", "")
    photos = f"{photos},{pid}" if photos else pid
    await DB.update_user(message.from_user.id, photos=photos, main_photo=pid)
    await state.clear()
    await message.answer("Фото обновлено 📸", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:interests")
async def edit_interests_start(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    selected = split_interests(user.get("interests"))
    await state.update_data(selected_interests=selected)
    await state.set_state(EditStates.edit_interests)
    await callback.message.answer("🎯 Выбери интересы:", reply_markup=KB.interests(selected), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.callback_query(EditStates.edit_interests, F.data.startswith("int:"))
async def edit_interests_save(callback: CallbackQuery, state: FSMContext):
    action = callback.data[4:]
    data = await state.get_data()
    selected = data.get("selected_interests", [])

    if action == "skip":
        await DB.update_user(callback.from_user.id, interests="")
        await state.clear()
        await callback.message.edit_text("Интересы очищены ✨")
        await callback.answer()
        return

    if action == "done":
        await DB.update_user(callback.from_user.id, interests=join_interests(selected))
        await state.clear()
        await callback.message.edit_text("Интересы обновлены ✨")
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
    text = "🎯 Выбери интересы:"
    if selected:
        text += f"\n\nВыбрано: {', '.join(selected)}"
    await callback.message.edit_text(text, reply_markup=KB.interests(selected), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.message(F.text == "🛍 Магазин")
async def shop_menu(message: Message):
    await message.answer(T.SHOP, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "sh:mn")
async def shop_main(callback: CallbackQuery):
    await callback.message.edit_text(T.SHOP, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "sh:compare")
async def shop_compare(callback: CallbackQuery):
    await callback.message.edit_text(T.COMPARE, reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "sh:subs")
async def shop_subs(callback: CallbackQuery):
    await callback.message.edit_text("🍷 *Выбери тариф:*", reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "tf:wine_spark")
async def tf_spark(callback: CallbackQuery):
    await callback.message.edit_text(T.SPARK, reply_markup=KB.buy_spark(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "tf:wine_rose")
async def tf_rose(callback: CallbackQuery):
    await callback.message.edit_text(T.ROSE, reply_markup=KB.buy_rose(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "tf:wine_grand")
async def tf_grand(callback: CallbackQuery):
    await callback.message.edit_text(T.GRAND, reply_markup=KB.buy_grand(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "tf:wine_forever")
async def tf_forever(callback: CallbackQuery):
    await callback.message.edit_text(T.FOREVER, reply_markup=KB.buy_forever(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "sh:boost")
async def shop_boost(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        await callback.answer("Ошибка")
        return

    has = user.get("boost_count", 0) > 0
    status = ""
    if DB.is_boosted(user):
        status += f"\n🚀 Буст активен до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if has:
        status += f"\n🎯 В запасе: {user['boost_count']}"
    if not has and not DB.is_boosted(user):
        status = "\nБустов пока нет."

    await callback.message.edit_text(T.BOOST_INFO.format(status=status), reply_markup=KB.boost_menu(has), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "bo:act")
async def activate_boost(callback: CallbackQuery, user: Optional[Dict]):
    if not user or user.get("boost_count", 0) <= 0:
        await callback.answer("Нет бустов", show_alert=True)
        return

    ok = await DB.use_boost(user["id"])
    if ok:
        fresh = await DB.get_user(callback.from_user.id)
        await animate_text(callback.message, ["🚀", "🚀✨", "🚀✨🍷"])
        await callback.message.edit_text(
            f"🚀 *Буст активирован!*\n\nДо {fresh['boost_expires_at'].strftime('%d.%m %H:%M')}\n"
            f"Осталось бустов: {fresh['boost_count']}",
            reply_markup=KB.shop(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await callback.answer("Ошибка", show_alert=True)


@rt.callback_query(F.data.startswith("by:"))
async def handle_buy(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        await callback.answer("Ошибка")
        return

    parts = callback.data.split(":")
    product = parts[1]
    param = int(parts[2])
    amount = int(parts[3])

    if product == "boost":
        result = await Pay.create(user, "boost", count=param, amount=amount)
    else:
        result = await Pay.create(user, "subscription", tier=product, dur=param, amount=amount)

    if "error" in result:
        await callback.answer(result["error"], show_alert=True)
        return

    text = f"*Покупка*\n\nСумма: *{amount / 100:.0f}₽*\n\n1. Оплати\n2. Нажми «Проверить оплату»"
    await callback.message.edit_text(text, reply_markup=KB.pay(result["url"], result["pid"]), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("ck:"))
async def check_payment(callback: CallbackQuery):
    pid = int(callback.data[3:])
    res = await Pay.check(pid)

    if res["status"] == "succeeded":
        if res.get("type") == "boost":
            await animate_text(callback.message, ["💳", "💳✨", "💳✨🚀"])
            await callback.message.edit_text(f"*{res.get('count', 1)} буст(ов) добавлено!*", parse_mode=ParseMode.MARKDOWN)
        else:
            await animate_text(callback.message, ["💳", "💳✨", "💳✨🍷"])
            await callback.message.edit_text("*Подписка активирована!*", parse_mode=ParseMode.MARKDOWN)
        await callback.message.answer("Спасибо 💕", reply_markup=KB.main())
    elif res["status"] == "pending":
        await callback.answer("Платёж ещё обрабатывается")
    else:
        await callback.answer("Ошибка оплаты", show_alert=True)


@rt.callback_query(F.data == "sh:promo")
async def promo_input(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🎁 *Введи промокод:*", parse_mode=ParseMode.MARKDOWN)
    await state.update_data(promo_user_mode=True)
    await state.set_state(AdminStates.promo_code)
    await callback.answer()


@rt.message(F.text == "❓ FAQ")
async def show_faq(message: Message):
    await message.answer(T.FAQ, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("rp:"))
async def start_report(callback: CallbackQuery, state: FSMContext):
    await state.update_data(rp_id=int(callback.data[3:]))
    try:
        await callback.message.edit_caption(caption="🚨 *Причина жалобы:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await callback.message.edit_text("🚨 *Причина жалобы:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("rr:"))
async def save_report(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    data = await state.get_data()
    rid = data.get("rp_id")
    if rid:
        await DB.create_report(user["id"], rid, callback.data[3:])
    await state.clear()

    try:
        await callback.message.edit_caption("Жалоба отправлена")
    except Exception:
        await callback.message.edit_text("Жалоба отправлена")

    profiles = await DB.search_profiles(user, 1)
    if profiles:
        await show_card(callback.message, profiles[0], user, bot=callback.bot)


@rt.callback_query(F.data == "mn")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer("Главное меню", reply_markup=KB.main())


async def background_cleanup():
    while True:
        try:
            await DB.cleanup_old_guests()
        except Exception as e:
            logger.error("cleanup error: %s", e)
        await asyncio.sleep(60 * 60 * 6)


async def main():
    await init_db()

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())
    dp.include_router(rt)

    logger.info("%s v4.3 starting...", BOT_NAME)

    cleanup_task = asyncio.create_task(background_cleanup())

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        cleanup_task.cancel()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
