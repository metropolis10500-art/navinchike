"""
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🍷 ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v4.0
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Запуск:
  pip install aiogram aiosqlite sqlalchemy yookassa python-dotenv
  python bot.py
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import os
import uuid
import logging
import json
import random
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Update
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean, DateTime,
    Text, ForeignKey, Enum as SQLEnum, Float,
    select, update, func, and_, or_, desc, delete
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

try:
    from yookassa import Configuration, Payment as YooPayment
    from yookassa.domain.common import ConfirmationType
    YOOKASSA_AVAILABLE = True
except ImportError:
    YOOKASSA_AVAILABLE = False

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

BOT_NAME = "🍷 Знакомства на Винчике"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///dating_bot.db")
    YOOKASSA_SHOP_ID: str = os.getenv("YOOKASSA_SHOP_ID", "")
    YOOKASSA_SECRET_KEY: str = os.getenv("YOOKASSA_SECRET_KEY", "")
    DOMAIN: str = os.getenv("DOMAIN", "https://yourdomain.ru")

    FREE_DAILY_LIKES: int = 30
    FREE_DAILY_MESSAGES: int = 10
    FREE_GUESTS_VISIBLE: int = 3

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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#              ENUMS & MODELS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class Gender(str, Enum):
    MALE = "male"
    FEMALE = "female"


class LookingFor(str, Enum):
    MALE = "male"
    FEMALE = "female"
    BOTH = "both"


class SubscriptionTier(str, Enum):
    FREE = "free"
    WINE_GLASS = "wine_glass"       # Бокал Вина (бывший VIP Light)
    WINE_BOTTLE = "wine_bottle"     # Бутылка Вина (бывший VIP Standard)
    SOMMELIER = "sommelier"         # Сомелье (бывший VIP Pro)
    WINE_CELLAR = "wine_cellar"     # Винный Погреб (бывший VIP Lifetime)


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
    looking_for = Column(SQLEnum(LookingFor), default=LookingFor.BOTH)
    age_from = Column(Integer, default=18)
    age_to = Column(Integer, default=99)
    photos = Column(Text, default="")
    main_photo = Column(String(255), nullable=True)

    is_active = Column(Boolean, default=True)
    is_banned = Column(Boolean, default=False)
    is_verified = Column(Boolean, default=False)
    is_profile_complete = Column(Boolean, default=False)

    subscription_tier = Column(SQLEnum(SubscriptionTier), default=SubscriptionTier.FREE)
    subscription_expires_at = Column(DateTime, nullable=True)

    daily_likes_remaining = Column(Integer, default=30)
    daily_messages_remaining = Column(Integer, default=10)
    last_limits_reset = Column(DateTime, nullable=True)

    boost_expires_at = Column(DateTime, nullable=True)
    boost_count = Column(Integer, default=0)

    views_count = Column(Integer, default=0)
    likes_received_count = Column(Integer, default=0)
    matches_count = Column(Integer, default=0)

    # Compatibility score cache
    interests = Column(Text, default="")

    referral_code = Column(String(20), unique=True, nullable=True)
    referred_by = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    last_active_at = Column(DateTime, default=datetime.utcnow)


class Like(Base):
    __tablename__ = "likes"
    id = Column(Integer, primary_key=True)
    from_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    to_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    is_super_like = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Match(Base):
    __tablename__ = "matches"
    id = Column(Integer, primary_key=True)
    user1_id = Column(Integer, ForeignKey("users.id"), index=True)
    user2_id = Column(Integer, ForeignKey("users.id"), index=True)
    is_active = Column(Boolean, default=True)
    compatibility_score = Column(Float, default=0.0)
    last_message_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatMessage(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey("matches.id"), index=True)
    sender_id = Column(Integer, ForeignKey("users.id"))
    text = Column(Text, nullable=True)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class GuestVisit(Base):
    __tablename__ = "guest_visits"
    id = Column(Integer, primary_key=True)
    visitor_id = Column(Integer, ForeignKey("users.id"))
    visited_user_id = Column(Integer, ForeignKey("users.id"), index=True)
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                FSM STATES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RegStates(StatesGroup):
    name = State()
    age = State()
    gender = State()
    city = State()
    photo = State()
    bio = State()
    looking_for = State()


class EditStates(StatesGroup):
    edit_name = State()
    edit_age = State()
    edit_city = State()
    edit_bio = State()
    add_photo = State()


class ChatStates(StatesGroup):
    chatting = State()


class PromoInputState(StatesGroup):
    waiting_code = State()


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
    report_action = State()
    ban_reason = State()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#             COMPATIBILITY ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CompatibilityEngine:
    """Расчёт совместимости двух анкет"""

    @staticmethod
    def calculate(user1: Dict, user2: Dict) -> float:
        score = 0.0
        max_score = 0.0

        # 1. Город (30 баллов)
        max_score += 30
        if user1.get("city", "").lower() == user2.get("city", "").lower():
            score += 30

        # 2. Возрастная совместимость (25 баллов)
        max_score += 25
        age1 = user1.get("age", 25)
        age2 = user2.get("age", 25)
        age_diff = abs(age1 - age2)
        if age_diff <= 2:
            score += 25
        elif age_diff <= 5:
            score += 20
        elif age_diff <= 10:
            score += 12
        elif age_diff <= 15:
            score += 5

        # 3. Взаимный поиск (20 баллов)
        max_score += 20
        lf1 = user1.get("looking_for", "both")
        lf2 = user2.get("looking_for", "both")
        g1 = user1.get("gender")
        g2 = user2.get("gender")

        mutual = True
        if lf1 != "both" and lf1 != g2:
            mutual = False
        if lf2 != "both" and lf2 != g1:
            mutual = False
        if mutual:
            score += 20

        # 4. Активность (15 баллов)
        max_score += 15
        la = user2.get("last_active_at")
        if la:
            hours_ago = (datetime.utcnow() - la).total_seconds() / 3600
            if hours_ago < 1:
                score += 15
            elif hours_ago < 6:
                score += 12
            elif hours_ago < 24:
                score += 8
            elif hours_ago < 72:
                score += 3

        # 5. Заполненность профиля (10 баллов)
        max_score += 10
        if user2.get("bio"):
            score += 5
        if user2.get("main_photo"):
            score += 5

        return round((score / max_score) * 100, 1) if max_score > 0 else 0.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#               ANIMATIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class Anim:
    """Анимированные текстовые сообщения"""

    WINE_POUR = ["🍷", "🍷🍷", "🍷🍷🍷", "🥂 Наливаем..."]
    HEARTS = ["💕", "💕💕", "💕💕💕", "💞 Это мэтч!"]
    SEARCH = ["🔍", "🔍👀", "🔍👀✨", "🍷 Ищем анкеты..."]
    LOADING = ["⏳", "⌛", "⏳", "🍷 Готово!"]

    @staticmethod
    async def animate(message: Message, frames: List[str], delay: float = 0.4) -> Message:
        """Проигрывает анимацию через редактирование сообщения"""
        msg = await message.answer(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(delay)
            try:
                await msg.edit_text(frame)
            except Exception:
                pass
        return msg

    @staticmethod
    async def match_animation(message: Message, name: str) -> None:
        """Красивая анимация мэтча"""
        frames = [
            "💕",
            "💕💕💕",
            "🎉💕🎉💕🎉",
            f"🍷✨ *Взаимная симпатия!* ✨🍷\n\n💕 Вы с *{name}* понравились друг другу!",
        ]
        msg = await message.answer(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(0.5)
            try:
                await msg.edit_text(frame, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass

    @staticmethod
    async def like_animation(callback: CallbackQuery) -> None:
        """Быстрая анимация лайка"""
        await callback.answer("❤️ Нравится!", show_alert=False)

    @staticmethod
    async def dislike_animation(callback: CallbackQuery) -> None:
        """Быстрая анимация дизлайка"""
        await callback.answer("👋 Следующая анкета", show_alert=False)

    @staticmethod
    async def boost_animation(message: Message) -> Message:
        frames = [
            "🚀",
            "🚀✨",
            "🚀✨🍷",
            "🚀✨🍷 *Буст активирован!*",
        ]
        msg = await message.answer(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(0.4)
            try:
                await msg.edit_text(frame, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        return msg

    @staticmethod
    async def payment_success_animation(message: Message) -> Message:
        frames = [
            "💳",
            "💳 ✅",
            "💳 ✅ 🎉",
            "🍷 *Оплата прошла успешно!* 🎉",
        ]
        msg = await message.answer(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(0.4)
            try:
                await msg.edit_text(frame, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        return msg

    @staticmethod
    def get_wine_emoji() -> str:
        """Случайный винный эмодзи"""
        return random.choice(["🍷", "🥂", "🍇", "🍾", "🏆"])

    @staticmethod
    def compatibility_bar(score: float) -> str:
        """Визуальная полоса совместимости"""
        filled = int(score / 10)
        empty = 10 - filled
        bar = "🟣" * filled + "⚪" * empty
        if score >= 80:
            emoji = "🔥"
        elif score >= 60:
            emoji = "💕"
        elif score >= 40:
            emoji = "👍"
        else:
            emoji = "🤔"
        return f"{emoji} {bar} {score:.0f}%"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#               DB SERVICE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
            "looking_for": u.looking_for.value if u.looking_for else "both",
            "age_from": u.age_from,
            "age_to": u.age_to,
            "photos": u.photos or "",
            "main_photo": u.main_photo,
            "is_active": u.is_active,
            "is_banned": u.is_banned,
            "is_verified": u.is_verified,
            "is_profile_complete": u.is_profile_complete,
            "subscription_tier": u.subscription_tier.value if u.subscription_tier else "free",
            "subscription_expires_at": u.subscription_expires_at,
            "daily_likes_remaining": u.daily_likes_remaining or 30,
            "daily_messages_remaining": u.daily_messages_remaining or 10,
            "last_limits_reset": u.last_limits_reset,
            "boost_expires_at": u.boost_expires_at,
            "boost_count": u.boost_count or 0,
            "views_count": u.views_count or 0,
            "likes_received_count": u.likes_received_count or 0,
            "matches_count": u.matches_count or 0,
            "interests": u.interests or "",
            "created_at": u.created_at,
            "last_active_at": u.last_active_at,
        }

    @staticmethod
    def is_vip(u: Dict) -> bool:
        t = u.get("subscription_tier", "free")
        if t == "wine_cellar":
            return True
        if t == "free":
            return False
        e = u.get("subscription_expires_at")
        return e is not None and e > datetime.utcnow()

    @staticmethod
    def is_boosted(u: Dict) -> bool:
        e = u.get("boost_expires_at")
        return e is not None and e > datetime.utcnow()

    @staticmethod
    def is_creator(u: Dict) -> bool:
        return u.get("telegram_id") in config.CREATOR_IDS

    @staticmethod
    def is_admin(u: Dict) -> bool:
        return u.get("telegram_id") in config.ADMIN_IDS

    @staticmethod
    def get_badge(u: Dict) -> str:
        if DB.is_creator(u):
            return "👑🍷 "
        tier = u.get("subscription_tier", "free")
        if tier == "wine_cellar":
            return "🏆 "
        if tier == "sommelier":
            return "🎖️ "
        if DB.is_vip(u):
            return "🍷 "
        if u.get("is_verified"):
            return "✅ "
        return ""

    @staticmethod
    def get_role_tag(u: Dict) -> str:
        if DB.is_creator(u):
            return " · 👑 Создатель"
        if DB.is_admin(u):
            return " · 🛡️ Админ"
        return ""

    @staticmethod
    def get_tier_limits(tier: str) -> Dict:
        """Возвращает лимиты для каждого тарифа"""
        limits = {
            "free": {"likes": 30, "messages": 10, "guests": 3, "boosts": 0, "super_likes": 0},
            "wine_glass": {"likes": 100, "messages": 999999, "guests": 10, "boosts": 0, "super_likes": 0},
            "wine_bottle": {"likes": 999999, "messages": 999999, "guests": 999, "boosts": 1, "super_likes": 0},
            "sommelier": {"likes": 999999, "messages": 999999, "guests": 999, "boosts": 3, "super_likes": 5},
            "wine_cellar": {"likes": 999999, "messages": 999999, "guests": 999, "boosts": 5, "super_likes": 10},
        }
        return limits.get(tier, limits["free"])

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
    async def create_user(tg_id: int, username: str = None) -> Dict:
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
        async with async_session_maker() as s:
            kw["updated_at"] = datetime.utcnow()
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
            tier = u.get("subscription_tier", "free")
            limits = DB.get_tier_limits(tier)
            return await DB.update_user(
                u["telegram_id"],
                daily_likes_remaining=limits["likes"],
                daily_messages_remaining=limits["messages"],
                last_limits_reset=now,
                last_active_at=now
            )
        await DB.update_user(u["telegram_id"], last_active_at=now)
        return u

    @staticmethod
    async def search_profiles(u: Dict, limit: int = 1) -> List[Dict]:
        async with async_session_maker() as s:
            # Получаем ID уже лайкнутых
            liked = await s.execute(
                select(Like.to_user_id).where(Like.from_user_id == u["id"])
            )
            exc = [r[0] for r in liked.fetchall()] + [u["id"]]

            # Базовый фильтр
            q = select(User).where(and_(
                User.is_active == True,
                User.is_banned == False,
                User.is_profile_complete == True,
                User.id.not_in(exc),
                User.age >= u["age_from"],
                User.age <= u["age_to"]
            ))

            # Фильтр по полу
            lf = u.get("looking_for", "both")
            if lf == "male":
                q = q.where(User.gender == Gender.MALE)
            elif lf == "female":
                q = q.where(User.gender == Gender.FEMALE)

            # Сначала тот же город, потом другие
            # Буст в приоритете, потом активность
            q = q.order_by(
                (User.city == u["city"]).desc(),
                User.boost_expires_at.desc().nullslast(),
                User.last_active_at.desc()
            ).limit(limit * 3)  # Берём больше для сортировки по совместимости

            r = await s.execute(q)
            candidates = [DB._to_dict(x) for x in r.scalars().all()]

            # Сортируем по совместимости
            for c in candidates:
                c["_compat"] = CompatibilityEngine.calculate(u, c)
            candidates.sort(key=lambda x: x["_compat"], reverse=True)

            return candidates[:limit]

    @staticmethod
    async def add_like(fid: int, tid: int, is_super: bool = False) -> Dict:
        """Возвращает {"is_match": bool, "match_id": int|None, "compatibility": float}"""
        async with async_session_maker() as s:
            # Проверяем дубль
            ex = await s.execute(
                select(Like).where(and_(Like.from_user_id == fid, Like.to_user_id == tid))
            )
            if ex.scalar_one_or_none():
                return {"is_match": False, "match_id": None, "compatibility": 0}

            s.add(Like(from_user_id=fid, to_user_id=tid, is_super_like=is_super))
            await s.execute(
                update(User).where(User.id == tid)
                .values(likes_received_count=User.likes_received_count + 1)
            )

            # Проверяем взаимность
            rev = await s.execute(
                select(Like).where(and_(Like.from_user_id == tid, Like.to_user_id == fid))
            )
            is_match = rev.scalar_one_or_none() is not None

            match_id = None
            compatibility = 0.0

            if is_match:
                # Рассчитываем совместимость
                u1_r = await s.execute(select(User).where(User.id == fid))
                u2_r = await s.execute(select(User).where(User.id == tid))
                u1 = u1_r.scalar_one_or_none()
                u2 = u2_r.scalar_one_or_none()
                if u1 and u2:
                    compatibility = CompatibilityEngine.calculate(
                        DB._to_dict(u1), DB._to_dict(u2)
                    )

                m = Match(user1_id=fid, user2_id=tid, compatibility_score=compatibility)
                s.add(m)
                await s.execute(
                    update(User).where(User.id.in_([fid, tid]))
                    .values(matches_count=User.matches_count + 1)
                )
                await s.flush()
                match_id = m.id

            await s.commit()
            return {"is_match": is_match, "match_id": match_id, "compatibility": compatibility}

    @staticmethod
    async def get_matches(uid: int) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(Match).where(and_(
                    or_(Match.user1_id == uid, Match.user2_id == uid),
                    Match.is_active == True
                )).order_by(Match.last_message_at.desc().nullslast())
            )
            out = []
            for m in r.scalars().all():
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                pr = await s.execute(select(User).where(User.id == pid))
                p = pr.scalar_one_or_none()
                if p:
                    # Считаем непрочитанные
                    unread_r = await s.execute(
                        select(func.count(ChatMessage.id)).where(and_(
                            ChatMessage.match_id == m.id,
                            ChatMessage.sender_id != uid,
                            ChatMessage.is_read == False
                        ))
                    )
                    unread = unread_r.scalar() or 0

                    out.append({
                        "match_id": m.id,
                        "user_id": p.id,
                        "telegram_id": p.telegram_id,
                        "name": p.name,
                        "age": p.age,
                        "photo": p.main_photo,
                        "compatibility": m.compatibility_score or 0,
                        "unread": unread,
                        "last_message_at": m.last_message_at,
                    })
            return out

    @staticmethod
    async def get_match_between(u1: int, u2: int) -> Optional[int]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(Match.id).where(and_(
                    Match.is_active == True,
                    or_(
                        and_(Match.user1_id == u1, Match.user2_id == u2),
                        and_(Match.user1_id == u2, Match.user2_id == u1)
                    )
                ))
            )
            row = r.first()
            return row[0] if row else None

    @staticmethod
    async def send_msg(mid: int, sid: int, txt: str):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=txt))
            await s.execute(
                update(Match).where(Match.id == mid)
                .values(last_message_at=datetime.utcnow())
            )
            await s.commit()

    @staticmethod
    async def get_msgs(mid: int, limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(ChatMessage).where(ChatMessage.match_id == mid)
                .order_by(ChatMessage.created_at.desc()).limit(limit)
            )
            return [
                {"sender_id": m.sender_id, "text": m.text, "created_at": m.created_at}
                for m in reversed(r.scalars().all())
            ]

    @staticmethod
    async def mark_messages_read(mid: int, reader_id: int):
        """Помечает сообщения как прочитанные"""
        async with async_session_maker() as s:
            await s.execute(
                update(ChatMessage).where(and_(
                    ChatMessage.match_id == mid,
                    ChatMessage.sender_id != reader_id,
                    ChatMessage.is_read == False
                )).values(is_read=True)
            )
            await s.commit()

    @staticmethod
    async def get_unread(uid: int) -> int:
        async with async_session_maker() as s:
            ms = await s.execute(
                select(Match.id).where(
                    or_(Match.user1_id == uid, Match.user2_id == uid)
                )
            )
            mids = [m[0] for m in ms.fetchall()]
            if not mids:
                return 0
            r = await s.execute(
                select(func.count(ChatMessage.id)).where(and_(
                    ChatMessage.match_id.in_(mids),
                    ChatMessage.sender_id != uid,
                    ChatMessage.is_read == False
                ))
            )
            return r.scalar() or 0

    @staticmethod
    async def add_guest(vid: int, uid: int):
        if vid == uid:
            return
        async with async_session_maker() as s:
            s.add(GuestVisit(visitor_id=vid, visited_user_id=uid))
            await s.execute(
                update(User).where(User.id == uid)
                .values(views_count=User.views_count + 1)
            )
            await s.commit()

    @staticmethod
    async def get_guests(uid: int, limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(GuestVisit.visitor_id, func.max(GuestVisit.created_at).label("last_visit"))
                .where(GuestVisit.visited_user_id == uid)
                .group_by(GuestVisit.visitor_id)
                .order_by(desc("last_visit"))
                .limit(limit)
            )
            rows = r.fetchall()
            if not rows:
                return []
            ids = [row[0] for row in rows]
            us = await s.execute(select(User).where(User.id.in_(ids)))
            user_map = {u.id: DB._to_dict(u) for u in us.scalars().all()}
            # Сохраняем порядок
            return [user_map[uid] for uid in ids if uid in user_map]

    @staticmethod
    async def dec_likes(tg_id: int):
        async with async_session_maker() as s:
            await s.execute(
                update(User).where(User.telegram_id == tg_id)
                .values(daily_likes_remaining=User.daily_likes_remaining - 1)
            )
            await s.commit()

    @staticmethod
    async def use_boost(uid: int) -> bool:
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u or (u.boost_count or 0) <= 0:
                return False
            now = datetime.utcnow()
            if u.boost_expires_at and u.boost_expires_at > now:
                ne = u.boost_expires_at + timedelta(hours=24)
            else:
                ne = now + timedelta(hours=24)
            await s.execute(
                update(User).where(User.id == uid)
                .values(boost_count=User.boost_count - 1, boost_expires_at=ne)
            )
            await s.commit()
            return True

    @staticmethod
    async def add_boosts(uid: int, c: int):
        async with async_session_maker() as s:
            await s.execute(
                update(User).where(User.id == uid)
                .values(boost_count=User.boost_count + c)
            )
            await s.commit()

    @staticmethod
    async def create_report(rid: int, ruid: int, reason: str):
        async with async_session_maker() as s:
            s.add(Report(reporter_id=rid, reported_user_id=ruid, reason=reason))
            await s.commit()

    @staticmethod
    async def unmatch(uid: int, match_id: int) -> bool:
        """Удаляет мэтч"""
        async with async_session_maker() as s:
            r = await s.execute(
                select(Match).where(and_(
                    Match.id == match_id,
                    Match.is_active == True,
                    or_(Match.user1_id == uid, Match.user2_id == uid)
                ))
            )
            m = r.scalar_one_or_none()
            if not m:
                return False
            await s.execute(
                update(Match).where(Match.id == match_id)
                .values(is_active=False)
            )
            await s.execute(
                update(User).where(User.id.in_([m.user1_id, m.user2_id]))
                .values(matches_count=func.greatest(User.matches_count - 1, 0))
            )
            await s.commit()
            return True

    # ═══ ADMIN DB ═══

    @staticmethod
    async def get_stats() -> Dict:
        async with async_session_maker() as s:
            total = (await s.execute(select(func.count(User.id)))).scalar() or 0
            complete = (await s.execute(
                select(func.count(User.id)).where(User.is_profile_complete == True)
            )).scalar() or 0

            now = datetime.utcnow()
            day_ago = now - timedelta(days=1)
            week_ago = now - timedelta(days=7)
            month_ago = now - timedelta(days=30)

            dau = (await s.execute(
                select(func.count(User.id)).where(User.last_active_at > day_ago)
            )).scalar() or 0
            wau = (await s.execute(
                select(func.count(User.id)).where(User.last_active_at > week_ago)
            )).scalar() or 0
            mau = (await s.execute(
                select(func.count(User.id)).where(User.last_active_at > month_ago)
            )).scalar() or 0

            vip = (await s.execute(
                select(func.count(User.id)).where(User.subscription_tier != SubscriptionTier.FREE)
            )).scalar() or 0
            banned = (await s.execute(
                select(func.count(User.id)).where(User.is_banned == True)
            )).scalar() or 0
            today_reg = (await s.execute(
                select(func.count(User.id)).where(User.created_at > day_ago)
            )).scalar() or 0

            total_matches = (await s.execute(select(func.count(Match.id)))).scalar() or 0
            total_msgs = (await s.execute(select(func.count(ChatMessage.id)))).scalar() or 0
            total_likes = (await s.execute(select(func.count(Like.id)))).scalar() or 0

            revenue = (await s.execute(
                select(func.sum(Payment.amount))
                .where(Payment.status == PaymentStatus.SUCCEEDED)
            )).scalar() or 0
            month_rev = (await s.execute(
                select(func.sum(Payment.amount)).where(and_(
                    Payment.status == PaymentStatus.SUCCEEDED,
                    Payment.paid_at > month_ago
                ))
            )).scalar() or 0

            pending_reports = (await s.execute(
                select(func.count(Report.id)).where(Report.status == "pending")
            )).scalar() or 0

            # Средняя совместимость мэтчей
            avg_compat = (await s.execute(
                select(func.avg(Match.compatibility_score))
                .where(Match.compatibility_score > 0)
            )).scalar() or 0

            return {
                "total": total, "complete": complete,
                "dau": dau, "wau": wau, "mau": mau,
                "vip": vip, "banned": banned, "today_reg": today_reg,
                "matches": total_matches, "messages": total_msgs, "likes": total_likes,
                "revenue": revenue / 100, "month_revenue": month_rev / 100,
                "pending_reports": pending_reports,
                "avg_compatibility": avg_compat,
                "conversion": (vip / complete * 100) if complete > 0 else 0,
            }

    @staticmethod
    async def search_users(query: str) -> List[Dict]:
        async with async_session_maker() as s:
            if query.isdigit():
                r = await s.execute(
                    select(User).where(
                        or_(User.id == int(query), User.telegram_id == int(query))
                    )
                )
            else:
                q = query.lstrip("@")
                r = await s.execute(
                    select(User).where(or_(
                        User.username.ilike(f"%{q}%"),
                        User.name.ilike(f"%{q}%")
                    )).limit(10)
                )
            return [DB._to_dict(u) for u in r.scalars().all()]

    @staticmethod
    async def get_all_user_ids(filter_type: str = "all") -> List[int]:
        async with async_session_maker() as s:
            q = select(User.telegram_id).where(
                and_(User.is_active == True, User.is_banned == False)
            )
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
            r = await s.execute(
                select(Report).where(Report.status == "pending")
                .order_by(Report.created_at.desc()).limit(limit)
            )
            out = []
            for rep in r.scalars().all():
                reporter = await DB.get_user_by_id(rep.reporter_id)
                reported = await DB.get_user_by_id(rep.reported_user_id)
                out.append({
                    "id": rep.id, "reason": rep.reason,
                    "created_at": rep.created_at,
                    "reporter": reporter, "reported": reported,
                })
            return out

    @staticmethod
    async def resolve_report(report_id: int, action: str, notes: str = ""):
        async with async_session_maker() as s:
            await s.execute(
                update(Report).where(Report.id == report_id)
                .values(status=action, admin_notes=notes, resolved_at=datetime.utcnow())
            )
            await s.commit()

    @staticmethod
    async def get_recent_payments(limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(Payment).order_by(Payment.created_at.desc()).limit(limit)
            )
            out = []
            for p in r.scalars().all():
                u = await DB.get_user_by_id(p.user_id)
                out.append({
                    "id": p.id, "amount": p.amount / 100,
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
            s.add(PromoCode(
                code=code.upper(), tier=tier,
                duration_days=days, max_uses=max_uses
            ))
            await s.commit()

    @staticmethod
    async def use_promo(user_id: int, code: str) -> Dict:
        async with async_session_maker() as s:
            r = await s.execute(
                select(PromoCode).where(and_(
                    PromoCode.code == code.upper(),
                    PromoCode.is_active == True
                ))
            )
            promo = r.scalar_one_or_none()
            if not promo:
                return {"error": "❌ Промокод не найден"}
            if promo.used_count >= promo.max_uses:
                return {"error": "❌ Промокод исчерпан"}

            used = await s.execute(
                select(PromoUse).where(and_(
                    PromoUse.promo_id == promo.id,
                    PromoUse.user_id == user_id
                ))
            )
            if used.scalar_one_or_none():
                return {"error": "❌ Ты уже использовал этот промокод"}

            s.add(PromoUse(promo_id=promo.id, user_id=user_id))
            await s.execute(
                update(PromoCode).where(PromoCode.id == promo.id)
                .values(used_count=PromoCode.used_count + 1)
            )
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
            if te == SubscriptionTier.WINE_CELLAR:
                exp = None
            elif u.subscription_expires_at and u.subscription_expires_at > now:
                exp = u.subscription_expires_at + timedelta(days=days)
            else:
                exp = now + timedelta(days=days)
            await s.execute(
                update(User).where(User.id == uid)
                .values(subscription_tier=te, subscription_expires_at=exp)
            )
            await s.commit()

    @staticmethod
    async def get_total_users() -> int:
        async with async_session_maker() as s:
            r = await s.execute(
                select(func.count(User.id)).where(User.is_profile_complete == True)
            )
            return r.scalar() or 0

    @staticmethod
    async def create_payment(uid, yid, amount, desc, ptype, ptier=None, pdur=None, pcount=None) -> int:
        async with async_session_maker() as s:
            p = Payment(
                user_id=uid, yookassa_payment_id=yid, amount=amount,
                description=desc, product_type=ptype,
                product_tier=ptier, product_duration=pdur, product_count=pcount
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
                    "id": p.id, "user_id": p.user_id,
                    "yookassa_payment_id": p.yookassa_payment_id,
                    "status": p.status.value, "product_type": p.product_type,
                    "product_tier": p.product_tier,
                    "product_duration": p.product_duration,
                    "product_count": p.product_count,
                }
            return None

    @staticmethod
    async def update_payment_status(pid: int, st: PaymentStatus):
        async with async_session_maker() as s:
            v = {"status": st}
            if st == PaymentStatus.SUCCEEDED:
                v["paid_at"] = datetime.utcnow()
            await s.execute(update(Payment).where(Payment.id == pid).values(**v))
            await s.commit()

    @staticmethod
    async def log_broadcast(admin_id, text, target, sent, failed):
        async with async_session_maker() as s:
            s.add(BroadcastLog(
                admin_id=admin_id, message_text=text,
                target_filter=target, sent_count=sent, failed_count=failed
            ))
            await s.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                  TEXTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TIER_NAMES = {
    "free": "🆓 Бесплатный",
    "wine_glass": "🥂 Бокал Вина",
    "wine_bottle": "🍾 Бутылка Вина",
    "sommelier": "🎖️ Сомелье",
    "wine_cellar": "🏆 Винный Погреб",
}

TIER_EMOJI = {
    "free": "🆓",
    "wine_glass": "🥂",
    "wine_bottle": "🍾",
    "sommelier": "🎖️",
    "wine_cellar": "🏆",
}


class T:
    WELCOME_NEW = f"""
🍷 *Добро пожаловать в {BOT_NAME}!*

Здесь ты найдёшь свою половинку
за бокалом хорошего вина! 🥂

_Тысячи людей уже ищут знакомства_ ✨

Давай создадим анкету 📝
"""

    WELCOME_BACK = """
🍷 *С возвращением, {name}!* 🥂

{status}

📊 Просмотров: {views} · 💕 Мэтчей: {matches} · ✉️ Сообщений: {msgs}
{compat_hint}
"""

    ASK_NAME = "✏️ Как тебя зовут?"
    ASK_AGE = "🎂 Сколько тебе лет? _(18-99)_"
    ASK_GENDER = "🚻 Твой пол:"
    ASK_CITY = "🏙️ Твой город:"
    ASK_PHOTO = "📸 Отправь своё лучшее фото или «Пропустить»:"
    ASK_BIO = "📝 Расскажи о себе _(до 500 симв.)_ или «Пропустить»:"
    ASK_LOOKING = "🔍 Кого ищешь?"

    BAD_NAME = "⚠️ Имя должно быть 2-50 символов:"
    BAD_AGE = "⚠️ Возраст должен быть от 18 до 99:"

    REG_DONE = f"🎉 *Анкета готова!*\n\nДобро пожаловать в {BOT_NAME}!\n🍷 Время искать свою половинку!"

    NO_PROFILES = "😔 *Анкеты закончились.*\n\n🍷 Попробуй позже или расширь фильтры!"
    LIKES_LIMIT = "⚠️ *Лимит лайков на сегодня!*\n\n🥂 *Бокал Вина* — 100 лайков/день!\n🍾 *Бутылка Вина* — безлимит!"

    NEW_MATCH = "🍷✨ *Взаимная симпатия с {name}!* ✨🍷\n\n💕 Совместимость: {compat}\n\n_Напишите друг другу!_ 💬"

    NO_MATCHES = "😔 Пока нет взаимных симпатий\n\n🍷 Продолжай листать анкеты!"
    NO_PROFILE = "📝 Сначала заполни профиль → /start"
    BANNED = "🚫 Аккаунт заблокирован. Обратитесь в поддержку."
    NO_GUESTS = "👻 Пока никто не заходил на твой профиль\n\n💡 _Поставь буст, чтобы привлечь больше внимания!_"
    NO_MSGS = "💬 Нет активных диалогов\n\n🍷 Найди кого-нибудь в анкетах!"

    PAY_PENDING = "💳 Нажми «Оплатить» для перехода:"

    SHOP = f"""
🍷 *Винная Карта · {BOT_NAME}*

🥂 *Подписки* — раскрой все возможности
🚀 *Буст анкеты* — попади в топ выдачи
📊 *Сравнить тарифы* — найди свой
🎁 *Промокод* — активировать скидку
"""

    FAQ = f"""
❓ *FAQ · {BOT_NAME}*

*🍷 Как работают симпатии?*
Ставь ❤️ — если взаимно, вы сможете общаться!
Мы покажем вашу совместимость 💕

*🚀 Что такое буст?*
Твоя анкета поднимается в топ на 24 часа.
+500% просмотров!

*🥂 Что дают подписки?*
Больше лайков, все гости, приоритет в выдаче,
невидимка и многое другое!
Жми «Сравнить тарифы» в Винной Карте 🍷

*💕 Что такое совместимость?*
Мы анализируем ваши профили и показываем
процент совместимости при мэтче!
"""

    BOOST_INFO = """
🚀 *БУСТ АНКЕТЫ*

Поднимает твой профиль в топ выдачи на 24ч!

📈 +500% просмотров · 💕 +300% лайков

💡 _Лучше активировать вечером 18:00-22:00_

{status}
"""

    COMPARE = """
📊 *ВИННАЯ КАРТА ТАРИФОВ*

🆓 *Бесплатный*
30 лайков/день · 10 сообщений · 3 гостя

🥂 *Бокал Вина*
100 лайков · ∞ сообщений · 10 гостей
Без рекламы

🍾 *Бутылка Вина* — 🔥 Популярный
∞ лайков · ∞ сообщений · Все гости
Приоритет · Невидимка · 1 буст/день

🎖️ *Сомелье* — Максимум
Всё из Бутылки + 3 буста · Суперлайки
VIP-бейдж · Приоритетная поддержка

🏆 *Винный Погреб* — Навсегда
Всё из Сомелье НАВСЕГДА
Бейдж «Основатель» 👑
"""

    WINE_GLASS = """
🥂 *БОКАЛ ВИНА*

✨ 100 лайков/день (×3 от бесплатного)
💬 Безлимитные сообщения
👻 10 гостей · 🚫 Без рекламы

_Для тех, кому не хватает лайков за ужином_ 🍷

• 299₽/неделя (43₽/день)
• 799₽/месяц (27₽/день) 🔥
"""

    WINE_BOTTLE = """
🍾 *БУТЫЛКА ВИНА* — 🔥 Популярный

♾️ Безлимитные лайки и сообщения
👻 Все гости · ⭐ Приоритет в выдаче
🕵️ Невидимка · 🚀 1 буст/день

_×3 мэтчей по сравнению с бесплатным_ 💕

• 499₽/месяц (17₽/день)
• 1199₽/3 мес (13₽/день) — *скидка 20%* 🎉
"""

    SOMMELIER = """
🎖️ *СОМЕЛЬЕ* — Максимум возможностей

✅ Всё из «Бутылки Вина»
🚀 3 буста/день · ⚡ 5 суперлайков/день
🎖️ VIP-бейдж · 🎁 Эксклюзивные подарки
🛡️ Приоритетная поддержка 24/7

_×5 мэтчей по сравнению с бесплатным_ 🔥

• 799₽/мес · 1999₽/3мес (-17%)
• 3499₽/6мес (-27%) 💰
"""

    WINE_CELLAR = """
🏆 *ВИННЫЙ ПОГРЕБ* — Навсегда

🍷 Всё из «Сомелье» навсегда
👑 Бейдж «Основатель»
🆕 Все будущие обновления бесплатно

📊 Сомелье на год = 9588₽
🏆 Винный Погреб = 4999₽ — окупается за 6 мес!

💎 *4999₽ один раз навсегда*
"""

    ADMIN_MAIN = """
🛡️ *Админ-панель · {bot_name}*

👤 *{admin_name}* {role}

🍷 Управление ботом 🍷
"""

    ADMIN_STATS = """
📊 *Статистика · {bot_name}*

👥 *Пользователи:*
├ Всего: {total}
├ С анкетой: {complete}
├ DAU: {dau} · WAU: {wau} · MAU: {mau}
├ VIP: {vip} ({conversion:.1f}%)
├ Забанено: {banned}
└ Сегодня: +{today_reg}

📈 *Активность:*
├ ❤️ Лайков: {likes}
├ 💕 Мэтчей: {matches}
├ 💬 Сообщений: {messages}
└ 💕 Ср. совместимость: {avg_compatibility:.0f}%

💰 *Финансы:*
├ Всего: {revenue:.0f}₽
└ За месяц: {month_revenue:.0f}₽

⚠️ Жалоб: {pending_reports}
"""

    ADMIN_USER_CARD = """
👤 *Карточка пользователя*

🆔 ID: `{id}` · TG: `{telegram_id}`
📱 @{username}
{badge}{name}, {age}
🏙️ {city}
📝 _{bio}_

🍷 Статус: {tier}
👁️ {views} · ❤️ {likes} · 💕 {matches}
🚀 Бустов: {boosts}
📅 Рег: {created} · Актив: {active}
🚫 Бан: {banned}
"""

    BROADCAST_CONFIRM = """
📢 *Рассылка*

📝 {text}

👥 Аудитория: *{target}*
📊 Получателей: *{count}*

Отправить?
"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#               KEYBOARDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class KB:

    @staticmethod
    def main(unread: int = 0):
        chats_text = f"💬 Чаты ({unread})" if unread > 0 else "💬 Чаты"
        return ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="🍷 Анкеты"), KeyboardButton(text="💕 Симпатии")],
            [KeyboardButton(text=chats_text), KeyboardButton(text="👻 Гости")],
            [KeyboardButton(text="🛒 Винная Карта"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="❓ FAQ")],
        ], resize_keyboard=True)

    @staticmethod
    def gender():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужской", callback_data="g:male"),
             InlineKeyboardButton(text="👩 Женский", callback_data="g:female")],
        ])

    @staticmethod
    def looking():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужчин", callback_data="l:male"),
             InlineKeyboardButton(text="👩 Женщин", callback_data="l:female")],
            [InlineKeyboardButton(text="👫 Всех", callback_data="l:both")],
        ])

    @staticmethod
    def skip():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏩ Пропустить", callback_data="skip")],
        ])

    @staticmethod
    def search(uid: int, compat: float = 0):
        compat_text = f" ({compat:.0f}%)" if compat > 0 else ""
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"❤️ Нравится{compat_text}", callback_data=f"lk:{uid}"),
             InlineKeyboardButton(text="👎 Дальше", callback_data=f"dl:{uid}")],
            [InlineKeyboardButton(text="⚠️ Жалоба", callback_data=f"rp:{uid}")],
        ])

    @staticmethod
    def matches(ms: List[Dict]):
        b = []
        for m in ms[:10]:
            unread_badge = f" 🔴{m['unread']}" if m.get('unread', 0) > 0 else ""
            compat = f" 💕{m['compatibility']:.0f}%" if m.get('compatibility', 0) > 0 else ""
            b.append([InlineKeyboardButton(
                text=f"💬 {m['name']}, {m['age']}{compat}{unread_badge}",
                callback_data=f"ch:{m['user_id']}"
            )])
        b.append([InlineKeyboardButton(text="🍷 Меню", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def chat_actions(match_id: int, partner_id: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💕 Симпатии", callback_data="bm")],
            [InlineKeyboardButton(text="💔 Отвязать", callback_data=f"um:{match_id}")],
        ])

    @staticmethod
    def back_matches():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💕 К симпатиям", callback_data="bm")],
        ])

    @staticmethod
    def confirm_unmatch(match_id: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, отвязать", callback_data=f"um_yes:{match_id}"),
             InlineKeyboardButton(text="❌ Отмена", callback_data="bm")],
        ])

    @staticmethod
    def shop():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Подписки", callback_data="sh:subs")],
            [InlineKeyboardButton(text="🚀 Буст анкеты", callback_data="sh:boost")],
            [InlineKeyboardButton(text="📊 Сравнить тарифы", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="sh:promo")],
            [InlineKeyboardButton(text="🍷 Меню", callback_data="mn")],
        ])

    @staticmethod
    def subs():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Бокал Вина", callback_data="tf:wine_glass")],
            [InlineKeyboardButton(text="🍾 Бутылка Вина 🔥", callback_data="tf:wine_bottle")],
            [InlineKeyboardButton(text="🎖️ Сомелье", callback_data="tf:sommelier")],
            [InlineKeyboardButton(text="🏆 Винный Погреб", callback_data="tf:wine_cellar")],
            [InlineKeyboardButton(text="📊 Сравнить", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🍷 Назад", callback_data="sh:mn")],
        ])

    @staticmethod
    def buy_wine_glass():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 299₽ / неделя", callback_data="by:wine_glass:7:29900")],
            [InlineKeyboardButton(text="🥂 799₽ / месяц 🔥", callback_data="by:wine_glass:30:79900")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sh:subs")],
        ])

    @staticmethod
    def buy_wine_bottle():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍾 499₽ / месяц", callback_data="by:wine_bottle:30:49900")],
            [InlineKeyboardButton(text="🍾 1199₽ / 3 мес 🔥 -20%", callback_data="by:wine_bottle:90:119900")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sh:subs")],
        ])

    @staticmethod
    def buy_sommelier():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎖️ 799₽ / мес", callback_data="by:sommelier:30:79900")],
            [InlineKeyboardButton(text="🎖️ 1999₽ / 3 мес 🔥 -17%", callback_data="by:sommelier:90:199900")],
            [InlineKeyboardButton(text="🎖️ 3499₽ / 6 мес 💰 -27%", callback_data="by:sommelier:180:349900")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sh:subs")],
        ])

    @staticmethod
    def buy_wine_cellar():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏆 4999₽ навсегда 💎", callback_data="by:wine_cellar:0:499900")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sh:subs")],
        ])

    @staticmethod
    def boost_menu(has_boosts: bool, is_active: bool):
        b = []
        if has_boosts:
            b.append([InlineKeyboardButton(text="🚀 Активировать буст", callback_data="bo:act")])
        b += [
            [InlineKeyboardButton(text="🚀 1шт — 99₽", callback_data="by:boost:1:9900")],
            [InlineKeyboardButton(text="🚀 5шт — 399₽ (-20%)", callback_data="by:boost:5:39900")],
            [InlineKeyboardButton(text="🚀 10шт — 699₽ (-30%)", callback_data="by:boost:10:69900")],
            [InlineKeyboardButton(text="🍷 Назад", callback_data="sh:mn")],
        ]
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def pay(url: str, pid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить", url=url)],
            [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"ck:{pid}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="sh:mn")],
        ])

    @staticmethod
    def profile():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Редактировать", callback_data="pe"),
             InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")],
            [InlineKeyboardButton(text="🚀 Буст", callback_data="sh:boost")],
        ])

    @staticmethod
    def edit():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Имя", callback_data="ed:name"),
             InlineKeyboardButton(text="🎂 Возраст", callback_data="ed:age")],
            [InlineKeyboardButton(text="🏙️ Город", callback_data="ed:city"),
             InlineKeyboardButton(text="📝 О себе", callback_data="ed:bio")],
            [InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="pv")],
        ])

    @staticmethod
    def report_reasons():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Спам", callback_data="rr:spam"),
             InlineKeyboardButton(text="🎭 Фейк", callback_data="rr:fake")],
            [InlineKeyboardButton(text="🔞 18+", callback_data="rr:nsfw"),
             InlineKeyboardButton(text="😡 Оскорбления", callback_data="rr:harass")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="mn")],
        ])

    # ═══ ADMIN KEYBOARDS ═══

    @staticmethod
    def admin():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
            [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="adm:search")],
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="⚠️ Жалобы", callback_data="adm:reports")],
            [InlineKeyboardButton(text="💰 Платежи", callback_data="adm:payments")],
            [InlineKeyboardButton(text="🎁 Создать промокод", callback_data="adm:promo")],
            [InlineKeyboardButton(text="🏆 Топ пользователей", callback_data="adm:top")],
            [InlineKeyboardButton(text="❌ Закрыть", callback_data="mn")],
        ])

    @staticmethod
    def admin_user(uid: int, is_banned: bool):
        ban_btn = (
            InlineKeyboardButton(text="🔓 Разбанить", callback_data=f"au:unban:{uid}")
            if is_banned else
            InlineKeyboardButton(text="🔒 Забанить", callback_data=f"au:ban:{uid}")
        )
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍷 Выдать VIP", callback_data=f"au:givevip:{uid}"),
             InlineKeyboardButton(text="🚀 Бусты", callback_data=f"au:giveboost:{uid}")],
            [ban_btn,
             InlineKeyboardButton(text="🗑️ Удалить фото", callback_data=f"au:delphotos:{uid}")],
            [InlineKeyboardButton(text="✅ Верифицировать", callback_data=f"au:verify:{uid}")],
            [InlineKeyboardButton(text="🛡️ Админка", callback_data="adm:main")],
        ])

    @staticmethod
    def admin_report(rid: int, ruid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔒 Забанить", callback_data=f"ar:ban:{rid}:{ruid}"),
             InlineKeyboardButton(text="⚠️ Предупредить", callback_data=f"ar:warn:{rid}:{ruid}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ar:dismiss:{rid}:{ruid}")],
            [InlineKeyboardButton(text="➡️ Следующая", callback_data="adm:reports")],
        ])

    @staticmethod
    def broadcast_targets():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 Все", callback_data="bc:all")],
            [InlineKeyboardButton(text="✅ С анкетой", callback_data="bc:complete")],
            [InlineKeyboardButton(text="🍷 VIP", callback_data="bc:vip")],
            [InlineKeyboardButton(text="🆓 Бесплатные", callback_data="bc:free")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:main")],
        ])

    @staticmethod
    def broadcast_confirm():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Отправить", callback_data="bc:send")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:main")],
        ])

    @staticmethod
    def give_vip_tiers():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Бокал", callback_data="gv:wine_glass"),
             InlineKeyboardButton(text="🍾 Бутылка", callback_data="gv:wine_bottle")],
            [InlineKeyboardButton(text="🎖️ Сомелье", callback_data="gv:sommelier"),
             InlineKeyboardButton(text="🏆 Погреб", callback_data="gv:wine_cellar")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:main")],
        ])

    @staticmethod
    def back_admin():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛡️ Админка", callback_data="adm:main")],
        ])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#             PAYMENT SERVICE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class Pay:

    @staticmethod
    async def create(user: Dict, ptype: str, tier: str = None,
                     dur: int = None, count: int = None, amount: int = 0) -> Dict:
        if not YOOKASSA_AVAILABLE or not config.YOOKASSA_SHOP_ID:
            return {"error": "ЮKassa не настроена"}

        if ptype == "subscription":
            desc = f"Подписка «{TIER_NAMES.get(tier, 'VIP')}» · {BOT_NAME}"
        else:
            desc = f"Буст анкеты ({count}шт) · {BOT_NAME}"

        try:
            p = YooPayment.create({
                "amount": {"value": f"{amount / 100:.2f}", "currency": "RUB"},
                "confirmation": {
                    "type": ConfirmationType.REDIRECT,
                    "return_url": f"{config.DOMAIN}/ok"
                },
                "capture": True,
                "description": desc,
                "metadata": {
                    "user_id": user["id"], "type": ptype,
                    "tier": tier, "dur": dur, "count": count,
                }
            }, str(uuid.uuid4()))

            pid = await DB.create_payment(
                user["id"], p.id, amount, desc, ptype, tier, dur, count
            )
            return {"pid": pid, "url": p.confirmation.confirmation_url}
        except Exception as e:
            logger.error(f"Payment error: {e}")
            return {"error": str(e)}

    @staticmethod
    async def check(pid: int) -> Dict:
        p = await DB.get_payment(pid)
        if not p:
            return {"status": "not_found"}
        try:
            y = YooPayment.find_one(p["yookassa_payment_id"])
            if y.status == "succeeded" and p["status"] != "succeeded":
                await DB.update_payment_status(pid, PaymentStatus.SUCCEEDED)
                if p["product_type"] == "subscription":
                    await DB.activate_subscription_by_id(
                        p["user_id"], p["product_tier"],
                        p["product_duration"] or 30
                    )
                    return {"status": "succeeded", "type": "subscription"}
                elif p["product_type"] == "boost":
                    await DB.add_boosts(p["user_id"], p.get("product_count") or 1)
                    return {
                        "status": "succeeded", "type": "boost",
                        "count": p.get("product_count", 1)
                    }
            return {"status": y.status}
        except Exception as e:
            logger.error(f"Payment check error: {e}")
            return {"status": "error"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#              MIDDLEWARE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        tg = event.from_user if isinstance(event, (Message, CallbackQuery)) else None
        u = None
        if tg:
            u = await DB.get_user(tg.id)
            if u:
                u = await DB.reset_limits(u)
                if u.get("is_banned"):
                    if isinstance(event, Message):
                        await event.answer(T.BANNED)
                    elif isinstance(event, CallbackQuery):
                        await event.answer(T.BANNED, show_alert=True)
                    return
        data["user"] = u
        return await handler(event, data)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#               HANDLERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

rt = Router()


# ═══ START ═══

@rt.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if user and user.get("is_profile_complete"):
        un = await DB.get_unread(user["id"])
        st = TIER_NAMES.get(user["subscription_tier"], "🆓 Бесплатный")
        if DB.is_boosted(user):
            st += " · 🚀 Буст"
        st += DB.get_role_tag(user)

        # Подсказка по совместимости
        compat_hint = ""
        if user["matches_count"] > 0:
            compat_hint = "\n💕 _Проверь совместимость в симпатиях!_"

        await message.answer(
            T.WELCOME_BACK.format(
                name=user["name"], status=st,
                views=user["views_count"],
                matches=user["matches_count"],
                msgs=un,
                compat_hint=compat_hint
            ),
            reply_markup=KB.main(un),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        if not user:
            await DB.create_user(message.from_user.id, message.from_user.username)

        # Анимация приветствия
        await Anim.animate(message, Anim.WINE_POUR, 0.5)
        await asyncio.sleep(0.3)

        await message.answer(T.WELCOME_NEW, parse_mode=ParseMode.MARKDOWN)
        await message.answer(T.ASK_NAME, reply_markup=ReplyKeyboardRemove())
        await state.set_state(RegStates.name)


# ═══ REGISTRATION ═══

@rt.message(RegStates.name)
async def reg_name(message: Message, state: FSMContext):
    n = message.text.strip()
    if len(n) < 2 or len(n) > 50:
        await message.answer(T.BAD_NAME)
        return
    await state.update_data(name=n)
    await message.answer(f"Приятно познакомиться, *{n}*! 🍷", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(0.5)
    await message.answer(T.ASK_AGE, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.age)


@rt.message(RegStates.age)
async def reg_age(message: Message, state: FSMContext):
    try:
        a = int(message.text.strip())
        if not 18 <= a <= 99:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer(T.BAD_AGE)
        return
    await state.update_data(age=a)
    await message.answer(T.ASK_GENDER, reply_markup=KB.gender())
    await state.set_state(RegStates.gender)


@rt.callback_query(RegStates.gender, F.data.startswith("g:"))
async def reg_gender(callback: CallbackQuery, state: FSMContext):
    gender = callback.data[2:]
    await state.update_data(gender=gender)
    gender_text = "👨 Мужской" if gender == "male" else "👩 Женский"
    await callback.message.edit_text(f"Пол: {gender_text} ✅")
    await asyncio.sleep(0.3)
    await callback.message.answer(T.ASK_CITY)
    await state.set_state(RegStates.city)
    await callback.answer()


@rt.message(RegStates.city)
async def reg_city(message: Message, state: FSMContext):
    c = message.text.strip().title()
    if len(c) < 2:
        await message.answer("🏙️ Введи название города:")
        return
    await state.update_data(city=c)
    await message.answer(f"🏙️ Город: *{c}* ✅", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(0.3)
    await message.answer(T.ASK_PHOTO, reply_markup=KB.skip())
    await state.set_state(RegStates.photo)


@rt.message(RegStates.photo, F.photo)
async def reg_photo(message: Message, state: FSMContext):
    await state.update_data(photo=message.photo[-1].file_id)
    await message.answer("📸 Отличное фото! ✅")
    await asyncio.sleep(0.3)
    await message.answer(T.ASK_BIO, reply_markup=KB.skip(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.bio)


@rt.callback_query(RegStates.photo, F.data == "skip")
async def reg_skip_photo(callback: CallbackQuery, state: FSMContext):
    await state.update_data(photo=None)
    await callback.message.edit_text("📸 Фото пропущено\n\n_Можешь добавить позже в профиле_",
                                     parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(0.3)
    await callback.message.answer(T.ASK_BIO, reply_markup=KB.skip(),
                                   parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.bio)
    await callback.answer()


@rt.message(RegStates.bio)
async def reg_bio(message: Message, state: FSMContext):
    bio = message.text.strip()[:500]
    await state.update_data(bio=bio)
    await message.answer(T.ASK_LOOKING, reply_markup=KB.looking())
    await state.set_state(RegStates.looking_for)


@rt.callback_query(RegStates.bio, F.data == "skip")
async def reg_skip_bio(callback: CallbackQuery, state: FSMContext):
    await state.update_data(bio="")
    await callback.message.edit_text(T.ASK_LOOKING, reply_markup=KB.looking())
    await state.set_state(RegStates.looking_for)
    await callback.answer()


@rt.callback_query(RegStates.looking_for, F.data.startswith("l:"))
async def reg_looking(callback: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    upd = {
        "name": d["name"],
        "age": d["age"],
        "gender": Gender(d["gender"]),
        "city": d["city"],
        "bio": d.get("bio", ""),
        "looking_for": LookingFor(callback.data[2:]),
        "is_profile_complete": True,
    }
    if d.get("photo"):
        upd["photos"] = d["photo"]
        upd["main_photo"] = d["photo"]

    await DB.update_user(callback.from_user.id, **upd)
    await state.clear()

    # Анимация завершения регистрации
    await callback.message.edit_text("⏳ Создаём анкету...")
    await asyncio.sleep(0.5)
    await callback.message.edit_text("✨ Настраиваем поиск...")
    await asyncio.sleep(0.5)
    await callback.message.edit_text(T.REG_DONE, parse_mode=ParseMode.MARKDOWN)

    total = await DB.get_total_users()
    await callback.message.answer(
        f"🍷 Уже *{total}* человек ищут знакомства!\n\nЖми «🍷 Анкеты» чтобы начать!",
        reply_markup=KB.main(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


# ═══ BROWSE ═══

@rt.message(F.text == "🍷 Анкеты")
async def browse(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    await state.clear()

    # Мини-анимация поиска
    msg = await message.answer("🔍 Ищем анкеты...")
    await asyncio.sleep(0.5)

    ps = await DB.search_profiles(user, 1)
    if not ps:
        await msg.edit_text(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)
        return

    await msg.delete()
    await show_card(message, ps[0], user)


async def show_card(message: Message, p: Dict, v: Dict):
    await DB.add_guest(v["id"], p["id"])

    lm = {"male": "👨 Мужчин", "female": "👩 Женщин", "both": "👫 Всех"}
    badge = DB.get_badge(p)
    role = DB.get_role_tag(p)
    boost = " 🚀" if DB.is_boosted(p) else ""

    # Рассчитываем совместимость
    compat = CompatibilityEngine.calculate(v, p)
    compat_bar = Anim.compatibility_bar(compat)

    txt = (
        f"{badge}*{p['name']}*{boost}, {p['age']}{role}\n"
        f"🏙️ {p['city']}\n\n"
        f"{p['bio'] or '_Нет описания_'}\n\n"
        f"🔍 Ищет: {lm.get(p.get('looking_for', 'both'), '👫 Всех')}\n"
        f"💕 Совместимость: {compat_bar}"
    )

    kb = KB.search(p["id"], compat)

    try:
        if p.get("main_photo"):
            await message.answer_photo(
                photo=p["main_photo"], caption=txt,
                reply_markup=kb, parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error showing card: {e}")
        await message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("lk:"))
async def handle_like(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        await callback.answer("❗ Ошибка")
        return

    if not DB.is_vip(user) and user.get("daily_likes_remaining", 0) <= 0:
        try:
            await callback.message.edit_caption(
                caption=T.LIKES_LIMIT, reply_markup=KB.shop(),
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            await callback.message.edit_text(
                T.LIKES_LIMIT, reply_markup=KB.shop(),
                parse_mode=ParseMode.MARKDOWN
            )
        await callback.answer()
        return

    tid = int(callback.data[3:])
    result = await DB.add_like(user["id"], tid)

    if not DB.is_vip(user):
        await DB.dec_likes(user["telegram_id"])

    if result["is_match"]:
        t = await DB.get_user_by_id(tid)
        tn = t["name"] if t else "?"
        compat_bar = Anim.compatibility_bar(result["compatibility"])

        match_text = T.NEW_MATCH.format(name=tn, compat=compat_bar)

        try:
            await callback.message.edit_caption(
                caption=match_text, parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            await callback.message.edit_text(
                match_text, parse_mode=ParseMode.MARKDOWN
            )

        # Уведомляем второго пользователя
        if t:
            try:
                compat_bar_rev = Anim.compatibility_bar(result["compatibility"])
                await callback.bot.send_message(
                    t["telegram_id"],
                    T.NEW_MATCH.format(name=user["name"], compat=compat_bar_rev),
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass

        await callback.answer("🍷✨ Мэтч! 💕")
    else:
        await Anim.like_animation(callback)

    # Показываем следующую анкету
    user = await DB.get_user(callback.from_user.id)
    ps = await DB.search_profiles(user, 1)
    if ps:
        await show_card(callback.message, ps[0], user)
    else:
        await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("dl:"))
async def handle_dislike(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    await Anim.dislike_animation(callback)

    ps = await DB.search_profiles(user, 1)
    if ps:
        await show_card(callback.message, ps[0], user)
    else:
        try:
            await callback.message.edit_caption(
                caption=T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)


# ═══ MATCHES & CHAT ═══

@rt.message(F.text == "💕 Симпатии")
async def show_matches(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return
    await state.clear()

    ms = await DB.get_matches(user["id"])
    if ms:
        total_unread = sum(m.get("unread", 0) for m in ms)
        header = f"💕 *Симпатии ({len(ms)})*"
        if total_unread > 0:
            header += f" · 🔴 {total_unread} новых"

        await message.answer(
            header, reply_markup=KB.matches(ms),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await message.answer(T.NO_MATCHES, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("ch:"))
async def start_chat(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    pid = int(callback.data[3:])
    p = await DB.get_user_by_id(pid)
    if not p:
        await callback.answer("❌ Пользователь не найден")
        return

    mid = await DB.get_match_between(user["id"], pid)
    if not mid:
        await callback.answer("❌ Нет взаимной симпатии")
        return

    # Помечаем сообщения как прочитанные
    await DB.mark_messages_read(mid, user["id"])

    msgs = await DB.get_msgs(mid, 8)
    badge = DB.get_badge(p)
    txt = f"💬 *Чат с {badge}{p['name']}*\n\n"

    for mg in msgs:
        sn = "📤 Вы" if mg["sender_id"] == user["id"] else f"📩 {p['name']}"
        time_str = mg["created_at"].strftime("%H:%M") if mg.get("created_at") else ""
        txt += f"*{sn}:* {mg['text']} _{time_str}_\n"

    if not msgs:
        wine = Anim.get_wine_emoji()
        txt += f"_Начните общение! Напишите первым {wine}_"

    await state.update_data(cp=pid, mi=mid)
    await state.set_state(ChatStates.chatting)

    await callback.message.edit_text(
        txt, reply_markup=KB.chat_actions(mid, pid),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.message(ChatStates.chatting)
async def send_chat_msg(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    d = await state.get_data()
    mid = d.get("mi")
    pid = d.get("cp")

    if not mid:
        await state.clear()
        await message.answer("💬 Чат закрыт", reply_markup=KB.main())
        return

    await DB.send_msg(mid, user["id"], message.text)

    p = await DB.get_user_by_id(pid)
    if p:
        try:
            await message.bot.send_message(
                p["telegram_id"],
                f"💬 *{user['name']}:* {message.text}",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

    await message.answer("✅ Отправлено")


@rt.callback_query(F.data == "bm")
async def back_to_matches(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user:
        return
    ms = await DB.get_matches(user["id"])
    if ms:
        await callback.message.edit_text(
            f"💕 *Симпатии ({len(ms)}):*",
            reply_markup=KB.matches(ms),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await callback.message.edit_text(T.NO_MATCHES, parse_mode=ParseMode.MARKDOWN)


# ═══ UNMATCH ═══

@rt.callback_query(F.data.startswith("um:"))
async def unmatch_confirm(callback: CallbackQuery, state: FSMContext):
    match_id = int(callback.data[3:])
    await callback.message.edit_text(
        "💔 *Точно хочешь отвязать этого человека?*\n\n_Все сообщения будут потеряны_",
        reply_markup=KB.confirm_unmatch(match_id),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data.startswith("um_yes:"))
async def unmatch_do(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    await state.clear()
    match_id = int(callback.data[7:])
    ok = await DB.unmatch(user["id"], match_id)
    if ok:
        await callback.message.edit_text(
            "💔 Отвязано. Вы больше не будете видеть друг друга в симпатиях.",
            reply_markup=KB.back_matches()
        )
    else:
        await callback.answer("❌ Ошибка", show_alert=True)


# ═══ CHATS & GUESTS ═══

@rt.message(F.text.startswith("💬 Чаты"))
async def show_chats(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return
    await state.clear()
    ms = await DB.get_matches(user["id"])
    if ms:
        await message.answer(
            "💬 *Диалоги:*",
            reply_markup=KB.matches(ms),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await message.answer(T.NO_MSGS, parse_mode=ParseMode.MARKDOWN)


@rt.message(F.text == "👻 Гости")
async def show_guests(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    lim = 20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE
    gs = await DB.get_guests(user["id"], lim)

    if not gs:
        await message.answer(T.NO_GUESTS, parse_mode=ParseMode.MARKDOWN)
        return

    txt = "👻 *Кто смотрел твой профиль:*\n\n"
    for i, g in enumerate(gs, 1):
        badge = DB.get_badge(g)
        txt += f"{i}. {badge}{g['name']}, {g['age']} — 🏙️ {g['city']}\n"

    if not DB.is_vip(user):
        hidden = max(0, user.get("views_count", 0) - lim)
        if hidden > 0:
            txt += f"\n🔒 _Ещё {hidden} гостей скрыто_"
        txt += "\n\n🍷 _Подписка — все гости!_"

    await message.answer(txt, parse_mode=ParseMode.MARKDOWN)


# ═══ PROFILE ═══

@rt.message(F.text == "👤 Профиль")
async def show_profile(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    badge = DB.get_badge(user)
    role = DB.get_role_tag(user)
    sub = TIER_NAMES.get(user["subscription_tier"], "🆓 Бесплатный")

    if (user.get("subscription_expires_at") and
            user["subscription_tier"] not in ("free", "wine_cellar")):
        sub += f" (до {user['subscription_expires_at'].strftime('%d.%m.%Y')})"

    bi = ""
    if DB.is_boosted(user):
        bi += f"\n🚀 Буст до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if user.get("boost_count", 0) > 0:
        bi += f"\n🚀 Запас: {user['boost_count']} бустов"

    # Лимиты
    tier = user["subscription_tier"]
    limits = DB.get_tier_limits(tier)
    remaining_likes = user.get("daily_likes_remaining", 0)
    if limits["likes"] >= 999999:
        likes_str = "♾️"
    else:
        likes_str = f"{remaining_likes}/{limits['likes']}"

    txt = (
        f"👤 *Мой профиль*\n\n"
        f"{badge}*{user['name']}*, {user['age']}{role}\n"
        f"🏙️ {user['city']}\n\n"
        f"{user['bio'] or '_Не указано_'}\n\n"
        f"📊 Статистика:\n"
        f"👁️ {user['views_count']} · "
        f"❤️ {user['likes_received_count']} · "
        f"💕 {user['matches_count']}\n"
        f"❤️ Лайков сегодня: {likes_str}\n\n"
        f"🍷 {sub}{bi}"
    )

    try:
        if user.get("main_photo"):
            await message.answer_photo(
                photo=user["main_photo"], caption=txt,
                reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer(
                txt, reply_markup=KB.profile(),
                parse_mode=ParseMode.MARKDOWN
            )
    except Exception:
        await message.answer(
            txt, reply_markup=KB.profile(),
            parse_mode=ParseMode.MARKDOWN
        )


@rt.callback_query(F.data == "pe")
async def profile_edit_menu(callback: CallbackQuery):
    try:
        await callback.message.edit_caption(
            caption="✏️ *Что изменить?*",
            reply_markup=KB.edit(),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        await callback.message.edit_text(
            "✏️ *Что изменить?*",
            reply_markup=KB.edit(),
            parse_mode=ParseMode.MARKDOWN
        )
    await callback.answer()


@rt.callback_query(F.data == "pv")
async def back_to_profile(callback: CallbackQuery, user: Optional[Dict]):
    if user:
        await callback.message.delete()
        # Отправляем новое сообщение с профилем
        await show_profile(callback.message, user)
    await callback.answer()


@rt.callback_query(F.data == "ed:name")
async def edit_name(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("✏️ Новое имя:")
    await state.set_state(EditStates.edit_name)
    await callback.answer()


@rt.message(EditStates.edit_name)
async def save_name(message: Message, state: FSMContext):
    n = message.text.strip()
    if len(n) < 2 or len(n) > 50:
        await message.answer(T.BAD_NAME)
        return
    await DB.update_user(message.from_user.id, name=n)
    await state.clear()
    await message.answer(f"✅ Имя изменено на *{n}*!", reply_markup=KB.main(),
                          parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "ed:age")
async def edit_age(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("🎂 Новый возраст:")
    await state.set_state(EditStates.edit_age)
    await callback.answer()


@rt.message(EditStates.edit_age)
async def save_age(message: Message, state: FSMContext):
    try:
        a = int(message.text.strip())
        if not 18 <= a <= 99:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer(T.BAD_AGE)
        return
    await DB.update_user(message.from_user.id, age=a)
    await state.clear()
    await message.answer(f"✅ Возраст изменён: *{a}*", reply_markup=KB.main(),
                          parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "ed:city")
async def edit_city(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("🏙️ Новый город:")
    await state.set_state(EditStates.edit_city)
    await callback.answer()


@rt.message(EditStates.edit_city)
async def save_city(message: Message, state: FSMContext):
    city = message.text.strip().title()
    await DB.update_user(message.from_user.id, city=city)
    await state.clear()
    await message.answer(f"✅ Город изменён: *{city}*", reply_markup=KB.main(),
                          parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "ed:bio")
async def edit_bio(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("📝 Новое описание:")
    await state.set_state(EditStates.edit_bio)
    await callback.answer()


@rt.message(EditStates.edit_bio)
async def save_bio(message: Message, state: FSMContext):
    bio = message.text.strip()[:500]
    await DB.update_user(message.from_user.id, bio=bio)
    await state.clear()
    await message.answer("✅ Описание обновлено!", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:photo")
async def edit_photo(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("📸 Отправь новое фото:")
    await state.set_state(EditStates.add_photo)
    await callback.answer()


@rt.message(EditStates.add_photo, F.photo)
async def save_photo(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    pid = message.photo[-1].file_id
    ph = user.get("photos", "")
    ph = (ph + "," + pid) if ph else pid
    await DB.update_user(message.from_user.id, photos=ph, main_photo=pid)
    await state.clear()
    await message.answer("📸 Фото обновлено! ✅", reply_markup=KB.main())


# ═══ SHOP ═══

@rt.message(F.text == "🛒 Винная Карта")
async def shop_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(T.SHOP, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "sh:mn")
async def shop_main(callback: CallbackQuery):
    await callback.message.edit_text(
        T.SHOP, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "sh:compare")
async def shop_compare(callback: CallbackQuery):
    await callback.message.edit_text(
        T.COMPARE, reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "sh:subs")
async def shop_subs(callback: CallbackQuery):
    await callback.message.edit_text(
        "🍷 *Выбери свой тариф:*",
        reply_markup=KB.subs(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "tf:wine_glass")
async def tf_wine_glass(callback: CallbackQuery):
    await callback.message.edit_text(
        T.WINE_GLASS, reply_markup=KB.buy_wine_glass(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "tf:wine_bottle")
async def tf_wine_bottle(callback: CallbackQuery):
    await callback.message.edit_text(
        T.WINE_BOTTLE, reply_markup=KB.buy_wine_bottle(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "tf:sommelier")
async def tf_sommelier(callback: CallbackQuery):
    await callback.message.edit_text(
        T.SOMMELIER, reply_markup=KB.buy_sommelier(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "tf:wine_cellar")
async def tf_wine_cellar(callback: CallbackQuery):
    await callback.message.edit_text(
        T.WINE_CELLAR, reply_markup=KB.buy_wine_cellar(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "sh:boost")
async def shop_boost(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        await callback.answer("❗")
        return

    has = user.get("boost_count", 0) > 0
    act = DB.is_boosted(user)
    st = ""

    if act:
        st += f"\n\n🚀 Буст активен до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if has:
        st += f"\n🚀 Запас: {user['boost_count']} бустов"
    if not has and not act:
        st = "\n\n📦 Нет бустов — купи ниже!"

    await callback.message.edit_text(
        T.BOOST_INFO.format(status=st),
        reply_markup=KB.boost_menu(has, act),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "bo:act")
async def activate_boost(callback: CallbackQuery, user: Optional[Dict]):
    if not user or user.get("boost_count", 0) <= 0:
        await callback.answer("🚫 Нет бустов!", show_alert=True)
        return

    ok = await DB.use_boost(user["id"])
    if ok:
        # Анимация буста
        await Anim.boost_animation(callback.message)
        u = await DB.get_user(callback.from_user.id)
        await callback.message.answer(
            f"🚀 *Буст активирован до {u['boost_expires_at'].strftime('%d.%m %H:%M')}!*\n"
            f"📦 Осталось: {u['boost_count']} бустов",
            reply_markup=KB.main(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await callback.answer("❌ Ошибка", show_alert=True)


@rt.callback_query(F.data.startswith("by:"))
async def handle_buy(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        await callback.answer("❗")
        return

    parts = callback.data.split(":")
    prod = parts[1]
    param = int(parts[2])
    amt = int(parts[3])

    if prod == "boost":
        res = await Pay.create(user, "boost", count=param, amount=amt)
    else:
        res = await Pay.create(user, "subscription", tier=prod, dur=param, amount=amt)

    if "error" in res:
        await callback.answer(f"❌ {res['error']}", show_alert=True)
        return

    txt = (
        f"💳 *Покупка*\n\n"
        f"💰 Сумма: {amt / 100:.0f}₽\n\n"
        f"1️⃣ Нажми «Оплатить»\n"
        f"2️⃣ Оплати на сайте\n"
        f"3️⃣ Нажми «Проверить оплату» 🍷"
    )
    await callback.message.edit_text(
        txt, reply_markup=KB.pay(res["url"], res["pid"]),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data.startswith("ck:"))
async def check_payment(callback: CallbackQuery):
    pid = int(callback.data[3:])
    res = await Pay.check(pid)

    if res["status"] == "succeeded":
        await Anim.payment_success_animation(callback.message)

        if res.get("type") == "boost":
            await callback.message.answer(
                f"🚀 *{res.get('count', 1)} бустов добавлено!*",
                reply_markup=KB.main(),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await callback.message.answer(
                "🍷 *Подписка активирована!*\n\n✨ Наслаждайтесь новыми возможностями!",
                reply_markup=KB.main(),
                parse_mode=ParseMode.MARKDOWN
            )
    elif res["status"] == "pending":
        await callback.answer("⏳ Оплата обрабатывается...", show_alert=True)
    else:
        await callback.answer("❌ Оплата не найдена или отменена", show_alert=True)


# ═══ PROMO (user input) ═══

@rt.callback_query(F.data == "sh:promo")
async def promo_input(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🎁 *Введи промокод:*\n\n_Например: WINE2024_",
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(PromoInputState.waiting_code)
    await callback.answer()


@rt.message(PromoInputState.waiting_code)
async def promo_activate(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user:
        await message.answer("❗ Ошибка", reply_markup=KB.main())
        return

    code = message.text.strip().upper()
    result = await DB.use_promo(user["id"], code)

    if "error" in result:
        await message.answer(f"❌ {result['error']}", reply_markup=KB.main())
    else:
        tn = TIER_NAMES.get(result["tier"], "VIP")
        # Анимация
        msg = await message.answer("🎁 Проверяем промокод...")
        await asyncio.sleep(0.5)
        await msg.edit_text("🎁 ✅ Промокод найден!")
        await asyncio.sleep(0.5)
        await msg.edit_text(
            f"🎉 *Промокод активирован!*\n\n"
            f"🍷 {tn} на {result['days']} дней!\n\n"
            f"_Наслаждайтесь!_ 🥂",
            parse_mode=ParseMode.MARKDOWN
        )
        await message.answer("🍷", reply_markup=KB.main())


# ═══ FAQ & REPORTS ═══

@rt.message(F.text == "❓ FAQ")
async def show_faq(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(T.FAQ, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("rp:"))
async def start_report(callback: CallbackQuery, state: FSMContext):
    await state.update_data(rp_id=int(callback.data[3:]))
    try:
        await callback.message.edit_caption(
            caption="⚠️ *Причина жалобы:*",
            reply_markup=KB.report_reasons(),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        await callback.message.edit_text(
            "⚠️ *Причина жалобы:*",
            reply_markup=KB.report_reasons(),
            parse_mode=ParseMode.MARKDOWN
        )
    await callback.answer()


@rt.callback_query(F.data.startswith("rr:"))
async def save_report(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    d = await state.get_data()
    rid = d.get("rp_id")
    if rid:
        await DB.create_report(user["id"], rid, callback.data[3:])
    await state.clear()

    try:
        await callback.message.edit_caption(caption="✅ Жалоба отправлена! Спасибо за обратную связь 🍷")
    except Exception:
        await callback.message.edit_text("✅ Жалоба отправлена! Спасибо за обратную связь 🍷")

    # Показываем следующую анкету
    await asyncio.sleep(1)
    ps = await DB.search_profiles(user, 1)
    if ps:
        await show_card(callback.message, ps[0], user)
    await callback.answer()


@rt.callback_query(F.data == "mn")
async def back_to_menu(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    try:
        await callback.message.delete()
    except Exception:
        pass

    unread = 0
    if user:
        unread = await DB.get_unread(user["id"])

    await callback.message.answer("🍷", reply_markup=KB.main(unread))
    await callback.answer()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#              ADMIN PANEL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def is_adm(user: Optional[Dict]) -> bool:
    return user is not None and user.get("telegram_id") in config.ADMIN_IDS


@rt.message(Command("admin"))
async def admin_cmd(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    await state.clear()
    role = "👑 Создатель" if DB.is_creator(user) else "🛡️ Админ"
    await message.answer(
        T.ADMIN_MAIN.format(
            bot_name=BOT_NAME,
            admin_name=user["name"] or "Admin",
            role=role
        ),
        reply_markup=KB.admin(),
        parse_mode=ParseMode.MARKDOWN
    )


@rt.callback_query(F.data == "adm:main")
async def admin_main(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    await state.clear()
    role = "👑 Создатель" if DB.is_creator(user) else "🛡️ Админ"
    await callback.message.edit_text(
        T.ADMIN_MAIN.format(
            bot_name=BOT_NAME,
            admin_name=user["name"] or "Admin",
            role=role
        ),
        reply_markup=KB.admin(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data == "adm:stats")
async def admin_stats(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return
    s = await DB.get_stats()
    await callback.message.edit_text(
        T.ADMIN_STATS.format(bot_name=BOT_NAME, **s),
        reply_markup=KB.back_admin(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


# ═══ SEARCH USER ═══

@rt.callback_query(F.data == "adm:search")
async def admin_search(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    await callback.message.edit_text(
        "🔍 *ID, telegram\\_id, @username или имя:*",
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(AdminStates.search_user)
    await callback.answer()


@rt.message(AdminStates.search_user)
async def admin_search_result(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    results = await DB.search_users(message.text.strip())
    await state.clear()

    if not results:
        await message.answer("❌ Не найдено", reply_markup=KB.back_admin())
        return

    u = results[0]
    badge = DB.get_badge(u)
    txt = T.ADMIN_USER_CARD.format(
        id=u["id"],
        telegram_id=u["telegram_id"],
        username=u.get("username") or "-",
        badge=badge,
        name=u["name"] or "-",
        age=u["age"] or "-",
        city=u["city"] or "-",
        bio=u["bio"] or "-",
        tier=TIER_NAMES.get(u["subscription_tier"], "🆓"),
        views=u["views_count"],
        likes=u["likes_received_count"],
        matches=u["matches_count"],
        boosts=u.get("boost_count", 0),
        created=u["created_at"].strftime("%d.%m.%Y") if u["created_at"] else "-",
        active=u["last_active_at"].strftime("%d.%m.%Y %H:%M") if u["last_active_at"] else "-",
        banned="Да 🚫" if u["is_banned"] else "Нет ✅",
    )
    await message.answer(
        txt, reply_markup=KB.admin_user(u["id"], u["is_banned"]),
        parse_mode=ParseMode.MARKDOWN
    )


# ═══ BAN / UNBAN / VERIFY / DELETE PHOTOS ═══

@rt.callback_query(F.data.startswith("au:ban:"))
async def admin_ban(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return
    uid = int(callback.data.split(":")[2])
    u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], is_banned=True)
        await callback.message.edit_text(
            f"🔒 *{u['name']}* забанен!",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN
        )
        try:
            await callback.bot.send_message(u["telegram_id"], T.BANNED)
        except Exception:
            pass
    await callback.answer()


@rt.callback_query(F.data.startswith("au:unban:"))
async def admin_unban(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return
    uid = int(callback.data.split(":")[2])
    u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], is_banned=False)
        await callback.message.edit_text(
            f"🔓 *{u['name']}* разбанен!",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN
        )
    await callback.answer()


@rt.callback_query(F.data.startswith("au:verify:"))
async def admin_verify(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return
    uid = int(callback.data.split(":")[2])
    u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], is_verified=True)
        await callback.message.edit_text(
            f"✅ *{u['name']}* верифицирован!",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN
        )
    await callback.answer()


@rt.callback_query(F.data.startswith("au:delphotos:"))
async def admin_del_photos(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return
    uid = int(callback.data.split(":")[2])
    u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], photos="", main_photo=None)
        await callback.message.edit_text(
            f"🗑️ Фото *{u['name']}* удалены!",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN
        )
    await callback.answer()


# ═══ GIVE VIP ═══

@rt.callback_query(F.data.startswith("au:givevip:"))
async def admin_give_vip(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    uid = int(callback.data.split(":")[2])
    await state.update_data(target_uid=uid)
    await callback.message.edit_text(
        "🍷 *Выбери тариф:*",
        reply_markup=KB.give_vip_tiers(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data.startswith("gv:"))
async def admin_gv_tier(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    tier = callback.data[3:]
    if tier == "wine_cellar":
        d = await state.get_data()
        await DB.activate_subscription_by_id(d["target_uid"], tier, 0)
        await state.clear()
        await callback.message.edit_text(
            "🏆 *Винный Погреб выдан навсегда!*",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await state.update_data(give_tier=tier)
        await callback.message.edit_text(
            "📅 *Сколько дней? Введи число:*",
            parse_mode=ParseMode.MARKDOWN
        )
        await state.set_state(AdminStates.give_vip_duration)
    await callback.answer()


@rt.message(AdminStates.give_vip_duration)
async def admin_gv_days(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    try:
        days = int(message.text.strip())
    except (ValueError, TypeError):
        await message.answer("❗ Введи число!")
        return
    d = await state.get_data()
    await DB.activate_subscription_by_id(d["target_uid"], d["give_tier"], days)
    await state.clear()
    tn = TIER_NAMES.get(d['give_tier'], 'VIP')
    await message.answer(
        f"🍷 *{tn}* на {days} дней выдан!",
        reply_markup=KB.main(),
        parse_mode=ParseMode.MARKDOWN
    )


# ═══ GIVE BOOSTS ═══

@rt.callback_query(F.data.startswith("au:giveboost:"))
async def admin_give_boost(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    uid = int(callback.data.split(":")[2])
    await state.update_data(target_uid=uid)
    await callback.message.edit_text(
        "🚀 *Сколько бустов выдать?*",
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(AdminStates.give_boost_count)
    await callback.answer()


@rt.message(AdminStates.give_boost_count)
async def admin_gb_count(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    try:
        count = int(message.text.strip())
    except (ValueError, TypeError):
        await message.answer("❗ Введи число!")
        return
    d = await state.get_data()
    await DB.add_boosts(d["target_uid"], count)
    await state.clear()
    await message.answer(
        f"🚀 *{count} бустов* выдано!",
        reply_markup=KB.main(),
        parse_mode=ParseMode.MARKDOWN
    )


# ═══ REPORTS ═══

@rt.callback_query(F.data == "adm:reports")
async def admin_reports(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return
    reps = await DB.get_pending_reports(5)
    if not reps:
        await callback.message.edit_text(
            "✅ *Нет новых жалоб!*",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN
        )
        await callback.answer()
        return

    rep = reps[0]
    rn = rep["reporter"]["name"] if rep["reporter"] else "?"
    rdn = rep["reported"]["name"] if rep["reported"] else "?"
    rid = rep["reported"]["id"] if rep["reported"] else 0

    txt = (
        f"⚠️ *Жалоба #{rep['id']}*\n\n"
        f"👤 На: *{rdn}* (ID:{rid})\n"
        f"👤 От: *{rn}*\n"
        f"📝 Причина: *{rep['reason']}*\n"
        f"📅 {rep['created_at'].strftime('%d.%m %H:%M')}\n\n"
        f"📊 Всего жалоб: {len(reps)}"
    )
    await callback.message.edit_text(
        txt, reply_markup=KB.admin_report(rep["id"], rid),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@rt.callback_query(F.data.startswith("ar:"))
async def admin_report_action(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return
    parts = callback.data.split(":")
    action = parts[1]
    rid = int(parts[2])
    ruid = int(parts[3])

    if action == "ban":
        u = await DB.get_user_by_id(ruid)
        if u:
            await DB.update_user(u["telegram_id"], is_banned=True)
        await DB.resolve_report(rid, "banned")
        await callback.message.edit_text("🔒 Забанен", reply_markup=KB.back_admin())

    elif action == "warn":
        u = await DB.get_user_by_id(ruid)
        if u:
            try:
                await callback.bot.send_message(
                    u["telegram_id"],
                    "⚠️ *Предупреждение от модерации!*\n\n"
                    "Ваш профиль получил жалобу. При повторном нарушении "
                    "аккаунт будет заблокирован.",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass
        await DB.resolve_report(rid, "warned")
        await callback.message.edit_text("⚠️ Предупреждён", reply_markup=KB.back_admin())

    elif action == "dismiss":
        await DB.resolve_report(rid, "dismissed")
        await callback.message.edit_text("❌ Отклонено", reply_markup=KB.back_admin())

    await callback.answer()


# ═══ PAYMENTS ═══

@rt.callback_query(F.data == "adm:payments")
async def admin_payments(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return
    pays = await DB.get_recent_payments(10)
    if not pays:
        await callback.message.edit_text(
            "💰 *Нет платежей*",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN
        )
        await callback.answer()
        return

    txt = "💰 *Последние платежи:*\n\n"
    for p in pays:
        st = {"pending": "⏳", "succeeded": "✅", "canceled": "❌"}.get(p["status"], "?")
        txt += f"{st} {p['amount']:.0f}₽ · {p['user_name']} · {p['description'] or '-'}\n"

    await callback.message.edit_text(
        txt, reply_markup=KB.back_admin(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


# ═══ TOP ═══

@rt.callback_query(F.data == "adm:top")
async def admin_top(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return
    async with async_session_maker() as s:
        tl = await s.execute(
            select(User).where(User.is_profile_complete == True)
            .order_by(desc(User.likes_received_count)).limit(5)
        )
        tv = await s.execute(
            select(User).where(User.is_profile_complete == True)
            .order_by(desc(User.views_count)).limit(5)
        )
        tm = await s.execute(
            select(User).where(User.is_profile_complete == True)
            .order_by(desc(User.matches_count)).limit(5)
        )

        txt = "🏆 *Топ по лайкам:*\n"
        for i, u in enumerate(tl.scalars().all(), 1):
            txt += f"{i}. {u.name} — ❤️ {u.likes_received_count}\n"

        txt += "\n👁️ *Топ по просмотрам:*\n"
        for i, u in enumerate(tv.scalars().all(), 1):
            txt += f"{i}. {u.name} — 👁️ {u.views_count}\n"

        txt += "\n💕 *Топ по мэтчам:*\n"
        for i, u in enumerate(tm.scalars().all(), 1):
            txt += f"{i}. {u.name} — 💕 {u.matches_count}\n"

    await callback.message.edit_text(
        txt, reply_markup=KB.back_admin(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


# ═══ BROADCAST ═══

@rt.callback_query(F.data == "adm:broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    await callback.message.edit_text(
        "📢 *Текст рассылки:*\n\n_Markdown поддерживается_",
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(AdminStates.broadcast_text)
    await callback.answer()


@rt.message(AdminStates.broadcast_text)
async def admin_bc_text(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    await state.update_data(bc_text=message.text)
    await message.answer(
        "👥 *Аудитория:*",
        reply_markup=KB.broadcast_targets(),
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(AdminStates.broadcast_confirm)


@rt.callback_query(AdminStates.broadcast_confirm, F.data.startswith("bc:"))
async def admin_bc_target(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return

    target = callback.data[3:]

    if target == "send":
        d = await state.get_data()
        txt = d["bc_text"]
        tgt = d.get("bc_target", "all")
        ids = await DB.get_all_user_ids(tgt)
        await state.clear()

        await callback.message.edit_text(
            f"📤 *Отправка {len(ids)} сообщений...*",
            parse_mode=ParseMode.MARKDOWN
        )

        sent = 0
        failed = 0
        for tid in ids:
            try:
                await callback.bot.send_message(
                    tid, txt, parse_mode=ParseMode.MARKDOWN
                )
                sent += 1
            except Exception:
                failed += 1
            if sent % 25 == 0:
                await asyncio.sleep(1)

        await DB.log_broadcast(user["telegram_id"], txt, tgt, sent, failed)
        await callback.message.answer(
            f"✅ *Рассылка завершена!*\n\n"
            f"📤 Отправлено: {sent}\n"
            f"❌ Ошибок: {failed}",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await state.update_data(bc_target=target)
        d = await state.get_data()
        names = {"all": "Все", "complete": "С анкетой", "vip": "VIP", "free": "Бесплатные"}
        ids = await DB.get_all_user_ids(target)

        await callback.message.edit_text(
            T.BROADCAST_CONFIRM.format(
                text=d["bc_text"][:200],
                target=names.get(target, target),
                count=len(ids)
            ),
            reply_markup=KB.broadcast_confirm(),
            parse_mode=ParseMode.MARKDOWN
        )
    await callback.answer()


# ═══ PROMO (admin create) ═══

@rt.callback_query(F.data == "adm:promo")
async def admin_promo(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    await callback.message.edit_text(
        "🎁 *Код промокода:*\n_(напр. WINE2024)_",
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(AdminStates.promo_code)
    await callback.answer()


@rt.message(AdminStates.promo_code)
async def admin_promo_code(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    await state.update_data(pc_code=message.text.strip().upper())
    await message.answer(
        "🍷 *Тариф:*",
        reply_markup=KB.give_vip_tiers(),
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(AdminStates.promo_tier)


@rt.callback_query(AdminStates.promo_tier, F.data.startswith("gv:"))
async def admin_promo_tier(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    await state.update_data(pc_tier=callback.data[3:])
    await callback.message.edit_text(
        "📅 *Сколько дней VIP?*",
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(AdminStates.promo_duration)
    await callback.answer()


@rt.message(AdminStates.promo_duration)
async def admin_promo_dur(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    try:
        days = int(message.text.strip())
    except (ValueError, TypeError):
        await message.answer("❗ Введи число!")
        return
    await state.update_data(pc_days=days)
    await message.answer("🔢 *Лимит использований?*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.promo_uses)


@rt.message(AdminStates.promo_uses)
async def admin_promo_uses(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return
    try:
        uses = int(message.text.strip())
    except (ValueError, TypeError):
        await message.answer("❗ Введи число!")
        return

    d = await state.get_data()
    await DB.create_promo(d["pc_code"], d["pc_tier"], d["pc_days"], uses)
    await state.clear()

    tn = TIER_NAMES.get(d["pc_tier"], "VIP")
    await message.answer(
        f"🎁 *Промокод создан!*\n\n"
        f"🔑 `{d['pc_code']}`\n"
        f"🍷 {tn} · {d['pc_days']} дн\n"
        f"🔢 Лимит: {uses} использований",
        reply_markup=KB.main(),
        parse_mode=ParseMode.MARKDOWN
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                  MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def main():
    await init_db()

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())

    dp.include_router(rt)

    logger.info(f"🍷 {BOT_NAME} v4.0 starting...")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
