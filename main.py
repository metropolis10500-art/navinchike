"""
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🍷 ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v5.5-fix
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import os
import uuid
import logging
import random
import json
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Set
from dataclasses import dataclass, field
from collections import defaultdict

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
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
    select, update, func, and_, or_, desc, text as sa_text
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
BOT_SHORT = "Винчик"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                      CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///dating_bot.db")
    YOOKASSA_SHOP_ID: str = os.getenv("YOOKASSA_SHOP_ID", "")
    YOOKASSA_SECRET_KEY: str = os.getenv("YOOKASSA_SECRET_KEY", "")
    DOMAIN: str = os.getenv("DOMAIN", "https://yourdomain.ru")

    FREE_DAILY_LIKES: int = 30
    FREE_DAILY_MESSAGES: int = 10
    FREE_DAILY_SUPER_LIKES: int = 1
    FREE_GUESTS_VISIBLE: int = 3

    ELO_DEFAULT: int = 1000
    ELO_K_FACTOR: int = 32

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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                   ENUMS & MODELS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class Gender(str, Enum):
    MALE = "male"
    FEMALE = "female"

class LookingFor(str, Enum):
    MALE = "male"
    FEMALE = "female"
    BOTH = "both"

class SubscriptionTier(str, Enum):
    FREE = "free"
    WINE_GLASS = "wine_glass"
    WINE_BOTTLE = "wine_bottle"
    SOMMELIER = "sommelier"
    WINE_CELLAR = "wine_cellar"

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
    interests = Column(Text, default="")
    zodiac = Column(String(30), nullable=True)
    height = Column(Integer, nullable=True)
    smoking = Column(String(20), nullable=True)
    drinking = Column(String(20), nullable=True)
    children = Column(String(20), nullable=True)
    job = Column(String(100), nullable=True)
    relationship_goal = Column(String(30), nullable=True)
    is_active = Column(Boolean, default=True)
    is_banned = Column(Boolean, default=False)
    is_verified = Column(Boolean, default=False)
    is_profile_complete = Column(Boolean, default=False)
    is_online = Column(Boolean, default=False)
    subscription_tier = Column(SQLEnum(SubscriptionTier), default=SubscriptionTier.FREE)
    subscription_expires_at = Column(DateTime, nullable=True)
    daily_likes_remaining = Column(Integer, default=30)
    daily_messages_remaining = Column(Integer, default=10)
    daily_super_likes_remaining = Column(Integer, default=1)
    last_limits_reset = Column(DateTime, nullable=True)
    boost_expires_at = Column(DateTime, nullable=True)
    boost_count = Column(Integer, default=0)
    invisible_mode = Column(Boolean, default=False)
    read_receipts = Column(Boolean, default=True)
    who_liked_visible = Column(Boolean, default=False)
    rewind_count = Column(Integer, default=0)
    views_count = Column(Integer, default=0)
    likes_received_count = Column(Integer, default=0)
    likes_given_count = Column(Integer, default=0)
    matches_count = Column(Integer, default=0)
    messages_sent_count = Column(Integer, default=0)
    elo_score = Column(Integer, default=1000)
    attractiveness_score = Column(Float, default=50.0)
    profile_quality_score = Column(Float, default=0.0)
    response_rate = Column(Float, default=0.0)
    notify_likes = Column(Boolean, default=True)
    notify_matches = Column(Boolean, default=True)
    notify_messages = Column(Boolean, default=True)
    notify_guests = Column(Boolean, default=True)
    referral_code = Column(String(20), unique=True, nullable=True)
    referred_by = Column(Integer, nullable=True)
    referral_bonus_claimed = Column(Integer, default=0)
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


class Dislike(Base):
    __tablename__ = "dislikes"
    id = Column(Integer, primary_key=True)
    from_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    to_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Match(Base):
    __tablename__ = "matches"
    id = Column(Integer, primary_key=True)
    user1_id = Column(Integer, ForeignKey("users.id"), index=True)
    user2_id = Column(Integer, ForeignKey("users.id"), index=True)
    is_active = Column(Boolean, default=True)
    compatibility_score = Column(Float, default=0.0)
    compatibility_details = Column(Text, default="{}")
    icebreaker_text = Column(Text, nullable=True)
    last_message_at = Column(DateTime, nullable=True)
    messages_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatMessage(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey("matches.id"), index=True)
    sender_id = Column(Integer, ForeignKey("users.id"))
    text = Column(Text, nullable=True)
    photo_id = Column(String(255), nullable=True)
    voice_id = Column(String(255), nullable=True)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class GuestVisit(Base):
    __tablename__ = "guest_visits"
    id = Column(Integer, primary_key=True)
    visitor_id = Column(Integer, ForeignKey("users.id"))
    visited_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class UserInteraction(Base):
    __tablename__ = "user_interactions"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    target_id = Column(Integer, ForeignKey("users.id"), index=True)
    interaction_type = Column(String(30))
    created_at = Column(DateTime, default=datetime.utcnow)


class UserPreferenceLearned(Base):
    __tablename__ = "user_preferences_learned"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, index=True)
    preferred_age_min = Column(Integer, nullable=True)
    preferred_age_max = Column(Integer, nullable=True)
    preferred_cities = Column(Text, default="")
    preferred_interests = Column(Text, default="")
    avg_liked_elo = Column(Float, default=1000)
    avg_liked_age = Column(Float, nullable=True)
    like_rate = Column(Float, default=0.5)
    total_interactions = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow)


class DailyMatch(Base):
    __tablename__ = "daily_matches"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    recommended_id = Column(Integer, ForeignKey("users.id"))
    compatibility_score = Column(Float, default=0)
    reason = Column(Text, nullable=True)
    is_seen = Column(Boolean, default=False)
    is_liked = Column(Boolean, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class TopPick(Base):
    __tablename__ = "top_picks"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    picked_user_id = Column(Integer, ForeignKey("users.id"))
    category = Column(String(50))
    score = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


class LikeQueue(Base):
    __tablename__ = "like_queue"
    id = Column(Integer, primary_key=True)
    target_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    from_user_id = Column(Integer, ForeignKey("users.id"))
    is_super = Column(Boolean, default=False)
    message = Column(Text, nullable=True)
    is_revealed = Column(Boolean, default=False)
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


# ═══ FIX 1: WAL mode + serialized access for SQLite ═══
engine = create_async_engine(
    config.DATABASE_URL,
    echo=False,
    # Ключевое: SQLite не поддерживает параллельные записи
    # pool_size=1 предотвращает deadlock
    pool_size=1,
    max_overflow=0,
    pool_timeout=30,
    connect_args={"timeout": 30} if "sqlite" in config.DATABASE_URL else {},
)
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# ═══ FIX 2: Глобальный лок для записи в SQLite ═══
_db_write_lock = asyncio.Lock()


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Включаем WAL mode для SQLite — предотвращает блокировки
        if "sqlite" in config.DATABASE_URL:
            await conn.execute(sa_text("PRAGMA journal_mode=WAL"))
            await conn.execute(sa_text("PRAGMA busy_timeout=5000"))

    migs = [
        ("users", "interests", "TEXT", "''"),
        ("users", "zodiac", "VARCHAR(30)", "NULL"),
        ("users", "height", "INTEGER", "NULL"),
        ("users", "smoking", "VARCHAR(20)", "NULL"),
        ("users", "drinking", "VARCHAR(20)", "NULL"),
        ("users", "children", "VARCHAR(20)", "NULL"),
        ("users", "job", "VARCHAR(100)", "NULL"),
        ("users", "relationship_goal", "VARCHAR(30)", "NULL"),
        ("users", "is_online", "BOOLEAN", "0"),
        ("users", "daily_super_likes_remaining", "INTEGER", "1"),
        ("users", "likes_given_count", "INTEGER", "0"),
        ("users", "messages_sent_count", "INTEGER", "0"),
        ("users", "elo_score", "INTEGER", "1000"),
        ("users", "attractiveness_score", "FLOAT", "50.0"),
        ("users", "profile_quality_score", "FLOAT", "0.0"),
        ("users", "response_rate", "FLOAT", "0.0"),
        ("users", "invisible_mode", "BOOLEAN", "0"),
        ("users", "read_receipts", "BOOLEAN", "1"),
        ("users", "who_liked_visible", "BOOLEAN", "0"),
        ("users", "rewind_count", "INTEGER", "0"),
        ("users", "notify_likes", "BOOLEAN", "1"),
        ("users", "notify_matches", "BOOLEAN", "1"),
        ("users", "notify_messages", "BOOLEAN", "1"),
        ("users", "notify_guests", "BOOLEAN", "1"),
        ("users", "referred_by", "INTEGER", "NULL"),
        ("users", "referral_bonus_claimed", "INTEGER", "0"),
        ("likes", "message", "TEXT", "NULL"),
        ("matches", "compatibility_details", "TEXT", "'{}'"),
        ("matches", "icebreaker_text", "TEXT", "NULL"),
        ("matches", "messages_count", "INTEGER", "0"),
        ("messages", "photo_id", "VARCHAR(255)", "NULL"),
        ("messages", "voice_id", "VARCHAR(255)", "NULL"),
    ]

    async with engine.begin() as conn:
        for t, c, tp, d in migs:
            try:
                dc = f"DEFAULT {d}" if d != "NULL" else ""
                await conn.execute(sa_text(f"ALTER TABLE {t} ADD COLUMN {c} {tp} {dc}"))
                logger.info(f"  Migration: added {t}.{c}")
            except Exception:
                pass  # Column already exists
    logger.info("🍷 DB ready (v5.5-fix)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    FSM STATES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RegStates(StatesGroup):
    name = State()
    age = State()
    gender = State()
    city = State()
    photo = State()
    bio = State()
    looking_for = State()
    interests = State()
    relationship_goal = State()

class EditStates(StatesGroup):
    edit_name = State()
    edit_age = State()
    edit_city = State()
    edit_bio = State()
    add_photo = State()
    edit_interests = State()
    edit_goal = State()
    edit_height = State()
    edit_job = State()

class ChatStates(StatesGroup):
    chatting = State()

class PromoInputState(StatesGroup):
    waiting_code = State()

class SuperLikeMessage(StatesGroup):
    writing = State()

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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    CONSTANTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

INTERESTS_LIST = [
    "🎵 Музыка", "🎬 Кино", "📚 Книги", "🎮 Игры",
    "⚽ Спорт", "🧘 Йога", "✈️ Путешествия", "🍳 Кулинария",
    "📸 Фото", "🎨 Искусство", "💻 IT", "🐕 Животные",
    "🌱 Природа", "🎭 Театр", "💃 Танцы", "🏋️ Фитнес",
    "🍷 Вино", "☕ Кофе", "🎸 Музыка вживую", "🏔️ Походы",
    "🎯 Настолки", "🧠 Психология", "🚗 Авто", "🎤 Караоке",
]

RELATIONSHIP_GOALS = {
    "serious": "💍 Серьёзные отношения",
    "casual": "🌸 Лёгкие отношения",
    "friends": "🤝 Дружба и общение",
    "not_sure": "🤷 Ещё не решил(а)",
}

DRINKING_OPTIONS = {
    "no": "🚫 Не пью", "wine_only": "🍷 Только вино",
    "sometimes": "🥂 Иногда", "yes": "🍻 Регулярно",
}

ZODIAC_SIGNS = [
    "♈ Овен", "♉ Телец", "♊ Близнецы", "♋ Рак",
    "♌ Лев", "♍ Дева", "♎ Весы", "♏ Скорпион",
    "♐ Стрелец", "♑ Козерог", "♒ Водолей", "♓ Рыбы"
]

TIER_NAMES = {
    "free": "🆓 Бесплатный",
    "wine_glass": "🥂 Бокал Вина",
    "wine_bottle": "🍾 Бутылка Вина",
    "sommelier": "🎖️ Сомелье",
    "wine_cellar": "🏆 Винный Погреб",
}

TIER_FEATURES = {
    "free": {
        "likes": 30, "messages": 10, "super_likes": 1,
        "guests": 3, "boosts": 0, "rewinds": 0,
        "who_liked": False, "invisible": False,
        "read_receipts": False, "priority": False,
        "ad_free": False, "badge": "",
    },
    "wine_glass": {
        "likes": 100, "messages": 999999, "super_likes": 5,
        "guests": 15, "boosts": 0, "rewinds": 3,
        "who_liked": False, "invisible": False,
        "read_receipts": True, "priority": False,
        "ad_free": True, "badge": "🥂",
    },
    "wine_bottle": {
        "likes": 999999, "messages": 999999, "super_likes": 10,
        "guests": 999, "boosts": 1, "rewinds": 10,
        "who_liked": True, "invisible": True,
        "read_receipts": True, "priority": True,
        "ad_free": True, "badge": "🍾",
    },
    "sommelier": {
        "likes": 999999, "messages": 999999, "super_likes": 30,
        "guests": 999, "boosts": 3, "rewinds": 999,
        "who_liked": True, "invisible": True,
        "read_receipts": True, "priority": True,
        "ad_free": True, "badge": "🎖️",
        "top_picks": True, "super_like_message": True,
    },
    "wine_cellar": {
        "likes": 999999, "messages": 999999, "super_likes": 999,
        "guests": 999, "boosts": 5, "rewinds": 999,
        "who_liked": True, "invisible": True,
        "read_receipts": True, "priority": True,
        "ad_free": True, "badge": "🏆",
        "top_picks": True, "super_like_message": True,
        "profile_boost_weekly": True, "founder_badge": True,
    },
}

ICEBREAKER_TEMPLATES = [
    "🍷 Какое вино предпочитаешь — красное или белое?",
    "✈️ Куда бы ты отправился(ась) прямо сейчас?",
    "🎬 Какой фильм посоветуешь на вечер?",
    "☕ Кофе или чай? Важный вопрос!",
    "🎵 Какая песня сейчас на повторе?",
    "🍕 Пицца или суши? Правильного ответа нет!",
    "📚 Что сейчас читаешь?",
    "🌟 Расскажи что-то, чего о тебе никто не знает!",
    "🎯 Какое хобби хотел(а) бы попробовать?",
    "🏆 Какое твоё самое крутое достижение?",
]

FLIRT_TIPS = [
    "💡 *Совет:* Задавай открытые вопросы — они помогают узнать человека глубже!",
    "💡 *Совет:* Расскажи забавную историю — юмор сближает!",
    "💡 *Совет:* Сделай комплимент чему-то в профиле — это приятно!",
    "💡 *Совет:* Предложи встретиться в реальной жизни после 10-15 сообщений!",
    "💡 *Совет:* Будь собой — искренность привлекает больше всего!",
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#             SMART COMPATIBILITY ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SmartMatch:
    @staticmethod
    def calculate(u1: Dict, u2: Dict, learned: Dict = None) -> Dict:
        details = {}

        # 1. БАЗА (25%)
        base = 0
        if (u1.get("city") or "").lower() == (u2.get("city") or "").lower():
            base += 40
        else:
            base += 5
        age_diff = abs((u1.get("age") or 25) - (u2.get("age") or 25))
        if age_diff <= 2: base += 30
        elif age_diff <= 5: base += 25
        elif age_diff <= 8: base += 15
        elif age_diff <= 12: base += 8
        else: base += 2
        lf1 = u1.get("looking_for", "both")
        lf2 = u2.get("looking_for", "both")
        g1 = u1.get("gender")
        g2 = u2.get("gender")
        mutual = True
        if lf1 != "both" and lf1 != g2: mutual = False
        if lf2 != "both" and lf2 != g1: mutual = False
        if mutual: base += 30
        else: base = int(base * 0.3)
        details["base"] = min(base, 100)

        # 2. ИНТЕРЕСЫ (20%)
        i1 = set(filter(None, (u1.get("interests") or "").split(",")))
        i2 = set(filter(None, (u2.get("interests") or "").split(",")))
        if i1 and i2:
            jaccard = len(i1 & i2) / len(i1 | i2)
            details["interests"] = min(jaccard * 200, 100)
        else:
            details["interests"] = 40

        # 3. ЦЕЛИ (15%)
        rg1 = u1.get("relationship_goal")
        rg2 = u2.get("relationship_goal")
        if rg1 and rg2:
            if rg1 == rg2: details["goals"] = 100
            elif {rg1, rg2} & {"not_sure"}: details["goals"] = 60
            elif {rg1, rg2} == {"serious", "casual"}: details["goals"] = 15
            else: details["goals"] = 45
        else:
            details["goals"] = 50

        # 4. ОБРАЗ ЖИЗНИ (10%)
        life = []
        for f in ["smoking", "drinking", "children"]:
            v1, v2 = u1.get(f), u2.get(f)
            if v1 and v2:
                if v1 == v2: life.append(100)
                elif "no" in (v1, v2) and "yes" in (v1, v2): life.append(20)
                else: life.append(60)
        details["lifestyle"] = sum(life) / len(life) if life else 50

        # 5. АКТИВНОСТЬ (10%)
        la = u2.get("last_active_at")
        if la:
            h = (datetime.utcnow() - la).total_seconds() / 3600
            if h < 0.5: details["activity"] = 100
            elif h < 1: details["activity"] = 95
            elif h < 3: details["activity"] = 85
            elif h < 12: details["activity"] = 65
            elif h < 24: details["activity"] = 45
            elif h < 72: details["activity"] = 20
            else: details["activity"] = 5
        else:
            details["activity"] = 10
        if u2.get("is_online"):
            details["activity"] = min(details["activity"] + 15, 100)

        # 6. КАЧЕСТВО ПРОФИЛЯ (5%)
        pq = 0
        if u2.get("main_photo"): pq += 25
        photos = list(filter(None, (u2.get("photos") or "").split(",")))
        pq += min(len(photos), 4) * 8
        if u2.get("bio") and len(u2["bio"]) > 20: pq += 20
        if u2.get("interests"): pq += 10
        if u2.get("relationship_goal"): pq += 8
        details["profile"] = min(pq, 100)

        # 7. ELO (5%)
        e_diff = abs((u1.get("elo_score") or 1000) - (u2.get("elo_score") or 1000))
        if e_diff <= 50: details["elo"] = 100
        elif e_diff <= 100: details["elo"] = 80
        elif e_diff <= 200: details["elo"] = 50
        else: details["elo"] = 20

        # 8. ЗОДИАК (5%)
        z1, z2 = u1.get("zodiac"), u2.get("zodiac")
        if z1 and z2:
            details["zodiac"] = 80 if z1 == z2 else 50 + random.randint(-10, 15)
        else:
            details["zodiac"] = 50

        # 9. ML (5%)
        if learned and learned.get("total_interactions", 0) > 5:
            ls = 50
            if learned.get("avg_liked_age"):
                ad = abs((u2.get("age") or 25) - learned["avg_liked_age"])
                if ad <= 2: ls += 25
                elif ad <= 5: ls += 15
            pi = set(filter(None, (learned.get("preferred_interests") or "").split(",")))
            if pi and i2:
                ls += min(len(pi & i2) * 10, 25)
            details["learned"] = min(ls, 100)
        else:
            details["learned"] = 50

        weights = {"base": 25, "interests": 20, "goals": 15, "lifestyle": 10,
                   "activity": 10, "profile": 5, "elo": 5, "zodiac": 5, "learned": 5}
        total = sum(details.get(k, 50) * w / 100 for k, w in weights.items())
        total = min(max(total, 0), 100)
        common_interests = list(i1 & i2) if i1 and i2 else []

        return {
            "total": round(total, 1),
            "details": details,
            "common_interests": common_interests,
            "city_match": (u1.get("city") or "").lower() == (u2.get("city") or "").lower(),
        }

    @staticmethod
    def make_icebreaker(u1: Dict, u2: Dict, compat: Dict) -> str:
        common = compat.get("common_interests", [])
        options = []
        if common:
            interest = random.choice(common)
            options.append(f"💡 У вас общее: *{interest}*! Обсудите!")
        if compat.get("city_match"):
            options.append(f"🏙️ Вы оба из *{u1.get('city')}*! Какое место любите?")
        if u1.get("zodiac") and u2.get("zodiac"):
            options.append(f"🔮 {u1['zodiac']} + {u2['zodiac']} — отличное сочетание!")
        if u1.get("drinking") == "wine_only" and u2.get("drinking") == "wine_only":
            options.append("🍷 Вы оба любители вина! Красное или белое?")
        options.extend(random.sample(ICEBREAKER_TEMPLATES, min(2, len(ICEBREAKER_TEMPLATES))))
        return random.choice(options)

    @staticmethod
    def bar(score: float, detailed: bool = False) -> str:
        filled = int(score / 10)
        bar = "🟣" * filled + "⚪" * (10 - filled)
        if score >= 85: e, l = "🔥", "Идеальная пара!"
        elif score >= 70: e, l = "💕", "Отличная совместимость!"
        elif score >= 55: e, l = "✨", "Хорошие шансы!"
        elif score >= 40: e, l = "👍", "Стоит попробовать"
        else: e, l = "🤔", "Есть потенциал"
        r = f"{e} {bar} *{score:.0f}%*"
        if detailed: r += f"\n_{l}_"
        return r

    @staticmethod
    def breakdown(details: Dict) -> str:
        labels = {
            "base": "🏠 База", "interests": "🎯 Интересы",
            "goals": "💍 Цели", "lifestyle": "🌿 Стиль жизни",
            "activity": "⚡ Активность", "profile": "📝 Профиль",
            "elo": "⚖️ Рейтинг", "zodiac": "🔮 Зодиак", "learned": "🧠 AI"
        }
        lines = []
        for k, label in labels.items():
            v = details.get(k, 50)
            b = int(v / 20)
            lines.append(f"  {label}: {'█' * b}{'░' * (5 - b)} {v:.0f}%")
        return "\n".join(lines)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#              ELO & LEARNING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class EloSystem:
    @staticmethod
    async def on_like(liker_id: int, liked_id: int):
        """FIX: использует лок для безопасной записи"""
        try:
            async with _db_write_lock:
                async with async_session_maker() as s:
                    u = await s.execute(select(User).where(User.id == liked_id))
                    liked = u.scalar_one_or_none()
                    if liked:
                        new_elo = min(2000, (liked.elo_score or 1000) + 8)
                        tv = max(liked.views_count or 1, 1)
                        attr = min(((liked.likes_received_count or 0) + 1) / tv * 100, 100)
                        await s.execute(update(User).where(User.id == liked_id).values(
                            elo_score=new_elo, attractiveness_score=attr))
                        await s.commit()
        except Exception as e:
            logger.error(f"EloSystem.on_like error: {e}")

    @staticmethod
    async def on_dislike(disliked_id: int):
        try:
            async with _db_write_lock:
                async with async_session_maker() as s:
                    await s.execute(update(User).where(User.id == disliked_id).values(
                        elo_score=func.greatest(User.elo_score - 3, 100)))
                    await s.commit()
        except Exception as e:
            logger.error(f"EloSystem.on_dislike error: {e}")


class Learner:
    @staticmethod
    async def record(uid: int, tid: int, itype: str):
        try:
            async with _db_write_lock:
                async with async_session_maker() as s:
                    s.add(UserInteraction(user_id=uid, target_id=tid, interaction_type=itype))
                    await s.commit()
        except Exception as e:
            logger.error(f"Learner.record error: {e}")

    @staticmethod
    async def learn(uid: int):
        try:
            async with async_session_maker() as s:
                likes_r = await s.execute(select(Like.to_user_id).where(Like.from_user_id == uid))
                liked_ids = [r[0] for r in likes_r.fetchall()]
                if len(liked_ids) < 3:
                    return

                liked_users_r = await s.execute(select(User).where(User.id.in_(liked_ids)))
                liked_users = liked_users_r.scalars().all()
                if not liked_users:
                    return

                ages = [u.age for u in liked_users if u.age]
                avg_age = sum(ages) / len(ages) if ages else None

                int_counter = defaultdict(int)
                for u in liked_users:
                    for i in filter(None, (u.interests or "").split(",")):
                        int_counter[i.strip()] += 1
                top_int = ",".join(
                    [k for k, _ in sorted(int_counter.items(), key=lambda x: x[1], reverse=True)[:10]]
                )

                elos = [u.elo_score for u in liked_users if u.elo_score]
                avg_elo = sum(elos) / len(elos) if elos else 1000

                dislikes_r = await s.execute(
                    select(func.count(Dislike.id)).where(Dislike.from_user_id == uid)
                )
                total_d = dislikes_r.scalar() or 0
                lr = len(liked_ids) / max(len(liked_ids) + total_d, 1)

            vals = {
                "preferred_age_min": int(avg_age - 3) if avg_age else None,
                "preferred_age_max": int(avg_age + 3) if avg_age else None,
                "preferred_interests": top_int,
                "avg_liked_elo": avg_elo,
                "avg_liked_age": avg_age,
                "like_rate": lr,
                "total_interactions": len(liked_ids) + total_d,
                "updated_at": datetime.utcnow(),
            }

            async with _db_write_lock:
                async with async_session_maker() as s:
                    ex = await s.execute(
                        select(UserPreferenceLearned).where(UserPreferenceLearned.user_id == uid)
                    )
                    if ex.scalar_one_or_none():
                        await s.execute(
                            update(UserPreferenceLearned)
                            .where(UserPreferenceLearned.user_id == uid)
                            .values(**vals)
                        )
                    else:
                        s.add(UserPreferenceLearned(user_id=uid, **vals))
                    await s.commit()
        except Exception as e:
            logger.error(f"Learner.learn error: {e}")

    @staticmethod
    async def get_prefs(uid: int) -> Optional[Dict]:
        try:
            async with async_session_maker() as s:
                r = await s.execute(
                    select(UserPreferenceLearned).where(UserPreferenceLearned.user_id == uid)
                )
                p = r.scalar_one_or_none()
                if not p:
                    return None
                return {
                    "preferred_interests": p.preferred_interests,
                    "avg_liked_age": p.avg_liked_age,
                    "avg_liked_elo": p.avg_liked_elo,
                    "total_interactions": p.total_interactions,
                }
        except Exception as e:
            logger.error(f"Learner.get_prefs error: {e}")
            return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#              PROFILE QUALITY ANALYZER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ProfileAnalyzer:
    @staticmethod
    def analyze(u: Dict) -> Dict:
        score, tips = 0, []
        photos = list(filter(None, (u.get("photos") or "").split(",")))
        if len(photos) >= 3: score += 30
        elif len(photos) == 2: score += 20; tips.append("📸 Добавь ещё фото — +40% лайков!")
        elif len(photos) == 1: score += 10; tips.append("📸 3+ фото = вдвое больше лайков!")
        else: tips.append("📸 Добавь фото! Без них почти нет лайков")

        bio = u.get("bio") or ""
        if len(bio) > 80: score += 20
        elif len(bio) > 20: score += 12; tips.append("📝 Расширь описание — расскажи о себе!")
        elif bio: score += 5; tips.append("📝 Описание слишком короткое")
        else: tips.append("📝 Добавь описание!")

        ints = list(filter(None, (u.get("interests") or "").split(",")))
        if len(ints) >= 3: score += 15
        elif ints: score += 8; tips.append("🎯 Добавь больше интересов!")
        else: tips.append("🎯 Укажи интересы — ключ к совместимости!")

        if u.get("relationship_goal"): score += 10
        else: tips.append("💍 Укажи цель знакомства!")

        extras = sum(1 for f in ["height", "zodiac", "drinking", "job"] if u.get(f))
        score += min(extras * 3, 10)
        if extras < 2: tips.append("📋 Заполни доп. инфо — рост, работа, знак зодиака")

        if u.get("is_verified"): score += 5
        la = u.get("last_active_at")
        if la and (datetime.utcnow() - la).total_seconds() < 86400: score += 10

        grade = "🏆" if score >= 85 else "🌟" if score >= 65 else "👍" if score >= 45 else "📈"
        return {"score": min(score, 100), "tips": tips[:3], "grade": grade}

    @staticmethod
    def format(a: Dict) -> str:
        s = a["score"]
        bar = "🟢" * (s // 10) + "⚪" * (10 - s // 10)
        txt = f"📊 *Профиль: {s}%* {a['grade']}\n{bar}\n"
        if a["tips"]:
            txt += "\n💡 *Улучши:*\n"
            for t in a["tips"]:
                txt += f"  {t}\n"
        return txt


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                  ANIMATIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class Anim:
    @staticmethod
    async def animate(msg: Message, frames: List[str], delay=0.4) -> Message:
        m = await msg.answer(frames[0])
        for f in frames[1:]:
            await asyncio.sleep(delay)
            try:
                await m.edit_text(f, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        return m

    @staticmethod
    async def match_wow(msg: Message, name: str, score: float, icebreaker: str):
        frames = ["💕", "💕✨💕", "🎉💕🎉", "🍷✨ *МЭТЧ!* ✨🍷"]
        m = await msg.answer(frames[0])
        for f in frames[1:]:
            await asyncio.sleep(0.45)
            try:
                await m.edit_text(f, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        await asyncio.sleep(0.5)
        bar = SmartMatch.bar(score, True)
        try:
            await m.edit_text(
                f"🍷✨ *Мэтч с {name}!* ✨🍷\n\n"
                f"📊 {bar}\n\n{icebreaker}\n\n_Напиши первым!_ 💬",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

    @staticmethod
    async def super_like_sent(msg: Message):
        frames = ["⚡", "⚡✨", "⚡✨💜", "⚡ *Суперлайк отправлен!*"]
        m = await msg.answer(frames[0])
        for f in frames[1:]:
            await asyncio.sleep(0.3)
            try:
                await m.edit_text(f, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        return m


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    DB SERVICE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DB:
    @staticmethod
    def _d(u: User) -> Dict:
        return {
            "id": u.id, "telegram_id": u.telegram_id,
            "username": u.username, "name": u.name, "age": u.age,
            "gender": u.gender.value if u.gender else None,
            "city": u.city, "bio": u.bio,
            "looking_for": u.looking_for.value if u.looking_for else "both",
            "age_from": u.age_from, "age_to": u.age_to,
            "photos": u.photos or "", "main_photo": u.main_photo,
            "interests": u.interests or "",
            "zodiac": u.zodiac, "height": u.height,
            "smoking": u.smoking, "drinking": u.drinking,
            "children": u.children, "job": u.job,
            "relationship_goal": u.relationship_goal,
            "is_active": u.is_active, "is_banned": u.is_banned,
            "is_verified": u.is_verified,
            "is_profile_complete": u.is_profile_complete,
            "is_online": u.is_online,
            "subscription_tier": u.subscription_tier.value if u.subscription_tier else "free",
            "subscription_expires_at": u.subscription_expires_at,
            "daily_likes_remaining": u.daily_likes_remaining or 30,
            "daily_messages_remaining": u.daily_messages_remaining or 10,
            "daily_super_likes_remaining": u.daily_super_likes_remaining or 1,
            "boost_expires_at": u.boost_expires_at,
            "boost_count": u.boost_count or 0,
            "invisible_mode": u.invisible_mode or False,
            "who_liked_visible": u.who_liked_visible or False,
            "rewind_count": u.rewind_count or 0,
            "views_count": u.views_count or 0,
            "likes_received_count": u.likes_received_count or 0,
            "likes_given_count": u.likes_given_count or 0,
            "matches_count": u.matches_count or 0,
            "elo_score": u.elo_score or 1000,
            "attractiveness_score": u.attractiveness_score or 50,
            "profile_quality_score": u.profile_quality_score or 0,
            "referral_code": u.referral_code,
            "notify_likes": u.notify_likes if u.notify_likes is not None else True,
            "notify_matches": u.notify_matches if u.notify_matches is not None else True,
            "notify_messages": u.notify_messages if u.notify_messages is not None else True,
            "created_at": u.created_at, "last_active_at": u.last_active_at,
        }

    @staticmethod
    def is_vip(u):
        t = u.get("subscription_tier", "free")
        return t == "wine_cellar" or (
            t != "free"
            and u.get("subscription_expires_at")
            and u["subscription_expires_at"] > datetime.utcnow()
        )

    @staticmethod
    def is_boosted(u):
        e = u.get("boost_expires_at")
        return e and e > datetime.utcnow()

    @staticmethod
    def is_creator(u):
        return u.get("telegram_id") in config.CREATOR_IDS

    @staticmethod
    def is_admin(u):
        return u.get("telegram_id") in config.ADMIN_IDS

    @staticmethod
    def badge(u):
        if DB.is_creator(u):
            return "👑 "
        t = u.get("subscription_tier", "free")
        f = TIER_FEATURES.get(t, {})
        return f.get("badge", "") + " " if f.get("badge") else ("✅ " if u.get("is_verified") else "")

    @staticmethod
    def features(u):
        return TIER_FEATURES.get(u.get("subscription_tier", "free"), TIER_FEATURES["free"])

    @staticmethod
    async def get_user(tg_id):
        async with async_session_maker() as s:
            r = await s.execute(select(User).where(User.telegram_id == tg_id))
            u = r.scalar_one_or_none()
            return DB._d(u) if u else None

    @staticmethod
    async def get_user_by_id(uid):
        async with async_session_maker() as s:
            r = await s.execute(select(User).where(User.id == uid))
            u = r.scalar_one_or_none()
            return DB._d(u) if u else None

    @staticmethod
    async def create_user(tg_id, username=None):
        async with _db_write_lock:
            async with async_session_maker() as s:
                u = User(
                    telegram_id=tg_id,
                    username=username,
                    referral_code=str(uuid.uuid4())[:8].upper(),
                    last_limits_reset=datetime.utcnow(),
                )
                s.add(u)
                await s.commit()
                await s.refresh(u)
                return DB._d(u)

    @staticmethod
    async def upd(tg_id, **kw):
        async with _db_write_lock:
            async with async_session_maker() as s:
                kw["updated_at"] = datetime.utcnow()
                await s.execute(update(User).where(User.telegram_id == tg_id).values(**kw))
                await s.commit()
                r = await s.execute(select(User).where(User.telegram_id == tg_id))
                u = r.scalar_one_or_none()
                return DB._d(u) if u else None

    @staticmethod
    async def reset_limits(u):
        """FIX: Не вызывает upd() если не нужно сбрасывать лимиты"""
        now = datetime.utcnow()
        lr = u.get("last_limits_reset")
        needs_reset = lr is None or lr.date() < now.date()

        if needs_reset:
            f = TIER_FEATURES.get(u.get("subscription_tier", "free"), TIER_FEATURES["free"])
            return await DB.upd(
                u["telegram_id"],
                daily_likes_remaining=f["likes"],
                daily_messages_remaining=f["messages"],
                daily_super_likes_remaining=f["super_likes"],
                last_limits_reset=now,
                last_active_at=now,
                is_online=True,
            )

        # FIX: Только обновляем last_active раз в 60 сек, не каждый запрос
        la = u.get("last_active_at")
        if la and (now - la).total_seconds() < 60:
            # Не обновляем — слишком часто
            return u

        return await DB.upd(u["telegram_id"], last_active_at=now, is_online=True)

    @staticmethod
    async def search(u, limit=1):
        async with async_session_maker() as s:
            liked = {
                r[0]
                for r in (
                    await s.execute(select(Like.to_user_id).where(Like.from_user_id == u["id"]))
                ).fetchall()
            }
            disliked = {
                r[0]
                for r in (
                    await s.execute(
                        select(Dislike.to_user_id).where(Dislike.from_user_id == u["id"])
                    )
                ).fetchall()
            }
            exc = liked | disliked | {u["id"]}

            q = select(User).where(
                and_(
                    User.is_active == True,
                    User.is_banned == False,
                    User.is_profile_complete == True,
                    User.id.not_in(exc),
                    User.age >= u["age_from"],
                    User.age <= u["age_to"],
                )
            )
            lf = u.get("looking_for", "both")
            if lf == "male":
                q = q.where(User.gender == Gender.MALE)
            elif lf == "female":
                q = q.where(User.gender == Gender.FEMALE)

            q = q.order_by(
                User.boost_expires_at.desc().nullslast(),
                (User.city == u["city"]).desc(),
                User.last_active_at.desc(),
            ).limit(limit * 10)
            r = await s.execute(q)
            cands = [DB._d(x) for x in r.scalars().all()]

        if not cands:
            return []

        learned = await Learner.get_prefs(u["id"])
        for c in cands:
            c["_compat"] = SmartMatch.calculate(u, c, learned)
            bonus = 0
            if DB.is_boosted(c):
                bonus += 15
            if c.get("created_at") and (datetime.utcnow() - c["created_at"]).total_seconds() < 86400:
                bonus += 10
            if c.get("is_online"):
                bonus += 8
            c["_score"] = min(c["_compat"]["total"] + bonus + random.uniform(-3, 3), 100)

        cands.sort(key=lambda x: x["_score"], reverse=True)
        return cands[:limit]

    @staticmethod
    async def add_like(fid, tid, is_super=False, msg_text=None):
        async with _db_write_lock:
            async with async_session_maker() as s:
                ex = await s.execute(
                    select(Like).where(and_(Like.from_user_id == fid, Like.to_user_id == tid))
                )
                if ex.scalar_one_or_none():
                    return {"is_match": False}

                s.add(Like(from_user_id=fid, to_user_id=tid, is_super_like=is_super, message=msg_text))
                s.add(LikeQueue(target_user_id=tid, from_user_id=fid, is_super=is_super, message=msg_text))

                await s.execute(
                    update(User)
                    .where(User.id == tid)
                    .values(likes_received_count=User.likes_received_count + 1)
                )
                await s.execute(
                    update(User)
                    .where(User.id == fid)
                    .values(likes_given_count=User.likes_given_count + 1)
                )

                rev = await s.execute(
                    select(Like).where(and_(Like.from_user_id == tid, Like.to_user_id == fid))
                )
                is_match = rev.scalar_one_or_none() is not None

                match_id, compat, icebreaker = None, {}, ""
                if is_match:
                    u1r = await s.execute(select(User).where(User.id == fid))
                    u2r = await s.execute(select(User).where(User.id == tid))
                    u1, u2 = u1r.scalar_one_or_none(), u2r.scalar_one_or_none()
                    if u1 and u2:
                        d1, d2 = DB._d(u1), DB._d(u2)
                        compat = SmartMatch.calculate(d1, d2)
                        icebreaker = SmartMatch.make_icebreaker(d1, d2, compat)
                    m = Match(
                        user1_id=fid, user2_id=tid,
                        compatibility_score=compat.get("total", 0),
                        compatibility_details=json.dumps(compat.get("details", {})),
                        icebreaker_text=icebreaker,
                    )
                    s.add(m)
                    await s.execute(
                        update(User)
                        .where(User.id.in_([fid, tid]))
                        .values(matches_count=User.matches_count + 1)
                    )
                    await s.flush()
                    match_id = m.id
                await s.commit()

        # FIX: Elo/Learner вызываются ПОСЛЕ commit, без create_task
        # чтобы избежать гонки. Но делаем их в фоне с задержкой.
        asyncio.get_event_loop().call_later(
            0.5, lambda: asyncio.ensure_future(_deferred_like_tasks(fid, tid, is_super))
        )

        return {
            "is_match": is_match,
            "match_id": match_id,
            "compatibility": compat,
            "icebreaker": icebreaker,
        }

    @staticmethod
    async def add_dislike(fid, tid):
        async with _db_write_lock:
            async with async_session_maker() as s:
                s.add(Dislike(from_user_id=fid, to_user_id=tid))
                await s.commit()
        # FIX: отложенный вызов
        asyncio.get_event_loop().call_later(
            0.5, lambda: asyncio.ensure_future(_deferred_dislike_tasks(fid, tid))
        )

    @staticmethod
    async def get_incoming_likes(uid, limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(LikeQueue)
                .where(and_(LikeQueue.target_user_id == uid, LikeQueue.is_revealed == False))
                .order_by(LikeQueue.created_at.desc())
                .limit(limit)
            )
            out = []
            for lq in r.scalars().all():
                from_u = await DB.get_user_by_id(lq.from_user_id)
                if from_u:
                    out.append({
                        "queue_id": lq.id,
                        "from_user": from_u,
                        "is_super": lq.is_super,
                        "message": lq.message,
                        "created_at": lq.created_at,
                    })
            return out

    @staticmethod
    async def get_incoming_likes_count(uid) -> int:
        async with async_session_maker() as s:
            r = await s.execute(
                select(func.count(LikeQueue.id)).where(
                    and_(LikeQueue.target_user_id == uid, LikeQueue.is_revealed == False)
                )
            )
            return r.scalar() or 0

    @staticmethod
    async def get_matches(uid):
        async with async_session_maker() as s:
            r = await s.execute(
                select(Match)
                .where(
                    and_(
                        or_(Match.user1_id == uid, Match.user2_id == uid),
                        Match.is_active == True,
                    )
                )
                .order_by(Match.last_message_at.desc().nullslast())
            )
            out = []
            for m in r.scalars().all():
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                pr = await s.execute(select(User).where(User.id == pid))
                p = pr.scalar_one_or_none()
                if p:
                    unread = (
                        await s.execute(
                            select(func.count(ChatMessage.id)).where(
                                and_(
                                    ChatMessage.match_id == m.id,
                                    ChatMessage.sender_id != uid,
                                    ChatMessage.is_read == False,
                                )
                            )
                        )
                    ).scalar() or 0
                    pd = DB._d(p)
                    out.append({
                        "match_id": m.id, "user_id": p.id,
                        "telegram_id": p.telegram_id,
                        "name": p.name, "age": p.age, "photo": p.main_photo,
                        "compatibility": m.compatibility_score or 0,
                        "icebreaker": m.icebreaker_text, "unread": unread,
                        "messages_count": m.messages_count or 0,
                        "is_online": pd.get("is_online"),
                        "badge": DB.badge(pd),
                    })
            return out

    @staticmethod
    async def get_match_between(u1, u2):
        async with async_session_maker() as s:
            r = await s.execute(
                select(Match.id).where(
                    and_(
                        Match.is_active == True,
                        or_(
                            and_(Match.user1_id == u1, Match.user2_id == u2),
                            and_(Match.user1_id == u2, Match.user2_id == u1),
                        ),
                    )
                )
            )
            row = r.first()
            return row[0] if row else None

    @staticmethod
    async def send_msg(mid, sid, txt, photo=None, voice=None):
        async with _db_write_lock:
            async with async_session_maker() as s:
                s.add(ChatMessage(match_id=mid, sender_id=sid, text=txt, photo_id=photo, voice_id=voice))
                await s.execute(
                    update(Match)
                    .where(Match.id == mid)
                    .values(last_message_at=datetime.utcnow(), messages_count=Match.messages_count + 1)
                )
                await s.execute(
                    update(User)
                    .where(User.id == sid)
                    .values(messages_sent_count=User.messages_sent_count + 1)
                )
                await s.commit()

    @staticmethod
    async def get_msgs(mid, limit=10):
        async with async_session_maker() as s:
            r = await s.execute(
                select(ChatMessage)
                .where(ChatMessage.match_id == mid)
                .order_by(ChatMessage.created_at.desc())
                .limit(limit)
            )
            return [
                {
                    "sender_id": m.sender_id, "text": m.text,
                    "photo_id": m.photo_id, "voice_id": m.voice_id,
                    "created_at": m.created_at,
                }
                for m in reversed(r.scalars().all())
            ]

    @staticmethod
    async def mark_read(mid, reader_id):
        async with _db_write_lock:
            async with async_session_maker() as s:
                await s.execute(
                    update(ChatMessage)
                    .where(
                        and_(
                            ChatMessage.match_id == mid,
                            ChatMessage.sender_id != reader_id,
                            ChatMessage.is_read == False,
                        )
                    )
                    .values(is_read=True)
                )
                await s.commit()

    @staticmethod
    async def get_unread(uid):
        async with async_session_maker() as s:
            ms = await s.execute(
                select(Match.id).where(or_(Match.user1_id == uid, Match.user2_id == uid))
            )
            mids = [m[0] for m in ms.fetchall()]
            if not mids:
                return 0
            r = await s.execute(
                select(func.count(ChatMessage.id)).where(
                    and_(
                        ChatMessage.match_id.in_(mids),
                        ChatMessage.sender_id != uid,
                        ChatMessage.is_read == False,
                    )
                )
            )
            return r.scalar() or 0

    @staticmethod
    async def add_guest(vid, uid):
        if vid == uid:
            return
        async with _db_write_lock:
            async with async_session_maker() as s:
                s.add(GuestVisit(visitor_id=vid, visited_user_id=uid))
                await s.execute(
                    update(User).where(User.id == uid).values(views_count=User.views_count + 1)
                )
                await s.commit()

    @staticmethod
    async def get_guests(uid, limit=10):
        async with async_session_maker() as s:
            r = await s.execute(
                select(GuestVisit.visitor_id, func.max(GuestVisit.created_at).label("lv"))
                .where(GuestVisit.visited_user_id == uid)
                .group_by(GuestVisit.visitor_id)
                .order_by(desc("lv"))
                .limit(limit)
            )
            ids = [row[0] for row in r.fetchall()]
            if not ids:
                return []
            us = await s.execute(select(User).where(User.id.in_(ids)))
            um = {u.id: DB._d(u) for u in us.scalars().all()}
            return [um[i] for i in ids if i in um]

    @staticmethod
    async def dec_likes(tg_id):
        async with _db_write_lock:
            async with async_session_maker() as s:
                await s.execute(
                    update(User)
                    .where(User.telegram_id == tg_id)
                    .values(daily_likes_remaining=User.daily_likes_remaining - 1)
                )
                await s.commit()

    @staticmethod
    async def dec_super(tg_id):
        async with _db_write_lock:
            async with async_session_maker() as s:
                await s.execute(
                    update(User)
                    .where(User.telegram_id == tg_id)
                    .values(daily_super_likes_remaining=User.daily_super_likes_remaining - 1)
                )
                await s.commit()

    @staticmethod
    async def use_boost(uid):
        async with _db_write_lock:
            async with async_session_maker() as s:
                ur = await s.execute(select(User).where(User.id == uid))
                u = ur.scalar_one_or_none()
                if not u or (u.boost_count or 0) <= 0:
                    return False
                now = datetime.utcnow()
                ne = (
                    u.boost_expires_at + timedelta(hours=24)
                    if u.boost_expires_at and u.boost_expires_at > now
                    else now + timedelta(hours=24)
                )
                await s.execute(
                    update(User)
                    .where(User.id == uid)
                    .values(boost_count=User.boost_count - 1, boost_expires_at=ne)
                )
                await s.commit()
                return True

    @staticmethod
    async def unmatch(uid, mid):
        async with _db_write_lock:
            async with async_session_maker() as s:
                r = await s.execute(
                    select(Match).where(
                        and_(
                            Match.id == mid,
                            Match.is_active == True,
                            or_(Match.user1_id == uid, Match.user2_id == uid),
                        )
                    )
                )
                m = r.scalar_one_or_none()
                if not m:
                    return False
                await s.execute(update(Match).where(Match.id == mid).values(is_active=False))
                await s.execute(
                    update(User)
                    .where(User.id.in_([m.user1_id, m.user2_id]))
                    .values(matches_count=func.greatest(User.matches_count - 1, 0))
                )
                await s.commit()
                return True

    @staticmethod
    async def create_report(rid, ruid, reason):
        async with _db_write_lock:
            async with async_session_maker() as s:
                s.add(Report(reporter_id=rid, reported_user_id=ruid, reason=reason))
                await s.commit()

    @staticmethod
    async def get_total():
        async with async_session_maker() as s:
            return (
                await s.execute(
                    select(func.count(User.id)).where(User.is_profile_complete == True)
                )
            ).scalar() or 0

    @staticmethod
    async def activate_sub(uid, tier, days):
        async with _db_write_lock:
            async with async_session_maker() as s:
                ur = await s.execute(select(User).where(User.id == uid))
                u = ur.scalar_one_or_none()
                if not u:
                    return
                te = SubscriptionTier(tier)
                now = datetime.utcnow()
                exp = (
                    None
                    if te == SubscriptionTier.WINE_CELLAR
                    else (
                        (u.subscription_expires_at + timedelta(days=days))
                        if u.subscription_expires_at and u.subscription_expires_at > now
                        else now + timedelta(days=days)
                    )
                )
                feats = TIER_FEATURES.get(tier, {})
                vals = {"subscription_tier": te, "subscription_expires_at": exp}
                if feats.get("who_liked"):
                    vals["who_liked_visible"] = True
                await s.execute(update(User).where(User.id == uid).values(**vals))
                await s.commit()

    @staticmethod
    async def use_promo(uid, code):
        async with _db_write_lock:
            async with async_session_maker() as s:
                r = await s.execute(
                    select(PromoCode).where(
                        and_(PromoCode.code == code.upper(), PromoCode.is_active == True)
                    )
                )
                p = r.scalar_one_or_none()
                if not p:
                    return {"error": "❌ Промокод не найден"}
                if p.used_count >= p.max_uses:
                    return {"error": "❌ Исчерпан"}
                used = await s.execute(
                    select(PromoUse).where(and_(PromoUse.promo_id == p.id, PromoUse.user_id == uid))
                )
                if used.scalar_one_or_none():
                    return {"error": "❌ Уже использован"}
                s.add(PromoUse(promo_id=p.id, user_id=uid))
                await s.execute(
                    update(PromoCode)
                    .where(PromoCode.id == p.id)
                    .values(used_count=PromoCode.used_count + 1)
                )
                await s.commit()
        await DB.activate_sub(uid, p.tier, p.duration_days)
        return {"ok": True, "tier": p.tier, "days": p.duration_days}

    @staticmethod
    async def create_payment(uid, yid, amt, desc, ptype, ptier=None, pdur=None, pcnt=None):
        async with _db_write_lock:
            async with async_session_maker() as s:
                p = Payment(
                    user_id=uid, yookassa_payment_id=yid, amount=amt,
                    description=desc, product_type=ptype, product_tier=ptier,
                    product_duration=pdur, product_count=pcnt,
                )
                s.add(p)
                await s.commit()
                await s.refresh(p)
                return p.id


# ═══ FIX 3: Отложенные фоновые задачи вместо create_task ═══

async def _deferred_like_tasks(fid: int, tid: int, is_super: bool):
    """Выполняет ELO + Learner последовательно, не параллельно"""
    await asyncio.sleep(0.2)  # Даём основной транзакции завершиться
    await EloSystem.on_like(fid, tid)
    await Learner.record(fid, tid, "super_like" if is_super else "like")


async def _deferred_dislike_tasks(fid: int, tid: int):
    await asyncio.sleep(0.2)
    await EloSystem.on_dislike(tid)
    await Learner.record(fid, tid, "dislike")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    KEYBOARDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class KB:
    @staticmethod
    def main(unread=0, likes_count=0):
        chats = f"💬 Чаты ({unread})" if unread else "💬 Чаты"
        likes_btn = f"❤️ Кто лайкнул ({likes_count})" if likes_count else "❤️ Кто лайкнул"
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🍷 Анкеты"), KeyboardButton(text="💕 Мэтчи")],
                [KeyboardButton(text=chats), KeyboardButton(text=likes_btn)],
                [KeyboardButton(text="👻 Гости"), KeyboardButton(text="🌟 Пара дня")],
                [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="💎 Премиум")],
            ],
            resize_keyboard=True,
        )

    @staticmethod
    def card(uid, compat=0, can_super=False):
        rows = []
        if can_super:
            rows.append([InlineKeyboardButton(text="⚡ Суперлайк", callback_data=f"sl:{uid}")])
        c = f" ({compat:.0f}%)" if compat else ""
        rows.append([
            InlineKeyboardButton(text=f"❤️{c}", callback_data=f"lk:{uid}"),
            InlineKeyboardButton(text="👎", callback_data=f"dl:{uid}"),
        ])
        rows.append([
            InlineKeyboardButton(text="📊 Подробнее", callback_data=f"cd:{uid}"),
            InlineKeyboardButton(text="⚠️", callback_data=f"rp:{uid}"),
        ])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def matches(ms):
        b = []
        for m in ms[:12]:
            online = "🟢" if m.get("is_online") else ""
            unread = f" 🔴{m['unread']}" if m.get("unread") else ""
            new = " 🆕" if not m.get("messages_count") else ""
            compat = f" {m['compatibility']:.0f}%" if m.get("compatibility") else ""
            b.append([
                InlineKeyboardButton(
                    text=f"{online}{m.get('badge', '')}{m['name']}, {m['age']}{compat}{unread}{new}",
                    callback_data=f"ch:{m['user_id']}",
                )
            ])
        b.append([InlineKeyboardButton(text="🍷", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def chat(mid, has_icebreaker=False):
        rows = []
        if has_icebreaker:
            rows.append([InlineKeyboardButton(text="💡 Подсказка", callback_data=f"ib:{mid}")])
        rows.append([
            InlineKeyboardButton(text="💕 Мэтчи", callback_data="bm"),
            InlineKeyboardButton(text="💔 Отвязать", callback_data=f"um:{mid}"),
        ])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def daily(uid, compat):
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=f"❤️ ({compat:.0f}%)", callback_data=f"dm_lk:{uid}"),
                    InlineKeyboardButton(text="⚡ Суперлайк", callback_data=f"dm_sl:{uid}"),
                ],
                [InlineKeyboardButton(text="👎 Не сегодня", callback_data=f"dm_skip:{uid}")],
            ]
        )

    @staticmethod
    def who_liked_card(uid, is_super=False, has_message=False):
        rows = []
        if is_super:
            rows.append([InlineKeyboardButton(text="⚡ Это суперлайк!", callback_data="nop")])
        rows.append([
            InlineKeyboardButton(text="❤️ Лайк в ответ", callback_data=f"wl_lk:{uid}"),
            InlineKeyboardButton(text="👎 Пропустить", callback_data=f"wl_skip:{uid}"),
        ])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def premium():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="━━━ 💎 ПРЕМИУМ СТАТУСЫ ━━━", callback_data="nop")],
                [InlineKeyboardButton(text="🥂 Бокал Вина — от 299₽", callback_data="pr:wine_glass")],
                [InlineKeyboardButton(text="🍾 Бутылка Вина — от 499₽ 🔥", callback_data="pr:wine_bottle")],
                [InlineKeyboardButton(text="🎖️ Сомелье — от 799₽", callback_data="pr:sommelier")],
                [InlineKeyboardButton(text="🏆 Винный Погреб — 4999₽ 💎", callback_data="pr:wine_cellar")],
                [InlineKeyboardButton(text="━━━━━━━━━━━━━━━━━━━━━━━", callback_data="nop")],
                [InlineKeyboardButton(text="📊 Сравнить тарифы", callback_data="pr:compare")],
                [InlineKeyboardButton(text="🚀 Буст анкеты", callback_data="pr:boost")],
                [InlineKeyboardButton(text="🎁 Промокод", callback_data="pr:promo")],
                [InlineKeyboardButton(text="🍷 Назад", callback_data="mn")],
            ]
        )

    @staticmethod
    def buy_tier(tier):
        prices = {
            "wine_glass": [("299₽/нед", 7, 29900), ("799₽/мес 🔥", 30, 79900)],
            "wine_bottle": [("499₽/мес", 30, 49900), ("1199₽/3мес -20%", 90, 119900)],
            "sommelier": [
                ("799₽/мес", 30, 79900),
                ("1999₽/3мес -17%", 90, 199900),
                ("3499₽/6мес -27%", 180, 349900),
            ],
            "wine_cellar": [("4999₽ навсегда 💎", 0, 499900)],
        }
        rows = []
        emoji = TIER_FEATURES.get(tier, {}).get("badge", "💎")
        for label, dur, amt in prices.get(tier, []):
            rows.append([
                InlineKeyboardButton(text=f"{emoji} {label}", callback_data=f"by:{tier}:{dur}:{amt}")
            ])
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="pr:main")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def boost_menu(has, active):
        b = []
        if has:
            b.append([InlineKeyboardButton(text="🚀 Активировать", callback_data="bo:act")])
        b += [
            [InlineKeyboardButton(text="🚀 1шт — 99₽", callback_data="by:boost:1:9900")],
            [InlineKeyboardButton(text="🚀 5шт — 399₽ -20%", callback_data="by:boost:5:39900")],
            [InlineKeyboardButton(text="🚀 10шт — 699₽ -30%", callback_data="by:boost:10:69900")],
            [InlineKeyboardButton(text="⬅️", callback_data="pr:main")],
        ]
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def pay(url, pid):
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить", url=url)],
                [InlineKeyboardButton(text="🔄 Проверить", callback_data=f"ck:{pid}")],
                [InlineKeyboardButton(text="❌", callback_data="pr:main")],
            ]
        )

    @staticmethod
    def profile(u):
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✏️ Основное", callback_data="pe"),
                    InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo"),
                ],
                [
                    InlineKeyboardButton(text="🎯 Интересы", callback_data="ed:interests"),
                    InlineKeyboardButton(text="💍 Цель", callback_data="ed:goal"),
                ],
                [
                    InlineKeyboardButton(text="📏 Рост", callback_data="ed:height"),
                    InlineKeyboardButton(text="💼 Работа", callback_data="ed:job"),
                ],
                [InlineKeyboardButton(text="📊 Анализ профиля", callback_data="pa")],
                [InlineKeyboardButton(text="🔗 Реферальная ссылка", callback_data="ref")],
            ]
        )

    @staticmethod
    def interests_picker(selected=None, page=0):
        if selected is None:
            selected = set()
        pp = 8
        start = page * pp
        items = INTERESTS_LIST[start : start + pp]
        rows = []
        for i in range(0, len(items), 2):
            row = [
                InlineKeyboardButton(
                    text=f"{'✅ ' if it in selected else ''}{it}",
                    callback_data=f"int:{it}",
                )
                for it in items[i : i + 2]
            ]
            rows.append(row)
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"int_p:{page - 1}"))
        if start + pp < len(INTERESTS_LIST):
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"int_p:{page + 1}"))
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton(text=f"✅ Готово ({len(selected)})", callback_data="int_done")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def goals():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=v, callback_data=f"goal:{k}")]
                for k, v in RELATIONSHIP_GOALS.items()
            ]
        )

    @staticmethod
    def gender():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="👨 Мужской", callback_data="g:male"),
                    InlineKeyboardButton(text="👩 Женский", callback_data="g:female"),
                ]
            ]
        )

    @staticmethod
    def looking():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="👨 Мужчин", callback_data="l:male"),
                    InlineKeyboardButton(text="👩 Женщин", callback_data="l:female"),
                ],
                [InlineKeyboardButton(text="👫 Всех", callback_data="l:both")],
            ]
        )

    @staticmethod
    def skip():
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⏩ Пропустить", callback_data="skip")]]
        )

    @staticmethod
    def report():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🚫 Спам", callback_data="rr:spam"),
                    InlineKeyboardButton(text="🎭 Фейк", callback_data="rr:fake"),
                ],
                [
                    InlineKeyboardButton(text="🔞 18+", callback_data="rr:nsfw"),
                    InlineKeyboardButton(text="😡 Оскорбления", callback_data="rr:harass"),
                ],
                [InlineKeyboardButton(text="❌", callback_data="mn")],
            ]
        )

    @staticmethod
    def confirm_unmatch(mid):
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Да", callback_data=f"um_yes:{mid}"),
                    InlineKeyboardButton(text="❌ Нет", callback_data="bm"),
                ]
            ]
        )

    @staticmethod
    def admin():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
                [InlineKeyboardButton(text="🔍 Найти", callback_data="adm:search")],
                [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:bc")],
                [InlineKeyboardButton(text="🎁 Промокод", callback_data="adm:promo")],
                [InlineKeyboardButton(text="❌", callback_data="mn")],
            ]
        )

    @staticmethod
    def back_admin():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🛡️ Админка", callback_data="adm:main")]
            ]
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                   MIDDLEWARE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class UserMW(BaseMiddleware):
    async def __call__(self, handler, event, data):
        tg = event.from_user if isinstance(event, (Message, CallbackQuery)) else None
        u = None
        if tg:
            u = await DB.get_user(tg.id)
            if u:
                # FIX: reset_limits теперь не делает лишних записей
                u = await DB.reset_limits(u)
                if u.get("is_banned"):
                    if isinstance(event, Message):
                        await event.answer("🚫 Заблокирован")
                    return
        data["user"] = u
        return await handler(event, data)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    HANDLERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

rt = Router()


@rt.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if user and user.get("is_profile_complete"):
        un = await DB.get_unread(user["id"])
        lc = await DB.get_incoming_likes_count(user["id"])
        tier = TIER_NAMES.get(user["subscription_tier"], "🆓")
        a = ProfileAnalyzer.analyze(user)
        hint = f"\n💡 _{a['tips'][0]}_" if a["tips"] and a["score"] < 60 else ""

        await msg.answer(
            f"🍷 *{user['name']}, с возвращением!* 🥂\n\n"
            f"{tier}{' · 🚀' if DB.is_boosted(user) else ''}\n"
            f"👁️ {user['views_count']} · ❤️ {user['likes_received_count']} · "
            f"💕 {user['matches_count']} · ⚖️ {user.get('elo_score', 1000)}"
            f"{hint}",
            reply_markup=KB.main(un, lc),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        if not user:
            await DB.create_user(msg.from_user.id, msg.from_user.username)
        await Anim.animate(msg, ["🍷", "🍷🥂", "🍷🥂✨", "✨ Добро пожаловать! ✨"], 0.5)
        total = await DB.get_total()
        await msg.answer(
            f"🍷 *{BOT_NAME}*\n\n"
            f"👥 *{total}* человек ищут пару!\n"
            f"Создай анкету за 2 минуты 📝",
            parse_mode=ParseMode.MARKDOWN,
        )
        await msg.answer("✏️ Как тебя зовут?", reply_markup=ReplyKeyboardRemove())
        await state.set_state(RegStates.name)


# ═══ REGISTRATION ═══

@rt.message(RegStates.name)
async def rn(m: Message, s: FSMContext):
    n = m.text.strip()
    if len(n) < 2 or len(n) > 50:
        await m.answer("⚠️ 2-50 символов")
        return
    await s.update_data(name=n)
    await m.answer(
        f"Привет, *{n}*! 🍷\n🎂 Возраст? _(18-99)_",
        parse_mode=ParseMode.MARKDOWN,
    )
    await s.set_state(RegStates.age)


@rt.message(RegStates.age)
async def ra(m: Message, s: FSMContext):
    try:
        a = int(m.text.strip())
        assert 18 <= a <= 99
    except Exception:
        await m.answer("⚠️ 18-99")
        return
    await s.update_data(age=a)
    await m.answer("🚻 Пол:", reply_markup=KB.gender())
    await s.set_state(RegStates.gender)


@rt.callback_query(RegStates.gender, F.data.startswith("g:"))
async def rg(c: CallbackQuery, s: FSMContext):
    await s.update_data(gender=c.data[2:])
    await c.message.edit_text("✅")
    await c.message.answer("🏙️ Город:")
    await s.set_state(RegStates.city)
    await c.answer()


@rt.message(RegStates.city)
async def rc(m: Message, s: FSMContext):
    await s.update_data(city=m.text.strip().title())
    await m.answer("📸 Фото или «Пропустить»:", reply_markup=KB.skip())
    await s.set_state(RegStates.photo)


@rt.message(RegStates.photo, F.photo)
async def rp(m: Message, s: FSMContext):
    await s.update_data(photo=m.photo[-1].file_id)
    await m.answer(
        "📝 О себе _(до 500)_ или «Пропустить»:",
        reply_markup=KB.skip(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await s.set_state(RegStates.bio)


@rt.callback_query(RegStates.photo, F.data == "skip")
async def rps(c: CallbackQuery, s: FSMContext):
    await s.update_data(photo=None)
    await c.message.edit_text("📸 Пропущено")
    await c.message.answer("📝 О себе или «Пропустить»:", reply_markup=KB.skip())
    await s.set_state(RegStates.bio)
    await c.answer()


@rt.message(RegStates.bio)
async def rb(m: Message, s: FSMContext):
    await s.update_data(bio=m.text.strip()[:500])
    await m.answer("🔍 Кого ищешь?", reply_markup=KB.looking())
    await s.set_state(RegStates.looking_for)


@rt.callback_query(RegStates.bio, F.data == "skip")
async def rbs(c: CallbackQuery, s: FSMContext):
    await s.update_data(bio="")
    await c.message.edit_text("🔍 Кого ищешь?", reply_markup=KB.looking())
    await s.set_state(RegStates.looking_for)
    await c.answer()


@rt.callback_query(RegStates.looking_for, F.data.startswith("l:"))
async def rl(c: CallbackQuery, s: FSMContext):
    await s.update_data(looking_for=c.data[2:])
    await c.message.edit_text("✅")
    await c.message.answer(
        "🎯 *Выбери интересы:*",
        reply_markup=KB.interests_picker(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await s.update_data(selected_interests=[])
    await s.set_state(RegStates.interests)
    await c.answer()


@rt.callback_query(RegStates.interests, F.data.startswith("int:"))
async def ri(c: CallbackQuery, s: FSMContext):
    i = c.data[4:]
    d = await s.get_data()
    sel = set(d.get("selected_interests", []))
    if i in sel:
        sel.discard(i)
    else:
        sel.add(i)
    await s.update_data(selected_interests=list(sel))
    await c.message.edit_reply_markup(reply_markup=KB.interests_picker(sel, d.get("ip", 0)))
    await c.answer()


@rt.callback_query(RegStates.interests, F.data.startswith("int_p:"))
async def rip(c: CallbackQuery, s: FSMContext):
    p = int(c.data.split(":")[1])
    d = await s.get_data()
    await s.update_data(ip=p)
    await c.message.edit_reply_markup(
        reply_markup=KB.interests_picker(set(d.get("selected_interests", [])), p)
    )
    await c.answer()


@rt.callback_query(RegStates.interests, F.data == "int_done")
async def rid(c: CallbackQuery, s: FSMContext):
    d = await s.get_data()
    await s.update_data(interests=",".join(d.get("selected_interests", [])))
    await c.message.edit_text(f"🎯 {len(d.get('selected_interests', []))} интересов ✅")
    await c.message.answer(
        "💍 *Цель знакомства?*",
        reply_markup=KB.goals(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await s.set_state(RegStates.relationship_goal)
    await c.answer()


@rt.callback_query(RegStates.relationship_goal, F.data.startswith("goal:"))
async def rgoal(c: CallbackQuery, s: FSMContext):
    d = await s.get_data()
    goal = c.data[5:]
    upd = {
        "name": d["name"],
        "age": d["age"],
        "gender": Gender(d["gender"]),
        "city": d["city"],
        "bio": d.get("bio", ""),
        "looking_for": LookingFor(d["looking_for"]),
        "interests": d.get("interests", ""),
        "relationship_goal": goal,
        "is_profile_complete": True,
    }
    if d.get("photo"):
        upd["photos"] = d["photo"]
        upd["main_photo"] = d["photo"]
    await DB.upd(c.from_user.id, **upd)
    await s.clear()

    await c.message.edit_text("⏳ Создаём...")
    await asyncio.sleep(0.5)
    await c.message.edit_text("🧠 Настраиваем AI...")
    await asyncio.sleep(0.5)
    await c.message.edit_text("🎉 *Готово!*", parse_mode=ParseMode.MARKDOWN)

    user = await DB.get_user(c.from_user.id)
    a = ProfileAnalyzer.analyze(user)
    total = await DB.get_total()
    await c.message.answer(
        f"🍷 Среди *{total}* человек найдём пару!\n\n{ProfileAnalyzer.format(a)}\nЖми «🍷 Анкеты»!",
        reply_markup=KB.main(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await c.answer()


# ═══ BROWSE ═══

@rt.message(F.text == "🍷 Анкеты")
async def browse(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await msg.answer("📝 /start")
        return
    await state.clear()
    m = await msg.answer("🧠 Ищем...")
    ps = await DB.search(user, 1)
    if not ps:
        await m.edit_text("😔 Анкеты закончились. Попробуй позже!")
        return
    await m.delete()
    await show_card(msg, ps[0], user)


async def show_card(msg, p, viewer):
    if not viewer.get("invisible_mode"):
        # FIX: add_guest в фоне чтобы не блокировать показ карточки
        asyncio.ensure_future(_safe_add_guest(viewer["id"], p["id"]))

    compat = p.get("_compat", SmartMatch.calculate(viewer, p))
    total = compat.get("total", 0) if isinstance(compat, dict) else compat
    badge = DB.badge(p)
    boost = " 🚀" if DB.is_boosted(p) else ""
    online = " 🟢" if p.get("is_online") else ""

    goal = RELATIONSHIP_GOALS.get(p.get("relationship_goal"), "")
    ints = list(filter(None, (p.get("interests") or "").split(",")))
    int_txt = "\n🎯 " + " · ".join(ints[:4]) if ints else ""
    zodiac = f"\n🔮 {p['zodiac']}" if p.get("zodiac") else ""
    height = f" · 📏 {p['height']}см" if p.get("height") else ""
    job = f"\n💼 {p['job']}" if p.get("job") else ""
    drink = f"\n🍷 {DRINKING_OPTIONS.get(p.get('drinking'), '')}" if p.get("drinking") else ""

    bar = SmartMatch.bar(total)

    txt = (
        f"{badge}*{p['name']}*{boost}{online}, {p['age']}{height}\n"
        f"🏙️ {p['city']}{zodiac}{job}{drink}\n\n"
        f"{p['bio'] or '_Нет описания_'}{int_txt}\n"
        f"{'💍 ' + goal if goal else ''}\n\n"
        f"💕 {bar}"
    )

    can_super = viewer.get("daily_super_likes_remaining", 0) > 0
    kb = KB.card(p["id"], total, can_super)

    try:
        if p.get("main_photo"):
            await msg.answer_photo(
                photo=p["main_photo"], caption=txt,
                reply_markup=kb, parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await msg.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await msg.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


async def _safe_add_guest(vid, uid):
    """Безопасное добавление гостя в фоне"""
    try:
        await DB.add_guest(vid, uid)
    except Exception as e:
        logger.error(f"add_guest error: {e}")


@rt.callback_query(F.data.startswith("lk:"))
async def lk(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    if not DB.is_vip(user) and user.get("daily_likes_remaining", 0) <= 0:
        await cb.answer("⚠️ Лимит! Подпишись 💎", show_alert=True)
        return
    tid = int(cb.data[3:])
    r = await DB.add_like(user["id"], tid)
    if not DB.is_vip(user):
        await DB.dec_likes(user["telegram_id"])
    if r.get("is_match"):
        t = await DB.get_user_by_id(tid)
        total = r["compatibility"].get("total", 0) if isinstance(r.get("compatibility"), dict) else 0
        ib = r.get("icebreaker", "")
        await Anim.match_wow(cb.message, t["name"] if t else "?", total, ib)
        if t:
            try:
                await cb.bot.send_message(
                    t["telegram_id"],
                    f"🍷✨ *Мэтч с {user['name']}!*\n\n💕 {SmartMatch.bar(total)}\n{ib}",
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass
        await cb.answer("🍷 МЭТЧ! 💕")
    else:
        target = await DB.get_user_by_id(tid)
        if target and target.get("notify_likes"):
            try:
                if DB.is_vip(target):
                    await cb.bot.send_message(
                        target["telegram_id"],
                        f"❤️ *{user['name']}*, {user['age']} оценил(а) тебя!\n"
                        f"_Листай анкеты — вдруг взаимно!_ 🍷",
                        parse_mode=ParseMode.MARKDOWN,
                    )
                else:
                    await cb.bot.send_message(
                        target["telegram_id"],
                        "❤️ *Кто-то оценил твой профиль!*\n🔒 _Узнай кто → Премиум_ 💎",
                        parse_mode=ParseMode.MARKDOWN,
                    )
            except Exception:
                pass
        await cb.answer("❤️")

    user = await DB.get_user(cb.from_user.id)
    ps = await DB.search(user, 1)
    if ps:
        await show_card(cb.message, ps[0], user)
    else:
        await cb.message.answer("😔 Анкеты закончились!")


@rt.callback_query(F.data.startswith("sl:"))
async def sl(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    if user.get("daily_super_likes_remaining", 0) <= 0:
        await cb.answer("⚡ Суперлайки кончились! 💎 Премиум = больше!", show_alert=True)
        return
    tid = int(cb.data[3:])
    r = await DB.add_like(user["id"], tid, is_super=True)
    await DB.dec_super(user["telegram_id"])

    if r.get("is_match"):
        t = await DB.get_user_by_id(tid)
        total = r["compatibility"].get("total", 0) if isinstance(r.get("compatibility"), dict) else 0
        await Anim.match_wow(cb.message, t["name"] if t else "?", total, r.get("icebreaker", ""))
    else:
        await Anim.super_like_sent(cb.message)
        target = await DB.get_user_by_id(tid)
        if target:
            try:
                await cb.bot.send_message(
                    target["telegram_id"],
                    f"⚡ *{user['name']}*, {user['age']} отправил(а) тебе суперлайк!*\n"
                    f"_Ты точно понравился!_ 💜",
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass

    user = await DB.get_user(cb.from_user.id)
    ps = await DB.search(user, 1)
    if ps:
        await show_card(cb.message, ps[0], user)


@rt.callback_query(F.data.startswith("dl:"))
async def dl(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    await DB.add_dislike(user["id"], int(cb.data[3:]))
    await cb.answer("👋")
    ps = await DB.search(user, 1)
    if ps:
        await show_card(cb.message, ps[0], user)
    else:
        try:
            await cb.message.edit_caption(caption="😔 Анкеты закончились!")
        except Exception:
            await cb.message.answer("😔 Закончились!")


# ═══ COMPATIBILITY DETAILS ═══

@rt.callback_query(F.data.startswith("cd:"))
async def compat_details(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    tid = int(cb.data[3:])
    t = await DB.get_user_by_id(tid)
    if not t:
        return
    learned = await Learner.get_prefs(user["id"])
    c = SmartMatch.calculate(user, t, learned)
    bar = SmartMatch.bar(c["total"], True)

    if DB.is_vip(user):
        breakdown = SmartMatch.breakdown(c["details"])
        common = c.get("common_interests", [])
        common_txt = "\n\n🎯 *Общие интересы:* " + ", ".join(common) if common else ""
        txt = f"📊 *Совместимость с {t['name']}*\n\n💕 {bar}\n\n*Разбивка:*\n{breakdown}{common_txt}"
    else:
        txt = (
            f"📊 *Совместимость с {t['name']}*\n\n💕 {bar}\n\n"
            f"🔒 _Детальная разбивка — в Премиум_ 💎"
        )

    await cb.message.answer(txt, parse_mode=ParseMode.MARKDOWN)
    await cb.answer()


# ═══ WHO LIKED ME ═══

@rt.message(F.text.startswith("❤️ Кто лайкнул"))
async def who_liked(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await msg.answer("📝 /start")
        return

    count = await DB.get_incoming_likes_count(user["id"])
    if count == 0:
        await msg.answer(
            "❤️ Пока никто не лайкнул\n\n🍷 _Листай анкеты — лайки придут!_",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if not DB.is_vip(user):
        await msg.answer(
            f"❤️ *Тебя лайкнули {count} раз!*\n\n"
            f"🔒 _Кто именно — видно с Премиум-статусом_\n\n"
            f"💎 Открой Премиум и узнай кто тебя лайкнул!\n"
            f"_Или продолжай листать — вдруг совпадёте!_ 🍷",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💎 Открыть Премиум", callback_data="pr:main")],
                    [InlineKeyboardButton(text="🍷 Листать анкеты", callback_data="go:browse")],
                ]
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    likes = await DB.get_incoming_likes(user["id"], 5)
    if not likes:
        await msg.answer("❤️ Нет новых лайков")
        return

    lk_item = likes[0]
    fu = lk_item["from_user"]
    badge = DB.badge(fu)
    sl_txt = "⚡ *СУПЕРЛАЙК!*\n\n" if lk_item["is_super"] else ""
    msg_txt = f"\n\n💬 _{lk_item['message']}_" if lk_item.get("message") else ""

    txt = (
        f"❤️ *Кто тебя лайкнул* ({count})\n\n"
        f"{sl_txt}{badge}*{fu['name']}*, {fu['age']}\n"
        f"🏙️ {fu['city']}\n"
        f"{fu['bio'] or ''}{msg_txt}"
    )

    try:
        if fu.get("main_photo"):
            await msg.answer_photo(
                photo=fu["main_photo"],
                caption=txt,
                reply_markup=KB.who_liked_card(fu["id"], lk_item["is_super"]),
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await msg.answer(
                txt,
                reply_markup=KB.who_liked_card(fu["id"], lk_item["is_super"]),
                parse_mode=ParseMode.MARKDOWN,
            )
    except Exception:
        await msg.answer(
            txt,
            reply_markup=KB.who_liked_card(fu["id"], lk_item["is_super"]),
            parse_mode=ParseMode.MARKDOWN,
        )


@rt.callback_query(F.data.startswith("wl_lk:"))
async def wl_like(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    tid = int(cb.data[6:])
    r = await DB.add_like(user["id"], tid)
    if r.get("is_match"):
        t = await DB.get_user_by_id(tid)
        total = r["compatibility"].get("total", 0) if isinstance(r.get("compatibility"), dict) else 0
        await Anim.match_wow(cb.message, t["name"] if t else "?", total, r.get("icebreaker", ""))
    else:
        await cb.answer("❤️ Лайк отправлен!")
        try:
            await cb.message.edit_caption(
                caption="❤️ *Лайк отправлен!*", parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass


@rt.callback_query(F.data.startswith("wl_skip:"))
async def wl_skip(cb: CallbackQuery, user: Optional[Dict]):
    try:
        await cb.message.edit_caption(caption="👋 Пропущено")
    except Exception:
        await cb.message.edit_text("👋 Пропущено")
    await cb.answer()


@rt.callback_query(F.data == "go:browse")
async def go_browse(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user:
        return
    try:
        await cb.message.delete()
    except Exception:
        pass
    ps = await DB.search(user, 1)
    if ps:
        await show_card(cb.message, ps[0], user)
    else:
        await cb.message.answer("😔 Закончились!")
    await cb.answer()


# ═══ MATCHES & CHAT ═══

@rt.message(F.text == "💕 Мэтчи")
async def matches(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        return
    await state.clear()
    ms = await DB.get_matches(user["id"])
    if ms:
        un = sum(m.get("unread", 0) for m in ms)
        h = f"💕 *Мэтчи ({len(ms)})*" + (f" · 🔴 {un}" if un else "")
        await msg.answer(h, reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer("😔 Нет мэтчей\n\n🍷 Листай анкеты!", parse_mode=ParseMode.MARKDOWN)


@rt.message(F.text.startswith("💬 Чаты"))
async def chats(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    await state.clear()
    ms = await DB.get_matches(user["id"])
    active = [m for m in ms if m.get("messages_count", 0) > 0]
    if active:
        await msg.answer(
            "💬 *Диалоги:*", reply_markup=KB.matches(active), parse_mode=ParseMode.MARKDOWN
        )
    else:
        await msg.answer(
            "💬 Нет активных чатов\n\n_Напиши своему мэтчу первым!_ 🍷",
            parse_mode=ParseMode.MARKDOWN,
        )


@rt.callback_query(F.data.startswith("ch:"))
async def open_chat(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    pid = int(cb.data[3:])
    p = await DB.get_user_by_id(pid)
    if not p:
        await cb.answer("❌")
        return
    mid = await DB.get_match_between(user["id"], pid)
    if not mid:
        await cb.answer("❌")
        return
    await DB.mark_read(mid, user["id"])

    msgs = await DB.get_msgs(mid, 8)
    badge = DB.badge(p)
    online = " 🟢" if p.get("is_online") else ""
    txt = f"💬 *{badge}{p['name']}{online}*\n\n"

    for mg in msgs:
        sn = "📤" if mg["sender_id"] == user["id"] else "📩"
        ts = mg["created_at"].strftime("%H:%M") if mg.get("created_at") else ""
        if mg.get("photo_id"):
            txt += f"{sn} 📸 _фото_ {ts}\n"
        elif mg.get("voice_id"):
            txt += f"{sn} 🎤 _голосовое_ {ts}\n"
        else:
            txt += f"{sn} {mg['text'] or ''} _{ts}_\n"

    if not msgs:
        tip = random.choice(FLIRT_TIPS)
        txt += f"_Напиши первым!_ 🍷\n\n{tip}"

    ib = None
    async with async_session_maker() as s:
        mr = await s.execute(select(Match).where(Match.id == mid))
        match_obj = mr.scalar_one_or_none()
        ib = match_obj.icebreaker_text if match_obj else None

    await state.update_data(cp=pid, mi=mid)
    await state.set_state(ChatStates.chatting)
    await cb.message.edit_text(
        txt, reply_markup=KB.chat(mid, bool(ib)), parse_mode=ParseMode.MARKDOWN
    )
    await cb.answer()


@rt.message(ChatStates.chatting, F.text)
async def send_txt(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    # FIX: Фильтруем команды меню чтобы не отправлять как чат-сообщения
    menu_texts = {"🍷 Анкеты", "💕 Мэтчи", "👤 Профиль", "💎 Премиум",
                  "👻 Гости", "🌟 Пара дня"}
    if msg.text in menu_texts or msg.text.startswith("💬 Чаты") or msg.text.startswith("❤️ Кто лайкнул"):
        await state.clear()
        # Пере-вызываем нужный обработчик
        if msg.text == "🍷 Анкеты":
            return await browse(msg, state, user)
        elif msg.text == "💕 Мэтчи":
            return await matches(msg, state, user)
        elif msg.text == "👤 Профиль":
            return await profile(msg, user)
        elif msg.text == "💎 Премиум":
            return await premium_menu(msg, state, user)
        elif msg.text == "👻 Гости":
            return await guests(msg, user)
        elif msg.text == "🌟 Пара дня":
            return await daily_match(msg, user)
        elif msg.text.startswith("💬 Чаты"):
            return await chats(msg, state, user)
        elif msg.text.startswith("❤️ Кто лайкнул"):
            return await who_liked(msg, user)
        return

    d = await state.get_data()
    mid, pid = d.get("mi"), d.get("cp")
    if not mid:
        await state.clear()
        await msg.answer("💬 Закрыт", reply_markup=KB.main())
        return
    await DB.send_msg(mid, user["id"], msg.text)
    p = await DB.get_user_by_id(pid)
    if p and p.get("notify_messages"):
        try:
            await msg.bot.send_message(
                p["telegram_id"],
                f"💬 *{user['name']}:* {msg.text}",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
    await msg.answer("✅")


@rt.message(ChatStates.chatting, F.photo)
async def send_photo(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    d = await state.get_data()
    mid, pid = d.get("mi"), d.get("cp")
    if not mid:
        return
    photo_id = msg.photo[-1].file_id
    await DB.send_msg(mid, user["id"], None, photo=photo_id)
    p = await DB.get_user_by_id(pid)
    if p:
        try:
            await msg.bot.send_photo(
                p["telegram_id"],
                photo_id,
                caption=f"📸 *{user['name']}* отправил(а) фото",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
    await msg.answer("📸 ✅")


@rt.callback_query(F.data.startswith("ib:"))
async def icebreaker(cb: CallbackQuery):
    mid = int(cb.data[3:])
    async with async_session_maker() as s:
        mr = await s.execute(select(Match).where(Match.id == mid))
        m = mr.scalar_one_or_none()
        if m and m.icebreaker_text:
            await cb.answer(m.icebreaker_text, show_alert=True)
        else:
            await cb.answer(random.choice(ICEBREAKER_TEMPLATES), show_alert=True)


@rt.callback_query(F.data == "bm")
async def bm(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user:
        return
    ms = await DB.get_matches(user["id"])
    if ms:
        await cb.message.edit_text(
            f"💕 *({len(ms)})*",
            reply_markup=KB.matches(ms),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await cb.message.edit_text("😔 Нет мэтчей")


@rt.callback_query(F.data.startswith("um:"))
async def um(cb: CallbackQuery):
    await cb.message.edit_text(
        "💔 *Отвязать?*",
        reply_markup=KB.confirm_unmatch(int(cb.data[3:])),
        parse_mode=ParseMode.MARKDOWN,
    )


@rt.callback_query(F.data.startswith("um_yes:"))
async def umy(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    await state.clear()
    await DB.unmatch(user["id"], int(cb.data[7:]))
    await cb.message.edit_text("💔 Отвязано")


# ═══ DAILY MATCH ═══

@rt.message(F.text == "🌟 Пара дня")
async def daily_match(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        return

    async with async_session_maker() as s:
        today = datetime.utcnow().replace(hour=0, minute=0, second=0)
        ex = await s.execute(
            select(DailyMatch).where(
                and_(
                    DailyMatch.user_id == user["id"],
                    DailyMatch.created_at >= today,
                    DailyMatch.is_seen == False,
                )
            )
        )
        dm = ex.scalar_one_or_none()

    if not dm:
        ps = await DB.search(user, 5)
        if not ps:
            await msg.answer("🌟 _Пара дня появится завтра!_", parse_mode=ParseMode.MARKDOWN)
            return
        best = ps[0]
        compat = best.get("_compat", SmartMatch.calculate(user, best))
        ib = SmartMatch.make_icebreaker(user, best, compat)
        async with _db_write_lock:
            async with async_session_maker() as s:
                s.add(
                    DailyMatch(
                        user_id=user["id"],
                        recommended_id=best["id"],
                        compatibility_score=compat["total"],
                        reason=ib,
                    )
                )
                await s.commit()
        rec, total = best, compat["total"]
    else:
        rec = await DB.get_user_by_id(dm.recommended_id)
        total = dm.compatibility_score
        ib = dm.reason
        if not rec:
            await msg.answer("🌟 Попробуй позже!")
            return

    badge = DB.badge(rec)
    bar = SmartMatch.bar(total, True)
    txt = (
        f"🌟 *ПАРА ДНЯ*\n\n"
        f"{badge}*{rec['name']}*, {rec['age']}\n"
        f"🏙️ {rec['city']}\n\n"
        f"{rec['bio'] or '_—_'}\n\n"
        f"💕 {bar}\n\n{ib or ''}"
    )

    try:
        if rec.get("main_photo"):
            await msg.answer_photo(
                photo=rec["main_photo"],
                caption=txt,
                reply_markup=KB.daily(rec["id"], total),
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await msg.answer(
                txt, reply_markup=KB.daily(rec["id"], total), parse_mode=ParseMode.MARKDOWN
            )
    except Exception:
        await msg.answer(
            txt, reply_markup=KB.daily(rec["id"], total), parse_mode=ParseMode.MARKDOWN
        )


@rt.callback_query(F.data.startswith("dm_lk:"))
async def dm_lk(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    r = await DB.add_like(user["id"], int(cb.data[6:]))
    if r.get("is_match"):
        t = await DB.get_user_by_id(int(cb.data[6:]))
        total = r["compatibility"].get("total", 0) if isinstance(r.get("compatibility"), dict) else 0
        await Anim.match_wow(cb.message, t["name"] if t else "?", total, r.get("icebreaker", ""))
    else:
        try:
            await cb.message.edit_caption(
                caption="🌟 *Лайк отправлен!* Ждём ответ 🍷",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            await cb.message.edit_text("🌟 *Лайк!*", parse_mode=ParseMode.MARKDOWN)
    await cb.answer()


# ═══ GUESTS ═══

@rt.message(F.text == "👻 Гости")
async def guests(msg: Message, user: Optional[Dict]):
    if not user:
        return
    feats = DB.features(user)
    lim = feats.get("guests", 3)
    gs = await DB.get_guests(user["id"], lim)
    if not gs:
        await msg.answer(
            "👻 Пока нет гостей\n\n💡 _Активируй буст!_", parse_mode=ParseMode.MARKDOWN
        )
        return
    txt = "👻 *Кто смотрел:*\n\n"
    for i, g in enumerate(gs, 1):
        online = "🟢 " if g.get("is_online") else ""
        txt += f"{i}. {online}{DB.badge(g)}{g['name']}, {g['age']} — {g['city']}\n"
    if not DB.is_vip(user):
        h = max(0, user.get("views_count", 0) - lim)
        if h:
            txt += f"\n🔒 _Ещё {h} скрыто_"
        txt += "\n\n💎 _Премиум = все гости!_"
    await msg.answer(txt, parse_mode=ParseMode.MARKDOWN)


# ═══ PROFILE ═══

@rt.message(F.text == "👤 Профиль")
async def profile(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await msg.answer("📝 /start")
        return
    badge = DB.badge(user)
    sub = TIER_NAMES.get(user["subscription_tier"], "🆓")
    feats = DB.features(user)

    ints = list(filter(None, (user.get("interests") or "").split(",")))
    int_txt = "\n🎯 " + " · ".join(ints[:5]) if ints else ""
    goal = RELATIONSHIP_GOALS.get(user.get("relationship_goal"), "")
    zodiac = f"\n🔮 {user['zodiac']}" if user.get("zodiac") else ""
    height = f" · 📏 {user['height']}см" if user.get("height") else ""
    job = f"\n💼 {user['job']}" if user.get("job") else ""

    likes_max = feats["likes"]
    likes_str = "♾️" if likes_max >= 999999 else f"{user.get('daily_likes_remaining', 0)}/{likes_max}"
    sl_str = f" · ⚡ {user.get('daily_super_likes_remaining', 0)}/{feats['super_likes']}"
    boost_txt = (
        f"\n🚀 Буст до {user['boost_expires_at'].strftime('%H:%M')}" if DB.is_boosted(user) else ""
    )
    boost_txt += f"\n🚀 Запас: {user['boost_count']}" if user.get("boost_count") else ""

    txt = (
        f"👤 *Профиль*\n\n"
        f"{badge}*{user['name']}*, {user['age']}{height}\n"
        f"🏙️ {user['city']}{zodiac}{job}\n\n"
        f"{user['bio'] or '_—_'}{int_txt}\n"
        f"{'💍 ' + goal if goal else ''}\n\n"
        f"📊 👁️ {user['views_count']} · ❤️ {user['likes_received_count']} · 💕 {user['matches_count']}\n"
        f"❤️ {likes_str}{sl_str} · ⚖️ {user.get('elo_score', 1000)}\n\n"
        f"💎 {sub}{boost_txt}"
    )

    try:
        if user.get("main_photo"):
            await msg.answer_photo(
                photo=user["main_photo"],
                caption=txt,
                reply_markup=KB.profile(user),
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await msg.answer(txt, reply_markup=KB.profile(user), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await msg.answer(txt, reply_markup=KB.profile(user), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "pa")
async def profile_analysis(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    a = ProfileAnalyzer.analyze(user)
    txt = ProfileAnalyzer.format(a)
    txt += (
        f"\n⚖️ Рейтинг: *{user.get('elo_score', 1000)}*\n"
        f"💕 Привлекательность: *{user.get('attractiveness_score', 50):.0f}%*"
    )
    try:
        await cb.message.edit_caption(caption=txt, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await cb.message.edit_text(txt, parse_mode=ParseMode.MARKDOWN)
    await cb.answer()


@rt.callback_query(F.data == "ref")
async def referral(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    code = user.get("referral_code", "???")
    link = f"https://t.me/{(await cb.bot.me()).username}?start=ref_{code}"
    await cb.message.answer(
        f"🔗 *Реферальная ссылка*\n\n"
        f"Приглашай друзей и получай бонусы!\n\n"
        f"📎 `{link}`\n\n"
        f"🎁 За каждого друга: +3 суперлайка!",
        parse_mode=ParseMode.MARKDOWN,
    )
    await cb.answer()


# Edit handlers
@rt.callback_query(F.data == "pe")
async def pe(cb: CallbackQuery):
    try:
        await cb.message.edit_caption(
            caption="✏️ *Редактировать:*",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✏️ Имя", callback_data="ed:name"),
                        InlineKeyboardButton(text="🎂 Возраст", callback_data="ed:age"),
                    ],
                    [
                        InlineKeyboardButton(text="🏙️ Город", callback_data="ed:city"),
                        InlineKeyboardButton(text="📝 О себе", callback_data="ed:bio"),
                    ],
                    [InlineKeyboardButton(text="⬅️", callback_data="pv")],
                ]
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass
    await cb.answer()


@rt.callback_query(F.data == "pv")
async def pv(cb: CallbackQuery, user: Optional[Dict]):
    if user:
        try:
            await cb.message.delete()
        except Exception:
            pass
        await profile(cb.message, user)
    await cb.answer()


@rt.callback_query(F.data == "ed:name")
async def en(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("✏️ Имя:")
    await state.set_state(EditStates.edit_name)
    await cb.answer()


@rt.message(EditStates.edit_name)
async def sn(m: Message, s: FSMContext):
    n = m.text.strip()
    if len(n) < 2:
        await m.answer("⚠️")
        return
    await DB.upd(m.from_user.id, name=n)
    await s.clear()
    await m.answer("✅", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:age")
async def ea(cb: CallbackQuery, s: FSMContext):
    await cb.message.answer("🎂:")
    await s.set_state(EditStates.edit_age)
    await cb.answer()


@rt.message(EditStates.edit_age)
async def sa(m: Message, s: FSMContext):
    try:
        a = int(m.text.strip())
        assert 18 <= a <= 99
    except Exception:
        await m.answer("⚠️")
        return
    await DB.upd(m.from_user.id, age=a)
    await s.clear()
    await m.answer("✅", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:city")
async def ec(cb: CallbackQuery, s: FSMContext):
    await cb.message.answer("🏙️:")
    await s.set_state(EditStates.edit_city)
    await cb.answer()


@rt.message(EditStates.edit_city)
async def sc(m: Message, s: FSMContext):
    await DB.upd(m.from_user.id, city=m.text.strip().title())
    await s.clear()
    await m.answer("✅", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:bio")
async def eb(cb: CallbackQuery, s: FSMContext):
    await cb.message.answer("📝:")
    await s.set_state(EditStates.edit_bio)
    await cb.answer()


@rt.message(EditStates.edit_bio)
async def sb(m: Message, s: FSMContext):
    await DB.upd(m.from_user.id, bio=m.text.strip()[:500])
    await s.clear()
    await m.answer("✅", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:photo")
async def eph(cb: CallbackQuery, s: FSMContext):
    await cb.message.answer("📸:")
    await s.set_state(EditStates.add_photo)
    await cb.answer()


@rt.message(EditStates.add_photo, F.photo)
async def sph(m: Message, s: FSMContext, user: Optional[Dict]):
    pid = m.photo[-1].file_id
    ph = user.get("photos", "")
    ph = (ph + "," + pid) if ph else pid
    await DB.upd(m.from_user.id, photos=ph, main_photo=pid)
    await s.clear()
    await m.answer("📸 ✅", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:interests")
async def ei(cb: CallbackQuery, s: FSMContext, user: Optional[Dict]):
    sel = set(filter(None, (user.get("interests") or "").split(",")))
    await s.update_data(selected_interests=list(sel))
    await cb.message.answer("🎯", reply_markup=KB.interests_picker(sel))
    await s.set_state(EditStates.edit_interests)
    await cb.answer()


@rt.callback_query(EditStates.edit_interests, F.data.startswith("int:"))
async def eit(cb: CallbackQuery, s: FSMContext):
    i = cb.data[4:]
    d = await s.get_data()
    sel = set(d.get("selected_interests", []))
    if i in sel:
        sel.discard(i)
    else:
        sel.add(i)
    await s.update_data(selected_interests=list(sel))
    await cb.message.edit_reply_markup(reply_markup=KB.interests_picker(sel))
    await cb.answer()


@rt.callback_query(EditStates.edit_interests, F.data.startswith("int_p:"))
async def eip(cb: CallbackQuery, s: FSMContext):
    p = int(cb.data.split(":")[1])
    d = await s.get_data()
    await cb.message.edit_reply_markup(
        reply_markup=KB.interests_picker(set(d.get("selected_interests", [])), p)
    )
    await cb.answer()


@rt.callback_query(EditStates.edit_interests, F.data == "int_done")
async def eid(cb: CallbackQuery, s: FSMContext):
    d = await s.get_data()
    sel = d.get("selected_interests", [])
    await DB.upd(cb.from_user.id, interests=",".join(sel))
    await s.clear()
    await cb.message.edit_text(f"🎯 {len(sel)} ✅")
    await cb.answer()


@rt.callback_query(F.data == "ed:goal")
async def eg(cb: CallbackQuery, s: FSMContext):
    await cb.message.answer("💍", reply_markup=KB.goals())
    await s.set_state(EditStates.edit_goal)
    await cb.answer()


@rt.callback_query(EditStates.edit_goal, F.data.startswith("goal:"))
async def sg(cb: CallbackQuery, s: FSMContext):
    await DB.upd(cb.from_user.id, relationship_goal=cb.data[5:])
    await s.clear()
    await cb.message.edit_text("💍 ✅")
    await cb.answer()


@rt.callback_query(F.data == "ed:height")
async def eh(cb: CallbackQuery, s: FSMContext):
    await cb.message.answer("📏 Рост (см):")
    await s.set_state(EditStates.edit_height)
    await cb.answer()


@rt.message(EditStates.edit_height)
async def sh(m: Message, s: FSMContext):
    try:
        h = int(m.text.strip())
        assert 100 <= h <= 250
    except Exception:
        await m.answer("⚠️ 100-250")
        return
    await DB.upd(m.from_user.id, height=h)
    await s.clear()
    await m.answer("📏 ✅", reply_markup=KB.main())


@rt.callback_query(F.data == "ed:job")
async def ej(cb: CallbackQuery, s: FSMContext):
    await cb.message.answer("💼:")
    await s.set_state(EditStates.edit_job)
    await cb.answer()


@rt.message(EditStates.edit_job)
async def sj(m: Message, s: FSMContext):
    await DB.upd(m.from_user.id, job=m.text.strip()[:100])
    await s.clear()
    await m.answer("💼 ✅", reply_markup=KB.main())


# ═══ PREMIUM SHOP ═══

@rt.message(F.text == "💎 Премиум")
async def premium_menu(msg: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    tier = user.get("subscription_tier", "free") if user else "free"
    current = TIER_NAMES.get(tier, "🆓")
    feats = TIER_FEATURES.get(tier, TIER_FEATURES["free"])

    txt = (
        f"💎 *ПРЕМИУМ СТАТУСЫ*\n\n"
        f"Твой статус: *{current}*\n\n"
        f"❤️ Лайков: {'♾️' if feats['likes'] >= 999999 else feats['likes']}/день\n"
        f"⚡ Суперлайков: {feats['super_likes']}/день\n"
        f"👻 Гостей: {'Все' if feats['guests'] >= 999 else feats['guests']}\n"
        f"{'✅ Кто лайкнул' if feats.get('who_liked') else '🔒 Кто лайкнул'}\n"
        f"{'✅ Невидимка' if feats.get('invisible') else '🔒 Невидимка'}\n"
        f"{'✅ Прочтения' if feats.get('read_receipts') else '🔒 Прочтения'}\n"
        f"{'✅ Приоритет' if feats.get('priority') else '🔒 Приоритет'}\n\n"
        f"_Выбери статус ниже_ 👇"
    )
    await msg.answer(txt, reply_markup=KB.premium(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "pr:main")
async def pr_main(cb: CallbackQuery):
    await cb.message.edit_text(
        "💎 *ПРЕМИУМ*", reply_markup=KB.premium(), parse_mode=ParseMode.MARKDOWN
    )
    await cb.answer()


@rt.callback_query(F.data == "pr:compare")
async def pr_compare(cb: CallbackQuery):
    await cb.message.edit_text(
        "📊 *СРАВНЕНИЕ СТАТУСОВ*\n\n"
        "🆓 *Бесплатный*\n30 лайков · 1 суперлайк · 3 гостя\n\n"
        "🥂 *Бокал Вина* — от 299₽\n100 лайков · 5 суперлайков · 15 гостей\n"
        "✅ Прочтения · Без рекламы · 3 возврата\n\n"
        "🍾 *Бутылка Вина* — от 499₽ 🔥\n♾️ лайков · 10 суперлайков · Все гости\n"
        "✅ Кто лайкнул · Невидимка · Приоритет · 1 буст\n\n"
        "🎖️ *Сомелье* — от 799₽\nВсё из Бутылки + 30 суперлайков · 3 буста\n"
        "✅ Топ-подборки · Сообщение с суперлайком · VIP-бейдж\n\n"
        "🏆 *Винный Погреб* — 4999₽ 💎\nВсё навсегда · ♾️ суперлайков · 5 бустов\n"
        "✅ Бейдж Основатель · Все будущие обновления",
        reply_markup=KB.premium(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await cb.answer()


@rt.callback_query(F.data.startswith("pr:wine_"))
async def pr_tier(cb: CallbackQuery):
    tier = cb.data[3:]
    tn = TIER_NAMES.get(tier, "")
    feats = TIER_FEATURES.get(tier, {})

    features_txt = ""
    if feats.get("who_liked"):
        features_txt += "✅ Видишь кто тебя лайкнул\n"
    if feats.get("invisible"):
        features_txt += "✅ Режим невидимки\n"
    if feats.get("read_receipts"):
        features_txt += "✅ Прочтения сообщений\n"
    if feats.get("priority"):
        features_txt += "✅ Приоритет в выдаче\n"
    if feats.get("top_picks"):
        features_txt += "✅ Топ-подборки\n"
    if feats.get("super_like_message"):
        features_txt += "✅ Сообщение с суперлайком\n"
    if feats.get("founder_badge"):
        features_txt += "✅ Бейдж «Основатель» 👑\n"

    await cb.message.edit_text(
        f"💎 *{tn}*\n\n"
        f"❤️ Лайков: {'♾️' if feats['likes'] >= 999999 else feats['likes']}/день\n"
        f"⚡ Суперлайков: {feats['super_likes']}/день\n"
        f"👻 Гостей: {'Все' if feats['guests'] >= 999 else feats['guests']}\n"
        f"🚀 Бустов: {feats.get('boosts', 0)}/день\n"
        f"↩️ Возвратов: {'♾️' if feats.get('rewinds', 0) >= 999 else feats.get('rewinds', 0)}\n\n"
        f"{features_txt}",
        reply_markup=KB.buy_tier(tier),
        parse_mode=ParseMode.MARKDOWN,
    )
    await cb.answer()


@rt.callback_query(F.data == "pr:sommelier")
async def pr_som(cb: CallbackQuery):
    tier = "sommelier"
    await cb.message.edit_text(
        f"💎 *{TIER_NAMES[tier]}*\n\n"
        f"♾️ Лайков · 30 суперлайков · Все гости\n"
        f"🚀 3 буста · ♾️ возвратов\n\n"
        f"✅ Кто лайкнул · Невидимка · Приоритет\n"
        f"✅ Топ-подборки · Сообщение с суперлайком\n"
        f"✅ VIP-бейдж 🎖️",
        reply_markup=KB.buy_tier(tier),
        parse_mode=ParseMode.MARKDOWN,
    )
    await cb.answer()


@rt.callback_query(F.data == "pr:boost")
async def pr_boost(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    has = user.get("boost_count", 0) > 0
    act = DB.is_boosted(user)
    st = (
        f"\n🚀 Активен до {user['boost_expires_at'].strftime('%H:%M')}" if act else ""
    )
    st += f"\n🚀 Запас: {user['boost_count']}" if has else "\n📦 Нет бустов"
    await cb.message.edit_text(
        f"🚀 *БУСТ АНКЕТЫ*\n\n+500% просмотров на 24ч!{st}",
        reply_markup=KB.boost_menu(has, act),
        parse_mode=ParseMode.MARKDOWN,
    )
    await cb.answer()


@rt.callback_query(F.data == "bo:act")
async def boost_act(cb: CallbackQuery, user: Optional[Dict]):
    if not user or user.get("boost_count", 0) <= 0:
        await cb.answer("🚫 Нет бустов!", show_alert=True)
        return
    ok = await DB.use_boost(user["id"])
    if ok:
        frames = ["🚀", "🚀✨", "🚀✨🍷", "🚀 *Буст!*"]
        m = await cb.message.answer(frames[0])
        for f in frames[1:]:
            await asyncio.sleep(0.3)
            try:
                await m.edit_text(f, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        u = await DB.get_user(cb.from_user.id)
        await cb.message.answer(
            f"🚀 До {u['boost_expires_at'].strftime('%H:%M')} · Запас: {u['boost_count']}",
            reply_markup=KB.main(),
        )
    await cb.answer()


@rt.callback_query(F.data.startswith("by:"))
async def buy(cb: CallbackQuery, user: Optional[Dict]):
    if not user:
        return
    parts = cb.data.split(":")
    prod, param, amt = parts[1], int(parts[2]), int(parts[3])
    if not YOOKASSA_AVAILABLE or not config.YOOKASSA_SHOP_ID:
        await cb.answer("💳 Оплата не настроена", show_alert=True)
        return

    if prod == "boost":
        desc = f"Буст ({param}шт) · {BOT_SHORT}"
    else:
        desc = f"{TIER_NAMES.get(prod, 'VIP')} · {BOT_SHORT}"

    try:
        p = YooPayment.create(
            {
                "amount": {"value": f"{amt / 100:.2f}", "currency": "RUB"},
                "confirmation": {
                    "type": ConfirmationType.REDIRECT,
                    "return_url": f"{config.DOMAIN}/ok",
                },
                "capture": True,
                "description": desc,
            },
            str(uuid.uuid4()),
        )
        pid = await DB.create_payment(
            user["id"], p.id, amt, desc,
            "boost" if prod == "boost" else "subscription",
            ptier=prod if prod != "boost" else None,
            pdur=param if prod != "boost" else None,
            pcnt=param if prod == "boost" else None,
        )
        await cb.message.edit_text(
            f"💳 *{amt / 100:.0f}₽*\n\nОплати и нажми «Проверить» ✅",
            reply_markup=KB.pay(p.confirmation.confirmation_url, pid),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        await cb.answer(f"❌ {str(e)[:50]}", show_alert=True)
    await cb.answer()


@rt.callback_query(F.data.startswith("ck:"))
async def check_pay(cb: CallbackQuery, user: Optional[Dict]):
    pid = int(cb.data[3:])
    async with async_session_maker() as s:
        pr = await s.execute(select(Payment).where(Payment.id == pid))
        p = pr.scalar_one_or_none()
    if not p:
        await cb.answer("❌", show_alert=True)
        return
    try:
        y = YooPayment.find_one(p.yookassa_payment_id)
        if y.status == "succeeded" and p.status != PaymentStatus.SUCCEEDED:
            async with _db_write_lock:
                async with async_session_maker() as s:
                    await s.execute(
                        update(Payment)
                        .where(Payment.id == pid)
                        .values(status=PaymentStatus.SUCCEEDED, paid_at=datetime.utcnow())
                    )
                    await s.commit()
            if p.product_type == "subscription":
                await DB.activate_sub(p.user_id, p.product_tier, p.product_duration or 30)
                frames = ["💳", "💳✅", "💳✅🎉", "💎 *Премиум активирован!* 🎉"]
            else:
                async with _db_write_lock:
                    async with async_session_maker() as s:
                        await s.execute(
                            update(User)
                            .where(User.id == p.user_id)
                            .values(boost_count=User.boost_count + (p.product_count or 1))
                        )
                        await s.commit()
                frames = ["💳", "💳✅", f"🚀 *{p.product_count or 1} бустов!*"]
            m = await cb.message.answer(frames[0])
            for f in frames[1:]:
                await asyncio.sleep(0.4)
                try:
                    await m.edit_text(f, parse_mode=ParseMode.MARKDOWN)
                except Exception:
                    pass
            await cb.message.answer("🍷", reply_markup=KB.main())
        elif y.status == "pending":
            await cb.answer("⏳ Обрабатывается...", show_alert=True)
        else:
            await cb.answer("❌ Не оплачено", show_alert=True)
    except Exception as e:
        await cb.answer(f"❌ {str(e)[:50]}", show_alert=True)


# ═══ PROMO ═══

@rt.callback_query(F.data == "pr:promo")
async def promo_input(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_text("🎁 *Промокод:*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(PromoInputState.waiting_code)
    await cb.answer()


@rt.message(PromoInputState.waiting_code)
async def promo_use(msg: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user:
        return
    r = await DB.use_promo(user["id"], msg.text.strip())
    if "error" in r:
        await msg.answer(r["error"], reply_markup=KB.main())
    else:
        tn = TIER_NAMES.get(r["tier"], "VIP")
        m = await msg.answer("🎁 Проверяем...")
        await asyncio.sleep(0.5)
        await m.edit_text(
            f"🎉 *{tn}* на {r['days']} дней! 🍷", parse_mode=ParseMode.MARKDOWN
        )
        await msg.answer("🍷", reply_markup=KB.main())


# ═══ REPORTS ═══

@rt.callback_query(F.data.startswith("rp:"))
async def rp_start(cb: CallbackQuery, state: FSMContext):
    await state.update_data(rp_id=int(cb.data[3:]))
    try:
        await cb.message.edit_caption(
            caption="⚠️ *Причина:*",
            reply_markup=KB.report(),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        await cb.message.edit_text(
            "⚠️ *Причина:*", reply_markup=KB.report(), parse_mode=ParseMode.MARKDOWN
        )
    await cb.answer()


@rt.callback_query(F.data.startswith("rr:"))
async def rp_save(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return
    d = await state.get_data()
    rid = d.get("rp_id")
    if rid:
        await DB.create_report(user["id"], rid, cb.data[3:])
    await state.clear()
    try:
        await cb.message.edit_caption(caption="✅ Жалоба отправлена 🍷")
    except Exception:
        await cb.message.edit_text("✅ Жалоба 🍷")
    ps = await DB.search(user, 1)
    if ps:
        await show_card(cb.message, ps[0], user)
    await cb.answer()


@rt.callback_query(F.data == "mn")
async def mn(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    try:
        await cb.message.delete()
    except Exception:
        pass
    un = await DB.get_unread(user["id"]) if user else 0
    lc = await DB.get_incoming_likes_count(user["id"]) if user else 0
    await cb.message.answer("🍷", reply_markup=KB.main(un, lc))
    await cb.answer()


@rt.callback_query(F.data == "nop")
async def nop(cb: CallbackQuery):
    await cb.answer()


# ═══ FAQ ═══

@rt.message(Command("faq"))
@rt.message(Command("help"))
async def faq(msg: Message):
    await msg.answer(
        f"❓ *{BOT_NAME}*\n\n"
        f"*❤️ Анкеты* — листай, лайкай, при взаимности мэтч!\n"
        f"*⚡ Суперлайк* — человек узнает что понравился\n"
        f"*🌟 Пара дня* — лучшая рекомендация дня от AI\n"
        f"*📊 Совместимость* — 9 факторов анализа\n"
        f"*🧠 AI подбор* — бот учится на твоих предпочтениях\n"
        f"*⚖️ ELO* — рейтинг балансирует показы\n"
        f"*❤️ Кто лайкнул* — узнай с Премиум\n"
        f"*💡 Подсказки* — icebreaker при мэтче\n"
        f"*🚀 Буст* — 24ч в топе\n"
        f"*💎 Премиум* — статусы и привилегии\n"
        f"*📸 Фото в чате* — делись моментами",
        parse_mode=ParseMode.MARKDOWN,
    )


# ═══ ADMIN ═══

@rt.message(Command("admin"))
async def admin(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not DB.is_admin(user):
        return
    await state.clear()
    await msg.answer("🛡️ *Админка*", reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "adm:main")
async def adm_main(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user or not DB.is_admin(user):
        return
    await state.clear()
    await cb.message.edit_text(
        "🛡️ *Админка*", reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN
    )
    await cb.answer()


@rt.callback_query(F.data == "adm:stats")
async def adm_stats(cb: CallbackQuery, user: Optional[Dict]):
    if not user or not DB.is_admin(user):
        return
    async with async_session_maker() as s:
        total = (await s.execute(select(func.count(User.id)))).scalar() or 0
        complete = (
            await s.execute(
                select(func.count(User.id)).where(User.is_profile_complete == True)
            )
        ).scalar() or 0
        now = datetime.utcnow()
        da = now - timedelta(days=1)
        dau = (
            await s.execute(select(func.count(User.id)).where(User.last_active_at > da))
        ).scalar() or 0
        vip = (
            await s.execute(
                select(func.count(User.id)).where(
                    User.subscription_tier != SubscriptionTier.FREE
                )
            )
        ).scalar() or 0
        matches_t = (await s.execute(select(func.count(Match.id)))).scalar() or 0
        msgs_t = (await s.execute(select(func.count(ChatMessage.id)))).scalar() or 0
        likes_t = (await s.execute(select(func.count(Like.id)))).scalar() or 0
        today_reg = (
            await s.execute(select(func.count(User.id)).where(User.created_at > da))
        ).scalar() or 0
        rev = (
            await s.execute(
                select(func.sum(Payment.amount)).where(
                    Payment.status == PaymentStatus.SUCCEEDED
                )
            )
        ).scalar() or 0

    await cb.message.edit_text(
        f"📊 *Статистика*\n\n"
        f"👥 {total} (анкет: {complete})\n"
        f"📈 DAU: {dau} · VIP: {vip} · Сегодня: +{today_reg}\n"
        f"❤️ {likes_t} · 💕 {matches_t} · 💬 {msgs_t}\n"
        f"💰 {rev / 100:.0f}₽",
        reply_markup=KB.back_admin(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await cb.answer()


# ═══ BACKGROUND TASKS ═══

async def bg_learner():
    """FIX: Добавлена обработка ошибок и увеличена пауза"""
    while True:
        try:
            async with async_session_maker() as s:
                active = await s.execute(
                    select(User.id)
                    .where(and_(User.is_active == True, User.likes_given_count > 5))
                    .limit(50)
                )
                ids = [row[0] for row in active.fetchall()]

            for uid in ids:
                try:
                    await Learner.learn(uid)
                except Exception as e:
                    logger.error(f"Learner error for {uid}: {e}")
                await asyncio.sleep(0.5)  # FIX: больше пауза

        except Exception as e:
            logger.error(f"bg_learner error: {e}")
        await asyncio.sleep(3600)


async def bg_offline():
    """FIX: Помечает offline с блокировкой"""
    while True:
        try:
            threshold = datetime.utcnow() - timedelta(minutes=15)
            async with _db_write_lock:
                async with async_session_maker() as s:
                    await s.execute(
                        update(User)
                        .where(and_(User.is_online == True, User.last_active_at < threshold))
                        .values(is_online=False)
                    )
                    await s.commit()
        except Exception as e:
            logger.error(f"bg_offline error: {e}")
        await asyncio.sleep(300)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                      MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def main():
    await init_db()

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.middleware(UserMW())
    dp.callback_query.middleware(UserMW())
    dp.include_router(rt)

    asyncio.create_task(bg_learner())
    asyncio.create_task(bg_offline())

    logger.info(f"🍷 {BOT_NAME} v5.5-fix")
    logger.info("🔧 Fixes: DB locking, deferred tasks, throttled activity updates")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
