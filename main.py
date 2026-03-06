"""
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🍷 ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v5.0
🍷 INTELLIGENT MATCHING SYSTEM
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Запуск:
  pip install aiogram aiosqlite sqlalchemy yookassa python-dotenv
  python bot.py
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import os
import uuid
import logging
import random
import math
import hashlib
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple, Set
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
    select, update, func, and_, or_, desc, delete, text
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                       CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
    FREE_DAILY_SUPER_LIKES: int = 1

    NOTIFY_BATCH_SIZE: int = 25
    NOTIFY_BATCH_DELAY: float = 1.0
    INACTIVE_REMINDER_HOURS: int = 24
    SUB_EXPIRY_WARN_DAYS: int = 3

    # Smart matching
    SEARCH_RADIUS_EXPAND: bool = True
    ELO_DEFAULT: int = 1000
    ELO_K_FACTOR: int = 32
    MIN_COMPATIBILITY_SHOW: float = 25.0

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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    ENUMS & MODELS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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

class InteractionType(str, Enum):
    LIKE = "like"
    SUPER_LIKE = "super_like"
    DISLIKE = "dislike"
    SKIP = "skip"
    VIEW = "view"
    MESSAGE = "message"
    BLOCK = "block"


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

    # Расширенный профиль
    interests = Column(Text, default="")  # JSON список интересов
    zodiac = Column(String(30), nullable=True)
    height = Column(Integer, nullable=True)
    smoking = Column(String(20), nullable=True)  # no/sometimes/yes
    drinking = Column(String(20), nullable=True)  # no/sometimes/yes/wine_only
    children = Column(String(20), nullable=True)  # no/want/have
    education = Column(String(50), nullable=True)
    job = Column(String(100), nullable=True)
    relationship_goal = Column(String(30), nullable=True)  # serious/casual/friends/not_sure
    languages = Column(Text, default="")

    is_active = Column(Boolean, default=True)
    is_banned = Column(Boolean, default=False)
    is_verified = Column(Boolean, default=False)
    is_profile_complete = Column(Boolean, default=False)

    subscription_tier = Column(SQLEnum(SubscriptionTier), default=SubscriptionTier.FREE)
    subscription_expires_at = Column(DateTime, nullable=True)

    daily_likes_remaining = Column(Integer, default=30)
    daily_messages_remaining = Column(Integer, default=10)
    daily_super_likes_remaining = Column(Integer, default=1)
    last_limits_reset = Column(DateTime, nullable=True)

    boost_expires_at = Column(DateTime, nullable=True)
    boost_count = Column(Integer, default=0)

    views_count = Column(Integer, default=0)
    likes_received_count = Column(Integer, default=0)
    likes_given_count = Column(Integer, default=0)
    matches_count = Column(Integer, default=0)
    messages_sent_count = Column(Integer, default=0)

    # ELO & Smart Matching
    elo_score = Column(Integer, default=1000)
    attractiveness_score = Column(Float, default=50.0)
    selectivity_score = Column(Float, default=50.0)
    response_rate = Column(Float, default=0.0)
    avg_response_time_min = Column(Float, default=0.0)
    profile_quality_score = Column(Float, default=0.0)

    # Notification settings
    notify_likes = Column(Boolean, default=True)
    notify_matches = Column(Boolean, default=True)
    notify_messages = Column(Boolean, default=True)
    notify_guests = Column(Boolean, default=True)
    notify_reminders = Column(Boolean, default=True)
    notify_sub_events = Column(Boolean, default=True)
    quiet_hours_start = Column(Integer, nullable=True)
    quiet_hours_end = Column(Integer, nullable=True)
    last_notified_at = Column(DateTime, nullable=True)
    last_reminder_at = Column(DateTime, nullable=True)
    last_weekly_stats_at = Column(DateTime, nullable=True)

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
    is_notified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Dislike(Base):
    """Трекинг дизлайков для обучения алгоритма"""
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
    is_read = Column(Boolean, default=False)
    is_notified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class GuestVisit(Base):
    __tablename__ = "guest_visits"
    id = Column(Integer, primary_key=True)
    visitor_id = Column(Integer, ForeignKey("users.id"))
    visited_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    is_notified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class UserInteraction(Base):
    """Все взаимодействия для ML-обучения алгоритма"""
    __tablename__ = "user_interactions"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    target_id = Column(Integer, ForeignKey("users.id"), index=True)
    interaction_type = Column(String(30))
    time_spent_sec = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


class UserPreferenceLearned(Base):
    """Выученные предпочтения из поведения"""
    __tablename__ = "user_preferences_learned"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, index=True)
    preferred_age_min = Column(Integer, nullable=True)
    preferred_age_max = Column(Integer, nullable=True)
    preferred_cities = Column(Text, default="")
    preferred_interests = Column(Text, default="")
    preferred_height_min = Column(Integer, nullable=True)
    preferred_height_max = Column(Integer, nullable=True)
    preferred_zodiac = Column(Text, default="")
    preferred_bio_keywords = Column(Text, default="")
    avg_liked_elo = Column(Float, default=1000)
    avg_liked_age = Column(Float, nullable=True)
    like_rate = Column(Float, default=0.5)
    total_interactions = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow)


class DailyMatch(Base):
    """Ежедневная рекомендация — 'Пара дня'"""
    __tablename__ = "daily_matches"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    recommended_id = Column(Integer, ForeignKey("users.id"))
    compatibility_score = Column(Float, default=0)
    reason = Column(Text, nullable=True)
    is_seen = Column(Boolean, default=False)
    is_liked = Column(Boolean, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class SpeedDatingRoom(Base):
    """Комнаты быстрых знакомств"""
    __tablename__ = "speed_dating_rooms"
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    theme = Column(String(100), nullable=True)
    city = Column(String(100), nullable=True)
    age_from = Column(Integer, default=18)
    age_to = Column(Integer, default=99)
    max_participants = Column(Integer, default=20)
    is_active = Column(Boolean, default=True)
    starts_at = Column(DateTime, nullable=True)
    ends_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class SpeedDatingParticipant(Base):
    __tablename__ = "speed_dating_participants"
    id = Column(Integer, primary_key=True)
    room_id = Column(Integer, ForeignKey("speed_dating_rooms.id"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    joined_at = Column(DateTime, default=datetime.utcnow)


class QuestionOfDay(Base):
    """Вопрос дня для профиля"""
    __tablename__ = "questions_of_day"
    id = Column(Integer, primary_key=True)
    question_text = Column(Text)
    category = Column(String(50), default="general")
    is_active = Column(Boolean, default=True)
    used_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class UserAnswer(Base):
    """Ответы пользователей на вопросы дня"""
    __tablename__ = "user_answers"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    question_id = Column(Integer, ForeignKey("questions_of_day.id"))
    answer_text = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class NotificationLog(Base):
    __tablename__ = "notification_logs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    notification_type = Column(String(50))
    message_text = Column(Text, nullable=True)
    is_delivered = Column(Boolean, default=False)
    error_text = Column(String(255), nullable=True)
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

    # Auto-migration
    migrations = [
        ("users", "interests", "TEXT", "''"),
        ("users", "zodiac", "VARCHAR(30)", "NULL"),
        ("users", "height", "INTEGER", "NULL"),
        ("users", "smoking", "VARCHAR(20)", "NULL"),
        ("users", "drinking", "VARCHAR(20)", "NULL"),
        ("users", "children", "VARCHAR(20)", "NULL"),
        ("users", "education", "VARCHAR(50)", "NULL"),
        ("users", "job", "VARCHAR(100)", "NULL"),
        ("users", "relationship_goal", "VARCHAR(30)", "NULL"),
        ("users", "languages", "TEXT", "''"),
        ("users", "daily_super_likes_remaining", "INTEGER", "1"),
        ("users", "likes_given_count", "INTEGER", "0"),
        ("users", "messages_sent_count", "INTEGER", "0"),
        ("users", "elo_score", "INTEGER", "1000"),
        ("users", "attractiveness_score", "FLOAT", "50.0"),
        ("users", "selectivity_score", "FLOAT", "50.0"),
        ("users", "response_rate", "FLOAT", "0.0"),
        ("users", "avg_response_time_min", "FLOAT", "0.0"),
        ("users", "profile_quality_score", "FLOAT", "0.0"),
        ("users", "notify_likes", "BOOLEAN", "1"),
        ("users", "notify_matches", "BOOLEAN", "1"),
        ("users", "notify_messages", "BOOLEAN", "1"),
        ("users", "notify_guests", "BOOLEAN", "1"),
        ("users", "notify_reminders", "BOOLEAN", "1"),
        ("users", "notify_sub_events", "BOOLEAN", "1"),
        ("users", "quiet_hours_start", "INTEGER", "NULL"),
        ("users", "quiet_hours_end", "INTEGER", "NULL"),
        ("users", "last_notified_at", "DATETIME", "NULL"),
        ("users", "last_reminder_at", "DATETIME", "NULL"),
        ("users", "last_weekly_stats_at", "DATETIME", "NULL"),
        ("users", "referred_by", "INTEGER", "NULL"),
        ("likes", "is_notified", "BOOLEAN", "0"),
        ("messages", "is_notified", "BOOLEAN", "0"),
        ("guest_visits", "is_notified", "BOOLEAN", "0"),
        ("matches", "compatibility_details", "TEXT", "'{}'"),
        ("matches", "icebreaker_text", "TEXT", "NULL"),
        ("matches", "messages_count", "INTEGER", "0"),
    ]

    async with engine.begin() as conn:
        for table, column, col_type, default in migrations:
            try:
                default_clause = f"DEFAULT {default}" if default != "NULL" else ""
                await conn.execute(
                    text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type} {default_clause}")
                )
                logger.info(f"✅ Migration: {table}.{column}")
            except Exception:
                pass

    logger.info("🍷 DB ready (v5.0)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                     FSM STATES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RegStates(StatesGroup):
    name = State()
    age = State()
    gender = State()
    city = State()
    photo = State()
    bio = State()
    looking_for = State()
    # Extended profile
    interests = State()
    relationship_goal = State()
    height = State()
    zodiac = State()
    drinking = State()

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

class AnswerQuestionState(StatesGroup):
    answering = State()

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
    add_question = State()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#            INTELLIGENT COMPATIBILITY ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Знаки зодиака и совместимость
ZODIAC_SIGNS = [
    "♈ Овен", "♉ Телец", "♊ Близнецы", "♋ Рак",
    "♌ Лев", "♍ Дева", "♎ Весы", "♏ Скорпион",
    "♐ Стрелец", "♑ Козерог", "♒ Водолей", "♓ Рыбы"
]

ZODIAC_COMPAT = {
    # (sign1_idx, sign2_idx) -> compatibility_boost (0-10)
    # Огонь + Огонь/Воздух = хорошо
    # Вода + Вода/Земля = хорошо
}

INTERESTS_LIST = [
    "🎵 Музыка", "🎬 Кино", "📚 Книги", "🎮 Игры",
    "⚽ Спорт", "🧘 Йога", "✈️ Путешествия", "🍳 Кулинария",
    "📸 Фото", "🎨 Искусство", "💻 Технологии", "🐕 Животные",
    "🌱 Природа", "🎭 Театр", "💃 Танцы", "🏋️ Фитнес",
    "🍷 Вино", "☕ Кофе", "🎸 Концерты", "🏔️ Походы",
    "🎯 Настолки", "📝 Писательство", "🧠 Психология", "🚗 Авто",
]

RELATIONSHIP_GOALS = {
    "serious": "💍 Серьёзные отношения",
    "casual": "🌸 Лёгкие отношения",
    "friends": "🤝 Дружба",
    "not_sure": "🤷 Пока не знаю",
}

DRINKING_OPTIONS = {
    "no": "🚫 Не пью",
    "wine_only": "🍷 Только вино",
    "sometimes": "🥂 Иногда",
    "yes": "🍻 Регулярно",
}

SMOKING_OPTIONS = {
    "no": "🚭 Не курю",
    "sometimes": "🚬 Иногда",
    "yes": "🚬 Курю",
}

CHILDREN_OPTIONS = {
    "no": "🚫 Нет и не хочу",
    "want": "👶 Хочу в будущем",
    "have": "👨‍👧 Уже есть",
}


class SmartCompatibility:
    """
    Интеллектуальная система совместимости v3.0

    Факторы:
    1. Базовые (город, возраст, пол) — 25%
    2. Интересы и хобби — 20%
    3. Цели отношений — 15%
    4. Образ жизни (курение, алкоголь, дети) — 10%
    5. Активность и вовлечённость — 10%
    6. Профиль качества — 5%
    7. ELO-баланс — 5%
    8. Зодиак (бонус) — 5%
    9. Выученные предпочтения (ML) — 5%
    """

    @staticmethod
    def calculate(u1: Dict, u2: Dict, learned_prefs: Dict = None) -> Dict:
        """Возвращает детальный расчёт совместимости"""
        details = {}
        weights = {}

        # 1. БАЗОВЫЕ (25%)
        base_score = 0.0
        weights["base"] = 25

        # Город
        city_match = (u1.get("city") or "").lower() == (u2.get("city") or "").lower()
        if city_match:
            base_score += 40
        else:
            base_score += 5  # Другой город но не ноль

        # Возраст
        age1, age2 = u1.get("age", 25), u2.get("age", 25)
        age_diff = abs(age1 - age2)
        if age_diff <= 2:
            base_score += 30
        elif age_diff <= 5:
            base_score += 25
        elif age_diff <= 8:
            base_score += 15
        elif age_diff <= 12:
            base_score += 8
        else:
            base_score += 2

        # Взаимный поиск
        lf1, lf2 = u1.get("looking_for", "both"), u2.get("looking_for", "both")
        g1, g2 = u1.get("gender"), u2.get("gender")
        mutual = True
        if lf1 != "both" and lf1 != g2:
            mutual = False
        if lf2 != "both" and lf2 != g1:
            mutual = False
        if mutual:
            base_score += 30
        else:
            base_score = base_score * 0.3  # Сильный штраф

        details["base"] = min(base_score, 100)

        # 2. ИНТЕРЕСЫ (20%)
        weights["interests"] = 20
        i1 = set((u1.get("interests") or "").split(",")) - {""}
        i2 = set((u2.get("interests") or "").split(",")) - {""}
        if i1 and i2:
            common = i1 & i2
            total = i1 | i2
            jaccard = len(common) / len(total) if total else 0
            interest_score = min(jaccard * 200, 100)  # Усиленный
        elif not i1 and not i2:
            interest_score = 40  # Оба не заполнили
        else:
            interest_score = 20  # Один заполнил
        details["interests"] = interest_score

        # 3. ЦЕЛИ ОТНОШЕНИЙ (15%)
        weights["goals"] = 15
        g1 = u1.get("relationship_goal")
        g2 = u2.get("relationship_goal")
        if g1 and g2:
            if g1 == g2:
                goal_score = 100
            elif {g1, g2} & {"not_sure"}:
                goal_score = 60
            elif {g1, g2} == {"serious", "casual"}:
                goal_score = 15  # Несовместимо
            else:
                goal_score = 40
        else:
            goal_score = 50
        details["goals"] = goal_score

        # 4. ОБРАЗ ЖИЗНИ (10%)
        weights["lifestyle"] = 10
        lifestyle_score = 0
        lifestyle_count = 0

        # Курение
        s1, s2 = u1.get("smoking"), u2.get("smoking")
        if s1 and s2:
            lifestyle_count += 1
            if s1 == s2:
                lifestyle_score += 100
            elif "no" in (s1, s2) and "yes" in (s1, s2):
                lifestyle_score += 20
            else:
                lifestyle_score += 60

        # Алкоголь
        d1, d2 = u1.get("drinking"), u2.get("drinking")
        if d1 and d2:
            lifestyle_count += 1
            if d1 == d2:
                lifestyle_score += 100
            elif {d1, d2} == {"no", "yes"}:
                lifestyle_score += 25
            else:
                lifestyle_score += 65

        # Дети
        c1, c2 = u1.get("children"), u2.get("children")
        if c1 and c2:
            lifestyle_count += 1
            if c1 == c2:
                lifestyle_score += 100
            elif {c1, c2} == {"no", "want"}:
                lifestyle_score += 20
            else:
                lifestyle_score += 50

        if lifestyle_count > 0:
            details["lifestyle"] = lifestyle_score / lifestyle_count
        else:
            details["lifestyle"] = 50

        # 5. АКТИВНОСТЬ (10%)
        weights["activity"] = 10
        la = u2.get("last_active_at")
        if la:
            hours = (datetime.utcnow() - la).total_seconds() / 3600
            if hours < 1:
                activity_score = 100
            elif hours < 3:
                activity_score = 90
            elif hours < 12:
                activity_score = 70
            elif hours < 24:
                activity_score = 50
            elif hours < 72:
                activity_score = 25
            else:
                activity_score = 5
        else:
            activity_score = 10
        details["activity"] = activity_score

        # 6. КАЧЕСТВО ПРОФИЛЯ (5%)
        weights["profile_quality"] = 5
        pq = 0
        if u2.get("main_photo"):
            pq += 25
        photos = (u2.get("photos") or "").split(",")
        pq += min(len([p for p in photos if p]), 3) * 10
        if u2.get("bio") and len(u2["bio"]) > 20:
            pq += 20
        if u2.get("interests"):
            pq += 10
        if u2.get("relationship_goal"):
            pq += 5
        if u2.get("height"):
            pq += 5
        details["profile_quality"] = min(pq, 100)

        # 7. ELO БАЛАНС (5%)
        weights["elo"] = 5
        e1 = u1.get("elo_score", 1000)
        e2 = u2.get("elo_score", 1000)
        elo_diff = abs(e1 - e2)
        if elo_diff <= 50:
            elo_score = 100
        elif elo_diff <= 100:
            elo_score = 80
        elif elo_diff <= 200:
            elo_score = 50
        else:
            elo_score = 20
        details["elo"] = elo_score

        # 8. ЗОДИАК БОНУС (5%)
        weights["zodiac"] = 5
        z1, z2 = u1.get("zodiac"), u2.get("zodiac")
        if z1 and z2 and z1 == z2:
            zodiac_score = 80
        elif z1 and z2:
            zodiac_score = 50 + random.randint(-10, 20)
        else:
            zodiac_score = 50
        details["zodiac"] = min(max(zodiac_score, 0), 100)

        # 9. ВЫУЧЕННЫЕ ПРЕДПОЧТЕНИЯ (5%)
        weights["learned"] = 5
        if learned_prefs and learned_prefs.get("total_interactions", 0) > 5:
            learned_score = 50
            # Предпочтительный возраст
            if learned_prefs.get("avg_liked_age"):
                age_from_pref = abs(u2.get("age", 25) - learned_prefs["avg_liked_age"])
                if age_from_pref <= 2:
                    learned_score += 25
                elif age_from_pref <= 5:
                    learned_score += 15

            # Предпочтительные интересы
            pref_interests = set((learned_prefs.get("preferred_interests") or "").split(",")) - {""}
            if pref_interests and i2:
                common_pref = pref_interests & i2
                if common_pref:
                    learned_score += min(len(common_pref) * 10, 25)

            details["learned"] = min(learned_score, 100)
        else:
            details["learned"] = 50

        # ИТОГОВЫЙ РАСЧЁТ
        total = sum(details[k] * weights[k] / 100 for k in details if k in weights)
        total = min(max(total, 0), 100)

        return {
            "total": round(total, 1),
            "details": details,
            "weights": weights,
            "city_match": city_match,
            "mutual_search": mutual,
        }

    @staticmethod
    def generate_icebreaker(u1: Dict, u2: Dict, compat: Dict) -> str:
        """Генерирует тему для начала разговора"""
        details = compat.get("details", {})

        # Общие интересы
        i1 = set((u1.get("interests") or "").split(",")) - {""}
        i2 = set((u2.get("interests") or "").split(",")) - {""}
        common = i1 & i2

        icebreakers = []

        if common:
            interest = random.choice(list(common))
            icebreakers.append(f"💡 У вас общий интерес: *{interest}*! Спросите об этом!")

        if u1.get("city") == u2.get("city"):
            icebreakers.append(f"🏙️ Вы оба из *{u1['city']}*! Обсудите любимые места!")

        if u1.get("zodiac") and u2.get("zodiac"):
            icebreakers.append(f"🔮 {u1['zodiac']} + {u2['zodiac']} — интересное сочетание! Верите в гороскопы?")

        if u1.get("drinking") == "wine_only" and u2.get("drinking") == "wine_only":
            icebreakers.append("🍷 Вы оба ценители вина! Какое предпочитаете?")

        # Дефолтные
        defaults = [
            f"🍷 Спросите {u2['name']}, какое вино любит!",
            f"✈️ Спросите о любимом месте для путешествий!",
            f"🎬 Узнайте какой фильм {u2['name']} смотрел(а) последним!",
            f"☕ Предложите встретиться за чашкой кофе!",
            f"📚 Спросите какую книгу сейчас читает!",
        ]

        if not icebreakers:
            icebreakers = defaults

        return random.choice(icebreakers)

    @staticmethod
    def compatibility_bar(score: float, detailed: bool = False) -> str:
        """Визуализация совместимости"""
        filled = int(score / 10)
        empty = 10 - filled
        bar = "🟣" * filled + "⚪" * empty

        if score >= 90:
            emoji, label = "🔥", "Идеальная пара!"
        elif score >= 75:
            emoji, label = "💕", "Отличная совместимость!"
        elif score >= 60:
            emoji, label = "✨", "Хорошие шансы!"
        elif score >= 45:
            emoji, label = "👍", "Стоит попробовать"
        elif score >= 30:
            emoji, label = "🤔", "Есть потенциал"
        else:
            emoji, label = "💫", "Противоположности притягиваются?"

        result = f"{emoji} {bar} *{score:.0f}%*"
        if detailed:
            result += f"\n_{label}_"
        return result

    @staticmethod
    def compatibility_breakdown(details: Dict) -> str:
        """Детальная разбивка совместимости (для VIP)"""
        labels = {
            "base": "🏠 База",
            "interests": "🎯 Интересы",
            "goals": "💍 Цели",
            "lifestyle": "🌿 Образ жизни",
            "activity": "⚡ Активность",
            "profile_quality": "📝 Профиль",
            "elo": "⚖️ Рейтинг",
            "zodiac": "🔮 Зодиак",
            "learned": "🧠 AI-подбор",
        }

        lines = []
        for key, label in labels.items():
            if key in details:
                val = details[key]
                bar_len = int(val / 20)
                mini_bar = "█" * bar_len + "░" * (5 - bar_len)
                lines.append(f"  {label}: {mini_bar} {val:.0f}%")

        return "\n".join(lines)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                   ELO RATING SYSTEM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class EloSystem:
    """
    Система рейтинга ELO для балансировки показов.
    Лайк = "победа" для получателя.
    Дизлайк = "проигрыш" для получателя.
    """

    @staticmethod
    def calculate_new_elo(winner_elo: int, loser_elo: int, k: int = 32) -> Tuple[int, int]:
        """Возвращает (new_winner_elo, new_loser_elo)"""
        expected_w = 1 / (1 + 10 ** ((loser_elo - winner_elo) / 400))
        expected_l = 1 - expected_w

        new_w = round(winner_elo + k * (1 - expected_w))
        new_l = round(loser_elo + k * (0 - expected_l))

        # Ограничение
        new_w = max(100, min(2000, new_w))
        new_l = max(100, min(2000, new_l))

        return new_w, new_l

    @staticmethod
    async def process_like(liker_id: int, liked_id: int):
        """Обновляет ELO при лайке"""
        async with async_session_maker() as s:
            liker = await s.execute(select(User).where(User.id == liker_id))
            liked = await s.execute(select(User).where(User.id == liked_id))
            u_liker = liker.scalar_one_or_none()
            u_liked = liked.scalar_one_or_none()
            if not u_liker or not u_liked:
                return

            # Liked получает "победу" — его ELO растёт
            _, new_liked = EloSystem.calculate_new_elo(
                u_liked.elo_score or 1000, u_liker.elo_score or 1000
            )
            await s.execute(
                update(User).where(User.id == liked_id)
                .values(elo_score=new_liked)
            )

            # Обновляем attractiveness
            total_likes = (u_liked.likes_received_count or 0)
            total_views = max(u_liked.views_count or 1, 1)
            attractiveness = min((total_likes / total_views) * 100, 100)
            await s.execute(
                update(User).where(User.id == liked_id)
                .values(attractiveness_score=attractiveness)
            )

            # Обновляем selectivity лайкера
            given = (u_liker.likes_given_count or 0) + 1
            await s.execute(
                update(User).where(User.id == liker_id)
                .values(likes_given_count=given)
            )

            await s.commit()

    @staticmethod
    async def process_dislike(disliker_id: int, disliked_id: int):
        """Обновляет ELO при дизлайке"""
        async with async_session_maker() as s:
            disliker = await s.execute(select(User).where(User.id == disliker_id))
            disliked = await s.execute(select(User).where(User.id == disliked_id))
            u_disliker = disliker.scalar_one_or_none()
            u_disliked = disliked.scalar_one_or_none()
            if not u_disliker or not u_disliked:
                return

            # Мягкий штраф за дизлайк
            new_elo = max(100, (u_disliked.elo_score or 1000) - 5)
            await s.execute(
                update(User).where(User.id == disliked_id)
                .values(elo_score=new_elo)
            )
            await s.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#              PREFERENCE LEARNING ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PreferenceLearner:
    """
    Обучается на поведении пользователя:
    - Какой возраст он лайкает чаще
    - Какие интересы у тех, кого он лайкает
    - Из каких городов
    - Какой ELO у тех, кого он лайкает
    """

    @staticmethod
    async def record_interaction(user_id: int, target_id: int, itype: str):
        """Записывает взаимодействие"""
        async with async_session_maker() as s:
            s.add(UserInteraction(
                user_id=user_id, target_id=target_id,
                interaction_type=itype
            ))
            await s.commit()

    @staticmethod
    async def learn(user_id: int):
        """Анализирует поведение и обновляет предпочтения"""
        async with async_session_maker() as s:
            # Берём все лайки пользователя
            likes = await s.execute(
                select(Like.to_user_id).where(Like.from_user_id == user_id)
            )
            liked_ids = [r[0] for r in likes.fetchall()]

            if len(liked_ids) < 3:
                return  # Мало данных

            # Получаем профили лайкнутых
            liked_users = await s.execute(
                select(User).where(User.id.in_(liked_ids))
            )
            liked_profiles = [u for u in liked_users.scalars().all()]

            if not liked_profiles:
                return

            # Анализ возраста
            ages = [u.age for u in liked_profiles if u.age]
            avg_age = sum(ages) / len(ages) if ages else None

            # Анализ городов
            cities = [u.city for u in liked_profiles if u.city]
            city_counter = defaultdict(int)
            for c in cities:
                city_counter[c.lower()] += 1
            top_cities = sorted(city_counter.items(), key=lambda x: x[1], reverse=True)[:5]
            pref_cities = ",".join([c[0] for c in top_cities])

            # Анализ интересов
            all_interests = []
            for u in liked_profiles:
                if u.interests:
                    all_interests.extend(u.interests.split(","))
            interest_counter = defaultdict(int)
            for i in all_interests:
                if i.strip():
                    interest_counter[i.strip()] += 1
            top_interests = sorted(interest_counter.items(), key=lambda x: x[1], reverse=True)[:10]
            pref_interests = ",".join([i[0] for i in top_interests])

            # Средний ELO лайкнутых
            elos = [u.elo_score for u in liked_profiles if u.elo_score]
            avg_elo = sum(elos) / len(elos) if elos else 1000

            # Дизлайки
            dislikes = await s.execute(
                select(func.count(Dislike.id)).where(Dislike.from_user_id == user_id)
            )
            total_dislikes = dislikes.scalar() or 0
            total_likes = len(liked_ids)
            like_rate = total_likes / max(total_likes + total_dislikes, 1)

            # Сохраняем
            existing = await s.execute(
                select(UserPreferenceLearned).where(UserPreferenceLearned.user_id == user_id)
            )
            pref = existing.scalar_one_or_none()

            values = {
                "preferred_age_min": int(avg_age - 3) if avg_age else None,
                "preferred_age_max": int(avg_age + 3) if avg_age else None,
                "preferred_cities": pref_cities,
                "preferred_interests": pref_interests,
                "avg_liked_elo": avg_elo,
                "avg_liked_age": avg_age,
                "like_rate": like_rate,
                "total_interactions": total_likes + total_dislikes,
                "updated_at": datetime.utcnow(),
            }

            if pref:
                await s.execute(
                    update(UserPreferenceLearned)
                    .where(UserPreferenceLearned.user_id == user_id)
                    .values(**values)
                )
            else:
                s.add(UserPreferenceLearned(user_id=user_id, **values))

            await s.commit()

    @staticmethod
    async def get_preferences(user_id: int) -> Optional[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(UserPreferenceLearned).where(UserPreferenceLearned.user_id == user_id)
            )
            p = r.scalar_one_or_none()
            if not p:
                return None
            return {
                "preferred_age_min": p.preferred_age_min,
                "preferred_age_max": p.preferred_age_max,
                "preferred_cities": p.preferred_cities,
                "preferred_interests": p.preferred_interests,
                "avg_liked_elo": p.avg_liked_elo,
                "avg_liked_age": p.avg_liked_age,
                "like_rate": p.like_rate,
                "total_interactions": p.total_interactions,
            }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#               SMART SEARCH ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SmartSearch:
    """
    Умный поиск анкет с многофакторным ранжированием:

    1. Фильтрация (пол, возраст, город)
    2. Исключение уже взаимодействовавших
    3. Подсчёт совместимости с учётом ML
    4. ELO-балансировка (показываем похожий уровень)
    5. Буст приоритет
    6. Новички-бонус (первые 24ч)
    7. Активность бонус
    8. Рандомизация (чтобы не повторялись)
    """

    @staticmethod
    async def find_profiles(user: Dict, limit: int = 1) -> List[Dict]:
        async with async_session_maker() as s:
            # Исключаем лайкнутых и дизлайкнутых
            liked = await s.execute(
                select(Like.to_user_id).where(Like.from_user_id == user["id"])
            )
            liked_ids = {r[0] for r in liked.fetchall()}

            disliked = await s.execute(
                select(Dislike.to_user_id).where(Dislike.from_user_id == user["id"])
            )
            disliked_ids = {r[0] for r in disliked.fetchall()}

            exclude_ids = liked_ids | disliked_ids | {user["id"]}

            # Базовый фильтр
            q = select(User).where(and_(
                User.is_active == True,
                User.is_banned == False,
                User.is_profile_complete == True,
                User.id.not_in(exclude_ids),
                User.age >= user["age_from"],
                User.age <= user["age_to"]
            ))

            # Фильтр по полу
            lf = user.get("looking_for", "both")
            if lf == "male":
                q = q.where(User.gender == Gender.MALE)
            elif lf == "female":
                q = q.where(User.gender == Gender.FEMALE)

            # Берём больше кандидатов для ранжирования
            pool_size = max(limit * 10, 30)
            q = q.order_by(
                User.boost_expires_at.desc().nullslast(),
                (User.city == user["city"]).desc(),
                User.last_active_at.desc()
            ).limit(pool_size)

            r = await s.execute(q)
            candidates = [DB._to_dict(x) for x in r.scalars().all()]

            if not candidates:
                # Расширяем поиск — без фильтра города
                q2 = select(User).where(and_(
                    User.is_active == True,
                    User.is_banned == False,
                    User.is_profile_complete == True,
                    User.id.not_in(exclude_ids),
                )).order_by(User.last_active_at.desc()).limit(pool_size)

                if lf == "male":
                    q2 = q2.where(User.gender == Gender.MALE)
                elif lf == "female":
                    q2 = q2.where(User.gender == Gender.FEMALE)

                r2 = await s.execute(q2)
                candidates = [DB._to_dict(x) for x in r2.scalars().all()]

        if not candidates:
            return []

        # Получаем выученные предпочтения
        learned = await PreferenceLearner.get_preferences(user["id"])

        # Ранжирование
        scored = []
        for c in candidates:
            compat = SmartCompatibility.calculate(user, c, learned)
            total = compat["total"]

            # Бонусы
            bonus = 0

            # Буст
            if DB.is_boosted(c):
                bonus += 15

            # Новичок (< 24ч)
            if c.get("created_at"):
                hours_since = (datetime.utcnow() - c["created_at"]).total_seconds() / 3600
                if hours_since < 24:
                    bonus += 10
                elif hours_since < 72:
                    bonus += 5

            # Хороший response rate
            if c.get("response_rate", 0) > 0.7:
                bonus += 5

            # Рандомизация
            bonus += random.uniform(-3, 3)

            final_score = min(total + bonus, 100)

            c["_compat"] = compat
            c["_score"] = final_score

            scored.append(c)

        # Сортировка по итоговому скору
        scored.sort(key=lambda x: x["_score"], reverse=True)

        return scored[:limit]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#               DAILY RECOMMENDATIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DailyRecommendations:
    """Система ежедневных рекомендаций: 'Пара дня' и 'Топ-5'"""

    @staticmethod
    async def generate_pair_of_day(user_id: int) -> Optional[Dict]:
        """Генерирует лучшую рекомендацию дня"""
        user = await DB.get_user_by_id(user_id)
        if not user:
            return None

        # Проверяем не было ли уже сегодня
        async with async_session_maker() as s:
            today = datetime.utcnow().replace(hour=0, minute=0, second=0)
            existing = await s.execute(
                select(DailyMatch).where(and_(
                    DailyMatch.user_id == user_id,
                    DailyMatch.created_at >= today
                ))
            )
            if existing.scalar_one_or_none():
                return None

        # Ищем лучшего кандидата
        candidates = await SmartSearch.find_profiles(user, 5)
        if not candidates:
            return None

        best = candidates[0]
        compat = best.get("_compat", {})
        icebreaker = SmartCompatibility.generate_icebreaker(user, best, compat)

        # Сохраняем
        async with async_session_maker() as s:
            dm = DailyMatch(
                user_id=user_id,
                recommended_id=best["id"],
                compatibility_score=compat.get("total", 0),
                reason=icebreaker
            )
            s.add(dm)
            await s.commit()

        return {
            "user": best,
            "compatibility": compat,
            "icebreaker": icebreaker,
        }

    @staticmethod
    async def get_todays_recommendation(user_id: int) -> Optional[Dict]:
        """Получает рекомендацию дня если есть"""
        async with async_session_maker() as s:
            today = datetime.utcnow().replace(hour=0, minute=0, second=0)
            r = await s.execute(
                select(DailyMatch).where(and_(
                    DailyMatch.user_id == user_id,
                    DailyMatch.created_at >= today,
                    DailyMatch.is_seen == False
                ))
            )
            dm = r.scalar_one_or_none()
            if not dm:
                return None

            rec = await DB.get_user_by_id(dm.recommended_id)
            if not rec:
                return None

            return {
                "daily_match_id": dm.id,
                "user": rec,
                "compatibility_score": dm.compatibility_score,
                "icebreaker": dm.reason,
            }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#               PROFILE QUALITY ANALYZER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ProfileAnalyzer:
    """Анализирует качество профиля и даёт рекомендации"""

    @staticmethod
    def analyze(user: Dict) -> Dict:
        score = 0
        tips = []
        sections = {}

        # Фото (30 баллов)
        photos = [p for p in (user.get("photos") or "").split(",") if p]
        if len(photos) >= 3:
            score += 30
            sections["photos"] = 100
        elif len(photos) == 2:
            score += 20
            sections["photos"] = 66
            tips.append("📸 Добавь ещё 1 фото — профили с 3+ фото получают в 2 раза больше лайков!")
        elif len(photos) == 1:
            score += 10
            sections["photos"] = 33
            tips.append("📸 Добавь ещё 2 фото! Покажи своё хобби и как проводишь время")
        else:
            sections["photos"] = 0
            tips.append("📸 Добавь фото! Без фото профиль получает в 10 раз меньше лайков")

        # Описание (20 баллов)
        bio = user.get("bio") or ""
        if len(bio) > 100:
            score += 20
            sections["bio"] = 100
        elif len(bio) > 30:
            score += 12
            sections["bio"] = 60
            tips.append("📝 Расширь описание — расскажи о хобби и чём мечтаешь!")
        elif len(bio) > 0:
            score += 5
            sections["bio"] = 25
            tips.append("📝 Описание слишком короткое. Напиши хотя бы 3-4 предложения!")
        else:
            sections["bio"] = 0
            tips.append("📝 Добавь описание! Это увеличит лайки на 40%")

        # Интересы (15 баллов)
        interests = [i for i in (user.get("interests") or "").split(",") if i]
        if len(interests) >= 3:
            score += 15
            sections["interests"] = 100
        elif len(interests) > 0:
            score += 8
            sections["interests"] = 50
            tips.append("🎯 Добавь больше интересов — так мы лучше подберём пару!")
        else:
            sections["interests"] = 0
            tips.append("🎯 Укажи свои интересы — это ключ к хорошей совместимости!")

        # Цель отношений (10 баллов)
        if user.get("relationship_goal"):
            score += 10
            sections["goal"] = 100
        else:
            sections["goal"] = 0
            tips.append("💍 Укажи цель знакомства — найдём людей с такими же планами!")

        # Доп. инфо (10 баллов)
        extras = 0
        if user.get("height"): extras += 1
        if user.get("zodiac"): extras += 1
        if user.get("drinking"): extras += 1
        if user.get("smoking"): extras += 1
        if user.get("job"): extras += 1

        extra_score = min(extras * 2, 10)
        score += extra_score
        sections["extras"] = extras * 20

        if extras < 3:
            tips.append("📋 Заполни доп. информацию (рост, знак зодиака, работа) — +15% к мэтчам!")

        # Верификация (5 баллов)
        if user.get("is_verified"):
            score += 5
            sections["verified"] = 100
        else:
            sections["verified"] = 0

        # Активность (10 баллов)
        la = user.get("last_active_at")
        if la:
            hours = (datetime.utcnow() - la).total_seconds() / 3600
            if hours < 24:
                score += 10
                sections["activity"] = 100
            elif hours < 72:
                score += 5
                sections["activity"] = 50
            else:
                sections["activity"] = 10
                tips.append("⚡ Заходи чаще — активные профили получают больше просмотров!")

        return {
            "score": min(score, 100),
            "sections": sections,
            "tips": tips[:3],  # Максимум 3 совета
            "grade": ProfileAnalyzer._get_grade(score),
        }

    @staticmethod
    def _get_grade(score: int) -> str:
        if score >= 90: return "🏆 Великолепно!"
        if score >= 75: return "🌟 Отлично!"
        if score >= 60: return "👍 Хорошо"
        if score >= 40: return "📈 Можно лучше"
        return "⚠️ Нужно доработать"

    @staticmethod
    def format_analysis(analysis: Dict) -> str:
        score = analysis["score"]
        grade = analysis["grade"]

        # Прогресс-бар
        filled = int(score / 10)
        bar = "🟢" * filled + "⚪" * (10 - filled)

        txt = f"📊 *Качество профиля: {score}%*\n{bar}\n{grade}\n"

        if analysis["tips"]:
            txt += "\n💡 *Советы для улучшения:*\n"
            for tip in analysis["tips"]:
                txt += f"  {tip}\n"

        return txt


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    ANIMATIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class Anim:
    @staticmethod
    async def animate(message: Message, frames: List[str], delay: float = 0.4) -> Message:
        msg = await message.answer(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(delay)
            try:
                await msg.edit_text(frame, parse_mode=ParseMode.MARKDOWN)
            except:
                pass
        return msg

    @staticmethod
    async def match_celebration(message: Message, name: str, compat: float, icebreaker: str) -> None:
        frames = [
            "💕",
            "💕✨💕",
            "🎉💕✨💕🎉",
            f"🍷✨ *МЭТЧ!* ✨🍷",
        ]
        msg = await message.answer(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(0.5)
            try:
                await msg.edit_text(frame, parse_mode=ParseMode.MARKDOWN)
            except:
                pass

        await asyncio.sleep(0.5)
        bar = SmartCompatibility.compatibility_bar(compat, detailed=True)
        final = (
            f"🍷✨ *Взаимная симпатия с {name}!* ✨🍷\n\n"
            f"📊 Совместимость: {bar}\n\n"
            f"{icebreaker}\n\n"
            f"_Напишите друг другу!_ 💬"
        )
        try:
            await msg.edit_text(final, parse_mode=ParseMode.MARKDOWN)
        except:
            await message.answer(final, parse_mode=ParseMode.MARKDOWN)

    @staticmethod
    async def boost_animation(message: Message) -> Message:
        frames = ["🚀", "🚀✨", "🚀✨🍷", "🚀✨🍷 *Буст!*"]
        msg = await message.answer(frames[0])
        for f in frames[1:]:
            await asyncio.sleep(0.4)
            try: await msg.edit_text(f, parse_mode=ParseMode.MARKDOWN)
            except: pass
        return msg

    @staticmethod
    async def payment_success(message: Message) -> Message:
        frames = ["💳", "💳✅", "💳✅🎉", "🍷 *Оплачено!* 🎉"]
        msg = await message.answer(frames[0])
        for f in frames[1:]:
            await asyncio.sleep(0.4)
            try: await msg.edit_text(f, parse_mode=ParseMode.MARKDOWN)
            except: pass
        return msg

    @staticmethod
    def get_wine_emoji() -> str:
        return random.choice(["🍷", "🥂", "🍇", "🍾", "🏆"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    DB SERVICE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TIER_NAMES = {
    "free": "🆓 Бесплатный",
    "wine_glass": "🥂 Бокал Вина",
    "wine_bottle": "🍾 Бутылка Вина",
    "sommelier": "🎖️ Сомелье",
    "wine_cellar": "🏆 Винный Погреб",
}


class DB:
    @staticmethod
    def _to_dict(u: User) -> Dict:
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
            "children": u.children, "education": u.education,
            "job": u.job, "relationship_goal": u.relationship_goal,
            "languages": u.languages or "",
            "is_active": u.is_active, "is_banned": u.is_banned,
            "is_verified": u.is_verified, "is_profile_complete": u.is_profile_complete,
            "subscription_tier": u.subscription_tier.value if u.subscription_tier else "free",
            "subscription_expires_at": u.subscription_expires_at,
            "daily_likes_remaining": u.daily_likes_remaining or 30,
            "daily_messages_remaining": u.daily_messages_remaining or 10,
            "daily_super_likes_remaining": u.daily_super_likes_remaining or 1,
            "last_limits_reset": u.last_limits_reset,
            "boost_expires_at": u.boost_expires_at,
            "boost_count": u.boost_count or 0,
            "views_count": u.views_count or 0,
            "likes_received_count": u.likes_received_count or 0,
            "likes_given_count": u.likes_given_count or 0,
            "matches_count": u.matches_count or 0,
            "messages_sent_count": u.messages_sent_count or 0,
            "elo_score": u.elo_score or 1000,
            "attractiveness_score": u.attractiveness_score or 50,
            "selectivity_score": u.selectivity_score or 50,
            "response_rate": u.response_rate or 0,
            "profile_quality_score": u.profile_quality_score or 0,
            "notify_likes": u.notify_likes if u.notify_likes is not None else True,
            "notify_matches": u.notify_matches if u.notify_matches is not None else True,
            "notify_messages": u.notify_messages if u.notify_messages is not None else True,
            "notify_guests": u.notify_guests if u.notify_guests is not None else True,
            "notify_reminders": u.notify_reminders if u.notify_reminders is not None else True,
            "notify_sub_events": u.notify_sub_events if u.notify_sub_events is not None else True,
            "quiet_hours_start": u.quiet_hours_start,
            "quiet_hours_end": u.quiet_hours_end,
            "last_notified_at": u.last_notified_at,
            "created_at": u.created_at, "last_active_at": u.last_active_at,
        }

    @staticmethod
    def is_vip(u: Dict) -> bool:
        t = u.get("subscription_tier", "free")
        if t == "wine_cellar": return True
        if t == "free": return False
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
        if DB.is_creator(u): return "👑🍷 "
        t = u.get("subscription_tier", "free")
        if t == "wine_cellar": return "🏆 "
        if t == "sommelier": return "🎖️ "
        if DB.is_vip(u): return "🍷 "
        if u.get("is_verified"): return "✅ "
        return ""

    @staticmethod
    def get_role_tag(u: Dict) -> str:
        if DB.is_creator(u): return " · 👑"
        if DB.is_admin(u): return " · 🛡️"
        return ""

    @staticmethod
    def get_tier_limits(tier: str) -> Dict:
        return {
            "free": {"likes": 30, "messages": 10, "guests": 3, "super_likes": 1, "boosts": 0},
            "wine_glass": {"likes": 100, "messages": 999999, "guests": 10, "super_likes": 3, "boosts": 0},
            "wine_bottle": {"likes": 999999, "messages": 999999, "guests": 999, "super_likes": 5, "boosts": 1},
            "sommelier": {"likes": 999999, "messages": 999999, "guests": 999, "super_likes": 10, "boosts": 3},
            "wine_cellar": {"likes": 999999, "messages": 999999, "guests": 999, "super_likes": 15, "boosts": 5},
        }.get(tier, {"likes": 30, "messages": 10, "guests": 3, "super_likes": 1, "boosts": 0})

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
                telegram_id=tg_id, username=username,
                referral_code=str(uuid.uuid4())[:8].upper(),
                last_limits_reset=datetime.utcnow()
            )
            s.add(u); await s.commit(); await s.refresh(u)
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
            limits = DB.get_tier_limits(u.get("subscription_tier", "free"))
            return await DB.update_user(u["telegram_id"],
                daily_likes_remaining=limits["likes"],
                daily_messages_remaining=limits["messages"],
                daily_super_likes_remaining=limits["super_likes"],
                last_limits_reset=now, last_active_at=now)
        await DB.update_user(u["telegram_id"], last_active_at=now)
        return u

    @staticmethod
    async def add_like(fid: int, tid: int, is_super: bool = False) -> Dict:
        async with async_session_maker() as s:
            ex = await s.execute(select(Like).where(and_(Like.from_user_id == fid, Like.to_user_id == tid)))
            if ex.scalar_one_or_none():
                return {"is_match": False, "match_id": None, "compatibility": {}, "icebreaker": ""}

            s.add(Like(from_user_id=fid, to_user_id=tid, is_super_like=is_super))
            await s.execute(update(User).where(User.id == tid).values(
                likes_received_count=User.likes_received_count + 1))

            rev = await s.execute(select(Like).where(and_(Like.from_user_id == tid, Like.to_user_id == fid)))
            is_match = rev.scalar_one_or_none() is not None

            match_id = None
            compat = {}
            icebreaker = ""

            if is_match:
                u1r = await s.execute(select(User).where(User.id == fid))
                u2r = await s.execute(select(User).where(User.id == tid))
                u1, u2 = u1r.scalar_one_or_none(), u2r.scalar_one_or_none()
                if u1 and u2:
                    d1, d2 = DB._to_dict(u1), DB._to_dict(u2)
                    learned = await PreferenceLearner.get_preferences(fid)
                    compat = SmartCompatibility.calculate(d1, d2, learned)
                    icebreaker = SmartCompatibility.generate_icebreaker(d1, d2, compat)

                import json
                m = Match(
                    user1_id=fid, user2_id=tid,
                    compatibility_score=compat.get("total", 0),
                    compatibility_details=json.dumps(compat.get("details", {})),
                    icebreaker_text=icebreaker
                )
                s.add(m)
                await s.execute(update(User).where(User.id.in_([fid, tid])).values(
                    matches_count=User.matches_count + 1))
                await s.flush()
                match_id = m.id

            await s.commit()

        # ELO update (async)
        asyncio.create_task(EloSystem.process_like(fid, tid))
        asyncio.create_task(PreferenceLearner.record_interaction(fid, tid, "like"))

        return {
            "is_match": is_match, "match_id": match_id,
            "compatibility": compat, "icebreaker": icebreaker,
            "is_super": is_super
        }

    @staticmethod
    async def add_dislike(fid: int, tid: int):
        async with async_session_maker() as s:
            s.add(Dislike(from_user_id=fid, to_user_id=tid))
            await s.commit()
        asyncio.create_task(EloSystem.process_dislike(fid, tid))
        asyncio.create_task(PreferenceLearner.record_interaction(fid, tid, "dislike"))

    @staticmethod
    async def get_matches(uid: int) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(
                or_(Match.user1_id == uid, Match.user2_id == uid),
                Match.is_active == True
            )).order_by(Match.last_message_at.desc().nullslast()))
            out = []
            for m in r.scalars().all():
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                pr = await s.execute(select(User).where(User.id == pid))
                p = pr.scalar_one_or_none()
                if p:
                    unread = (await s.execute(select(func.count(ChatMessage.id)).where(and_(
                        ChatMessage.match_id == m.id, ChatMessage.sender_id != uid,
                        ChatMessage.is_read == False
                    )))).scalar() or 0
                    out.append({
                        "match_id": m.id, "user_id": p.id,
                        "telegram_id": p.telegram_id, "name": p.name,
                        "age": p.age, "photo": p.main_photo,
                        "compatibility": m.compatibility_score or 0,
                        "icebreaker": m.icebreaker_text,
                        "unread": unread,
                        "messages_count": m.messages_count or 0,
                    })
            return out

    @staticmethod
    async def get_match_between(u1: int, u2: int) -> Optional[int]:
        async with async_session_maker() as s:
            r = await s.execute(select(Match.id).where(and_(
                Match.is_active == True,
                or_(and_(Match.user1_id == u1, Match.user2_id == u2),
                    and_(Match.user1_id == u2, Match.user2_id == u1)))))
            row = r.first()
            return row[0] if row else None

    @staticmethod
    async def send_msg(mid: int, sid: int, txt: str):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=txt))
            await s.execute(update(Match).where(Match.id == mid).values(
                last_message_at=datetime.utcnow(),
                messages_count=Match.messages_count + 1))
            await s.execute(update(User).where(User.id == sid).values(
                messages_sent_count=User.messages_sent_count + 1))
            await s.commit()

    @staticmethod
    async def get_msgs(mid: int, limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(ChatMessage).where(ChatMessage.match_id == mid)
                .order_by(ChatMessage.created_at.desc()).limit(limit))
            return [{"sender_id": m.sender_id, "text": m.text, "created_at": m.created_at}
                    for m in reversed(r.scalars().all())]

    @staticmethod
    async def mark_messages_read(mid: int, reader_id: int):
        async with async_session_maker() as s:
            await s.execute(update(ChatMessage).where(and_(
                ChatMessage.match_id == mid, ChatMessage.sender_id != reader_id,
                ChatMessage.is_read == False)).values(is_read=True))
            await s.commit()

    @staticmethod
    async def get_unread(uid: int) -> int:
        async with async_session_maker() as s:
            ms = await s.execute(select(Match.id).where(
                or_(Match.user1_id == uid, Match.user2_id == uid)))
            mids = [m[0] for m in ms.fetchall()]
            if not mids: return 0
            r = await s.execute(select(func.count(ChatMessage.id)).where(and_(
                ChatMessage.match_id.in_(mids), ChatMessage.sender_id != uid,
                ChatMessage.is_read == False)))
            return r.scalar() or 0

    @staticmethod
    async def add_guest(vid: int, uid: int):
        if vid == uid: return
        async with async_session_maker() as s:
            s.add(GuestVisit(visitor_id=vid, visited_user_id=uid))
            await s.execute(update(User).where(User.id == uid).values(
                views_count=User.views_count + 1))
            await s.commit()

    @staticmethod
    async def get_guests(uid: int, limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(GuestVisit.visitor_id, func.max(GuestVisit.created_at).label("lv"))
                .where(GuestVisit.visited_user_id == uid)
                .group_by(GuestVisit.visitor_id).order_by(desc("lv")).limit(limit))
            rows = r.fetchall()
            if not rows: return []
            ids = [row[0] for row in rows]
            us = await s.execute(select(User).where(User.id.in_(ids)))
            um = {u.id: DB._to_dict(u) for u in us.scalars().all()}
            return [um[uid] for uid in ids if uid in um]

    @staticmethod
    async def dec_likes(tg_id: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(
                daily_likes_remaining=User.daily_likes_remaining - 1))
            await s.commit()

    @staticmethod
    async def dec_super_likes(tg_id: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(
                daily_super_likes_remaining=User.daily_super_likes_remaining - 1))
            await s.commit()

    @staticmethod
    async def use_boost(uid: int) -> bool:
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u or (u.boost_count or 0) <= 0: return False
            now = datetime.utcnow()
            ne = (u.boost_expires_at + timedelta(hours=24)
                  if u.boost_expires_at and u.boost_expires_at > now
                  else now + timedelta(hours=24))
            await s.execute(update(User).where(User.id == uid).values(
                boost_count=User.boost_count - 1, boost_expires_at=ne))
            await s.commit()
            return True

    @staticmethod
    async def add_boosts(uid: int, c: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.id == uid).values(
                boost_count=User.boost_count + c))
            await s.commit()

    @staticmethod
    async def unmatch(uid: int, match_id: int) -> bool:
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(
                Match.id == match_id, Match.is_active == True,
                or_(Match.user1_id == uid, Match.user2_id == uid))))
            m = r.scalar_one_or_none()
            if not m: return False
            await s.execute(update(Match).where(Match.id == match_id).values(is_active=False))
            await s.execute(update(User).where(User.id.in_([m.user1_id, m.user2_id])).values(
                matches_count=func.greatest(User.matches_count - 1, 0)))
            await s.commit()
            return True

    @staticmethod
    async def create_report(rid: int, ruid: int, reason: str):
        async with async_session_maker() as s:
            s.add(Report(reporter_id=rid, reported_user_id=ruid, reason=reason))
            await s.commit()

    @staticmethod
    async def get_total_users() -> int:
        async with async_session_maker() as s:
            r = await s.execute(select(func.count(User.id)).where(User.is_profile_complete == True))
            return r.scalar() or 0

    @staticmethod
    async def get_question_of_day() -> Optional[Dict]:
        async with async_session_maker() as s:
            today = datetime.utcnow().replace(hour=0, minute=0, second=0)
            r = await s.execute(select(QuestionOfDay).where(and_(
                QuestionOfDay.is_active == True,
                or_(QuestionOfDay.used_date == None, QuestionOfDay.used_date >= today)
            )).order_by(func.random()).limit(1))
            q = r.scalar_one_or_none()
            if q:
                if not q.used_date or q.used_date < today:
                    await s.execute(update(QuestionOfDay).where(QuestionOfDay.id == q.id).values(used_date=today))
                    await s.commit()
                return {"id": q.id, "text": q.question_text, "category": q.category}
            return None

    @staticmethod
    async def answer_question(user_id: int, question_id: int, answer: str):
        async with async_session_maker() as s:
            s.add(UserAnswer(user_id=user_id, question_id=question_id, answer_text=answer))
            await s.commit()

    # ... (остальные методы DB аналогичны v4.1 — payments, promos, admin и т.д.)
    # Для краткости опущены — они идентичны предыдущей версии


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                    KEYBOARDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class KB:
    @staticmethod
    def main(unread: int = 0, has_daily: bool = False):
        chats = f"💬 Чаты ({unread})" if unread > 0 else "💬 Чаты"
        rows = [
            [KeyboardButton(text="🍷 Анкеты"), KeyboardButton(text="💕 Симпатии")],
            [KeyboardButton(text=chats), KeyboardButton(text="👻 Гости")],
            [KeyboardButton(text="🛒 Винная Карта"), KeyboardButton(text="👤 Профиль")],
        ]
        if has_daily:
            rows.append([KeyboardButton(text="🌟 Пара дня"), KeyboardButton(text="❓ Вопрос дня")])
        else:
            rows.append([KeyboardButton(text="❓ Вопрос дня"), KeyboardButton(text="🔔 Уведомления")])
        rows.append([KeyboardButton(text="❓ FAQ")])
        return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

    @staticmethod
    def search_card(uid: int, compat: float = 0, can_super: bool = False):
        compat_text = f" ({compat:.0f}%)" if compat > 0 else ""
        rows = [
            [InlineKeyboardButton(text=f"❤️ Нравится{compat_text}", callback_data=f"lk:{uid}"),
             InlineKeyboardButton(text="👎 Дальше", callback_data=f"dl:{uid}")],
        ]
        if can_super:
            rows.insert(0, [InlineKeyboardButton(text="⚡ Суперлайк!", callback_data=f"sl:{uid}")])
        rows.append([InlineKeyboardButton(text="⚠️ Жалоба", callback_data=f"rp:{uid}")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def daily_match(uid: int, compat: float):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"❤️ Нравится ({compat:.0f}%)", callback_data=f"dm_lk:{uid}")],
            [InlineKeyboardButton(text="⚡ Суперлайк!", callback_data=f"dm_sl:{uid}")],
            [InlineKeyboardButton(text="👎 Не сегодня", callback_data=f"dm_skip:{uid}")],
        ])

    @staticmethod
    def interests_picker(selected: Set[str] = None, page: int = 0):
        if selected is None:
            selected = set()
        per_page = 8
        start = page * per_page
        items = INTERESTS_LIST[start:start + per_page]

        rows = []
        for i in range(0, len(items), 2):
            row = []
            for item in items[i:i + 2]:
                check = "✅ " if item in selected else ""
                row.append(InlineKeyboardButton(
                    text=f"{check}{item}",
                    callback_data=f"int:{item}"
                ))
            rows.append(row)

        # Навигация
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"int_page:{page - 1}"))
        if start + per_page < len(INTERESTS_LIST):
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"int_page:{page + 1}"))
        if nav:
            rows.append(nav)

        rows.append([InlineKeyboardButton(
            text=f"✅ Готово ({len(selected)} выбрано)",
            callback_data="int_done"
        )])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def relationship_goals():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💍 Серьёзные отношения", callback_data="goal:serious")],
            [InlineKeyboardButton(text="🌸 Лёгкие отношения", callback_data="goal:casual")],
            [InlineKeyboardButton(text="🤝 Дружба", callback_data="goal:friends")],
            [InlineKeyboardButton(text="🤷 Пока не знаю", callback_data="goal:not_sure")],
        ])

    @staticmethod
    def zodiac_picker():
        rows = []
        for i in range(0, len(ZODIAC_SIGNS), 3):
            row = [InlineKeyboardButton(text=z, callback_data=f"zod:{z}")
                   for z in ZODIAC_SIGNS[i:i + 3]]
            rows.append(row)
        rows.append([InlineKeyboardButton(text="⏩ Пропустить", callback_data="zod:skip")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def drinking_picker():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=v, callback_data=f"drink:{k}")]
            for k, v in DRINKING_OPTIONS.items()
        ] + [[InlineKeyboardButton(text="⏩ Пропустить", callback_data="drink:skip")]])

    @staticmethod
    def matches(ms: List[Dict]):
        b = []
        for m in ms[:10]:
            unread = f" 🔴{m['unread']}" if m.get('unread', 0) > 0 else ""
            compat = f" 💕{m['compatibility']:.0f}%" if m.get('compatibility', 0) > 0 else ""
            new = " 🆕" if m.get('messages_count', 0) == 0 else ""
            b.append([InlineKeyboardButton(
                text=f"💬 {m['name']}, {m['age']}{compat}{unread}{new}",
                callback_data=f"ch:{m['user_id']}"
            )])
        b.append([InlineKeyboardButton(text="🍷 Меню", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def chat_actions(match_id: int, icebreaker: str = None):
        rows = []
        if icebreaker:
            rows.append([InlineKeyboardButton(text="💡 Подсказка", callback_data=f"ib:{match_id}")])
        rows.append([InlineKeyboardButton(text="💕 Симпатии", callback_data="bm")])
        rows.append([InlineKeyboardButton(text="💔 Отвязать", callback_data=f"um:{match_id}")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def profile_detailed(user: Dict):
        rows = [
            [InlineKeyboardButton(text="✏️ Основное", callback_data="pe"),
             InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")],
            [InlineKeyboardButton(text="🎯 Интересы", callback_data="ed:interests"),
             InlineKeyboardButton(text="💍 Цель", callback_data="ed:goal")],
            [InlineKeyboardButton(text="📏 Рост", callback_data="ed:height"),
             InlineKeyboardButton(text="💼 Работа", callback_data="ed:job")],
            [InlineKeyboardButton(text="📊 Анализ профиля", callback_data="profile_analysis")],
            [InlineKeyboardButton(text="🚀 Буст", callback_data="sh:boost")],
        ]
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def compat_detail_btn(uid: int):
        """Кнопка для VIP — детали совместимости"""
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Детали совместимости", callback_data=f"cd:{uid}")],
        ])

    # ... (остальные KB методы из v4.1 — shop, admin и т.д.)
    @staticmethod
    def skip():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏩ Пропустить", callback_data="skip")]])

    @staticmethod
    def gender():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужской", callback_data="g:male"),
             InlineKeyboardButton(text="👩 Женский", callback_data="g:female")]])

    @staticmethod
    def looking():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужчин", callback_data="l:male"),
             InlineKeyboardButton(text="👩 Женщин", callback_data="l:female")],
            [InlineKeyboardButton(text="👫 Всех", callback_data="l:both")]])

    @staticmethod
    def back_matches():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💕 К симпатиям", callback_data="bm")]])

    @staticmethod
    def confirm_unmatch(mid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да", callback_data=f"um_yes:{mid}"),
             InlineKeyboardButton(text="❌ Нет", callback_data="bm")]])

    @staticmethod
    def shop():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Подписки", callback_data="sh:subs")],
            [InlineKeyboardButton(text="🚀 Буст", callback_data="sh:boost")],
            [InlineKeyboardButton(text="📊 Сравнить", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🎁 Промокод", callback_data="sh:promo")],
            [InlineKeyboardButton(text="🍷 Меню", callback_data="mn")]])

    @staticmethod
    def report_reasons():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Спам", callback_data="rr:spam"),
             InlineKeyboardButton(text="🎭 Фейк", callback_data="rr:fake")],
            [InlineKeyboardButton(text="🔞 18+", callback_data="rr:nsfw"),
             InlineKeyboardButton(text="😡 Оскорбления", callback_data="rr:harass")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="mn")]])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                     MIDDLEWARE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
                        await event.answer("🚫 Аккаунт заблокирован.")
                    return
        data["user"] = u
        return await handler(event, data)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                     HANDLERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

rt = Router()


# ═══ START ═══

@rt.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if user and user.get("is_profile_complete"):
        un = await DB.get_unread(user["id"])
        st = TIER_NAMES.get(user["subscription_tier"], "🆓")
        if DB.is_boosted(user): st += " · 🚀"
        st += DB.get_role_tag(user)

        # Проверяем пару дня
        daily = await DailyRecommendations.get_todays_recommendation(user["id"])
        has_daily = daily is not None

        # Качество профиля
        analysis = ProfileAnalyzer.analyze(user)
        quality_hint = ""
        if analysis["score"] < 60:
            quality_hint = f"\n\n💡 _Профиль: {analysis['score']}% — {analysis['tips'][0] if analysis['tips'] else 'улучши!'}_"

        await message.answer(
            f"🍷 *С возвращением, {user['name']}!* 🥂\n\n"
            f"{st}\n"
            f"📊 👁️ {user['views_count']} · ❤️ {user['likes_received_count']} · "
            f"💕 {user['matches_count']} · ✉️ {un}\n"
            f"⚖️ Рейтинг: {user.get('elo_score', 1000)}"
            f"{quality_hint}",
            reply_markup=KB.main(un, has_daily),
            parse_mode=ParseMode.MARKDOWN
        )

        if has_daily:
            await message.answer(
                "🌟 У тебя есть *Пара дня*! Нажми кнопку чтобы посмотреть!",
                parse_mode=ParseMode.MARKDOWN
            )
    else:
        if not user:
            await DB.create_user(message.from_user.id, message.from_user.username)
        await Anim.animate(message, ["🍷", "🍷🥂", "🍷🥂✨", "✨ Добро пожаловать! ✨"], 0.5)
        await asyncio.sleep(0.3)
        total = await DB.get_total_users()
        await message.answer(
            f"🍷 *Добро пожаловать в {BOT_NAME}!*\n\n"
            f"👥 Уже *{total}* человек ищут знакомства!\n\n"
            f"Создадим твою анкету за 2 минуты 📝",
            parse_mode=ParseMode.MARKDOWN
        )
        await message.answer("✏️ Как тебя зовут?", reply_markup=ReplyKeyboardRemove())
        await state.set_state(RegStates.name)


# ═══ REGISTRATION (расширенная) ═══

@rt.message(RegStates.name)
async def reg_name(msg: Message, state: FSMContext):
    n = msg.text.strip()
    if len(n) < 2 or len(n) > 50: await msg.answer("⚠️ 2-50 символов:"); return
    await state.update_data(name=n)
    await msg.answer(f"Привет, *{n}*! 🍷\n\n🎂 Сколько лет? _(18-99)_", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.age)

@rt.message(RegStates.age)
async def reg_age(msg: Message, state: FSMContext):
    try:
        a = int(msg.text.strip())
        if not 18 <= a <= 99: raise ValueError
    except: await msg.answer("⚠️ 18-99:"); return
    await state.update_data(age=a)
    await msg.answer("🚻 Твой пол:", reply_markup=KB.gender())
    await state.set_state(RegStates.gender)

@rt.callback_query(RegStates.gender, F.data.startswith("g:"))
async def reg_gender(cb: CallbackQuery, state: FSMContext):
    await state.update_data(gender=cb.data[2:])
    await cb.message.edit_text("✅")
    await cb.message.answer("🏙️ Твой город:")
    await state.set_state(RegStates.city)
    await cb.answer()

@rt.message(RegStates.city)
async def reg_city(msg: Message, state: FSMContext):
    c = msg.text.strip().title()
    if len(c) < 2: await msg.answer("🏙️ Город:"); return
    await state.update_data(city=c)
    await msg.answer("📸 Фото или «Пропустить»:", reply_markup=KB.skip())
    await state.set_state(RegStates.photo)

@rt.message(RegStates.photo, F.photo)
async def reg_photo(msg: Message, state: FSMContext):
    await state.update_data(photo=msg.photo[-1].file_id)
    await msg.answer("📝 О себе _(до 500)_ или «Пропустить»:", reply_markup=KB.skip(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.bio)

@rt.callback_query(RegStates.photo, F.data == "skip")
async def reg_skip_photo(cb: CallbackQuery, state: FSMContext):
    await state.update_data(photo=None)
    await cb.message.edit_text("📸 Пропущено")
    await cb.message.answer("📝 О себе или «Пропустить»:", reply_markup=KB.skip())
    await state.set_state(RegStates.bio)
    await cb.answer()

@rt.message(RegStates.bio)
async def reg_bio(msg: Message, state: FSMContext):
    await state.update_data(bio=msg.text.strip()[:500])
    await msg.answer("🔍 Кого ищешь?", reply_markup=KB.looking())
    await state.set_state(RegStates.looking_for)

@rt.callback_query(RegStates.bio, F.data == "skip")
async def reg_skip_bio(cb: CallbackQuery, state: FSMContext):
    await state.update_data(bio="")
    await cb.message.edit_text("🔍 Кого ищешь?", reply_markup=KB.looking())
    await state.set_state(RegStates.looking_for)
    await cb.answer()

@rt.callback_query(RegStates.looking_for, F.data.startswith("l:"))
async def reg_looking(cb: CallbackQuery, state: FSMContext):
    await state.update_data(looking_for=cb.data[2:])
    await cb.message.edit_text("✅")
    # Переходим к расширенным полям
    await cb.message.answer(
        "🎯 *Выбери свои интересы:*\n\n_Это поможет найти людей с похожими увлечениями!_",
        reply_markup=KB.interests_picker(),
        parse_mode=ParseMode.MARKDOWN
    )
    await state.update_data(selected_interests=set())
    await state.set_state(RegStates.interests)
    await cb.answer()


# Интересы — пагинация и выбор
@rt.callback_query(RegStates.interests, F.data.startswith("int:"))
async def reg_interest_toggle(cb: CallbackQuery, state: FSMContext):
    interest = cb.data[4:]
    d = await state.get_data()
    selected = d.get("selected_interests", set())
    if isinstance(selected, list):
        selected = set(selected)
    if interest in selected:
        selected.discard(interest)
    else:
        selected.add(interest)
    await state.update_data(selected_interests=list(selected))
    page = d.get("interests_page", 0)
    await cb.message.edit_reply_markup(reply_markup=KB.interests_picker(selected, page))
    await cb.answer(f"{'✅' if interest in selected else '❌'} {interest}")

@rt.callback_query(RegStates.interests, F.data.startswith("int_page:"))
async def reg_interest_page(cb: CallbackQuery, state: FSMContext):
    page = int(cb.data.split(":")[1])
    d = await state.get_data()
    selected = set(d.get("selected_interests", []))
    await state.update_data(interests_page=page)
    await cb.message.edit_reply_markup(reply_markup=KB.interests_picker(selected, page))
    await cb.answer()

@rt.callback_query(RegStates.interests, F.data == "int_done")
async def reg_interests_done(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    selected = d.get("selected_interests", [])
    await state.update_data(interests=",".join(selected))
    await cb.message.edit_text(f"🎯 Выбрано интересов: {len(selected)} ✅")

    await cb.message.answer(
        "💍 *Какие отношения ищешь?*",
        reply_markup=KB.relationship_goals(),
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(RegStates.relationship_goal)
    await cb.answer()


@rt.callback_query(RegStates.relationship_goal, F.data.startswith("goal:"))
async def reg_goal(cb: CallbackQuery, state: FSMContext):
    goal = cb.data[5:]
    await state.update_data(relationship_goal=goal)
    await cb.message.edit_text(f"💍 {RELATIONSHIP_GOALS.get(goal, goal)} ✅")

    await cb.message.answer(
        "🔮 *Твой знак зодиака?*",
        reply_markup=KB.zodiac_picker()
    )
    await state.set_state(RegStates.zodiac)
    await cb.answer()


@rt.callback_query(RegStates.zodiac, F.data.startswith("zod:"))
async def reg_zodiac(cb: CallbackQuery, state: FSMContext):
    z = cb.data[4:]
    if z == "skip":
        await state.update_data(zodiac=None)
    else:
        await state.update_data(zodiac=z)
    await cb.message.edit_text(f"🔮 {z if z != 'skip' else 'Пропущено'} ✅")

    await cb.message.answer(
        "🍷 *Отношение к алкоголю?*",
        reply_markup=KB.drinking_picker()
    )
    await state.set_state(RegStates.drinking)
    await cb.answer()


@rt.callback_query(RegStates.drinking, F.data.startswith("drink:"))
async def reg_drinking(cb: CallbackQuery, state: FSMContext):
    d_val = cb.data[6:]
    if d_val == "skip":
        await state.update_data(drinking=None)
    else:
        await state.update_data(drinking=d_val)

    # Финализация
    data = await state.get_data()
    upd = {
        "name": data["name"], "age": data["age"],
        "gender": Gender(data["gender"]), "city": data["city"],
        "bio": data.get("bio", ""),
        "looking_for": LookingFor(data["looking_for"]),
        "interests": data.get("interests", ""),
        "relationship_goal": data.get("relationship_goal"),
        "zodiac": data.get("zodiac"),
        "drinking": data.get("drinking"),
        "is_profile_complete": True,
    }
    if data.get("photo"):
        upd["photos"] = data["photo"]
        upd["main_photo"] = data["photo"]

    await DB.update_user(cb.from_user.id, **upd)
    await state.clear()

    # Анимация
    await cb.message.edit_text("⏳ Создаём анкету...")
    await asyncio.sleep(0.5)
    await cb.message.edit_text("🧠 Настраиваем умный подбор...")
    await asyncio.sleep(0.5)
    await cb.message.edit_text("✨ Анализируем совместимость...")
    await asyncio.sleep(0.5)
    await cb.message.edit_text("🎉 *Анкета готова!*", parse_mode=ParseMode.MARKDOWN)

    # Анализ профиля
    user = await DB.get_user(cb.from_user.id)
    analysis = ProfileAnalyzer.analyze(user)

    total = await DB.get_total_users()
    await cb.message.answer(
        f"🍷 *Добро пожаловать!*\n\n"
        f"👥 Среди *{total}* человек мы найдём тебе пару!\n\n"
        f"{ProfileAnalyzer.format_analysis(analysis)}\n"
        f"Жми «🍷 Анкеты» чтобы начать!",
        reply_markup=KB.main(),
        parse_mode=ParseMode.MARKDOWN
    )
    await cb.answer()


# ═══ BROWSE (Smart Search) ═══

@rt.message(F.text == "🍷 Анкеты")
async def browse(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer("📝 Заполни профиль → /start"); return
    await state.clear()
    msg = await message.answer("🧠 Умный поиск...")
    await asyncio.sleep(0.5)
    ps = await SmartSearch.find_profiles(user, 1)
    if not ps:
        await msg.edit_text("😔 *Анкеты закончились.* Попробуй позже!", parse_mode=ParseMode.MARKDOWN)
        return
    await msg.delete()
    await show_smart_card(message, ps[0], user)


async def show_smart_card(message: Message, p: Dict, viewer: Dict):
    """Показывает анкету с умной совместимостью"""
    await DB.add_guest(viewer["id"], p["id"])

    compat = p.get("_compat", SmartCompatibility.calculate(viewer, p))
    total = compat.get("total", 0) if isinstance(compat, dict) else compat
    badge = DB.get_badge(p)
    boost = " 🚀" if DB.is_boosted(p) else ""

    # Основной текст
    lm = {"male": "👨", "female": "👩", "both": "👫"}
    goal_txt = RELATIONSHIP_GOALS.get(p.get("relationship_goal"), "")
    zodiac_txt = f"\n🔮 {p['zodiac']}" if p.get("zodiac") else ""
    drinking_txt = f"\n🍷 {DRINKING_OPTIONS.get(p.get('drinking'), '')}" if p.get("drinking") else ""
    height_txt = f"\n📏 {p['height']} см" if p.get("height") else ""
    job_txt = f"\n💼 {p['job']}" if p.get("job") else ""

    interests = [i for i in (p.get("interests") or "").split(",") if i]
    interests_txt = "\n🎯 " + " · ".join(interests[:5]) if interests else ""

    bar = SmartCompatibility.compatibility_bar(total, detailed=True)

    txt = (
        f"{badge}*{p['name']}*{boost}, {p['age']}\n"
        f"🏙️ {p['city']}"
        f"{zodiac_txt}{height_txt}{job_txt}{drinking_txt}\n\n"
        f"{p['bio'] or '_Нет описания_'}"
        f"{interests_txt}\n"
        f"{f'💍 {goal_txt}' if goal_txt else ''}\n\n"
        f"💕 {bar}"
    )

    can_super = viewer.get("daily_super_likes_remaining", 0) > 0
    kb = KB.search_card(p["id"], total, can_super)

    try:
        if p.get("main_photo"):
            await message.answer_photo(photo=p["main_photo"], caption=txt,
                reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        else:
            await message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except:
        await message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


# ═══ LIKE / DISLIKE / SUPER LIKE ═══

@rt.callback_query(F.data.startswith("lk:"))
async def handle_like(cb: CallbackQuery, user: Optional[Dict]):
    if not user: await cb.answer("❗"); return
    if not DB.is_vip(user) and user.get("daily_likes_remaining", 0) <= 0:
        await cb.answer("⚠️ Лимит лайков! Подпишись 🍷", show_alert=True); return

    tid = int(cb.data[3:])
    result = await DB.add_like(user["id"], tid)
    if not DB.is_vip(user): await DB.dec_likes(user["telegram_id"])

    if result["is_match"]:
        target = await DB.get_user_by_id(tid)
        compat = result.get("compatibility", {})
        total = compat.get("total", 0) if isinstance(compat, dict) else 0
        icebreaker = result.get("icebreaker", "")
        tn = target["name"] if target else "?"

        await Anim.match_celebration(cb.message, tn, total, icebreaker)
        await cb.answer("🍷✨ МЭТЧ! 💕")
    else:
        await cb.answer("❤️")

    user = await DB.get_user(cb.from_user.id)
    ps = await SmartSearch.find_profiles(user, 1)
    if ps: await show_smart_card(cb.message, ps[0], user)
    else: await cb.message.answer("😔 Анкеты закончились!", parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("sl:"))
async def handle_super_like(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return
    if user.get("daily_super_likes_remaining", 0) <= 0:
        await cb.answer("⚡ Суперлайки закончились! VIP = больше!", show_alert=True); return

    tid = int(cb.data[3:])
    result = await DB.add_like(user["id"], tid, is_super=True)
    await DB.dec_super_likes(user["telegram_id"])

    if result["is_match"]:
        target = await DB.get_user_by_id(tid)
        compat = result.get("compatibility", {})
        total = compat.get("total", 0) if isinstance(compat, dict) else 0
        icebreaker = result.get("icebreaker", "")
        await Anim.match_celebration(cb.message, target["name"] if target else "?", total, icebreaker)
    else:
        await cb.answer("⚡ Суперлайк отправлен! Они узнают!")

    user = await DB.get_user(cb.from_user.id)
    ps = await SmartSearch.find_profiles(user, 1)
    if ps: await show_smart_card(cb.message, ps[0], user)


@rt.callback_query(F.data.startswith("dl:"))
async def handle_dislike(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return
    tid = int(cb.data[3:])
    await DB.add_dislike(user["id"], tid)
    await cb.answer("👋")

    ps = await SmartSearch.find_profiles(user, 1)
    if ps: await show_smart_card(cb.message, ps[0], user)
    else:
        try: await cb.message.edit_caption(caption="😔 Анкеты закончились!")
        except: await cb.message.answer("😔 Анкеты закончились!")


# ═══ DAILY MATCH ═══

@rt.message(F.text == "🌟 Пара дня")
async def show_daily_match(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer("📝 Заполни профиль → /start"); return

    rec = await DailyRecommendations.get_todays_recommendation(user["id"])
    if not rec:
        # Генерируем новую
        rec_data = await DailyRecommendations.generate_pair_of_day(user["id"])
        if not rec_data:
            await message.answer("🌟 _Пара дня появится завтра!_", parse_mode=ParseMode.MARKDOWN)
            return
        rec_user = rec_data["user"]
        compat = rec_data["compatibility"]
        total = compat.get("total", 0)
        icebreaker = rec_data["icebreaker"]
    else:
        rec_user = rec["user"]
        total = rec["compatibility_score"]
        icebreaker = rec["icebreaker"]

    bar = SmartCompatibility.compatibility_bar(total, detailed=True)
    badge = DB.get_badge(rec_user)

    txt = (
        f"🌟 *ПАРА ДНЯ*\n\n"
        f"{badge}*{rec_user['name']}*, {rec_user['age']}\n"
        f"🏙️ {rec_user['city']}\n\n"
        f"{rec_user['bio'] or '_—_'}\n\n"
        f"💕 {bar}\n\n"
        f"{icebreaker}"
    )

    try:
        if rec_user.get("main_photo"):
            await message.answer_photo(photo=rec_user["main_photo"], caption=txt,
                reply_markup=KB.daily_match(rec_user["id"], total), parse_mode=ParseMode.MARKDOWN)
        else:
            await message.answer(txt, reply_markup=KB.daily_match(rec_user["id"], total),
                parse_mode=ParseMode.MARKDOWN)
    except:
        await message.answer(txt, reply_markup=KB.daily_match(rec_user["id"], total),
            parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("dm_lk:"))
async def daily_match_like(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return
    tid = int(cb.data[6:])
    result = await DB.add_like(user["id"], tid)
    if result["is_match"]:
        target = await DB.get_user_by_id(tid)
        compat = result.get("compatibility", {})
        total = compat.get("total", 0)
        icebreaker = result.get("icebreaker", "")
        await Anim.match_celebration(cb.message, target["name"] if target else "?", total, icebreaker)
    else:
        await cb.answer("❤️ Лайк отправлен! Ждём ответ...")
        try: await cb.message.edit_caption(caption="🌟 *Лайк отправлен!*\n\nЕсли взаимно — будет мэтч!", parse_mode=ParseMode.MARKDOWN)
        except: await cb.message.edit_text("🌟 *Лайк отправлен!*", parse_mode=ParseMode.MARKDOWN)


# ═══ QUESTION OF DAY ═══

@rt.message(F.text == "❓ Вопрос дня")
async def show_question(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    q = await DB.get_question_of_day()
    if not q:
        await message.answer("❓ _Вопрос дня появится скоро!_", parse_mode=ParseMode.MARKDOWN)
        return

    await state.update_data(current_question_id=q["id"])
    await state.set_state(AnswerQuestionState.answering)
    await message.answer(
        f"❓ *Вопрос дня:*\n\n"
        f"_{q['text']}_\n\n"
        f"Напиши свой ответ 👇\n"
        f"_Ответ будет виден в твоём профиле!_",
        parse_mode=ParseMode.MARKDOWN
    )


@rt.message(AnswerQuestionState.answering)
async def answer_question(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d = await state.get_data()
    qid = d.get("current_question_id")
    if qid:
        await DB.answer_question(user["id"], qid, message.text.strip()[:500])
    await state.clear()
    await message.answer("✅ Ответ сохранён!\n\n_Его увидят те, кто смотрит твой профиль_ 🍷",
        reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)


# ═══ PROFILE ANALYSIS ═══

@rt.callback_query(F.data == "profile_analysis")
async def show_profile_analysis(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return
    analysis = ProfileAnalyzer.analyze(user)
    txt = ProfileAnalyzer.format_analysis(analysis)

    txt += f"\n\n📈 *Твой рейтинг:* {user.get('elo_score', 1000)}\n"
    txt += f"💕 Привлекательность: {user.get('attractiveness_score', 50):.0f}%\n"
    txt += f"📨 Отвечаемость: {user.get('response_rate', 0) * 100:.0f}%"

    try:
        await cb.message.edit_caption(caption=txt, parse_mode=ParseMode.MARKDOWN)
    except:
        await cb.message.edit_text(txt, parse_mode=ParseMode.MARKDOWN)
    await cb.answer()


# ═══ COMPATIBILITY DETAILS (VIP) ═══

@rt.callback_query(F.data.startswith("cd:"))
async def show_compat_details(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return
    if not DB.is_vip(user):
        await cb.answer("🔒 Детали совместимости — только для VIP! 🍷", show_alert=True)
        return

    tid = int(cb.data[3:])
    target = await DB.get_user_by_id(tid)
    if not target: return

    learned = await PreferenceLearner.get_preferences(user["id"])
    compat = SmartCompatibility.calculate(user, target, learned)
    breakdown = SmartCompatibility.compatibility_breakdown(compat["details"])
    bar = SmartCompatibility.compatibility_bar(compat["total"], detailed=True)

    txt = (
        f"📊 *Детали совместимости с {target['name']}*\n\n"
        f"💕 Общий: {bar}\n\n"
        f"*Разбивка:*\n{breakdown}"
    )

    await cb.message.answer(txt, parse_mode=ParseMode.MARKDOWN)
    await cb.answer()


# ═══ ICEBREAKER ═══

@rt.callback_query(F.data.startswith("ib:"))
async def show_icebreaker(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return
    mid = int(cb.data[3:])
    async with async_session_maker() as s:
        r = await s.execute(select(Match).where(Match.id == mid))
        m = r.scalar_one_or_none()
        if m and m.icebreaker_text:
            await cb.answer(m.icebreaker_text, show_alert=True)
        else:
            await cb.answer("💡 Просто скажи привет! 🍷", show_alert=True)


# ═══ MATCHES & CHAT ═══

@rt.message(F.text == "💕 Симпатии")
async def show_matches(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): await msg.answer("📝 /start"); return
    await state.clear()
    ms = await DB.get_matches(user["id"])
    if ms:
        total_unread = sum(m.get("unread", 0) for m in ms)
        header = f"💕 *Симпатии ({len(ms)})*"
        if total_unread > 0: header += f" · 🔴 {total_unread}"
        await msg.answer(header, reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer("😔 Пока нет мэтчей\n\n🍷 Листай анкеты!", parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("ch:"))
async def start_chat(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    pid = int(cb.data[3:])
    p = await DB.get_user_by_id(pid)
    if not p: await cb.answer("❌"); return
    mid = await DB.get_match_between(user["id"], pid)
    if not mid: await cb.answer("❌ Нет мэтча"); return
    await DB.mark_messages_read(mid, user["id"])

    msgs = await DB.get_msgs(mid, 8)
    badge = DB.get_badge(p)
    txt = f"💬 *Чат с {badge}{p['name']}*\n\n"
    for mg in msgs:
        sn = "📤" if mg["sender_id"] == user["id"] else "📩"
        ts = mg["created_at"].strftime("%H:%M") if mg.get("created_at") else ""
        txt += f"{sn} {mg['text']} _{ts}_\n"
    if not msgs:
        txt += f"_Напиши первым!_ {Anim.get_wine_emoji()}"

    # Получаем icebreaker
    async with async_session_maker() as s:
        mr = await s.execute(select(Match).where(Match.id == mid))
        match = mr.scalar_one_or_none()
        icebreaker = match.icebreaker_text if match else None

    await state.update_data(cp=pid, mi=mid)
    await state.set_state(ChatStates.chatting)
    await cb.message.edit_text(txt, reply_markup=KB.chat_actions(mid, icebreaker),
        parse_mode=ParseMode.MARKDOWN)
    await cb.answer()


@rt.message(ChatStates.chatting)
async def send_chat_msg(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d = await state.get_data()
    mid, pid = d.get("mi"), d.get("cp")
    if not mid: await state.clear(); await msg.answer("💬 Чат закрыт", reply_markup=KB.main()); return
    await DB.send_msg(mid, user["id"], msg.text)
    p = await DB.get_user_by_id(pid)
    if p:
        try: await msg.bot.send_message(p["telegram_id"],
            f"💬 *{user['name']}:* {msg.text}", parse_mode=ParseMode.MARKDOWN)
        except: pass
    await msg.answer("✅")


@rt.callback_query(F.data == "bm")
async def back_matches(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user: return
    ms = await DB.get_matches(user["id"])
    if ms:
        await cb.message.edit_text(f"💕 *({len(ms)}):*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else:
        await cb.message.edit_text("😔 Нет мэтчей", parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("um:"))
async def unmatch_ask(cb: CallbackQuery):
    mid = int(cb.data[3:])
    await cb.message.edit_text("💔 *Отвязать?*", reply_markup=KB.confirm_unmatch(mid), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("um_yes:"))
async def unmatch_do(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    await state.clear()
    mid = int(cb.data[7:])
    await DB.unmatch(user["id"], mid)
    await cb.message.edit_text("💔 Отвязано.", reply_markup=KB.back_matches())


# ═══ CHATS & GUESTS ═══

@rt.message(F.text.startswith("💬 Чаты"))
async def show_chats(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return
    await state.clear()
    ms = await DB.get_matches(user["id"])
    if ms: await msg.answer("💬 *Диалоги:*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else: await msg.answer("💬 Нет диалогов", parse_mode=ParseMode.MARKDOWN)

@rt.message(F.text == "👻 Гости")
async def show_guests(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return
    lim = 20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE
    gs = await DB.get_guests(user["id"], lim)
    if not gs: await msg.answer("👻 Пока нет гостей\n💡 _Поставь буст!_", parse_mode=ParseMode.MARKDOWN); return
    txt = "👻 *Гости:*\n\n"
    for i, g in enumerate(gs, 1):
        txt += f"{i}. {DB.get_badge(g)}{g['name']}, {g['age']} — {g['city']}\n"
    if not DB.is_vip(user): txt += "\n🔒 _Подписка = все гости!_"
    await msg.answer(txt, parse_mode=ParseMode.MARKDOWN)


# ═══ PROFILE ═══

@rt.message(F.text == "👤 Профиль")
async def show_profile(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): await msg.answer("📝 /start"); return
    badge = DB.get_badge(user)
    sub = TIER_NAMES.get(user["subscription_tier"], "🆓")

    interests = [i for i in (user.get("interests") or "").split(",") if i]
    int_txt = "\n🎯 " + " · ".join(interests[:5]) if interests else ""
    goal_txt = f"\n💍 {RELATIONSHIP_GOALS.get(user.get('relationship_goal'), '')}" if user.get("relationship_goal") else ""
    zodiac_txt = f"\n🔮 {user['zodiac']}" if user.get("zodiac") else ""
    drinking_txt = f"\n🍷 {DRINKING_OPTIONS.get(user.get('drinking'), '')}" if user.get("drinking") else ""
    height_txt = f"\n📏 {user['height']} см" if user.get("height") else ""
    job_txt = f"\n💼 {user['job']}" if user.get("job") else ""

    limits = DB.get_tier_limits(user["subscription_tier"])
    likes_str = "♾️" if limits["likes"] >= 999999 else f"{user.get('daily_likes_remaining', 0)}/{limits['likes']}"
    sl_str = f" · ⚡ {user.get('daily_super_likes_remaining', 0)}/{limits['super_likes']}"

    bi = ""
    if DB.is_boosted(user): bi += f"\n🚀 Буст до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if user.get("boost_count", 0) > 0: bi += f"\n🚀 Запас: {user['boost_count']}"

    txt = (
        f"👤 *Профиль*\n\n"
        f"{badge}*{user['name']}*, {user['age']}{DB.get_role_tag(user)}\n"
        f"🏙️ {user['city']}{zodiac_txt}{height_txt}{job_txt}{drinking_txt}\n\n"
        f"{user['bio'] or '_—_'}{int_txt}{goal_txt}\n\n"
        f"📊 👁️ {user['views_count']} · ❤️ {user['likes_received_count']} · 💕 {user['matches_count']}\n"
        f"❤️ {likes_str}{sl_str}\n"
        f"⚖️ Рейтинг: {user.get('elo_score', 1000)}\n\n"
        f"🍷 {sub}{bi}"
    )

    try:
        if user.get("main_photo"):
            await msg.answer_photo(photo=user["main_photo"], caption=txt,
                reply_markup=KB.profile_detailed(user), parse_mode=ParseMode.MARKDOWN)
        else:
            await msg.answer(txt, reply_markup=KB.profile_detailed(user), parse_mode=ParseMode.MARKDOWN)
    except:
        await msg.answer(txt, reply_markup=KB.profile_detailed(user), parse_mode=ParseMode.MARKDOWN)


# ═══ EDIT PROFILE ═══

@rt.callback_query(F.data == "pe")
async def edit_menu(cb: CallbackQuery):
    try: await cb.message.edit_caption(caption="✏️ *Что изменить?*",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Имя", callback_data="ed:name"),
             InlineKeyboardButton(text="🎂 Возраст", callback_data="ed:age")],
            [InlineKeyboardButton(text="🏙️ Город", callback_data="ed:city"),
             InlineKeyboardButton(text="📝 О себе", callback_data="ed:bio")],
            [InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")],
            [InlineKeyboardButton(text="⬅️", callback_data="pv")]
        ]), parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text("✏️ *Что изменить?*",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Имя", callback_data="ed:name"),
             InlineKeyboardButton(text="🎂 Возраст", callback_data="ed:age")],
            [InlineKeyboardButton(text="🏙️ Город", callback_data="ed:city"),
             InlineKeyboardButton(text="📝 О себе", callback_data="ed:bio")],
            [InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")],
            [InlineKeyboardButton(text="⬅️", callback_data="pv")]
        ]), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "pv")
async def back_profile(cb: CallbackQuery, user: Optional[Dict]):
    if user:
        try: await cb.message.delete()
        except: pass
        await show_profile(cb.message, user)
    await cb.answer()

@rt.callback_query(F.data == "ed:name")
async def ed_name(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("✏️ Новое имя:")
    await state.set_state(EditStates.edit_name); await cb.answer()

@rt.message(EditStates.edit_name)
async def save_name(msg: Message, state: FSMContext):
    n = msg.text.strip()
    if len(n) < 2 or len(n) > 50: await msg.answer("⚠️ 2-50:"); return
    await DB.update_user(msg.from_user.id, name=n); await state.clear()
    await msg.answer(f"✅ *{n}*", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data == "ed:age")
async def ed_age(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("🎂 Возраст:")
    await state.set_state(EditStates.edit_age); await cb.answer()

@rt.message(EditStates.edit_age)
async def save_age(msg: Message, state: FSMContext):
    try:
        a = int(msg.text.strip())
        if not 18 <= a <= 99: raise ValueError
    except: await msg.answer("⚠️ 18-99:"); return
    await DB.update_user(msg.from_user.id, age=a); await state.clear()
    await msg.answer(f"✅ {a}", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:city")
async def ed_city(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("🏙️ Город:")
    await state.set_state(EditStates.edit_city); await cb.answer()

@rt.message(EditStates.edit_city)
async def save_city(msg: Message, state: FSMContext):
    await DB.update_user(msg.from_user.id, city=msg.text.strip().title())
    await state.clear(); await msg.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:bio")
async def ed_bio(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("📝 О себе:")
    await state.set_state(EditStates.edit_bio); await cb.answer()

@rt.message(EditStates.edit_bio)
async def save_bio(msg: Message, state: FSMContext):
    await DB.update_user(msg.from_user.id, bio=msg.text.strip()[:500])
    await state.clear(); await msg.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:photo")
async def ed_photo(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("📸 Фото:")
    await state.set_state(EditStates.add_photo); await cb.answer()

@rt.message(EditStates.add_photo, F.photo)
async def save_photo(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    pid = msg.photo[-1].file_id
    ph = user.get("photos", "")
    ph = (ph + "," + pid) if ph else pid
    await DB.update_user(msg.from_user.id, photos=ph, main_photo=pid)
    await state.clear(); await msg.answer("📸 ✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:interests")
async def ed_interests(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    selected = set((user.get("interests") or "").split(",")) - {""}
    await state.update_data(selected_interests=list(selected))
    await cb.message.answer("🎯 *Интересы:*", reply_markup=KB.interests_picker(selected),
        parse_mode=ParseMode.MARKDOWN)
    await state.set_state(EditStates.edit_interests)
    await cb.answer()

@rt.callback_query(EditStates.edit_interests, F.data.startswith("int:"))
async def ed_int_toggle(cb: CallbackQuery, state: FSMContext):
    interest = cb.data[4:]
    d = await state.get_data()
    selected = set(d.get("selected_interests", []))
    if interest in selected: selected.discard(interest)
    else: selected.add(interest)
    await state.update_data(selected_interests=list(selected))
    await cb.message.edit_reply_markup(reply_markup=KB.interests_picker(selected))
    await cb.answer()

@rt.callback_query(EditStates.edit_interests, F.data.startswith("int_page:"))
async def ed_int_page(cb: CallbackQuery, state: FSMContext):
    page = int(cb.data.split(":")[1])
    d = await state.get_data()
    selected = set(d.get("selected_interests", []))
    await cb.message.edit_reply_markup(reply_markup=KB.interests_picker(selected, page))
    await cb.answer()

@rt.callback_query(EditStates.edit_interests, F.data == "int_done")
async def ed_int_done(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    selected = d.get("selected_interests", [])
    await DB.update_user(cb.from_user.id, interests=",".join(selected))
    await state.clear()
    await cb.message.edit_text(f"🎯 Сохранено: {len(selected)} интересов ✅")
    await cb.answer()

@rt.callback_query(F.data == "ed:goal")
async def ed_goal(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("💍 *Цель:*", reply_markup=KB.relationship_goals(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(EditStates.edit_goal); await cb.answer()

@rt.callback_query(EditStates.edit_goal, F.data.startswith("goal:"))
async def save_goal(cb: CallbackQuery, state: FSMContext):
    goal = cb.data[5:]
    await DB.update_user(cb.from_user.id, relationship_goal=goal)
    await state.clear()
    await cb.message.edit_text(f"💍 {RELATIONSHIP_GOALS.get(goal)} ✅")
    await cb.answer()

@rt.callback_query(F.data == "ed:height")
async def ed_height(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("📏 Рост (см):")
    await state.set_state(EditStates.edit_height); await cb.answer()

@rt.message(EditStates.edit_height)
async def save_height(msg: Message, state: FSMContext):
    try:
        h = int(msg.text.strip())
        if not 100 <= h <= 250: raise ValueError
    except: await msg.answer("⚠️ 100-250 см"); return
    await DB.update_user(msg.from_user.id, height=h)
    await state.clear(); await msg.answer(f"📏 {h} см ✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:job")
async def ed_job(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("💼 Профессия:")
    await state.set_state(EditStates.edit_job); await cb.answer()

@rt.message(EditStates.edit_job)
async def save_job(msg: Message, state: FSMContext):
    await DB.update_user(msg.from_user.id, job=msg.text.strip()[:100])
    await state.clear(); await msg.answer("💼 ✅", reply_markup=KB.main())


# ═══ FAQ, REPORTS, MENU ═══

@rt.message(F.text == "❓ FAQ")
async def show_faq(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(
        f"❓ *FAQ · {BOT_NAME}*\n\n"
        f"*❤️ Симпатии* — лайкай, при взаимности мэтч!\n"
        f"*⚡ Суперлайк* — человек узнает что понравился\n"
        f"*📊 Совместимость* — 9 факторов анализа\n"
        f"*🌟 Пара дня* — лучшая рекомендация дня\n"
        f"*🧠 AI-подбор* — бот учится на твоих предпочтениях\n"
        f"*⚖️ ELO рейтинг* — балансирует показы\n"
        f"*🚀 Буст* — 24ч в топе\n"
        f"*❓ Вопрос дня* — покажи себя!\n"
        f"*🍷 Подписки* — больше возможностей",
        parse_mode=ParseMode.MARKDOWN
    )

@rt.callback_query(F.data.startswith("rp:"))
async def start_report(cb: CallbackQuery, state: FSMContext):
    await state.update_data(rp_id=int(cb.data[3:]))
    try: await cb.message.edit_caption(caption="⚠️ *Причина:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text("⚠️ *Причина:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("rr:"))
async def save_report(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d = await state.get_data()
    rid = d.get("rp_id")
    if rid: await DB.create_report(user["id"], rid, cb.data[3:])
    await state.clear()
    try: await cb.message.edit_caption(caption="✅ Жалоба отправлена 🍷")
    except: await cb.message.edit_text("✅ Жалоба отправлена 🍷")
    ps = await SmartSearch.find_profiles(user, 1)
    if ps: await show_smart_card(cb.message, ps[0], user)
    await cb.answer()

@rt.callback_query(F.data == "mn")
async def back_menu(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    try: await cb.message.delete()
    except: pass
    un = await DB.get_unread(user["id"]) if user else 0
    await cb.message.answer("🍷", reply_markup=KB.main(un))
    await cb.answer()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#              BACKGROUND TASKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def background_learner():
    """Фоновое обучение предпочтений"""
    while True:
        try:
            async with async_session_maker() as s:
                active = await s.execute(
                    select(User.id).where(and_(
                        User.is_active == True,
                        User.is_profile_complete == True,
                        User.likes_given_count > 5
                    )).limit(50)
                )
                for row in active.fetchall():
                    await PreferenceLearner.learn(row[0])
                    await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Learner error: {e}")
        await asyncio.sleep(3600)  # Каждый час


async def background_daily_matches(bot: Bot):
    """Генерация пар дня"""
    while True:
        try:
            now = datetime.utcnow()
            if now.hour == 9:  # В 9 утра
                async with async_session_maker() as s:
                    users = await s.execute(
                        select(User.id).where(and_(
                            User.is_active == True,
                            User.is_profile_complete == True,
                            User.last_active_at > now - timedelta(days=7)
                        )).limit(200)
                    )
                    for row in users.fetchall():
                        rec = await DailyRecommendations.generate_pair_of_day(row[0])
                        if rec:
                            user = await DB.get_user_by_id(row[0])
                            if user:
                                try:
                                    await bot.send_message(
                                        user["telegram_id"],
                                        f"🌟 *Пара дня готова!*\n\n"
                                        f"Мы нашли для тебя отличную рекомендацию!\n"
                                        f"Нажми «🌟 Пара дня» чтобы посмотреть 🍷",
                                        parse_mode=ParseMode.MARKDOWN
                                    )
                                except: pass
                        await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Daily matches error: {e}")
        await asyncio.sleep(3600)


async def background_profile_quality():
    """Обновление качества профилей"""
    while True:
        try:
            async with async_session_maker() as s:
                users = await s.execute(
                    select(User).where(User.is_profile_complete == True).limit(100)
                )
                for u in users.scalars().all():
                    ud = DB._to_dict(u)
                    analysis = ProfileAnalyzer.analyze(ud)
                    await s.execute(
                        update(User).where(User.id == u.id)
                        .values(profile_quality_score=analysis["score"])
                    )
                await s.commit()
        except Exception as e:
            logger.error(f"Profile quality error: {e}")
        await asyncio.sleep(7200)  # Каждые 2 часа


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                      MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def main():
    await init_db()

    # Seed вопросы дня
    async with async_session_maker() as s:
        existing = (await s.execute(select(func.count(QuestionOfDay.id)))).scalar() or 0
        if existing == 0:
            questions = [
                "Какой суперспособностью ты бы хотел(а) обладать?",
                "Если бы ты мог(ла) жить в любой стране — какую выбрал(а) бы?",
                "Какой фильм ты можешь пересматривать бесконечно?",
                "Красное или белое вино? 🍷",
                "Утро в горах или вечер на пляже?",
                "Какая песня описывает твою жизнь?",
                "Идеальное свидание — это...",
                "Какой навык ты бы хотел(а) освоить?",
                "Кот или собака?",
                "Какое твоё самое необычное хобби?",
                "Чему научил тебя последний год?",
                "Какой подарок ты хотел(а) бы получить?",
                "Опиши себя тремя словами",
                "Какой совет ты бы дал(а) себе 5 лет назад?",
                "Пицца или суши? 🍕🍣",
            ]
            for q in questions:
                s.add(QuestionOfDay(question_text=q))
            await s.commit()
            logger.info(f"🍷 Seeded {len(questions)} questions")

    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())
    dp.include_router(rt)

    # Запускаем фоновые задачи
    asyncio.create_task(background_learner())
    asyncio.create_task(background_daily_matches(bot))
    asyncio.create_task(background_profile_quality())

    logger.info(f"🍷 {BOT_NAME} v5.0 starting...")
    logger.info("🧠 Smart Matching: ELO + ML + 9-factor compatibility")
    logger.info("🌟 Daily Recommendations: active")
    logger.info("📊 Profile Quality Analyzer: active")
    logger.info("❓ Question of Day: active")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
