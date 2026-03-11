"""
ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v3.7 (ULTIMATE)
Запуск:
pip install aiogram==3.7.0 aiosqlite sqlalchemy yookassa python-dotenv
python bot.py
"""
import asyncio
import os
import uuid
import logging
import json
import time
import random
import hashlib
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Update,
    InputMediaPhoto
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean, DateTime, Float,
    Text, ForeignKey, Enum as SQLEnum, select, update, delete,
    func, and_, or_, desc, asc, case, UniqueConstraint
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

# ==========================================
# CONFIG
# ==========================================
@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///dating_bot.db")
    YOOKASSA_SHOP_ID: str = os.getenv("YOOKASSA_SHOP_ID", "")
    YOOKASSA_SECRET_KEY: str = os.getenv("YOOKASSA_SECRET_KEY", "")
    DOMAIN: str = os.getenv("DOMAIN", "https://yourdomain.ru")
    FREE_DAILY_LIKES: int = 15
    FREE_DAILY_MESSAGES: int = 5
    FREE_GUESTS_VISIBLE: int = 2
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

# ==========================================
# ENUMS
# ==========================================
class Gender(str, Enum):
    MALE = "male"
    FEMALE = "female"

class LookingFor(str, Enum):
    MALE = "male"
    FEMALE = "female"
    BOTH = "both"

class SubscriptionTier(str, Enum):
    FREE = "free"
    VIP_LIGHT = "vip_light"
    VIP_STANDARD = "vip_standard"
    VIP_PRO = "vip_pro"
    VIP_LIFETIME = "vip_lifetime"

class PaymentStatus(str, Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    CANCELED = "canceled"

class NotificationType(str, Enum):
    LIKE = "like"
    SUPERLIKE = "superlike"
    MATCH = "match"
    MESSAGE = "message"
    GUEST = "guest"
    SYSTEM = "system"

# ==========================================
# MODELS
# ==========================================
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
    interests = Column(Text, nullable=True)
    looking_for = Column(SQLEnum(LookingFor), default=LookingFor.BOTH)
    age_from = Column(Integer, default=18)
    age_to = Column(Integer, default=99)
    photos = Column(Text, default="")
    main_photo = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    is_banned = Column(Boolean, default=False)
    is_verified = Column(Boolean, default=False)
    is_profile_complete = Column(Boolean, default=False)
    is_invisible = Column(Boolean, default=False)
    subscription_tier = Column(SQLEnum(SubscriptionTier), default=SubscriptionTier.FREE)
    subscription_expires_at = Column(DateTime, nullable=True)
    daily_likes_remaining = Column(Integer, default=15)
    daily_messages_remaining = Column(Integer, default=5)
    daily_superlikes_remaining = Column(Integer, default=0)
    last_limits_reset = Column(DateTime, nullable=True)
    boost_expires_at = Column(DateTime, nullable=True)
    boost_count = Column(Integer, default=0)
    views_count = Column(Integer, default=0)
    likes_received_count = Column(Integer, default=0)
    likes_sent_count = Column(Integer, default=0)
    matches_count = Column(Integer, default=0)
    popularity_score = Column(Float, default=0.0)
    hidden_likes_count = Column(Integer, default=0)
    last_teaser_shown = Column(DateTime, nullable=True)
    trial_used = Column(Boolean, default=False)
    referral_code = Column(String(20), unique=True, nullable=True)
    referred_by = Column(Integer, nullable=True)
    referral_bonus_count = Column(Integer, default=0)
    notification_likes = Column(Boolean, default=True)
    notification_matches = Column(Boolean, default=True)
    notification_messages = Column(Boolean, default=True)
    notification_guests = Column(Boolean, default=True)
    streak_days = Column(Integer, default=0)
    last_streak_date = Column(DateTime, nullable=True)
    total_online_minutes = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    last_active_at = Column(DateTime, default=datetime.utcnow)

class Like(Base):
    __tablename__ = "likes"
    id = Column(Integer, primary_key=True)
    from_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    to_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    is_super_like = Column(Boolean, default=False)
    message = Column(String(200), nullable=True)
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
    icebreaker_sent = Column(Boolean, default=False)
    last_message_at = Column(DateTime, nullable=True)
    msg_count = Column(Integer, default=0)
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
    visited_user_id = Column(Integer, ForeignKey("users.id"))
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
    description = Column(Text, nullable=True)
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

class DailyReward(Base):
    __tablename__ = "daily_rewards"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    day_number = Column(Integer)
    reward_type = Column(String(50))
    reward_amount = Column(Integer, default=1)
    claimed_at = Column(DateTime, default=datetime.utcnow)

class Achievement(Base):
    __tablename__ = "achievements"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    achievement_type = Column(String(50))
    earned_at = Column(DateTime, default=datetime.utcnow)

class BroadcastLog(Base):
    __tablename__ = "broadcast_logs"
    id = Column(Integer, primary_key=True)
    admin_id = Column(Integer)
    message_text = Column(Text)
    target_filter = Column(String(50), default="all")
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

# ==========================================
# DATABASE INIT
# ==========================================
engine = create_async_engine(config.DATABASE_URL, echo=False)
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ DB ready")

# ==========================================
# FSM
# ==========================================
class RegStates(StatesGroup):
    name = State(); age = State(); gender = State(); city = State()
    photo = State(); bio = State(); interests = State()
    looking_for = State(); age_range = State()

class EditStates(StatesGroup):
    edit_name = State(); edit_age = State(); edit_city = State()
    edit_bio = State(); edit_interests = State(); edit_age_range = State()
    add_photo = State(); superlike_msg = State()

class ChatStates(StatesGroup):
    chatting = State()

class SearchStates(StatesGroup):
    browsing = State()

class AdminStates(StatesGroup):
    broadcast_text = State(); broadcast_confirm = State()
    search_user = State(); give_vip_duration = State()
    give_boost_count = State(); promo_code = State()
    promo_tier = State(); promo_duration = State(); promo_uses = State()

# ==========================================
# GAMIFICATION
# ==========================================
class Gamification:
    DAILY_REWARDS = {
        1: {"type": "likes", "amount": 5, "text": "🎁 +5 лайков"},
        2: {"type": "likes", "amount": 5, "text": "🎁 +5 лайков"},
        3: {"type": "superlike", "amount": 1, "text": "⭐ +1 суперлайк"},
        4: {"type": "likes", "amount": 10, "text": "🎁 +10 лайков"},
        5: {"type": "boost", "amount": 1, "text": "🚀 +1 буст"},
        6: {"type": "likes", "amount": 10, "text": "🎁 +10 лайков"},
        7: {"type": "trial", "amount": 1, "text": "👑 1 день VIP бесплатно!"},
    }

    ACHIEVEMENTS = {
        "first_like": {"title": "🎯 Первый лайк", "desc": "Поставь первый лайк", "reward": "likes:3"},
        "first_match": {"title": "🔥 Первый мэтч", "desc": "Получи первый мэтч", "reward": "superlike:1"},
        "first_message": {"title": "💬 Первое сообщение", "desc": "Напиши первое сообщение", "reward": "likes:5"},
        "photo_added": {"title": "📸 Фотограф", "desc": "Добавь фото профиля", "reward": "likes:5"},
        "bio_added": {"title": "✍️ Писатель", "desc": "Напиши о себе", "reward": "likes:3"},
        "interests_added": {"title": "🎨 Интересный", "desc": "Укажи интересы", "reward": "likes:5"},
        "streak_3": {"title": "⚡ 3 дня подряд", "desc": "Заходи 3 дня подряд", "reward": "superlike:1"},
        "streak_7": {"title": "📅 Неделя!", "desc": "Заходи 7 дней подряд", "reward": "boost:1"},
        "streak_30": {"title": "🏆 Месяц!", "desc": "30 дней подряд", "reward": "boost:3"},
        "likes_10": {"title": "💝 10 лайков", "desc": "Получи 10 лайков", "reward": "likes:5"},
        "likes_50": {"title": "💖 50 лайков", "desc": "Получи 50 лайков", "reward": "boost:1"},
        "likes_100": {"title": "💯 Сотня!", "desc": "100 лайков", "reward": "superlike:3"},
        "matches_5": {"title": "💕 5 мэтчей", "desc": "Получи 5 мэтчей", "reward": "boost:1"},
        "matches_10": {"title": "💞 10 мэтчей", "desc": "10 мэтчей!", "reward": "superlike:2"},
        "referral_1": {"title": "🤝 Первый друг", "desc": "Пригласи 1 друга", "reward": "boost:1"},
        "referral_5": {"title": "🫂 5 друзей", "desc": "Пригласи 5 друзей", "reward": "boost:3"},
    }

    ICEBREAKERS = [
        "Привет! Что тебя вдохновляет?",
        "Привет! Какой был самый крутой день в твоей жизни?",
        "Привет! Если бы ты мог(ла) телепортироваться куда угодно — куда?",
        "Привет! Вино красное или белое? 🍷",
        "Привет! Какая суперспособность была бы у тебя?",
        "Привет! Опиши себя тремя эмодзи 🙈",
        "Привет! Какой последний сериал тебя зацепил?",
        "Привет! Утро или вечер — когда ты на максимуме?",
        "Привет! Если бы твоя жизнь была фильмом — какой жанр?",
        "Привет! Что для тебя идеальный вечер?",
    ]

    @staticmethod
    def get_icebreaker() -> str:
        return random.choice(Gamification.ICEBREAKERS)

    @staticmethod
    async def update_streak(user: Dict) -> Tuple[int, Optional[Dict]]:
        now = datetime.utcnow()
        last = user.get("last_streak_date")
        streak = user.get("streak_days", 0)
        if last is None:
            streak = 1
        elif last.date() == now.date():
            return streak, None
        elif last.date() == (now - timedelta(days=1)).date():
            streak += 1
        else:
            streak = 1
        day_in_cycle = ((streak - 1) % 7) + 1
        reward = Gamification.DAILY_REWARDS.get(day_in_cycle)

        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == user["telegram_id"]).values(streak_days=streak, last_streak_date=now))
            await s.commit()

        return streak, reward

    @staticmethod
    async def claim_reward(user_id: int, tg_id: int, reward: Dict) -> str:
        rtype = reward["type"]
        amount = reward["amount"]
        async with async_session_maker() as s:
            today = datetime.utcnow().date()
            existing = await s.execute(
                select(DailyReward).where(and_(DailyReward.user_id == user_id, func.date(DailyReward.claimed_at) == today))
            )
            if existing.scalar_one_or_none():
                return ""
            s.add(DailyReward(user_id=user_id, day_number=0, reward_type=rtype, reward_amount=amount))

            if rtype == "likes":
                await s.execute(update(User).where(User.telegram_id == tg_id).values(daily_likes_remaining=User.daily_likes_remaining + amount))
            elif rtype == "superlike":
                await s.execute(update(User).where(User.telegram_id == tg_id).values(daily_superlikes_remaining=User.daily_superlikes_remaining + amount))
            elif rtype == "boost":
                await s.execute(update(User).where(User.telegram_id == tg_id).values(boost_count=User.boost_count + amount))
            elif rtype == "trial":
                now = datetime.utcnow()
                await s.execute(update(User).where(User.telegram_id == tg_id).values(subscription_tier=SubscriptionTier.VIP_LIGHT, subscription_expires_at=now + timedelta(days=1)))
            await s.commit()

        return reward["text"]

    @staticmethod
    async def check_achievements(user: Dict) -> List[Dict]:
        earned = []
        async with async_session_maker() as s:
            existing = await s.execute(select(Achievement.achievement_type).where(Achievement.user_id == user["id"]))
            already = set(r[0] for r in existing.fetchall())

            checks = {
                "first_like": user.get("likes_sent_count", 0) >= 1,
                "first_match": user.get("matches_count", 0) >= 1,
                "photo_added": bool(user.get("main_photo")),
                "bio_added": bool(user.get("bio")),
                "interests_added": bool(user.get("interests")),
                "streak_3": user.get("streak_days", 0) >= 3,
                "streak_7": user.get("streak_days", 0) >= 7,
                "streak_30": user.get("streak_days", 0) >= 30,
                "likes_10": user.get("likes_received_count", 0) >= 10,
                "likes_50": user.get("likes_received_count", 0) >= 50,
                "likes_100": user.get("likes_received_count", 0) >= 100,
                "matches_5": user.get("matches_count", 0) >= 5,
                "matches_10": user.get("matches_count", 0) >= 10,
                "referral_1": user.get("referral_bonus_count", 0) >= 1,
                "referral_5": user.get("referral_bonus_count", 0) >= 5,
            }

            for ach_type, condition in checks.items():
                if condition and ach_type not in already:
                    ach_info = Gamification.ACHIEVEMENTS[ach_type]
                    s.add(Achievement(user_id=user["id"], achievement_type=ach_type))
                    reward_parts = ach_info["reward"].split(":")
                    r_type, r_amount = reward_parts[0], int(reward_parts[1])

                    if r_type == "likes":
                        await s.execute(update(User).where(User.id == user["id"]).values(daily_likes_remaining=User.daily_likes_remaining + r_amount))
                    elif r_type == "superlike":
                        await s.execute(update(User).where(User.id == user["id"]).values(daily_superlikes_remaining=User.daily_superlikes_remaining + r_amount))
                    elif r_type == "boost":
                        await s.execute(update(User).where(User.id == user["id"]).values(boost_count=User.boost_count + r_amount))

                    earned.append(ach_info)

            if earned:
                await s.commit()
        return earned

# ==========================================
# COMPATIBILITY
# ==========================================
class Compatibility:
    INTERESTS_LIST = [
        "🎧 Музыка", "🎬 Кино", "📚 Книги", "⚽ Спорт", "✈️ Путешествия",
        "🍳 Кулинария", "🎮 Игры", "📷 Фото", "🎨 Искусство", "💻 IT",
        "🐾 Животные", "🧘 Йога", "🌿 Природа", "🍷 Вино", "💃 Танцы",
        "🎸 Концерты", "💪 Фитнес", "📺 Сериалы", "🏖 Пляж", "☕ Кофе",
    ]

    @staticmethod
    def calc_score(u1: Dict, u2: Dict) -> float:
        score = 0.0
        i1 = set(x.strip() for x in (u1.get("interests") or "").split(",") if x.strip())
        i2 = set(x.strip() for x in (u2.get("interests") or "").split(",") if x.strip())
        if i1 and i2:
            score += (len(i1 & i2) / len(i1 | i2)) * 40
        ad = abs((u1.get("age") or 25) - (u2.get("age") or 25))
        score += max(0, 20 - ad * 2)
        if (u1.get("city") or "").lower() == (u2.get("city") or "").lower():
            score += 15
        g1, g2 = u1.get("gender"), u2.get("gender")
        lf1, lf2 = u1.get("looking_for", "both"), u2.get("looking_for", "both")
        if (lf1 == "both" or lf1 == g2) and (lf2 == "both" or lf2 == g1):
            score += 15
        now = datetime.utcnow()
        for u in [u1, u2]:
            la = u.get("last_active_at") or now
            if (now - la).days <= 1: score += 5
        return round(min(score, 100), 1)

    @staticmethod
    def calc_popularity(u: Dict) -> float:
        likes = u.get("likes_received_count", 0)
        views = max(u.get("views_count", 0), 1)
        return round(likes * 0.4 + u.get("matches_count", 0) * 2 + (likes / views) * 50, 1)

# ==========================================
# MONETIZATION ENGINE
# ==========================================
class Monetization:
    @staticmethod
    def get_likes_limit_msg(user: Dict) -> Tuple[str, InlineKeyboardMarkup]:
        hidden = user.get("hidden_likes_count", 0)
        msgs = [
            f"🛑 *Лайки закончились!*\n{hidden} человек ждут ответа...\n\n👑 Безлимит от 149₽/мес!",
            f"🛑 *Лимит!* За сегодня тебя лайкнули {hidden} раз!\n\n👑 Разблокируй ⬇️",
        ]
        return random.choice(msgs), InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👑 Безлимит 149₽", callback_data="tf:vip_light")],
            [InlineKeyboardButton(text="🎁 3 дня бесплатно", callback_data="trial:start")],
        ])

    @staticmethod
    def get_msg_limit_msg(name: str) -> Tuple[str, InlineKeyboardMarkup]:
        return f"🛑 *Лимит сообщений!*\n{name} ждёт ответа 😢\n\n👑 Безлимит от 149₽!", InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👑 Безлимит", callback_data="tf:vip_light")],
            [InlineKeyboardButton(text="🎁 Бесплатно", callback_data="trial:start")],
        ])

    @staticmethod
    def get_hidden_likes_msg(count: int) -> Tuple[str, InlineKeyboardMarkup]:
        return f"❤️ *{count} скрытых лайков!*\nУзнай кто — с VIP!", InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"👁 Открыть ({count})", callback_data="likes:list")],
            [InlineKeyboardButton(text="👑 VIP", callback_data="sh:subs")],
        ])

    @staticmethod
    def get_low_likes_warning(remaining: int) -> Optional[str]:
        if remaining == 5: return "⚠️ *5 лайков!* VIP = безлимит 👑"
        if remaining == 3: return "⚠️ *3 лайка!* VIP от 149₽/мес"
        if remaining == 1: return "⚠️ *Последний лайк!*"
        return None

    @staticmethod
    def should_show_teaser(user: Dict) -> bool:
        if DB.is_vip(user): return False
        last = user.get("last_teaser_shown")
        return not last or (datetime.utcnow() - last).total_seconds() > 300

# ==========================================
# DATABASE SERVICE
# ==========================================
TIER_NAMES = {"free": "🆓 Бесплатный", "vip_light": "⭐ Винчик Light",
              "vip_standard": "🌟 Винчик Standard", "vip_pro": "👑 Винчик Pro",
              "vip_lifetime": "💎 Винчик Forever"}

class DB:
    @staticmethod
    def _to_dict(u: User) -> Dict:
        return {
            "id": u.id, "telegram_id": u.telegram_id, "username": u.username,
            "name": u.name, "age": u.age, "gender": u.gender.value if u.gender else None,
            "city": u.city, "bio": u.bio, "interests": u.interests or "",
            "looking_for": u.looking_for.value if u.looking_for else "both",
            "age_from": u.age_from, "age_to": u.age_to,
            "photos": u.photos or "", "main_photo": u.main_photo,
            "is_active": u.is_active, "is_banned": u.is_banned,
            "is_verified": u.is_verified, "is_profile_complete": u.is_profile_complete,
            "is_invisible": u.is_invisible or False,
            "subscription_tier": u.subscription_tier.value if u.subscription_tier else "free",
            "subscription_expires_at": u.subscription_expires_at,
            "daily_likes_remaining": u.daily_likes_remaining or 0,
            "daily_messages_remaining": u.daily_messages_remaining or 0,
            "daily_superlikes_remaining": u.daily_superlikes_remaining or 0,
            "last_limits_reset": u.last_limits_reset,
            "boost_expires_at": u.boost_expires_at, "boost_count": u.boost_count or 0,
            "views_count": u.views_count or 0,
            "likes_received_count": u.likes_received_count or 0,
            "likes_sent_count": u.likes_sent_count or 0,
            "matches_count": u.matches_count or 0,
            "popularity_score": u.popularity_score or 0.0,
            "hidden_likes_count": u.hidden_likes_count or 0,
            "last_teaser_shown": u.last_teaser_shown,
            "trial_used": u.trial_used or False,
            "referral_code": u.referral_code,
            "referred_by": u.referred_by,
            "referral_bonus_count": u.referral_bonus_count or 0,
            "streak_days": u.streak_days or 0,
            "last_streak_date": u.last_streak_date,
            "created_at": u.created_at, "last_active_at": u.last_active_at,
        }

    @staticmethod
    def is_vip(u: Dict) -> bool:
        t = u.get("subscription_tier", "free")
        if t == "vip_lifetime": return True
        if t == "free": return False
        e = u.get("subscription_expires_at")
        return e is not None and e > datetime.utcnow()

    @staticmethod
    def is_boosted(u: Dict) -> bool:
        e = u.get("boost_expires_at")
        return e is not None and e > datetime.utcnow()

    @staticmethod
    def is_creator(u: Dict) -> bool: return u.get("telegram_id") in config.CREATOR_IDS

    @staticmethod
    def is_admin(u: Dict) -> bool: return u.get("telegram_id") in config.ADMIN_IDS

    @staticmethod
    def get_badge(u: Dict) -> str:
        if DB.is_creator(u): return "👨‍💻 "
        if u.get("subscription_tier") == "vip_lifetime": return "💎 "
        if u.get("subscription_tier") == "vip_pro": return "👑 "
        if DB.is_vip(u): return "⭐ "
        if u.get("is_verified"): return "✅ "
        return ""

    @staticmethod
    def get_online_status(u: Dict) -> str:
        la = u.get("last_active_at")
        if not la: return ""
        diff = (datetime.utcnow() - la).total_seconds()
        if diff < 300: return "🟢"
        if diff < 3600: return "🟡"
        return "⚪"

    @staticmethod
    def get_superlikes_limit(u: Dict) -> int:
        return {"vip_pro": 5, "vip_lifetime": 5, "vip_standard": 2, "vip_light": 1}.get(u.get("subscription_tier", "free"), 0)

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
    async def create_user(tg_id: int, username=None, referred_by=None) -> Dict:
        async with async_session_maker() as s:
            u = User(telegram_id=tg_id, username=username,
                     referral_code=str(uuid.uuid4())[:8].upper(),
                     referred_by=referred_by, last_limits_reset=datetime.utcnow())
            s.add(u); await s.commit(); await s.refresh(u)
            return DB._to_dict(u)

    @staticmethod
    async def update_user(tg_id: int, **kw) -> Optional[Dict]:
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
            sl = DB.get_superlikes_limit(u)
            t = u.get("subscription_tier", "free")
            dl = 9999 if t in ("vip_standard", "vip_pro", "vip_lifetime") else (100 if t == "vip_light" else config.FREE_DAILY_LIKES)
            dm = 9999 if DB.is_vip(u) else config.FREE_DAILY_MESSAGES
            return await DB.update_user(u["telegram_id"], daily_likes_remaining=dl,
                                        daily_messages_remaining=dm, daily_superlikes_remaining=sl, last_limits_reset=now,
                                        last_active_at=now)
        await DB.update_user(u["telegram_id"], last_active_at=now)
        return u

    @staticmethod
    async def get_who_liked_me(uid: int) -> List[int]:
        async with async_session_maker() as s:
            liked = await s.execute(select(Like.from_user_id).where(Like.to_user_id == uid))
            my_likes = await s.execute(select(Like.to_user_id).where(Like.from_user_id == uid))
            my_dis = await s.execute(select(Dislike.to_user_id).where(Dislike.from_user_id == uid))
            seen = set(r[0] for r in my_likes.fetchall()) | set(r[0] for r in my_dis.fetchall())
            return [r[0] for r in liked.fetchall() if r[0] not in seen]

    @staticmethod
    async def update_hidden_likes(uid: int):
        who = await DB.get_who_liked_me(uid)
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.id == uid).values(hidden_likes_count=len(who)))
            await s.commit()

    @staticmethod
    async def search_profiles(u: Dict, limit=5) -> List[Dict]:
        async with async_session_maker() as s:
            my_likes = await s.execute(select(Like.to_user_id).where(Like.from_user_id == u["id"]))
            my_dis = await s.execute(select(Dislike.to_user_id).where(Dislike.from_user_id == u["id"]))
            exc = set(r[0] for r in my_likes.fetchall()) | set(r[0] for r in my_dis.fetchall())
            exc.add(u["id"])
            results = []

            # Кто лайкнул меня
            who = await DB.get_who_liked_me(u["id"])
            pids = [uid for uid in who if uid not in exc]
            if pids:
                pr = await s.execute(select(User).where(and_(User.id.in_(pids), User.is_active == True,
                                                             User.is_banned == False, User.is_profile_complete == True)))
                for p in pr.scalars().all():
                    d = DB._to_dict(p); d["_priority"] = "liked_you"; d["_compat"] = Compatibility.calc_score(u, d)
                    results.append(d); exc.add(p.id)

            # Основной поиск
            rem = limit - len(results)
            if rem > 0:
                q = select(User).where(and_(User.is_active == True, User.is_banned == False,
                                            User.is_profile_complete == True, User.id.not_in(exc), User.age >= u["age_from"], User.age <= u["age_to"], User.city == u["city"]))
                lf = u.get("looking_for", "both")
                if lf == "male": q = q.where(User.gender == Gender.MALE)
                elif lf == "female": q = q.where(User.gender == Gender.FEMALE)
                mg = u.get("gender")
                if mg: q = q.where(or_(User.looking_for == LookingFor.BOTH, User.looking_for == LookingFor(mg)))
                q = q.order_by(User.boost_expires_at.desc().nullslast(), User.popularity_score.desc(), User.last_active_at.desc()).limit(rem * 3)
                r = await s.execute(q)
                cands = [DB._to_dict(p) for p in r.scalars().all()]
                for c in cands:
                    c["_priority"] = "boosted" if DB.is_boosted(c) else "normal"
                    c["_compat"] = Compatibility.calc_score(u, c)
                cands.sort(key=lambda x: (x["_priority"] == "boosted", x["_compat"] + random.uniform(0, 10)), reverse=True)
                results.extend(cands[:rem])

            # Другие города
            if len(results) < limit:
                need = limit - len(results)
                all_exc = exc | set(r["id"] for r in results)
                q2 = select(User).where(and_(User.is_active == True, User.is_banned == False,
                                             User.is_profile_complete == True, User.id.not_in(all_exc), User.city != u["city"], User.age >= u["age_from"], User.age <= u["age_to"]))
                lf = u.get("looking_for", "both")
                if lf == "male": q2 = q2.where(User.gender == Gender.MALE)
                elif lf == "female": q2 = q2.where(User.gender == Gender.FEMALE)
                mg = u.get("gender")
                if mg: q2 = q2.where(or_(User.looking_for == LookingFor.BOTH, User.looking_for == LookingFor(mg)))
                q2 = q2.order_by(User.popularity_score.desc()).limit(need)
                r2 = await s.execute(q2)
                for p in r2.scalars().all():
                    d = DB._to_dict(p); d["_priority"] = "other_city"; d["_compat"] = Compatibility.calc_score(u, d)
                    results.append(d)

            return results[:limit]

    @staticmethod
    async def add_like(fd: int, tid: int, is_super=False, message=None) -> Dict:
        async with async_session_maker() as s:
            ex = await s.execute(select(Like).where(and_(Like.from_user_id == fd, Like.to_user_id == tid)))
            if ex.scalar_one_or_none(): return {"is_match": False, "match_id": None, "compat": 0}
            s.add(Like(from_user_id=fd, to_user_id=tid, is_super_like=is_super, message=message))
            await s.execute(update(User).where(User.id == tid).values(likes_received_count=User.likes_received_count + 1))
            await s.execute(update(User).where(User.id == fd).values(likes_sent_count=User.likes_sent_count + 1))
            rev = await s.execute(select(Like).where(and_(Like.from_user_id == tid, Like.to_user_id == fd)))
            is_match = rev.scalar_one_or_none() is not None
            match_id = None; compat = 0.0

            if is_match:
                existing = await s.execute(select(Match).where(or_(and_(Match.user1_id == fd, Match.user2_id == tid), and_(Match.user1_id == tid, Match.user2_id == fd))))
                if not existing.scalar_one_or_none():
                    u1r = await s.execute(select(User).where(User.id == fd)); u2r = await s.execute(select(User).where(User.id == tid))
                    u1, u2 = u1r.scalar_one_or_none(), u2r.scalar_one_or_none()
                    if u1 and u2: compat = Compatibility.calc_score(DB._to_dict(u1), DB._to_dict(u2))
                    m = Match(user1_id=min(fd, tid), user2_id=max(fd, tid), compatibility_score=compat)
                    s.add(m); await s.flush(); match_id = m.id
                    await s.execute(update(User).where(User.id.in_([fd, tid])).values(matches_count=User.matches_count + 1))
            await s.commit()
            await DB.update_hidden_likes(tid)
            return {"is_match": is_match, "match_id": match_id, "compat": compat}

    @staticmethod
    async def add_dislike(fd, tid):
        async with async_session_maker() as s:
            ex = await s.execute(select(Dislike).where(and_(Dislike.from_user_id == fd, Dislike.to_user_id == tid)))
            if not ex.scalar_one_or_none(): s.add(Dislike(from_user_id=fd, to_user_id=tid)); await s.commit()

    @staticmethod
    async def reset_dislikes(uid) -> int:
        async with async_session_maker() as s:
            r = await s.execute(select(func.count(Dislike.id)).where(Dislike.from_user_id == uid))
            c = r.scalar() or 0
            await s.execute(delete(Dislike).where(Dislike.from_user_id == uid)); await s.commit(); return c

    @staticmethod
    async def get_likes_received(uid, limit=20):
        async with async_session_maker() as s:
            r = await s.execute(select(Like).where(Like.to_user_id == uid).order_by(Like.created_at.desc()).limit(limit))
            out = []
            for lk in r.scalars().all():
                u = await DB.get_user_by_id(lk.from_user_id)
                if u: u["is_super_like"] = lk.is_super_like; u["like_message"] = lk.message; out.append(u)
            return out

    @staticmethod
    async def get_matches(uid):
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(or_(Match.user1_id == uid, Match.user2_id == uid), Match.is_active == True)).order_by(Match.last_message_at.desc().nullslast(), Match.compatibility_score.desc()))
            out = []
            for m in r.scalars().all():
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                pr = await s.execute(select(User).where(User.id == pid)); p = pr.scalar_one_or_none()
                if p:
                    unread = await s.execute(select(func.count(ChatMessage.id)).where(and_(ChatMessage.match_id == m.id, ChatMessage.sender_id != uid, ChatMessage.is_read == False)))
                    pd = DB._to_dict(p)
                    out.append({
                        "match_id": m.id, "user_id": p.id, "telegram_id": p.telegram_id,
                        "name": p.name, "age": p.age, "photo": p.main_photo,
                        "compat": m.compatibility_score, "unread": unread.scalar() or 0,
                        "online": DB.get_online_status(pd),
                        "icebreaker_sent": m.icebreaker_sent, "msg_count": m.msg_count or 0,
                    })
            return out

    @staticmethod
    async def unmatch(uid, mid) -> bool:
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(Match.id == mid, or_(Match.user1_id == uid, Match.user2_id == uid))))
            m = r.scalar_one_or_none()
            if not m: return False
            await s.execute(update(Match).where(Match.id == mid).values(is_active=False))
            await s.execute(update(User).where(User.id.in_([m.user1_id, m.user2_id])).values(matches_count=func.greatest(User.matches_count - 1, 0)))
            await s.commit(); return True

    @staticmethod
    async def get_match_between(u1, u2):
        async with async_session_maker() as s:
            r = await s.execute(select(Match.id).where(and_(or_(and_(Match.user1_id == u1, Match.user2_id == u2), and_(Match.user1_id == u2, Match.user2_id == u1)), Match.is_active == True)))
            row = r.first(); return row[0] if row else None

    @staticmethod
    async def send_msg(mid, sid, txt, photo_id=None, voice_id=None):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=txt, photo_id=photo_id, voice_id=voice_id))
            await s.execute(update(Match).where(Match.id == mid).values(last_message_at=datetime.utcnow(), msg_count=Match.msg_count + 1))
            await s.commit()

    @staticmethod
    async def get_msgs(mid, limit=10):
        async with async_session_maker() as s:
            r = await s.execute(select(ChatMessage).where(ChatMessage.match_id == mid).order_by(ChatMessage.created_at.desc()).limit(limit))
            return [{"sender_id": m.sender_id, "text": m.text, "photo_id": m.photo_id, "created_at": m.created_at} for m in reversed(r.scalars().all())]

    @staticmethod
    async def mark_read(mid, uid):
        async with async_session_maker() as s:
            await s.execute(update(ChatMessage).where(and_(ChatMessage.match_id == mid, ChatMessage.sender_id != uid, ChatMessage.is_read == False)).values(is_read=True))
            await s.commit()

    @staticmethod
    async def get_unread(uid):
        async with async_session_maker() as s:
            ms = await s.execute(select(Match.id).where(and_(or_(Match.user1_id == uid, Match.user2_id == uid), Match.is_active == True)))
            mids = [m[0] for m in ms.fetchall()]
            if not mids: return 0
            r = await s.execute(select(func.count(ChatMessage.id)).where(and_(ChatMessage.match_id.in_(mids), ChatMessage.sender_id != uid, ChatMessage.is_read == False)))
            return r.scalar() or 0

    @staticmethod
    async def add_guest(vid, uid):
        async with async_session_maker() as s:
            s.add(GuestVisit(visitor_id=vid, visited_user_id=uid))
            await s.execute(update(User).where(User.id == uid).values(views_count=User.views_count + 1))
            await s.commit()

    @staticmethod
    async def get_guests(uid, limit=10):
        async with async_session_maker() as s:
            r = await s.execute(select(GuestVisit.visitor_id).where(GuestVisit.visited_user_id == uid).order_by(GuestVisit.created_at.desc()).distinct().limit(limit))
            ids = [row[0] for row in r.fetchall()]
            if not ids: return []
            us = await s.execute(select(User).where(User.id.in_(ids)))
            return [DB._to_dict(u) for u in us.scalars().all()]

    @staticmethod
    async def dec_likes(tg_id):
        u = await DB.get_user(tg_id)
        if u and u.get("daily_likes_remaining", 0) > 9000: return
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(daily_likes_remaining=func.greatest(User.daily_likes_remaining - 1, 0)))
            await s.commit()

    @staticmethod
    async def dec_superlikes(tg_id):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(daily_superlikes_remaining=func.greatest(User.daily_superlikes_remaining - 1, 0)))
            await s.commit()

    @staticmethod
    async def dec_messages(tg_id):
        u = await DB.get_user(tg_id)
        if u and u.get("daily_messages_remaining", 0) > 9000: return
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(daily_messages_remaining=func.greatest(User.daily_messages_remaining - 1, 0)))
            await s.commit()

    @staticmethod
    async def use_boost(uid) -> bool:
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u or (u.boost_count or 0) <= 0: return False
            now = datetime.utcnow()
            ne = (u.boost_expires_at + timedelta(hours=24)) if u.boost_expires_at and u.boost_expires_at > now else now + timedelta(hours=24)
            await s.execute(update(User).where(User.id == uid).values(boost_count=User.boost_count - 1, boost_expires_at=ne))
            await s.commit(); return True

    @staticmethod
    async def add_boosts(uid, c):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.id == uid).values(boost_count=User.boost_count + c)); await s.commit()

    @staticmethod
    async def activate_trial(uid) -> bool:
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u or u.trial_used: return False
            await s.execute(update(User).where(User.id == uid).values(subscription_tier=SubscriptionTier.VIP_LIGHT, subscription_expires_at=datetime.utcnow() + timedelta(days=3), trial_used=True))
            await s.commit(); return True

    @staticmethod
    async def process_referral(new_uid, ref_code):
        async with async_session_maker() as s:
            r = await s.execute(select(User).where(User.referral_code == ref_code.upper()))
            ref = r.scalar_one_or_none()
            if not ref or ref.id == new_uid: return
            await s.execute(update(User).where(User.id == ref.id).values(boost_count=User.boost_count + 1, referral_bonus_count=User.referral_bonus_count + 1))
            await s.execute(update(User).where(User.id == new_uid).values(daily_likes_remaining=User.daily_likes_remaining + 5, referred_by=ref.id))
            await s.commit()

    @staticmethod
    async def send_icebreaker(mid):
        async with async_session_maker() as s:
            await s.execute(update(Match).where(Match.id == mid).values(icebreaker_sent=True))
            await s.commit()

    @staticmethod
    async def toggle_invisible(tg_id) -> bool:
        u = await DB.get_user(tg_id)
        if not u: return False
        new_val = not u.get("is_invisible", False)
        await DB.update_user(tg_id, is_invisible=new_val)
        return new_val

    @staticmethod
    async def create_report(rid, ruid, reason):
        async with async_session_maker() as s:
            s.add(Report(reporter_id=rid, reported_user_id=ruid, reason=reason)); await s.commit()

    @staticmethod
    async def get_stats():
        async with async_session_maker() as s:
            total = (await s.execute(select(func.count(User.id)))).scalar() or 0
            complete = (await s.execute(select(func.count(User.id)).where(User.is_profile_complete == True))).scalar() or 0
            now = datetime.utcnow(); day_ago = now - timedelta(days=1); month_ago = now - timedelta(days=30)
            dau = (await s.execute(select(func.count(User.id)).where(User.last_active_at > day_ago))).scalar() or 0
            wau = (await s.execute(select(func.count(User.id)).where(User.last_active_at > now - timedelta(days=7)))).scalar() or 0
            mau = (await s.execute(select(func.count(User.id)).where(User.last_active_at > month_ago))).scalar() or 0
            vip = (await s.execute(select(func.count(User.id)).where(User.subscription_tier != SubscriptionTier.FREE))).scalar() or 0
            banned = (await s.execute(select(func.count(User.id)).where(User.is_banned == True))).scalar() or 0
            today_reg = (await s.execute(select(func.count(User.id)).where(User.created_at > day_ago))).scalar() or 0
            matches = (await s.execute(select(func.count(Match.id)))).scalar() or 0
            msgs = (await s.execute(select(func.count(ChatMessage.id)))).scalar() or 0
            likes = (await s.execute(select(func.count(Like.id)))).scalar() or 0
            revenue = (await s.execute(select(func.sum(Payment.amount)).where(Payment.status == PaymentStatus.SUCCEEDED))).scalar() or 0
            mrev = (await s.execute(select(func.sum(Payment.amount)).where(and_(Payment.status == PaymentStatus.SUCCEEDED, Payment.paid_at > month_ago)))).scalar() or 0
            trials = (await s.execute(select(func.count(User.id)).where(User.trial_used == True))).scalar() or 0
            reports = (await s.execute(select(func.count(Report.id)).where(Report.status == "pending"))).scalar() or 0
            return {"total": total, "complete": complete, "dau": dau, "wau": wau, "mau": mau, "vip": vip, "banned": banned, "today_reg": today_reg, "matches": matches, "messages": msgs, "likes": likes, "trials": trials, "revenue": revenue / 100 if revenue else 0, "month_revenue": mrev / 100 if mrev else 0, "pending_reports": reports, "conversion": (vip / complete * 100) if complete > 0 else 0}

    @staticmethod
    async def search_users(query):
        async with async_session_maker() as s:
            if query.isdigit(): r = await s.execute(select(User).where(or_(User.id == int(query), User.telegram_id == int(query))))
            else: r = await s.execute(select(User).where(or_(User.username.ilike(f"%{query}%"), User.name.ilike(f"%{query}%"))).limit(10))
            return [DB._to_dict(u) for u in r.scalars().all()]

    @staticmethod
    async def get_all_user_ids(ft="all"):
        async with async_session_maker() as s:
            q = select(User.telegram_id).where(and_(User.is_active == True, User.is_banned == False))
            if ft == "complete": q = q.where(User.is_profile_complete == True)
            elif ft == "vip": q = q.where(User.subscription_tier != SubscriptionTier.FREE)
            elif ft == "free": q = q.where(User.subscription_tier == SubscriptionTier.FREE)
            r = await s.execute(q); return [row[0] for row in r.fetchall()]

    @staticmethod
    async def get_pending_reports(limit=10):
        async with async_session_maker() as s:
            r = await s.execute(select(Report).where(Report.status == "pending").order_by(Report.created_at.desc()).limit(limit))
            out = []
            for rep in r.scalars().all():
                reporter = await DB.get_user_by_id(rep.reporter_id); reported = await DB.get_user_by_id(rep.reported_user_id)
                out.append({"id": rep.id, "reason": rep.reason, "created_at": rep.created_at, "reporter": reporter, "reported": reported})
            return out

    @staticmethod
    async def resolve_report(rid, action, notes=""):
        async with async_session_maker() as s:
            await s.execute(update(Report).where(Report.id == rid).values(status=action, admin_notes=notes, resolved_at=datetime.utcnow())); await s.commit()

    @staticmethod
    async def activate_subscription_by_id(uid, tier, days):
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid)); u = ur.scalar_one_or_none()
            if not u: return
            te = SubscriptionTier(tier); now = datetime.utcnow()
            exp = None if te == SubscriptionTier.VIP_LIFETIME else ((u.subscription_expires_at + timedelta(days=days)) if u.subscription_expires_at and u.subscription_expires_at > now else now + timedelta(days=days))
            await s.execute(update(User).where(User.id == uid).values(subscription_tier=te, subscription_expires_at=exp)); await s.commit()

    @staticmethod
    async def create_payment(uid, yid, amount, desc, ptype, ptier=None, pdur=None, pcount=None):
        async with async_session_maker() as s:
            p = Payment(user_id=uid, yookassa_payment_id=yid, amount=amount, description=desc, product_type=ptype, product_tier=ptier, product_duration=pdur, product_count=pcount)
            s.add(p); await s.commit(); await s.refresh(p); return p.id

    @staticmethod
    async def get_payment(pid):
        async with async_session_maker() as s:
            r = await s.execute(select(Payment).where(Payment.id == pid)); p = r.scalar_one_or_none()
            return {"id": p.id, "user_id": p.user_id, "yookassa_payment_id": p.yookassa_payment_id, "status": p.status.value, "product_type": p.product_type, "product_tier": p.product_tier, "product_duration": p.product_duration, "product_count": p.product_count} if p else None

    @staticmethod
    async def update_payment_status(pid, st):
        async with async_session_maker() as s:
            v = {"status": st}; v["paid_at"] = datetime.utcnow() if st == PaymentStatus.SUCCEEDED else v.get("paid_at")
            await s.execute(update(Payment).where(Payment.id == pid).values(**v)); await s.commit()

    @staticmethod
    async def create_promo(code, tier, days, max_uses):
        async with async_session_maker() as s:
            s.add(PromoCode(code=code.upper(), tier=tier, duration_days=days, max_uses=max_uses)); await s.commit()

    @staticmethod
    async def use_promo(user_id, code):
        async with async_session_maker() as s:
            r = await s.execute(select(PromoCode).where(and_(PromoCode.code == code.upper(), PromoCode.is_active == True)))
            promo = r.scalar_one_or_none()
            if not promo: return {"error": "Не найден"}
            if promo.used_count >= promo.max_uses: return {"error": "Исчерпан"}
            used = await s.execute(select(PromoUse).where(and_(PromoUse.promo_id == promo.id, PromoUse.user_id == user_id)))
            if used.scalar_one_or_none(): return {"error": "Уже использован"}
            s.add(PromoUse(promo_id=promo.id, user_id=user_id))
            await s.execute(update(PromoCode).where(PromoCode.id == promo.id).values(used_count=PromoCode.used_count + 1)); await s.commit()
            await DB.activate_subscription_by_id(user_id, promo.tier, promo.duration_days)
            return {"success": True, "tier": promo.tier, "days": promo.duration_days}

    @staticmethod
    async def log_broadcast(admin_id, text, target, sent, failed):
        async with async_session_maker() as s:
            s.add(BroadcastLog(admin_id=admin_id, message_text=text, target_filter=target, sent_count=sent, failed_count=failed)); await s.commit()

# ==========================================
# ANTI-SPAM & PAY
# ==========================================
class AntiSpam:
    def __init__(self): self.u = {}
    async def check(self, uid, act, limit=5, tw=60):
        now = time.time(); k = f"{uid}:{act}"
        if k not in self.u: self.u[k] = []
        self.u[k] = [t for t in self.u[k] if now - t < tw]
        if len(self.u[k]) >= limit: return False
        self.u[k].append(now); return True

anti_spam = AntiSpam()

class Pay:
    @staticmethod
    async def create(user, ptype, tier=None, dur=None, count=None, amount=0):
        if not YOOKASSA_AVAILABLE or not config.YOOKASSA_SHOP_ID: return {"error": "ЮKassa не настроена"}
        desc = TIER_NAMES.get(tier, '') if ptype == "subscription" else f"Буст×{count}"
        try:
            p = YooPayment.create({"amount": {"value": f"{amount/100:.2f}", "currency": "RUB"}, "confirmation": {"type": ConfirmationType.REDIRECT, "return_url": f"{config.DOMAIN}/ok"}, "capture": True, "description": desc}, str(uuid.uuid4()))
            pid = await DB.create_payment(user["id"], p.id, amount, desc, ptype, tier, dur, count)
            return {"pid": pid, "url": p.confirmation.confirmation_url}
        except Exception as e: return {"error": str(e)}

    @staticmethod
    async def check(pid):
        p = await DB.get_payment(pid)
        if not p: return {"status": "not_found"}
        try:
            y = YooPayment.find_one(p["yookassa_payment_id"])
            if y.status == "succeeded" and p["status"] != "succeeded":
                await DB.update_payment_status(pid, PaymentStatus.SUCCEEDED)
                if p["product_type"] == "subscription":
                    await DB.activate_subscription_by_id(p["user_id"], p["product_tier"], p["product_duration"] or 30)
                    return {"status": "succeeded", "type": "subscription"}
                elif p["product_type"] == "boost":
                    await DB.add_boosts(p["user_id"], p.get("product_count") or 1)
                    return {"status": "succeeded", "type": "boost", "count": p.get("product_count", 1)}
            return {"status": y.status}
        except: return {"status": "error"}

# ==========================================
# KEYBOARDS
# ==========================================
class KB:
    @staticmethod
    def main(unread=0, hidden=0):
        return ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="❤️ Анкеты"), KeyboardButton(text=f"💝 Симпатии{f' ({hidden})' if hidden else ''}")],
            [KeyboardButton(text=f"💬 Чаты{f' ({unread})' if unread else ''}"), KeyboardButton(text="👀 Гости")],
            [KeyboardButton(text="💎 Магазин"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="🎁 Награды"), KeyboardButton(text="❓ FAQ")],
        ], resize_keyboard=True)

    @staticmethod
    def gender(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👨 М", callback_data="g:male"), InlineKeyboardButton(text="👩 Ж", callback_data="g:female")]])

    @staticmethod
    def looking(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👨", callback_data="l:male"), InlineKeyboardButton(text="👩", callback_data="l:female")], [InlineKeyboardButton(text="🌈 Всех", callback_data="l:both")]])

    @staticmethod
    def skip(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip")]])

    @staticmethod
    def interests(sel=None):
        if not sel: sel = set()
        rows = []
        for i in range(0, len(Compatibility.INTERESTS_LIST), 2):
            row = []
            for j in range(2):
                if i+j < len(Compatibility.INTERESTS_LIST):
                    item = Compatibility.INTERESTS_LIST[i+j]
                    row.append(InlineKeyboardButton(text=f"{'✅' if item in sel else ''}{item}", callback_data=f"int:{i+j}"))
            rows.append(row)
        rows.append([InlineKeyboardButton(text="✅ Готово", callback_data="int:done")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def search(uid, sl=False):
        row = [InlineKeyboardButton(text="👎", callback_data=f"dl:{uid}"), InlineKeyboardButton(text="❤️", callback_data=f"lk:{uid}")]
        if sl: row.insert(1, InlineKeyboardButton(text="⭐", callback_data=f"sl:{uid}"))
        return InlineKeyboardMarkup(inline_keyboard=[row, [InlineKeyboardButton(text="⚠️", callback_data=f"rp:{uid}")]])

    @staticmethod
    def no_profiles(vip=False):
        b = [[InlineKeyboardButton(text="🌍 Другие города", callback_data="sr:expand")]]
        b.append([InlineKeyboardButton(text="🔄 Сброс" + (" (VIP)" if not vip else ""), callback_data="sr:reset" if vip else "sr:reset_locked")])
        b += [[InlineKeyboardButton(text="🔄 Ещё", callback_data="sr:retry")], [InlineKeyboardButton(text="🔙 В меню", callback_data="mn")]]
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def matches(ms):
        b = [[InlineKeyboardButton(text=f"{m['online']} {m['name']},{m['age']}{' '+str(int(m['compat']))+'%' if m.get('compat') else ''}{' 💬'+str(m['unread']) if m.get('unread') else ''}", callback_data=f"ch:{m['user_id']}")] for m in ms[:10]]
        b.append([InlineKeyboardButton(text="🔙 В меню", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def chat_actions(mid, pid, icebreaker_sent=False, msg_count=0):
        b = []
        if not icebreaker_sent and msg_count == 0:
            b.append([InlineKeyboardButton(text="🧊 Айсбрейкер", callback_data=f"ice:{mid}:{pid}")])
        b += [[InlineKeyboardButton(text="🔙 Мэтчи", callback_data="bm")], [InlineKeyboardButton(text="💔 Размэтч", callback_data=f"um:{mid}")]]
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def shop():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👑 VIP", callback_data="sh:subs"), InlineKeyboardButton(text="🚀 Буст", callback_data="sh:boost")],
            [InlineKeyboardButton(text="⚖️ Сравнить", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🎟 Промо", callback_data="sh:promo"), InlineKeyboardButton(text="🎁 Пробный", callback_data="trial:start")],
            [InlineKeyboardButton(text="🤝 Друзья", callback_data="referral:info")],
            [InlineKeyboardButton(text="🔙 В меню", callback_data="mn")]])

    @staticmethod
    def subs():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⭐ Винчик LIGHT — от 149₽", callback_data="tf:vip_light")],
            [InlineKeyboardButton(text="🌟 Винчик STANDARD — от 349₽", callback_data="tf:vip_standard")],
            [InlineKeyboardButton(text="👑 Винчик PRO — от 599₽", callback_data="tf:vip_pro")],
            [InlineKeyboardButton(text="💎 FOREVER — Навсегда", callback_data="tf:vip_lifetime")],
            [InlineKeyboardButton(text="🎁 3 дня бесплатно!", callback_data="trial:start")],
            [InlineKeyboardButton(text="⚖️ Сравнить все", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🔙 В магазин", callback_data="sh:mn")]
        ])

    @staticmethod
    def buy(tier):
        prices = {
            "vip_light": [
                (30, 14900, "1 месяц — 149₽"),
                (90, 37900, "3 месяца — 379₽ (🔥 -15%)"),
                (180, 64900, "6 месяцев — 649₽ (💎 -25%)")
            ],
            "vip_standard": [
                (30, 34900, "1 месяц — 349₽"),
                (90, 84900, "3 месяца — 849₽ (🔥 -19%)"),
                (180, 144900, "6 месяцев — 1449₽ (💎 -30%)")
            ],
            "vip_pro": [
                (30, 59900, "1 месяц — 599₽"),
                (90, 149900, "3 месяца — 1499₽ (🔥 -16%)"),
                (180, 259900, "6 месяцев — 2599₽ (💎 -27%)")
            ],
            "vip_lifetime": [
                (0, 299900, "💎 Навсегда — 2999₽")
            ]
        }
        
        b = []
        if tier != "vip_lifetime": 
            b.append([InlineKeyboardButton(text="🎁 Попробовать 3 дня бесплатно", callback_data="trial:start")])
            
        for dur, price, label in prices.get(tier, []):
            b.append([InlineKeyboardButton(text=label, callback_data=f"by:{tier}:{dur}:{price}")])
            
        b.append([InlineKeyboardButton(text="🔙 К выбору тарифа", callback_data="sh:subs")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def profile(vip=False, hidden=0):
        b = [[InlineKeyboardButton(text="✏️", callback_data="pe"), InlineKeyboardButton(text="📸", callback_data="ed:photo"), InlineKeyboardButton(text="🎨", callback_data="ed:interests")],
             [InlineKeyboardButton(text="🚀 Буст", callback_data="profile:boost"), InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")]]
        if vip: b.append([InlineKeyboardButton(text=f"❤️ Лайки ({hidden})", callback_data="likes:list")])
        elif hidden: b.append([InlineKeyboardButton(text=f"❤️ {hidden} лайков 👑 VIP", callback_data="sh:subs")])
        b.append([InlineKeyboardButton(text="🏆 Достижения", callback_data="achievements"), InlineKeyboardButton(text="🤝 Друзья", callback_data="referral:info")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def settings(user):
        inv = "🟢 Невидимка ❌" if user.get("is_invisible") else "⚪ Невидимка ✅"
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=inv, callback_data="set:invisible")],
            [InlineKeyboardButton(text="📏 Диапазон", callback_data="ed:agerange")],
            [InlineKeyboardButton(text="🎯 Кого ищу", callback_data="ed:looking")],
            [InlineKeyboardButton(text="🔙 Профиль", callback_data="pv")]])

    @staticmethod
    def edit():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Имя", callback_data="ed:name"), InlineKeyboardButton(text="🎂 Возраст", callback_data="ed:age"), InlineKeyboardButton(text="🏙 Город", callback_data="ed:city")],
            [InlineKeyboardButton(text="✍️ О себе", callback_data="ed:bio")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="pv")]])

    @staticmethod
    def report_reasons():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Спам", callback_data="rr:spam"), InlineKeyboardButton(text="🎭 Фейк", callback_data="rr:fake")],
            [InlineKeyboardButton(text="🔞 18+", callback_data="rr:nsfw"), InlineKeyboardButton(text="🤬 Оскорб", callback_data="rr:harass")],
            [InlineKeyboardButton(text="🔙 Отмена", callback_data="mn")]])

    @staticmethod
    def admin(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📊 Стата", callback_data="adm:stats"), InlineKeyboardButton(text="🔍 Поиск", callback_data="adm:search")], [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:broadcast"), InlineKeyboardButton(text="🚩 Репорты", callback_data="adm:reports")], [InlineKeyboardButton(text="🎟 Промокоды", callback_data="adm:promo")], [InlineKeyboardButton(text="🔙 Выход", callback_data="mn")]])

    @staticmethod
    def admin_user(uid, banned):
        ban = InlineKeyboardButton(text="✅ Разбан", callback_data=f"au:unban:{uid}") if banned else InlineKeyboardButton(text="🚫 Бан", callback_data=f"au:ban:{uid}")
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👑 Дать VIP", callback_data=f"au:givevip:{uid}"), InlineKeyboardButton(text="🚀 Дать Буст", callback_data=f"au:giveboost:{uid}")], [ban], [InlineKeyboardButton(text="🔙 Админка", callback_data="adm:main")]])

    @staticmethod
    def give_vip_tiers(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⭐ Light", callback_data="gv:vip_light"), InlineKeyboardButton(text="🌟 Standard", callback_data="gv:vip_standard")], [InlineKeyboardButton(text="👑 Pro", callback_data="gv:vip_pro"), InlineKeyboardButton(text="💎 Forever", callback_data="gv:vip_lifetime")], [InlineKeyboardButton(text="🔙 Админка", callback_data="adm:main")]])

    @staticmethod
    def back_admin(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="adm:main")]])

    @staticmethod
    def broadcast_targets(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🌍 Всем", callback_data="bc:all"), InlineKeyboardButton(text="Только Free", callback_data="bc:free")], [InlineKeyboardButton(text="🔙 Отмена", callback_data="adm:main")]])

    @staticmethod
    def broadcast_confirm(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Отправить", callback_data="bc:send")], [InlineKeyboardButton(text="🔙 Отмена", callback_data="adm:main")]])

# ==========================================
# MIDDLEWARE & ROUTER
# ==========================================
class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        tg = event.from_user if isinstance(event, (Message, CallbackQuery)) else None
        u = None
        if tg:
            u = await DB.get_user(tg.id)
            if u:
                u = await DB.reset_limits(u)
                if u.get("is_banned"):
                    if isinstance(event, Message): await event.answer("🚫 Ваш аккаунт заблокирован.")
                    return
        data["user"] = u
        return await handler(event, data)

rt = Router()

# ==========================================
# HELPERS
# ==========================================
def profile_text(u):
    badge = DB.get_badge(u); sub = TIER_NAMES.get(u["subscription_tier"], "🆓")
    if u.get("subscription_expires_at") and u["subscription_tier"] not in ("free", "vip_lifetime"):
        sub += f" (до {u['subscription_expires_at'].strftime('%d.%m')})"
    bi = ""
    if DB.is_boosted(u): bi += f"\n🚀 Буст до {u['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if u.get("boost_count"): bi += f" · 🚀{u['boost_count']}"
    streak = f"\n⚡ Серия: {u['streak_days']} дней" if u.get("streak_days", 0) > 1 else ""
    online = DB.get_online_status(u)
    return (f"👤 *Профиль* {online}\n\n{badge}*{u['name']}*, {u['age']}\n📍 {u['city']}\n"
            f"{u['bio'] or '_Нет описания_'}\n{'🎨 '+u['interests'] if u.get('interests') else ''}\n\n"
            f"👀 {u['views_count']} · ❤️ {u['likes_received_count']} · 💕 {u['matches_count']}"
            f"\n🎯 {u['age_from']}-{u['age_to']}\nСтатус: {sub}{bi}{streak}")

def card_text(p, v):
    badge = DB.get_badge(p); boost = " 🚀" if DB.is_boosted(p) else ""
    compat = Compatibility.calc_score(v, p); online = DB.get_online_status(p)
    pb = ""
    if p.get("_priority") == "liked_you":
        pb = "\n❤️ _Лайкнул(а) тебя!_" if DB.is_vip(v) else "\n❤️ _Может быть взаимно..._"
    elif p.get("_priority") == "other_city": pb = "\n🌍 _Из другого города_"
    return (f"{badge}*{p['name']}*{boost}, {p['age']} {online}\n📍 {p['city']}{pb}\n"
            f"{p['bio'] or ''}\n{'🎨 '+p['interests'] if p.get('interests') else ''}\n\n"
            f"💖 *{compat:.0f}%*")

# ==========================================
# HANDLERS — СТАРТ И КРАСИВАЯ РЕГИСТРАЦИЯ
# ==========================================
@rt.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    ref_code = msg.text.split()[1] if msg.text and len(msg.text.split()) > 1 else None

    if user and user.get("is_profile_complete"):
        un = await DB.get_unread(user["id"])
        who = await DB.get_who_liked_me(user["id"]); await DB.update_hidden_likes(user["id"])
        
        # Серия дней и награда
        streak, reward = await Gamification.update_streak(user)
        reward_text = ""
        if reward:
            claimed = await Gamification.claim_reward(user["id"], user["telegram_id"], reward)
            if claimed: reward_text = f"\n\n🎁 *Награда за {streak} день:* {claimed}"

        # Достижения
        user = await DB.get_user(msg.from_user.id)
        achievements = await Gamification.check_achievements(user)
        ach_text = ""
        if achievements:
            ach_text = "\n\n🏆 *Новые достижения:*\n" + "\n".join(f"🏅 {a['title']} — {a['desc']}" for a in achievements)

        st = TIER_NAMES.get(user["subscription_tier"], "🆓")
        if DB.is_boosted(user): st += " · 🚀"
        extras = ""
        if who and not DB.is_vip(user): extras = f"\n❤️ *{len(who)} скрытых лайков!*"
        elif who: extras = f"\n❤️ Лайков: {len(who)}"

        await msg.answer(
            f"👋 *С возвращением, {user['name']}!*\n\n{st}\n👀 {user['views_count']} просмотров · 💕 {user['matches_count']} мэтчей · 💬 {un} сообщений"
            f"\n⚡ Серия: {streak} дн{extras}{reward_text}{ach_text}",
            reply_markup=KB.main(un, len(who)), parse_mode=ParseMode.MARKDOWN)
    else:
        if not user:
            user = await DB.create_user(msg.from_user.id, msg.from_user.username)
        if ref_code: await DB.process_referral(user["id"], ref_code)
        
        welcome_text = (
            f"🍷 *Добро пожаловать в {BOT_NAME}!*\n\n"
            "Здесь встречаются классные люди, завязываются интересные диалоги и случаются настоящие мэтчи. 💕\n\n"
            "Давай создадим твою анкету, чтобы другие могли тебя найти!\n\n"
            "📝 *Для начала, как к тебе обращаться?*"
        )
        await msg.answer(welcome_text, parse_mode=ParseMode.MARKDOWN, reply_markup=ReplyKeyboardRemove())
        await state.set_state(RegStates.name)

@rt.message(RegStates.name)
async def rn(msg: Message, state: FSMContext):
    n = msg.text.strip()
    if len(n) < 2 or len(n) > 50:
        await msg.answer("❌ Имя должно быть от 2 до 50 символов. Попробуй еще раз:")
    else:
        await state.update_data(name=n)
        await msg.answer(f"Приятно познакомиться, *{n}*! ✨\n\n🎂 *Сколько тебе лет?*\n_Укажи цифрами, например: 21_", parse_mode=ParseMode.MARKDOWN)
        await state.set_state(RegStates.age)

@rt.message(RegStates.age)
async def ra(msg: Message, state: FSMContext):
    try:
        a = int(msg.text.strip()); assert 18 <= a <= 99
    except:
        return await msg.answer("❌ Пожалуйста, введи корректный возраст цифрами (от 18 до 99).")
    await state.update_data(age=a)
    await msg.answer("🚻 *Укажи свой пол:*", reply_markup=KB.gender(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.gender)

@rt.callback_query(RegStates.gender, F.data.startswith("g:"))
async def rg(call: CallbackQuery, state: FSMContext):
    await state.update_data(gender=call.data[2:])
    await call.message.edit_text("🏙 *Из какого ты города?*\n_Напиши название, например: Москва_", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.city)
    await call.answer()

@rt.message(RegStates.city)
async def rc(msg: Message, state: FSMContext):
    c = msg.text.strip().title()
    if len(c) < 2:
        await msg.answer("❌ Название города слишком короткое. Попробуй еще раз:")
    else:
        await state.update_data(city=c)
        await msg.answer(
            "📸 *Покажи себя!*\n\n"
            "Отправь своё лучшее фото. \n"
            "💡 _Секрет: Фотографии с искренней улыбкой получают на 40% больше лайков!_", 
            reply_markup=KB.skip(), parse_mode=ParseMode.MARKDOWN
        )
        await state.set_state(RegStates.photo)

@rt.message(RegStates.photo, F.photo)
async def rp(msg: Message, state: FSMContext):
    await state.update_data(photo=msg.photo[-1].file_id)
    await msg.answer(
        "✍️ *Расскажи немного о себе!*\n\n"
        "Кого ищешь? Чем увлекаешься? Хорошее описание — залог отличного мэтча.\n"
        "_Можно пропустить, но мы очень советуем написать хотя бы пару слов._", 
        reply_markup=KB.skip(), parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(RegStates.bio)

@rt.callback_query(RegStates.photo, F.data=="skip")
async def rps(call: CallbackQuery, state: FSMContext):
    await state.update_data(photo=None)
    await call.message.edit_text(
        "✍️ *Расскажи немного о себе!*\n\n"
        "Кого ищешь? Чем увлекаешься? Хорошее описание — залог отличного мэтча.\n"
        "_Можно пропустить, но мы очень советуем написать хотя бы пару слов._", parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(RegStates.bio)
    await call.answer()

@rt.message(RegStates.bio)
async def rb(msg: Message, state: FSMContext):
    await state.update_data(bio=msg.text.strip()[:500])
    await msg.answer("🎨 *Выбери свои интересы*\n_Это поможет нам подбирать тебе идеальные пары:_", reply_markup=KB.interests(), parse_mode=ParseMode.MARKDOWN)
    await state.update_data(si=set())
    await state.set_state(RegStates.interests)

@rt.callback_query(RegStates.bio, F.data=="skip")
async def rbs(call: CallbackQuery, state: FSMContext):
    await state.update_data(bio="")
    await call.message.edit_text("🎨 *Выбери свои интересы*\n_Это поможет нам подбирать тебе идеальные пары:_", reply_markup=KB.interests(), parse_mode=ParseMode.MARKDOWN)
    await state.update_data(si=set())
    await state.set_state(RegStates.interests)
    await call.answer()

@rt.callback_query(RegStates.interests, F.data.startswith("int:"))
async def ri(call: CallbackQuery, state: FSMContext):
    v=call.data[4:]
    if v=="done":
        d=await state.get_data()
        await state.update_data(interests=",".join(d.get("si",set())))
        await call.message.edit_text("🎯 *Кого ты хочешь найти?*", reply_markup=KB.looking(), parse_mode=ParseMode.MARKDOWN)
        await state.set_state(RegStates.looking_for)
    else:
        d=await state.get_data(); sel=d.get("si",set()); item=Compatibility.INTERESTS_LIST[int(v)]
        sel.discard(item) if item in sel else sel.add(item)
        await state.update_data(si=sel)
        await call.message.edit_reply_markup(reply_markup=KB.interests(sel))
        await call.answer()

@rt.callback_query(RegStates.looking_for, F.data.startswith("l:"))
async def rl(call: CallbackQuery, state: FSMContext):
    await state.update_data(lf=call.data[2:])
    await call.message.edit_text(
        "📏 *В каком возрасте ищем?*\n\n"
        "Отправь диапазон через дефис.\n"
        "_Например: 18-25_", 
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(RegStates.age_range)
    await call.answer()

@rt.message(RegStates.age_range)
async def rar(msg: Message, state: FSMContext):
    try:
        parts=msg.text.strip().replace(" ","").split("-")
        af,at=int(parts[0]),int(parts[1])
        assert 18<=af<=99 and 18<=at<=99 and af<=at
    except:
        return await msg.answer("❌ Ошибка формата. Напиши диапазон вот так: `18-25`", parse_mode=ParseMode.MARKDOWN)
        
    d=await state.get_data()
    upd={
        "name":d["name"], "age":d["age"], "gender":Gender(d["gender"]),
        "city":d["city"], "bio":d.get("bio",""), "interests":d.get("interests",""),
        "looking_for":LookingFor(d["lf"]), "age_from":af, "age_to":at,
        "is_profile_complete":True
    }
    if d.get("photo"):
        upd["photos"]=d["photo"]
        upd["main_photo"]=d["photo"]
        
    await DB.update_user(msg.from_user.id, **upd)
    
    # Создаем красивую карточку результата
    gender_emoji = "👨" if d["gender"] == "male" else "👩"
    
    finish_text = (
        f"🎉 *Анкета успешно создана!*\n\n"
        f"Вот как тебя увидят другие:\n"
        f"━━━━━━━━━━━━━━\n"
        f"{gender_emoji} *{d['name']}*, {d['age']}\n"
        f"📍 {d['city']}\n"
        f"{d.get('bio', '')}\n"
        f"🎨 {', '.join(d.get('si', set()))}\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"🎁 *Сюрприз!* Мы дарим тебе 3 дня VIP-доступа абсолютно бесплатно, чтобы ты оценил(а) все фишки.\n\n"
        f"Жми кнопку ниже и начинай знакомиться! 👇"
    )
    
    await state.clear()
    
    if d.get("photo"):
        await msg.answer_photo(photo=d["photo"], caption=finish_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Забрать VIP и начать!", callback_data="trial:start")]
        ]), parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer(finish_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Забрать VIP и начать!", callback_data="trial:start")]
        ]), parse_mode=ParseMode.MARKDOWN)

# ==========================================
# BROWSE
# ==========================================
@rt.message(F.text=="❤️ Анкеты")
async def browse(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return await msg.answer("❌ Сначала зарегистрируйся: /start")
    await state.set_state(SearchStates.browsing)
    ps=await DB.search_profiles(user,5)
    if not ps: return await msg.answer("🛑 *Анкеты закончились!*", reply_markup=KB.no_profiles(DB.is_vip(user)), parse_mode=ParseMode.MARKDOWN)
    await state.update_data(sq=[p["id"] for p in ps[1:]])
    await show_card(msg, ps[0], user)

async def show_card(msg, p, v):
    if not v.get("is_invisible"): await DB.add_guest(v["id"], p["id"])
    txt=card_text(p, v)
    sl=v.get("daily_superlikes_remaining",0)>0
    if p.get("main_photo"):
        await msg.answer_photo(photo=p["main_photo"], caption=txt, reply_markup=KB.search(p["id"],sl), parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer(txt, reply_markup=KB.search(p["id"],sl), parse_mode=ParseMode.MARKDOWN)

async def next_card(ev, state: FSMContext, user: Dict):
    d=await state.get_data(); q=d.get("sq",[])
    if q:
        nid=q.pop(0)
        await state.update_data(sq=q)
        p=await DB.get_user_by_id(nid)
        if p and p.get("is_active") and not p.get("is_banned"):
            p["_priority"]="normal"
            p["_compat"]=Compatibility.calc_score(user,p)
            msg=ev.message if isinstance(ev,CallbackQuery) else ev
            return await show_card(msg,p,user)
            
    ps=await DB.search_profiles(user,5)
    msg=ev.message if isinstance(ev,CallbackQuery) else ev
    if ps:
        await state.update_data(sq=[p["id"] for p in ps[1:]])
        await show_card(msg,ps[0],user)
    else:
        await msg.answer("🛑 *Закончились!*", reply_markup=KB.no_profiles(DB.is_vip(user)), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("lk:"))
async def h_like(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return await call.answer("❌ Ошибка профиля")
    if not DB.is_vip(user) and user.get("daily_likes_remaining",0)<=0:
        t,k=Monetization.get_likes_limit_msg(user)
        try: await call.message.edit_caption(caption=t, reply_markup=k, parse_mode=ParseMode.MARKDOWN)
        except: await call.message.edit_text(t, reply_markup=k, parse_mode=ParseMode.MARKDOWN)
        return
        
    if not await anti_spam.check(call.from_user.id,"like"): return await call.answer("⚠️ Не так быстро!",show_alert=True)
    
    tid=int(call.data[3:])
    res=await DB.add_like(user["id"],tid)
    await DB.dec_likes(user["telegram_id"])
    
    user=await DB.get_user(call.from_user.id)
    w=Monetization.get_low_likes_warning(user.get("daily_likes_remaining",0))
    if w and not DB.is_vip(user):
        await call.message.answer(w, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👑 Безлимит", callback_data="sh:subs")]]), parse_mode=ParseMode.MARKDOWN)
        
    if res["is_match"]:
        t=await DB.get_user_by_id(tid)
        tn=t["name"] if t else "?"
        compat=res.get("compat",0)
        try: await call.message.edit_caption(caption=f"🔥 *Мэтч с {tn}!* \n💖 Совместимость: {compat}%", parse_mode=ParseMode.MARKDOWN)
        except: await call.message.edit_text(f"🔥 *Мэтч с {tn}!* \n💖 Совместимость: {compat}%", parse_mode=ParseMode.MARKDOWN)
        if t:
            try: await call.bot.send_message(t["telegram_id"], f"🔥 *У вас новый мэтч с {user['name']}!* \n💖 Совместимость: {compat}%", parse_mode=ParseMode.MARKDOWN)
            except: pass
            
        # Достижения
        user=await DB.get_user(call.from_user.id)
        achs=await Gamification.check_achievements(user)
        if achs: await call.message.answer("🏆 Открыто достижение:\n" + "\n".join(a["title"] for a in achs))
    else:
        await call.answer("❤️ Лайк отправлен")
        
    await next_card(call, state, user)
    await call.answer()

@rt.callback_query(F.data.startswith("sl:"))
async def h_superlike(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    if user.get("daily_superlikes_remaining",0)<=0: return await call.answer("⭐ Суперлайки закончились. Доступно для VIP!", show_alert=True)
    
    tid=int(call.data[3:])
    res=await DB.add_like(user["id"],tid,is_super=True)
    await DB.dec_superlikes(user["telegram_id"])
    await DB.dec_likes(user["telegram_id"])
    
    t=await DB.get_user_by_id(tid)
    if res["is_match"]:
        tn=t["name"] if t else "?"
        compat=res.get("compat",0)
        try: await call.message.edit_caption(caption=f"🔥 *Мэтч с {tn}!* \n💖 {compat}%", parse_mode=ParseMode.MARKDOWN)
        except: pass
        if t:
            try: await call.bot.send_message(t["telegram_id"], f"🔥 *Мэтч с {user['name']}!* ", parse_mode=ParseMode.MARKDOWN)
            except: pass
    else:
        await call.answer("⭐ Суперлайк отправлен!")
        if t:
            try: await call.bot.send_message(t["telegram_id"], f"⭐ *{user['name']}* отправил(а) тебе суперлайк!", parse_mode=ParseMode.MARKDOWN)
            except: pass
            
    user=await DB.get_user(call.from_user.id)
    await next_card(call, state, user)
    await call.answer()

@rt.callback_query(F.data.startswith("dl:"))
async def h_dislike(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    await DB.add_dislike(user["id"], int(call.data[3:]))
    await next_card(call, state, user)
    await call.answer()

@rt.callback_query(F.data=="sr:expand")
async def sr_expand(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    ps=await DB.search_profiles(user,5)
    if ps:
        await state.update_data(sq=[p["id"] for p in ps[1:]])
        await state.set_state(SearchStates.browsing)
        await show_card(call.message,ps[0],user)
    else:
        await call.message.edit_text("⏳ Анкет пока нет. Загляни позже!")
    await call.answer()

@rt.callback_query(F.data=="sr:reset")
async def sr_reset(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user or not DB.is_vip(user): return await call.answer("👑 Доступно только для VIP!", show_alert=True)
    cnt=await DB.reset_dislikes(user["id"])
    await call.answer(f"🔄 История очищена. Сброшено {cnt} анкет!", show_alert=True)
    user=await DB.get_user(call.from_user.id)
    ps=await DB.search_profiles(user,5)
    if ps:
        await state.update_data(sq=[p["id"] for p in ps[1:]])
        await state.set_state(SearchStates.browsing)
        await show_card(call.message,ps[0],user)

@rt.callback_query(F.data=="sr:reset_locked")
async def sr_locked(call: CallbackQuery, user: Optional[Dict]):
    await call.message.answer("👑 *Сброс дизлайков доступен только VIP-пользователям!*\n\n🌟 От 349₽/мес", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🌟 Тариф Standard", callback_data="tf:vip_standard")],[InlineKeyboardButton(text="🎁 Попробовать бесплатно", callback_data="trial:start")]]), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="sr:retry")
async def sr_retry(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    ps=await DB.search_profiles(user,5)
    if ps:
        await state.update_data(sq=[p["id"] for p in ps[1:]])
        await state.set_state(SearchStates.browsing)
        await show_card(call.message,ps[0],user)
    else:
        await call.answer("⏳ Новых анкет пока нет", show_alert=True)
    await call.answer()

# ==========================================
# MATCHES, CHAT, ICEBREAKER
# ==========================================
@rt.message(F.text.startswith("💬"))
async def show_chats(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return
    ms=await DB.get_matches(user["id"])
    if ms: await msg.answer("💬 *Твои диалоги:*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else: await msg.answer("⏳ У тебя пока нет активных диалогов.")

@rt.message(F.text.startswith("💝"))
async def show_matches(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return await msg.answer("❌ Для начала зарегистрируйся: /start")
    ms=await DB.get_matches(user["id"])
    if ms: await msg.answer(f"💕 *Твои мэтчи ({len(ms)}):*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else: await msg.answer("⏳ У тебя пока нет мэтчей. Продолжай ставить лайки!")

@rt.callback_query(F.data.startswith("ch:"))
async def start_chat(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    pid=int(call.data[3:])
    p=await DB.get_user_by_id(pid)
    if not p: return await call.answer("Пользователь не найден")
    mid=await DB.get_match_between(user["id"],pid)
    if not mid: return await call.answer("Мэтч больше не существует")
    
    await DB.mark_read(mid, user["id"])
    msgs=await DB.get_msgs(mid,5)
    ms = await DB.get_matches(user["id"])
    match_info = next((m for m in ms if m["user_id"] == pid), {})
    
    txt=f"👤 *{p['name']}* {DB.get_online_status(p)}\n\n"
    for mg in msgs:
        sn="Вы" if mg["sender_id"]==user["id"] else p["name"]
        txt+=f"*{sn}:* {mg['text']}\n"
    if not msgs: txt+="_Напиши первым!_"
    
    await state.update_data(cp=pid,mi=mid)
    await state.set_state(ChatStates.chatting)
    await call.message.edit_text(txt, reply_markup=KB.chat_actions(mid, pid, match_info.get("icebreaker_sent", False), match_info.get("msg_count", 0)), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data.startswith("ice:"))
async def send_icebreaker(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    """Отправить айсбрейкер"""
    if not user: return
    parts = call.data.split(":")
    mid, pid = int(parts[1]), int(parts[2])
    icebreaker = Gamification.get_icebreaker()
    await DB.send_msg(mid, user["id"], icebreaker)
    await DB.send_icebreaker(mid)
    p = await DB.get_user_by_id(pid)
    if p:
        try: await call.bot.send_message(p["telegram_id"], f"💬 *{user['name']}:* {icebreaker}", parse_mode=ParseMode.MARKDOWN)
        except: pass
    await call.message.answer(f"🧊 Айсбрейкер отправлен:\n_{icebreaker}_", parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.message(ChatStates.chatting)
async def send_msg(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d=await state.get_data()
    mid,pid=d.get("mi"),d.get("cp")
    if not mid:
        await state.clear()
        return await msg.answer("Чат закрыт", reply_markup=KB.main())
        
    if not DB.is_vip(user) and user.get("daily_messages_remaining",0)<=0:
        p=await DB.get_user_by_id(pid)
        t,k=Monetization.get_msg_limit_msg(p["name"] if p else "")
        return await msg.answer(t, reply_markup=k, parse_mode=ParseMode.MARKDOWN)
        
    photo_id = None
    if msg.photo:
        photo_id = msg.photo[-1].file_id
        await DB.send_msg(mid, user["id"], "[Фото]", photo_id=photo_id)
    else:
        if not msg.text: return
        await DB.send_msg(mid, user["id"], msg.text)
        
    await DB.dec_messages(user["telegram_id"])
    p=await DB.get_user_by_id(pid)
    
    if p:
        try:
            if photo_id:
                await msg.bot.send_photo(p["telegram_id"], photo=photo_id, caption=f"💬 *{user['name']}*", parse_mode=ParseMode.MARKDOWN)
            else:
                await msg.bot.send_message(p["telegram_id"], f"💬 *{user['name']}:* {msg.text}", parse_mode=ParseMode.MARKDOWN)
        except: pass
        
    # Достижение «первое сообщение»
    user=await DB.get_user(msg.from_user.id)
    achs=await Gamification.check_achievements(user)
    if achs: await msg.answer("🏆 " + ", ".join(a["title"] for a in achs))

@rt.callback_query(F.data.startswith("um:"))
async def unmatch(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    await DB.unmatch(user["id"],int(call.data[3:]))
    await state.clear()
    await call.message.edit_text("💔 Мэтч удален.")
    await call.answer()

@rt.callback_query(F.data=="bm")
async def back_matches(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user: return
    ms=await DB.get_matches(user["id"])
    if ms: await call.message.edit_text(f"💕 *Твои мэтчи ({len(ms)}):*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else: await call.message.edit_text("⏳ Нет мэтчей")
    await call.answer()

@rt.message(F.text=="👀 Гости")
async def show_guests(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return
    lim=20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE
    gs=await DB.get_guests(user["id"],lim)
    if not gs: return await msg.answer("⏳ У тебя пока нет гостей профиля.")
    txt="👀 *Твои гости:*\n\n"
    for i,g in enumerate(gs,1): txt+=f"{i}. {g['name']}, {g['age']} — {g['city']}\n"
    if not DB.is_vip(user): txt+="\n👑 _Больше гостей доступно с VIP-статусом._"
    await msg.answer(txt, parse_mode=ParseMode.MARKDOWN)

# ==========================================
# WHO LIKED & TRIAL & REFERRAL
# ==========================================
@rt.callback_query(F.data=="likes:list")
async def who_liked(call: CallbackQuery, user: Optional[Dict]):
    if not user: return
    if not DB.is_vip(user):
        who=await DB.get_who_liked_me(user["id"])
        t,k=Monetization.get_hidden_likes_msg(len(who))
        await call.message.answer(t, reply_markup=k, parse_mode=ParseMode.MARKDOWN)
        return await call.answer()
        
    users=await DB.get_likes_received(user["id"],20)
    if not users: return await call.answer("⏳ Новых лайков пока нет", show_alert=True)
    b = [[InlineKeyboardButton(text=f"{'⭐' if u.get('is_super_like') else '❤️'} {u['name']},{u['age']}", callback_data=f"wl:{u['id']}")] for u in users[:10]]
    b.append([InlineKeyboardButton(text="🔙 В меню", callback_data="mn")])
    await call.message.edit_text(f"❤️ *Твои лайки ({len(users)}):*", reply_markup=InlineKeyboardMarkup(inline_keyboard=b), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data.startswith("wl:"))
async def who_liked_view(call: CallbackQuery, user: Optional[Dict]):
    if not user or not DB.is_vip(user): return
    p=await DB.get_user_by_id(int(call.data[3:]))
    if not p: return await call.answer("Пользователь не найден")
    txt=card_text(p, user)
    if p.get("like_message"): txt += f"\n\n💌 _{p['like_message']}_"
    await call.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❤️ Ответить взаимностью", callback_data=f"lk:{p['id']}"), InlineKeyboardButton(text="👎", callback_data=f"dl:{p['id']}")],
        [InlineKeyboardButton(text="🔙 К списку лайков", callback_data="likes:list")]
    ]), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="trial:start")
async def trial(call: CallbackQuery, user: Optional[Dict]):
    if not user: return
    if DB.is_vip(user): return await call.answer("👑 У тебя уже есть VIP!", show_alert=True)
    if user.get("trial_used"): return await call.answer("❌ Пробный период уже был использован!", show_alert=True)
    ok=await DB.activate_trial(user["id"])
    if ok:
        await call.message.answer("🎉 *VIP Light на 3 дня активирован!*\n\n❤️ 100 лайков ежедневно\n💬 Безлимитные сообщения\n⭐ 1 суперлайк в день\n\nЧерез 3 дня статус автоматически отключится.", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)
    else:
        await call.answer("❌ Пробный период уже использован", show_alert=True)
    await call.answer()

@rt.callback_query(F.data=="referral:info")
async def ref_info(call: CallbackQuery, user: Optional[Dict]):
    if not user: return
    code=user.get("referral_code","???"); bonus=user.get("referral_bonus_count",0)
    me = await call.bot.me()
    link=f"https://t.me/{me.username}?start={code}"
    await call.message.answer(f"🤝 *Пригласи друга и получи бонусы!*\n\nТвоя ссылка:\n`{link}`\n\n🎁 Тебе: 🚀 1 Буст\n🎁 Другу: +5 лайков к лимиту\n\nПриглашено друзей: *{bonus}*",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📤 Отправить другу", url=f"https://t.me/share/url?url={link}&text=Тут классные знакомства, присоединяйся!")],
            [InlineKeyboardButton(text="🔙 В профиль", callback_data="pv")]
        ]), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

# ==========================================
# REWARDS & ACHIEVEMENTS
# ==========================================
@rt.message(F.text=="🎁 Награды")
async def show_rewards(msg: Message, user: Optional[Dict]):
    if not user: return
    streak = user.get("streak_days", 0)
    day_in_cycle = ((streak - 1) % 7) + 1 if streak > 0 else 1
    txt = f"🎁 *Твои ежедневные награды*\n\n⚡ Серия заходов: *{streak} дней подряд*\n\n"
    for day, reward in Gamification.DAILY_REWARDS.items():
        marker = "✅" if day < day_in_cycle else ("🎯" if day == day_in_cycle else "⏳")
        txt += f"{marker} День {day}: {reward['text']}\n"
    txt += f"\n_Заходи в бота каждый день, чтобы забирать крутые бонусы!_"
    await msg.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Мои достижения", callback_data="achievements")],
    ]), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data=="achievements")
async def show_achievements(call: CallbackQuery, user: Optional[Dict]):
    if not user: return
    async with async_session_maker() as s:
        earned = await s.execute(select(Achievement.achievement_type).where(Achievement.user_id == user["id"]))
        earned_set = set(r[0] for r in earned.fetchall())
    txt = "🏆 *Твои достижения*\n\n"
    for ach_type, info in Gamification.ACHIEVEMENTS.items():
        status = "✅" if ach_type in earned_set else "🔒"
        txt += f"{status} {info['title']} — _{info['desc']}_\n"
    txt += f"\n\nОткрыто: *{len(earned_set)} из {len(Gamification.ACHIEVEMENTS)}*"
    await call.message.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 В профиль", callback_data="pv")]
    ]), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

# ==========================================
# PROFILE & SETTINGS
# ==========================================
@rt.message(F.text=="👤 Профиль")
async def show_profile(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return await msg.answer("❌ Сначала создай анкету: /start")
    await DB.update_hidden_likes(user["id"]); user=await DB.get_user(msg.from_user.id)
    txt=profile_text(user); hidden=user.get("hidden_likes_count",0)
    if user.get("main_photo"):
        await msg.answer_photo(photo=user["main_photo"], caption=txt, reply_markup=KB.profile(DB.is_vip(user),hidden), parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer(txt, reply_markup=KB.profile(DB.is_vip(user),hidden), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data=="pv")
async def back_profile(call: CallbackQuery, user: Optional[Dict]):
    if not user: return
    await DB.update_hidden_likes(user["id"])
    user=await DB.get_user(call.from_user.id)
    txt=profile_text(user); hidden=user.get("hidden_likes_count",0)
    try: await call.message.edit_caption(caption=txt, reply_markup=KB.profile(DB.is_vip(user),hidden), parse_mode=ParseMode.MARKDOWN)
    except: await call.message.edit_text(txt, reply_markup=KB.profile(DB.is_vip(user),hidden), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="pe")
async def pe(call: CallbackQuery):
    try: await call.message.edit_caption(caption="✏️ Редактирование анкеты", reply_markup=KB.edit())
    except: await call.message.edit_text("✏️ Редактирование анкеты", reply_markup=KB.edit())
    await call.answer()

@rt.callback_query(F.data=="settings")
async def settings(call: CallbackQuery, user: Optional[Dict]):
    if not user: return
    inv_note = "\n\n🥷 _Режим Невидимки позволяет смотреть чужие анкеты, не оставляя следов в разделе Гости._" if DB.is_vip(user) else "\n\n🥷 _Режим Невидимки доступен только для VIP пользователей._"
    await call.message.answer(f"⚙️ *Настройки профиля*{inv_note}", reply_markup=KB.settings(user), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="set:invisible")
async def toggle_invisible(call: CallbackQuery, user: Optional[Dict]):
    if not user: return
    if not DB.is_vip(user): return await call.answer("👑 Режим Невидимки доступен только с VIP!", show_alert=True)
    new_val = await DB.toggle_invisible(user["telegram_id"])
    await call.answer(f"🥷 Невидимка {'включена ✅' if new_val else 'выключена ❌'}", show_alert=True)
    user = await DB.get_user(call.from_user.id)
    await call.message.edit_reply_markup(reply_markup=KB.settings(user))

@rt.callback_query(F.data=="ed:looking")
async def edit_looking(call: CallbackQuery, state: FSMContext):
    await call.message.answer("🎯 Кого ты хочешь искать?", reply_markup=KB.looking())
    await state.update_data(editing_looking=True)
    await call.answer()

@rt.callback_query(F.data.startswith("l:"))
async def handle_looking_edit(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d = await state.get_data()
    if d.get("editing_looking"):
        lf = call.data[2:]
        await DB.update_user(call.from_user.id, looking_for=LookingFor(lf))
        await state.update_data(editing_looking=False)
        await call.message.edit_text(f"🎯 Теперь мы ищем: {'👨 Мужчин' if lf=='male' else '👩 Женщин' if lf=='female' else '🌈 Всех подряд'}")
        await call.answer()

@rt.callback_query(F.data=="ed:name")
async def en(call: CallbackQuery, state: FSMContext): 
    await call.message.answer("📝 Введи свое новое имя:")
    await state.set_state(EditStates.edit_name)
    await call.answer()

@rt.message(EditStates.edit_name)
async def sn(msg: Message, state: FSMContext):
    if len(msg.text.strip())<2: return await msg.answer("❌ Имя слишком короткое.")
    await DB.update_user(msg.from_user.id, name=msg.text.strip())
    await state.clear()
    await msg.answer("✅ Имя успешно обновлено!", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:age")
async def ea(call: CallbackQuery, state: FSMContext): 
    await call.message.answer("🎂 Напиши свой реальный возраст цифрами:")
    await state.set_state(EditStates.edit_age)
    await call.answer()

@rt.message(EditStates.edit_age)
async def sa(msg: Message, state: FSMContext):
    try: a=int(msg.text.strip()); assert 18<=a<=99
    except: return await msg.answer("❌ Введи число от 18 до 99.")
    await DB.update_user(msg.from_user.id, age=a)
    await state.clear()
    await msg.answer("✅ Возраст успешно обновлен!", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:city")
async def ec(call: CallbackQuery, state: FSMContext): 
    await call.message.answer("🏙 Напиши свой новый город:")
    await state.set_state(EditStates.edit_city)
    await call.answer()

@rt.message(EditStates.edit_city)
async def sc(msg: Message, state: FSMContext): 
    await DB.update_user(msg.from_user.id, city=msg.text.strip().title())
    await state.clear()
    await msg.answer("✅ Город успешно обновлен!", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:bio")
async def eb(call: CallbackQuery, state: FSMContext): 
    await call.message.answer("✍️ Напиши пару слов о себе:")
    await state.set_state(EditStates.edit_bio)
    await call.answer()

@rt.message(EditStates.edit_bio)
async def sb(msg: Message, state: FSMContext): 
    await DB.update_user(msg.from_user.id, bio=msg.text.strip()[:500])
    await state.clear()
    await msg.answer("✅ Описание профиля обновлено!", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:interests")
async def ei(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    cur=set(i.strip() for i in (user.get("interests") or "").split(",") if i.strip())
    await state.update_data(si=cur)
    await call.message.answer("🎨 Выбери свои интересы:", reply_markup=KB.interests(cur))
    await state.set_state(EditStates.edit_interests)
    await call.answer()

@rt.callback_query(EditStates.edit_interests, F.data.startswith("int:"))
async def si(call: CallbackQuery, state: FSMContext):
    v=call.data[4:]
    if v=="done":
        d=await state.get_data()
        await DB.update_user(call.from_user.id, interests=",".join(d.get("si",set())))
        await state.clear()
        await call.message.edit_text("✅ Интересы сохранены.")
        await call.message.answer("✅ Отлично!", reply_markup=KB.main())
        user=await DB.get_user(call.from_user.id)
        achs=await Gamification.check_achievements(user)
        if achs: await call.message.answer("🏆 Открыто достижение:\n" + "\n".join(a["title"] for a in achs))
    else:
        d=await state.get_data(); sel=d.get("si",set()); item=Compatibility.INTERESTS_LIST[int(v)]
        sel.discard(item) if item in sel else sel.add(item)
        await state.update_data(si=sel)
        await call.message.edit_reply_markup(reply_markup=KB.interests(sel))
        await call.answer()

@rt.callback_query(F.data=="ed:agerange")
async def ear(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    await call.message.answer(f"🎯 Твой текущий фильтр: {user['age_from']}-{user['age_to']}\nНапиши новый диапазон (например, `18-30`):", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(EditStates.edit_age_range)
    await call.answer()

@rt.message(EditStates.edit_age_range)
async def sar(msg: Message, state: FSMContext):
    try: 
        p=msg.text.strip().replace(" ","").split("-")
        af,at=int(p[0]),int(p[1])
        assert 18<=af<=99 and 18<=at<=99 and af<=at
    except: 
        return await msg.answer("❌ Ошибка. Напиши формат вот так: `18-30`", parse_mode=ParseMode.MARKDOWN)
    await DB.update_user(msg.from_user.id, age_from=af, age_to=at)
    await state.clear()
    await msg.answer(f"✅ Теперь ищем людей от {af} до {at} лет.", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:photo")
async def ep(call: CallbackQuery, state: FSMContext): 
    await call.message.answer("📸 Отправь мне своё лучшее фото:")
    await state.set_state(EditStates.add_photo)
    await call.answer()

@rt.message(EditStates.add_photo, F.photo)
async def sp(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    pid=msg.photo[-1].file_id
    pl=[p for p in (user.get("photos","") or "").split(",") if p.strip()]
    if len(pl)>=5: 
        await state.clear()
        return await msg.answer("❌ Максимум 5 фото. Удали старые, чтобы добавить новые.", reply_markup=KB.main())
    pl.append(pid)
    await DB.update_user(msg.from_user.id, photos=",".join(pl), main_photo=pid)
    await state.clear()
    await msg.answer("📸 Главное фото успешно обновлено!", reply_markup=KB.main())
    user=await DB.get_user(msg.from_user.id)
    achs=await Gamification.check_achievements(user)
    if achs: await msg.answer("🏆 Открыто достижение:\n" + "\n".join(a["title"] for a in achs))

# ==========================================
# ПРЕМИУМ МАГАЗИН И ТАРИФЫ
# ==========================================
@rt.message(F.text=="💎 Магазин")
async def shop(msg: Message):
    txt = (
        "🛍 *Магазин премиум-функций*\n\n"
        "Прокачай свой профиль, чтобы получать в 5 раз больше внимания и находить идеальные мэтчи быстрее!"
    )
    await msg.answer(txt, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data=="sh:mn")
async def shmn(call: CallbackQuery): 
    txt = (
        "🛍 *Магазин премиум-функций*\n\n"
        "Прокачай свой профиль, чтобы получать в 5 раз больше внимания и находить идеальные мэтчи быстрее!"
    )
    await call.message.edit_text(txt, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="sh:compare")
async def shcmp(call: CallbackQuery):
    txt = (
        "⚖️ *Краткое сравнение тарифов*\n\n"
        "⭐ *LIGHT*\n"
        "100 лайков, безлимит сообщений и 1 Суперлайк в день. Отличный старт.\n\n"
        "🌟 *STANDARD* 🏆 _Хит продаж_\n"
        "Скрытые лайки (видишь кто тебя лайкнул), безлимит анкет, режим Невидимки.\n\n"
        "👑 *PRO*\n"
        "Твоя анкета всегда в ТОПе. Максимум показов, 5 Суперлайков в день и 3 бесплатных Буста каждый месяц.\n\n"
        "💎 *FOREVER*\n"
        "Уровень PRO навсегда за один платеж."
    )
    await call.message.edit_text(txt, reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="sh:subs")
async def shsubs(call: CallbackQuery): 
    txt = (
        "💎 *ПРЕМИУМ КЛУБ*\n\n"
        "Выбери статус, который подходит именно тебе. Нажми на тариф, чтобы узнать, что внутри 👇"
    )
    await call.message.edit_text(txt, reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data.startswith("tf:"))
async def tf(call: CallbackQuery):
    tier = call.data[3:]
    descs = {
        "vip_light": (
            "⭐ *ТАРИФ LIGHT* \n"
            "_Сними базовые ограничения_\n\n"
            "❤️ *100 лайков* в день\n"
            "💬 *Безлимитные* сообщения мэтчам\n"
            "👁 До 10 гостей профиля\n"
            "🌟 *1 Суперлайк* ежедневно\n"
            "🚀 Приоритет в поиске\n\n"
            "👇 _Выбери период:_"
        ),
        "vip_standard": (
            "🌟 *ТАРИФ STANDARD*\n"
            "_Самый популярный выбор_ 🏆\n\n"
            "Включает всё из Light, а также:\n"
            "💖 *Скрытые лайки:* сразу видишь, кому ты нравишься!\n"
            "♾ *Полный безлимит* на лайки и гостей\n"
            "🥷 *Невидимка:* смотри анкеты без следов\n"
            "🌟 *2 Суперлайка* ежедневно\n\n"
            "👇 _Выбери период:_"
        ),
        "vip_pro": (
            "👑 *ТАРИФ PRO*\n"
            "_Для тех, кто забирает лучшее_\n\n"
            "Включает всё из Standard, а также:\n"
            "🔥 *Ты всегда в ТОПе:* твою анкету видят первой\n"
            "🚀 *3 бесплатных Буста* каждый месяц\n"
            "🌟 *5 Суперлайков* ежедневно\n"
            "👑 Эксклюзивная корона в профиле\n\n"
            "👇 _Выбери период:_"
        ),
        "vip_lifetime": (
            "💎 *ТАРИФ FOREVER*\n"
            "_Максимальный статус навсегда_\n\n"
            "Получи **ВСЕ** привилегии тарифа PRO навсегда.\n"
            "Никаких ежемесячных списаний. Оплати один раз — наслаждайся знакомствами без границ вечно.\n\n"
            "👇 _Оформить доступ:_"
        ),
    }
    await call.message.edit_text(descs.get(tier, ""), reply_markup=KB.buy(tier), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="sh:boost")
async def shboost(call: CallbackQuery, user: Optional[Dict]):
    if not user: return
    has = user.get("boost_count", 0) > 0
    act = DB.is_boosted(user)
    
    st = f"\n🔥 Активен до: {user['boost_expires_at'].strftime('%d.%m %H:%M')}" if act else ""
    if has: st += f"\n📦 В запасе: {user['boost_count']} шт."
    if not has and not act: st = "\n❌ Сейчас у тебя нет Бустов."
    
    txt = (
        "🚀 *ПРОФИЛЬ В ТОП-1*\n\n"
        "Твоя анкета будет показываться первой в твоем городе целых 24 часа! "
        "Это увеличивает количество просмотров и мэтчей **до 5 раз**.\n"
        f"{st}"
    )
    
    bk = InlineKeyboardMarkup(inline_keyboard=(
        ([[InlineKeyboardButton(text="🚀 Активировать Буст", callback_data="bo:act")]] if has else []) + 
        [
            [InlineKeyboardButton(text="1 Буст — 39₽", callback_data="by:boost:1:3900")],
            [InlineKeyboardButton(text="5 Бустов — 149₽ (🔥 Выгода)", callback_data="by:boost:5:14900")],
            [InlineKeyboardButton(text="10 Бустов — 249₽ (💎 Супер-цена)", callback_data="by:boost:10:24900")],
            [InlineKeyboardButton(text="🔙 В магазин", callback_data="sh:mn")]
        ]
    ))
    await call.message.edit_text(txt, reply_markup=bk, parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="profile:boost")
async def pboost(call: CallbackQuery, user: Optional[Dict]):
    if not user: return
    has=user.get("boost_count",0)>0; act=DB.is_boosted(user)
    st = f"\n🔥 Активен до: {user['boost_expires_at'].strftime('%d.%m %H:%M')}" if act else ""
    if has: st+=f"\n📦 В запасе: {user['boost_count']} шт."
    if not has and not act: st="\n❌ Сейчас у тебя нет Бустов."
    
    bk=InlineKeyboardMarkup(inline_keyboard=(
        ([[InlineKeyboardButton(text="🚀 Активировать", callback_data="bo:act:profile")]] if has else []) + 
        [[InlineKeyboardButton(text="1 Буст — 39₽", callback_data="by:boost:1:3900"), InlineKeyboardButton(text="5 Бустов — 149₽", callback_data="by:boost:5:14900")],
         [InlineKeyboardButton(text="🔙 В профиль", callback_data="pv")]]
    ))
    try: await call.message.edit_caption(caption=f"🚀 *БУСТ ПРОФИЛЯ*\nТвоя анкета в ТОП-1 на 24 часа.{st}", reply_markup=bk, parse_mode=ParseMode.MARKDOWN)
    except: await call.message.edit_text(f"🚀 *БУСТ ПРОФИЛЯ*\nТвоя анкета в ТОП-1 на 24 часа.{st}", reply_markup=bk, parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data.startswith("bo:act"))
async def act_boost(call: CallbackQuery, user: Optional[Dict]):
    if not user or user.get("boost_count",0)<=0: return await call.answer("❌ У тебя нет бустов!", show_alert=True)
    ok=await DB.use_boost(user["id"])
    if ok:
        u=await DB.get_user(call.from_user.id)
        back="pv" if ":profile" in call.data else "sh:mn"
        try: await call.message.edit_caption(caption=f"🚀 *Буст активирован!*\nТеперь ты в ТОПе до {u['boost_expires_at'].strftime('%d.%m %H:%M')}\nВ запасе: {u['boost_count']} шт.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data=back)]]), parse_mode=ParseMode.MARKDOWN)
        except: await call.message.edit_text(f"✅ Буст успешно активирован!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data=back)]]))
    await call.answer()

@rt.callback_query(F.data.startswith("by:"))
async def handle_buy(call: CallbackQuery, user: Optional[Dict]):
    if not user: return await call.answer("❌")
    parts=call.data.split(":")
    prod,param,amt=parts[1],int(parts[2]),int(parts[3])
    res = await Pay.create(user, "boost", count=param, amount=amt) if prod=="boost" else await Pay.create(user, "subscription", tier=prod, dur=param, amount=amt)
    if "error" in res: return await call.answer(f"❌ {res['error']}", show_alert=True)
    await call.message.edit_text(f"💳 *К оплате: {amt//100}₽*\n\n1️⃣ Нажми кнопку оплаты ниже\n2️⃣ Заверши платеж в системе ЮKassa\n3️⃣ Нажми «Проверить»", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💳 Оплатить", url=res["url"])],[InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"ck:{res['pid']}")],[InlineKeyboardButton(text="🔙 Отмена", callback_data="sh:mn")]]), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data.startswith("ck:"))
async def check_pay(call: CallbackQuery):
    res=await Pay.check(int(call.data[3:]))
    if res["status"]=="succeeded":
        t="🎉 *Оплата прошла успешно!*\nПодписка активирована. Наслаждайся!" if res.get("type")!="boost" else f"🚀 *Успешно!*\nНачислено: {res.get('count',1)} бустов."
        await call.message.edit_text(t, parse_mode=ParseMode.MARKDOWN)
        await call.message.answer("🎉 Спасибо за покупку!", reply_markup=KB.main())
    elif res["status"]=="pending": 
        await call.answer("⏳ Платеж еще обрабатывается. Подожди пару минут и попробуй снова.", show_alert=True)
    else: 
        await call.answer("❌ Оплата не найдена или отменена.", show_alert=True)
    await call.answer()

# ==========================================
# PROMO & FAQ
# ==========================================
@rt.callback_query(F.data=="sh:promo")
async def sh_promo(call: CallbackQuery, state: FSMContext): 
    await call.message.edit_text("🎟 *Введи промокод:*\n_Напиши его в чат_", parse_mode=ParseMode.MARKDOWN)
    await state.update_data(pum=True)
    await state.set_state(AdminStates.promo_code)
    await call.answer()

@rt.message(AdminStates.promo_code)
async def promo_input(msg: Message, state: FSMContext, user: Optional[Dict]):
    d=await state.get_data()
    if d.get("pum"):
        code=msg.text.strip().upper()
        await state.clear()
        if not user: return
        res=await DB.use_promo(user["id"],code)
        if "error" in res: 
            await msg.answer(f"❌ Ошибка: {res['error']}", reply_markup=KB.main())
        else: 
            await msg.answer(f"🎉 Промокод сработал!\nАктивирован статус *{TIER_NAMES.get(res['tier'],'VIP')}* на {res['days']} дней! ", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)
        return
        
    if not user or not DB.is_admin(user): return
    await state.update_data(pc_code=msg.text.strip().upper())
    await msg.answer("Тариф для промокода:", reply_markup=KB.give_vip_tiers())
    await state.set_state(AdminStates.promo_tier)

@rt.callback_query(AdminStates.promo_tier, F.data.startswith("gv:"))
async def pt(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user or not DB.is_admin(user): return
    await state.update_data(pc_tier=call.data[3:])
    await call.message.edit_text("На сколько дней даем подписку?")
    await state.set_state(AdminStates.promo_duration)
    await call.answer()

@rt.message(AdminStates.promo_duration)
async def pd(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not DB.is_admin(user): return
    try: days=int(msg.text.strip())
    except: return await msg.answer("❌ Введи число.")
    await state.update_data(pc_days=days)
    await msg.answer("Лимит использований (кол-во активаций)?")
    await state.set_state(AdminStates.promo_uses)

@rt.message(AdminStates.promo_uses)
async def pu(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not DB.is_admin(user): return
    try: uses=int(msg.text.strip())
    except: return await msg.answer("❌ Введи число.")
    d=await state.get_data()
    await DB.create_promo(d["pc_code"],d["pc_tier"],d["pc_days"],uses)
    await state.clear()
    await msg.answer(f"✅ Промокод создан!\n\nКод: `{d['pc_code']}`\nДней: {d['pc_days']}\nИспользований: {uses}", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("rp:"))
async def start_report(call: CallbackQuery, state: FSMContext):
    await state.update_data(rp_id=int(call.data[3:]))
    try: await call.message.edit_caption(caption="🚩 *Выбери причину жалобы:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    except: await call.message.edit_text("🚩 *Выбери причину жалобы:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data.startswith("rr:"))
async def save_report(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d=await state.get_data()
    rid=d.get("rp_id")
    if rid: await DB.create_report(user["id"], rid, call.data[3:])
    await state.clear()
    try: await call.message.edit_caption(caption="✅ Жалоба отправлена модераторам. Спасибо!")
    except: await call.message.edit_text("✅ Жалоба отправлена модераторам. Спасибо!")
    await next_card(call, state, user)
    await call.answer()

@rt.callback_query(F.data=="mn")
async def back_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    try: await call.message.delete()
    except: pass
    await call.message.answer("🔙 Возврат в меню", reply_markup=KB.main())
    await call.answer()

# ==========================================
# ADMIN
# ==========================================
def is_adm(u): return u and u.get("telegram_id") in config.ADMIN_IDS

@rt.message(Command("admin"))
async def admin_cmd(msg: Message, user: Optional[Dict]):
    if not is_adm(user): return
    await msg.answer(f"👑 *Панель администратора*", reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data=="adm:main")
async def adm_main(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.clear()
    await call.message.edit_text("👑 *Панель администратора*", reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="adm:stats")
async def adm_stats(call: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    st=await DB.get_stats()
    txt = (f"📊 *Глобальная статистика*\n\n👥 Всего/Анкеты: {st['total']}/{st['complete']}\n"
           f"📈 DAU: {st['dau']} | WAU: {st['wau']} | MAU: {st['mau']}\n"
           f"👑 VIP: {st['vip']} (Конверсия: {st['conversion']:.1f}%)\n"
           f"🎁 Пробные: {st['trials']}\n"
           f"🚫 В бане: {st['banned']}\n"
           f"🆕 Новых сегодня: +{st['today_reg']}\n\n"
           f"❤️ Лайков: {st['likes']}\n💕 Мэтчей: {st['matches']}\n💬 Сообщений: {st['messages']}\n\n"
           f"💳 Выручка: {st['revenue']:.0f}₽ (Месяц: {st['month_revenue']:.0f}₽)\n"
           f"🚩 Открытых репортов: {st['pending_reports']}")
    await call.message.edit_text(txt, reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="adm:search")
async def adm_search(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await call.message.edit_text("🔍 Введи ID пользователя, username или Имя:")
    await state.set_state(AdminStates.search_user)
    await call.answer()

@rt.message(AdminStates.search_user)
async def adm_search_result(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    results=await DB.search_users(msg.text.strip())
    await state.clear()
    if not results: return await msg.answer("❌ Никто не найден.", reply_markup=KB.back_admin())
    u=results[0]
    await msg.answer(f"ID:`{u['id']}` @{u.get('username') or '-'}\n{DB.get_badge(u)}*{u['name']}*,{u['age']} {u['city']}\n{TIER_NAMES.get(u['subscription_tier'],'')}\n👀 Просмотры: {u['views_count']} ❤️Лайки: {u['likes_received_count']} 💕Мэтчи: {u['matches_count']}", reply_markup=KB.admin_user(u["id"], u["is_banned"]), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("au:ban:"))
async def adm_ban(call: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    u=await DB.get_user_by_id(int(call.data.split(":")[2]))
    if u: 
        await DB.update_user(u["telegram_id"], is_banned=True)
        await call.message.edit_text("🚫 Пользователь забанен!", reply_markup=KB.back_admin())
    await call.answer()

@rt.callback_query(F.data.startswith("au:unban:"))
async def adm_unban(call: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    u=await DB.get_user_by_id(int(call.data.split(":")[2]))
    if u: 
        await DB.update_user(u["telegram_id"], is_banned=False)
        await call.message.edit_text("✅ Пользователь разбанен!", reply_markup=KB.back_admin())
    await call.answer()

@rt.callback_query(F.data.startswith("au:verify:"))
async def adm_verify(call: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    u=await DB.get_user_by_id(int(call.data.split(":")[2]))
    if u: 
        await DB.update_user(u["telegram_id"], is_verified=True)
        await call.message.edit_text("✅ Пользователь верифицирован!", reply_markup=KB.back_admin())
    await call.answer()

@rt.callback_query(F.data.startswith("au:givevip:"))
async def adm_givevip(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.update_data(target_uid=int(call.data.split(":")[2]))
    await call.message.edit_text("Какой тариф выдать?", reply_markup=KB.give_vip_tiers())
    await call.answer()

@rt.callback_query(F.data.startswith("gv:"))
async def adm_gv(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    tier=call.data[3:]
    if tier=="vip_lifetime":
        d=await state.get_data()
        if d.get("target_uid"): await DB.activate_subscription_by_id(d["target_uid"], tier, 0)
        await state.clear()
        await call.message.edit_text("✅ VIP навсегда выдан!", reply_markup=KB.back_admin())
    else:
        await state.update_data(give_tier=tier)
        await call.message.edit_text("На сколько дней выдать?")
        await state.set_state(AdminStates.give_vip_duration)
    await call.answer()

@rt.message(AdminStates.give_vip_duration)
async def adm_gvd(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    try: days=int(msg.text.strip())
    except: return await msg.answer("❌ Нужно число.")
    d=await state.get_data()
    await DB.activate_subscription_by_id(d["target_uid"],d["give_tier"],days)
    await state.clear()
    await msg.answer(f"✅ VIP выдан на {days} дней!", reply_markup=KB.main())

@rt.callback_query(F.data.startswith("au:giveboost:"))
async def adm_gb(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.update_data(target_uid=int(call.data.split(":")[2]))
    await call.message.edit_text("Сколько бустов выдать?")
    await state.set_state(AdminStates.give_boost_count)
    await call.answer()

@rt.message(AdminStates.give_boost_count)
async def adm_gbc(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    try: c=int(msg.text.strip())
    except: return await msg.answer("❌ Нужно число.")
    d=await state.get_data()
    await DB.add_boosts(d["target_uid"],c)
    await state.clear()
    await msg.answer(f"✅ Выдано {c} бустов!", reply_markup=KB.main())

@rt.callback_query(F.data=="adm:reports")
async def adm_reports(call: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    reps=await DB.get_pending_reports(5)
    if not reps: 
        await call.message.edit_text("✅ Новых репортов нет!", reply_markup=KB.back_admin())
        return await call.answer()
        
    rep=reps[0]
    rdn=rep["reported"]["name"] if rep["reported"] else "?"
    rid=rep["reported"]["id"] if rep["reported"] else 0
    await call.message.edit_text(f"🚩 Репорт #{rep['id']}\nНа кого: {rdn} (ID:{rid})\nПричина: {rep['reason']}\nОсталось в очереди: {len(reps)}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Забанить", callback_data=f"ar:ban:{rep['id']}:{rid}"), InlineKeyboardButton(text="✅ Отклонить", callback_data=f"ar:dismiss:{rep['id']}:{rid}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="adm:reports")]
    ]), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data.startswith("ar:"))
async def adm_report_action(call: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    parts=call.data.split(":")
    action,rid,ruid=parts[1],int(parts[2]),int(parts[3])
    if action=="ban":
        u=await DB.get_user_by_id(ruid)
        if u: await DB.update_user(u["telegram_id"], is_banned=True)
        await DB.resolve_report(rid,"banned")
        await call.message.edit_text("🚫 Нарушитель заблокирован.", reply_markup=KB.back_admin())
    elif action=="dismiss":
        await DB.resolve_report(rid,"dismissed")
        await call.message.edit_text("✅ Репорт отклонен.", reply_markup=KB.back_admin())
    await call.answer()

@rt.callback_query(F.data=="adm:broadcast")
async def adm_bc(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await call.message.edit_text("📢 *Введи текст рассылки:*\n_Поддерживается Markdown разметка_", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.broadcast_text)
    await call.answer()

@rt.message(AdminStates.broadcast_text)
async def adm_bct(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.update_data(bc_text=msg.text)
    await msg.answer("Кому отправляем?", reply_markup=KB.broadcast_targets())
    await state.set_state(AdminStates.broadcast_confirm)

@rt.callback_query(AdminStates.broadcast_confirm, F.data.startswith("bc:"))
async def adm_bcs(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    target=call.data[3:]
    if target=="send":
        d=await state.get_data()
        txt=d["bc_text"]
        tgt=d.get("bc_target","all")
        ids=await DB.get_all_user_ids(tgt)
        await state.clear()
        await call.message.edit_text(f"🚀 Запуск рассылки для {len(ids)} пользователей...")
        sent=failed=0
        for tid in ids:
            try: 
                await call.bot.send_message(tid, txt, parse_mode=ParseMode.MARKDOWN)
                sent+=1
            except: 
                failed+=1
            if sent%25==0: await asyncio.sleep(1)
        await DB.log_broadcast(user["telegram_id"], txt, tgt, sent, failed)
        await call.message.answer(f"✅ Рассылка завершена!\nУспешно: {sent}\nОшибок: {failed}", reply_markup=KB.back_admin())
    else:
        await state.update_data(bc_target=target)
        d=await state.get_data()
        ids=await DB.get_all_user_ids(target)
        await call.message.edit_text(f"📢 *Предпросмотр:*\n\n{d['bc_text'][:100]}...\n\nАудитория: {target} (примерно {len(ids)} чел.)\nНачинаем?", reply_markup=KB.broadcast_confirm(), parse_mode=ParseMode.MARKDOWN)
    await call.answer()

@rt.callback_query(F.data=="adm:promo")
async def adm_promo(call: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await call.message.edit_text("🎟 Введи название для нового промокода:")
    await state.update_data(pum=False)
    await state.set_state(AdminStates.promo_code)
    await call.answer()

# ==========================================
# BACKGROUND TASKS
# ==========================================
async def hidden_likes_reminders(bot: Bot):
    while True:
        await asyncio.sleep(14400)
        try:
            async with async_session_maker() as s:
                users = await s.execute(select(User).where(and_(User.is_active == True, User.is_banned == False,
                                                                User.is_profile_complete == True, User.subscription_tier == SubscriptionTier.FREE,
                                                                User.hidden_likes_count > 0, User.last_active_at > datetime.utcnow() -
                                                                timedelta(days=3))).limit(50))
                for u in users.scalars().all():
                    try:
                        t, k = Monetization.get_hidden_likes_msg(u.hidden_likes_count)
                        await bot.send_message(u.telegram_id, t, reply_markup=k, parse_mode=ParseMode.MARKDOWN)
                    except: pass
                    await asyncio.sleep(0.5)
        except Exception as e: logger.error(f"Reminder err: {e}")

async def evening_boost_suggestions(bot: Bot):
    while True:
        now = datetime.utcnow(); target = now.replace(hour=19, minute=0, second=0)
        if now > target: target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            async with async_session_maker() as s:
                users = await s.execute(select(User).where(and_(User.is_active == True, User.is_banned == False,
                                                                User.is_profile_complete == True, User.last_active_at > datetime.utcnow() -
                                                                timedelta(days=2))).limit(100))
                for u in users.scalars().all():
                    if not u.boost_expires_at or u.boost_expires_at < datetime.utcnow():
                        try:
                            await bot.send_message(u.telegram_id, "🚀 *Вечер — лучшее время для буста!*\nПолучи в 5 раз больше просмотров и мэтчей всего за 39₽!",
                                                   reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 Купить Буст", callback_data="by:boost:1:3900")]]),
                                                   parse_mode=ParseMode.MARKDOWN)
                        except: pass
                        await asyncio.sleep(0.5)
        except Exception as e: logger.error(f"Boost suggest err: {e}")

async def streak_reminders(bot: Bot):
    while True:
        now = datetime.utcnow(); target = now.replace(hour=18, minute=0, second=0)
        if now > target: target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            async with async_session_maker() as s:
                users = await s.execute(select(User).where(and_(
                    User.is_active == True, User.is_banned == False,
                    User.streak_days > 2,
                    func.date(User.last_streak_date) < datetime.utcnow().date(),
                    User.last_active_at > datetime.utcnow() - timedelta(days=3),
                )).limit(50))
                for u in users.scalars().all():
                    try:
                        day = ((u.streak_days) % 7) + 1
                        reward = Gamification.DAILY_REWARDS.get(day, {})
                        await bot.send_message(u.telegram_id,
                                               f"⚡ *Не потеряй свою серию из {u.streak_days} дней!*\n\nЗайди в бота сегодня и забери награду: {reward.get('text', '')}\n\n_Если не зайти сегодня, серия обнулится!_",
                                               parse_mode=ParseMode.MARKDOWN)
                    except: pass
                    await asyncio.sleep(0.5)
        except Exception as e: logger.error(f"Streak remind err: {e}")

# ==========================================
# MAIN
# ==========================================
async def main():
    await init_db()
    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(rt)
    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())

    asyncio.create_task(hidden_likes_reminders(bot))
    asyncio.create_task(evening_boost_suggestions(bot))
    asyncio.create_task(streak_reminders(bot))

    logger.info(f"🚀 {BOT_NAME} v3.7 запущен")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
