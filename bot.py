"""
ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v3.6 (MONETIZATION BOOST)

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
    Column, Integer, BigInteger, String, Boolean, DateTime, Float,
    Text, ForeignKey, Enum as SQLEnum, select, update, delete,
    func, and_, or_, desc, asc, case
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

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///dating_bot.db")
    YOOKASSA_SHOP_ID: str = os.getenv("YOOKASSA_SHOP_ID", "")
    YOOKASSA_SECRET_KEY: str = os.getenv("YOOKASSA_SECRET_KEY", "")
    DOMAIN: str = os.getenv("DOMAIN", "https://yourdomain.ru")
    FREE_DAILY_LIKES: int = 15          # снижено для мотивации
    FREE_DAILY_MESSAGES: int = 5        # снижено для мотивации
    FREE_GUESTS_VISIBLE: int = 2        # снижено для мотивации
    SEARCH_EXPAND_RADIUS: bool = True
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

# ═══════════════════════════════════════════════════════════════════════════════
# ENUMS & MODELS
# ═══════════════════════════════════════════════════════════════════════════════

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
    search_expand_city = Column(Boolean, default=False)
    hidden_likes_count = Column(Integer, default=0)  # скрытые лайки для тизера
    last_teaser_shown = Column(DateTime, nullable=True)
    trial_used = Column(Boolean, default=False)
    referral_code = Column(String(20), unique=True, nullable=True)
    referred_by = Column(Integer, nullable=True)
    referral_bonus_count = Column(Integer, default=0)
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

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

engine = create_async_engine(config.DATABASE_URL, echo=False)
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ DB ready")

# ═══════════════════════════════════════════════════════════════════════════════
# FSM
# ═══════════════════════════════════════════════════════════════════════════════

class RegStates(StatesGroup):
    name = State(); age = State(); gender = State(); city = State()
    photo = State(); bio = State(); interests = State()
    looking_for = State(); age_range = State()

class EditStates(StatesGroup):
    edit_name = State(); edit_age = State(); edit_city = State()
    edit_bio = State(); edit_interests = State(); edit_age_range = State()
    add_photo = State()

class ChatStates(StatesGroup):
    chatting = State()

class SearchStates(StatesGroup):
    browsing = State()

class AdminStates(StatesGroup):
    broadcast_text = State(); broadcast_confirm = State()
    search_user = State(); give_vip_duration = State()
    give_boost_count = State(); promo_code = State()
    promo_tier = State(); promo_duration = State(); promo_uses = State()

# ═══════════════════════════════════════════════════════════════════════════════
# COMPATIBILITY ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class Compatibility:
    INTERESTS_LIST = [
        "🎵 Музыка", "🎬 Кино", "📚 Книги", "🏃 Спорт", "✈️ Путешествия",
        "🍳 Кулинария", "🎮 Игры", "📷 Фото", "🎨 Искусство", "💻 IT",
        "🐾 Животные", "🧘 Йога", "🏕️ Природа", "🍷 Вино", "💃 Танцы",
        "🎸 Концерты", "🏋️ Фитнес", "📺 Сериалы", "🏖️ Пляж", "☕ Кофе",
    ]

    @staticmethod
    def calc_score(u1: Dict, u2: Dict) -> float:
        score = 0.0
        i1 = set((u1.get("interests") or "").split(",")); i2 = set((u2.get("interests") or "").split(","))
        i1.discard(""); i2.discard("")
        if i1 and i2:
            score += (len(i1 & i2) / len(i1 | i2)) * 40
        a1, a2 = u1.get("age") or 25, u2.get("age") or 25
        ad = abs(a1 - a2)
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

# ═══════════════════════════════════════════════════════════════════════════════
# MONETIZATION TRIGGERS — движок триггеров покупок
# ═══════════════════════════════════════════════════════════════════════════════

class MonetizationEngine:
    """Система умных триггеров для стимуляции покупок"""

    # Тизерные сообщения — показываем что упускает пользователь
    TEASERS = {
        "likes_limit": [
            "💔 *Лайки закончились!*\nА ведь {hidden} человек ждут твоего ответа...\n\n🥂 С Винчик VIP — *безлимит лайков* всего от 149₽/мес!",
            "🚫 *Лимит лайков на сегодня!*\nНе упусти шанс — за сегодня тебя лайкнули {hidden} раз!\n\n✨ Разблокируй безлимит →",
            "❌ *{remaining} лайков осталось...*\n⭐ VIP = безлимит лайков + суперлайки + кто тебя лайкнул",
        ],
        "msg_limit": [
            "💬 *Лимит сообщений!*\n{name} ждёт твоего ответа, а ты не можешь написать 😔\n\n🥂 Винчик VIP — безлимит сообщений!",
            "🔒 *Сообщения закончились!*\nС VIP ты бы уже общался(ась) без ограничений.\n\nОт *149₽/мес* — разблокируй!",
        ],
        "hidden_likes": [
            "👀 *{count} человек лайкнули тебя!*\nУзнай кто — с VIP ты увидишь всех!\n\n💡 Один из них — прямо сейчас в выдаче...",
            "❤️ *Тебя оценили {count} раз!*\nНе гадай — узнай кто именно!\n\n🥂 Открой VIP и увидь всех →",
            "🔥 *{count} скрытых лайков!*\nСреди них может быть твоя пара.\nОткрой с VIP всего от 149₽!",
        ],
        "boost_tease": [
            "📊 *Твоя анкета на {position}-м месте*\nС бустом ты будешь в ТОП-3!\n\n🚀 +500% просмотров за 39₽",
            "👁️ *Тебя посмотрели {views} раз за неделю*\nС бустом было бы в 5 раз больше!\n\n🚀 Один буст — 39₽",
            "💡 *Вечер — лучшее время для буста!*\n80% мэтчей происходят с 18 до 23.\n\n🚀 Поднимись в топ прямо сейчас!",
        ],
        "superlike_tease": [
            "⭐ *Суперлайк = шанс мэтча ×3!*\nЭтот человек увидит что ты его выделил.\n\n🥂 VIP Light — 1 суперлайк/день бесплатно!",
        ],
        "match_celebration": [
            "🎉 *Новый мэтч!*\nVIP-пользователи получают в среднем *×4 больше мэтчей*.\n\nПопробуй VIP Light за 149₽!",
        ],
        "inactive_reminder": [
            "😔 *{name}, тебя давно не было!*\nЗа это время тебя лайкнули {likes} раз.\n\nЗаходи — там ждут!",
            "🍷 *{name}, бокал вина ждёт!*\nУ тебя {likes} новых лайков и {matches} мэтчей.\n\nВернись и познакомься!",
        ],
        "profile_incomplete": [
            "📸 *Добавь фото — получи +300% лайков!*\nАнкеты с фото просматривают в 5 раз чаще.",
            "🎯 *Укажи интересы — точнее подбор!*\nЛюди с интересами получают больше мэтчей.",
        ],
        "trial_offer": [
            "🎁 *Специально для тебя!*\n\nПопробуй VIP Light *3 дня бесплатно*!\nБезлимит лайков, суперлайки, без рекламы.\n\nПросто нажми кнопку 👇",
        ],
        "weekly_report": [
            "📊 *Твоя неделя в цифрах:*\n\n👁️ Просмотров: {views}\n❤️ Лайков: {likes}\n💘 Мэтчей: {matches}\n\n{tip}",
        ],
    }

    WEEKLY_TIPS = [
        "💡 *Совет:* Буст вечером даёт ×5 просмотров!",
        "💡 *Совет:* С VIP ты увидишь кто тебя лайкнул!",
        "💡 *Совет:* Суперлайк повышает шанс мэтча в 3 раза!",
        "💡 *Совет:* Добавь больше фото — +200% к лайкам!",
        "💡 *Совет:* Активные профили показываются чаще!",
    ]

    @staticmethod
    def get_likes_limit_msg(user: Dict) -> tuple:
        """Сообщение при лимите лайков + клавиатура"""
        hidden = user.get("hidden_likes_count", 0)
        remaining = user.get("daily_likes_remaining", 0)
        msgs = MonetizationEngine.TEASERS["likes_limit"]
        txt = random.choice(msgs).format(hidden=hidden or "несколько", remaining=remaining)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Безлимит от 149₽", callback_data="tf:vip_light")],
            [InlineKeyboardButton(text="📊 Сравнить тарифы", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🎁 Попробовать бесплатно", callback_data="trial:start")],
        ])
        return txt, kb

    @staticmethod
    def get_msg_limit_msg(partner_name: str) -> tuple:
        msgs = MonetizationEngine.TEASERS["msg_limit"]
        txt = random.choice(msgs).format(name=partner_name)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Безлимит сообщений", callback_data="tf:vip_light")],
            [InlineKeyboardButton(text="🎁 3 дня бесплатно", callback_data="trial:start")],
        ])
        return txt, kb

    @staticmethod
    def get_hidden_likes_msg(count: int) -> tuple:
        msgs = MonetizationEngine.TEASERS["hidden_likes"]
        txt = random.choice(msgs).format(count=count)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"❤️ Узнать кто ({count})", callback_data="likes:list")],
            [InlineKeyboardButton(text="🥂 Открыть VIP", callback_data="sh:subs")],
        ])
        return txt, kb

    @staticmethod
    def get_boost_tease(user: Dict) -> tuple:
        views = user.get("views_count", 0)
        msgs = MonetizationEngine.TEASERS["boost_tease"]
        txt = random.choice(msgs).format(position=random.randint(50, 200), views=views)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Буст за 39₽", callback_data="by:boost:1:3900")],
            [InlineKeyboardButton(text="🚀 5 бустов за 149₽ (-24%)", callback_data="by:boost:5:14900")],
        ])
        return txt, kb

    @staticmethod
    def get_superlike_tease() -> tuple:
        txt = random.choice(MonetizationEngine.TEASERS["superlike_tease"])
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Получить суперлайки", callback_data="sh:subs")],
        ])
        return txt, kb

    @staticmethod
    def get_match_upsell(user: Dict) -> Optional[tuple]:
        """После мэтча — предложение VIP если бесплатный"""
        if DB.is_vip(user):
            return None
        if random.random() > 0.4:  # показываем в 40% случаев
            return None
        txt = random.choice(MonetizationEngine.TEASERS["match_celebration"])
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Попробовать VIP", callback_data="trial:start")],
        ])
        return txt, kb

    @staticmethod
    def get_weekly_report(user: Dict, period_views: int, period_likes: int, period_matches: int) -> str:
        tip = random.choice(MonetizationEngine.WEEKLY_TIPS)
        msgs = MonetizationEngine.TEASERS["weekly_report"]
        return random.choice(msgs).format(
            views=period_views, likes=period_likes, matches=period_matches, tip=tip
        )

    @staticmethod
    def should_show_teaser(user: Dict, teaser_type: str) -> bool:
        """Контроль частоты тизеров — не спамим"""
        if DB.is_vip(user):
            return False
        last = user.get("last_teaser_shown")
        if last and (datetime.utcnow() - last).total_seconds() < 300:  # не чаще 5 мин
            return False
        return True

    @staticmethod
    def get_profile_tips(user: Dict) -> List[str]:
        """Подсказки для улучшения профиля"""
        tips = []
        if not user.get("main_photo"):
            tips.append("📸 Добавь фото — анкеты с фото получают в 5 раз больше лайков!")
        photos = [p for p in (user.get("photos") or "").split(",") if p.strip()]
        if len(photos) < 3:
            tips.append(f"📸 У тебя {len(photos)} фото. Добавь ещё — больше фото = больше лайков!")
        if not user.get("interests"):
            tips.append("🎯 Укажи интересы — подбор станет точнее, мэтчей будет больше!")
        if not user.get("bio"):
            tips.append("✍️ Напиши о себе — людям интересно узнать тебя!")
        return tips

    @staticmethod
    def get_low_likes_warning(remaining: int) -> Optional[str]:
        """Предупреждение когда осталось мало лайков"""
        if remaining == 5:
            return "⚡ *Осталось 5 лайков!* Используй с умом или открой безлимит с VIP 🥂"
        if remaining == 3:
            return "⚠️ *Только 3 лайка!* Не хватает? VIP = безлимит от 149₽/мес"
        if remaining == 1:
            return "🔴 *Последний лайк!* Завтра будет ещё 15... или безлимит с VIP прямо сейчас!"
        return None

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE SERVICE
# ═══════════════════════════════════════════════════════════════════════════════

class DB:
    @staticmethod
    def _to_dict(u: User) -> Dict:
        return {
            "id": u.id, "telegram_id": u.telegram_id, "username": u.username,
            "name": u.name, "age": u.age,
            "gender": u.gender.value if u.gender else None,
            "city": u.city, "bio": u.bio, "interests": u.interests or "",
            "looking_for": u.looking_for.value if u.looking_for else "both",
            "age_from": u.age_from, "age_to": u.age_to,
            "photos": u.photos or "", "main_photo": u.main_photo,
            "is_active": u.is_active, "is_banned": u.is_banned,
            "is_verified": u.is_verified, "is_profile_complete": u.is_profile_complete,
            "subscription_tier": u.subscription_tier.value if u.subscription_tier else "free",
            "subscription_expires_at": u.subscription_expires_at,
            "daily_likes_remaining": u.daily_likes_remaining or 0,
            "daily_messages_remaining": u.daily_messages_remaining or 0,
            "daily_superlikes_remaining": u.daily_superlikes_remaining or 0,
            "last_limits_reset": u.last_limits_reset,
            "boost_expires_at": u.boost_expires_at,
            "boost_count": u.boost_count or 0,
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
    def is_creator(u: Dict) -> bool:
        return u.get("telegram_id") in config.CREATOR_IDS

    @staticmethod
    def is_admin(u: Dict) -> bool:
        return u.get("telegram_id") in config.ADMIN_IDS

    @staticmethod
    def get_badge(u: Dict) -> str:
        if DB.is_creator(u): return "👑 "
        if u.get("subscription_tier") == "vip_lifetime": return "💎 "
        if u.get("subscription_tier") == "vip_pro": return "⭐ "
        if DB.is_vip(u): return "✨ "
        if u.get("is_verified"): return "✅ "
        return ""

    @staticmethod
    def get_role_tag(u: Dict) -> str:
        if DB.is_creator(u): return " · 👑"
        if DB.is_admin(u): return " · 🛡️"
        return ""

    @staticmethod
    def get_tier_name(u: Dict) -> str:
        return {"free": "🍷 Бесплатный", "vip_light": "🥂 Винчик Light",
                "vip_standard": "🍾 Винчик Standard", "vip_pro": "👑 Винчик Pro",
                "vip_lifetime": "💎 Винчик Forever"}.get(u.get("subscription_tier", "free"), "🍷")

    @staticmethod
    def get_superlikes_limit(u: Dict) -> int:
        t = u.get("subscription_tier", "free")
        return {"vip_pro": 5, "vip_lifetime": 5, "vip_standard": 2, "vip_light": 1}.get(t, 0)

    @staticmethod
    def get_daily_likes_limit(u: Dict) -> int:
        t = u.get("subscription_tier", "free")
        if t in ("vip_standard", "vip_pro", "vip_lifetime"): return 9999
        if t == "vip_light": return 100
        return config.FREE_DAILY_LIKES

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
    async def create_user(tg_id: int, username: str = None, referred_by: int = None) -> Dict:
        async with async_session_maker() as s:
            u = User(telegram_id=tg_id, username=username,
                     referral_code=str(uuid.uuid4())[:8].upper(),
                     referred_by=referred_by,
                     last_limits_reset=datetime.utcnow())
            s.add(u)
            await s.commit()
            await s.refresh(u)
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
            dl = DB.get_daily_likes_limit(u)
            dm = 9999 if DB.is_vip(u) else config.FREE_DAILY_MESSAGES
            return await DB.update_user(
                u["telegram_id"],
                daily_likes_remaining=dl,
                daily_messages_remaining=dm,
                daily_superlikes_remaining=sl,
                last_limits_reset=now, last_active_at=now
            )
        await DB.update_user(u["telegram_id"], last_active_at=now)
        return u

    @staticmethod
    async def get_who_liked_me(uid: int) -> List[int]:
        async with async_session_maker() as s:
            liked_me = await s.execute(select(Like.from_user_id).where(Like.to_user_id == uid))
            liked_me_ids = set(r[0] for r in liked_me.fetchall())
            my_likes = await s.execute(select(Like.to_user_id).where(Like.from_user_id == uid))
            my_dislikes = await s.execute(select(Dislike.to_user_id).where(Dislike.from_user_id == uid))
            seen = set(r[0] for r in my_likes.fetchall()) | set(r[0] for r in my_dislikes.fetchall())
            return list(liked_me_ids - seen)

    @staticmethod
    async def update_hidden_likes(uid: int):
        """Обновить счётчик скрытых лайков (для тизера)"""
        who = await DB.get_who_liked_me(uid)
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.id == uid).values(hidden_likes_count=len(who)))
            await s.commit()

    @staticmethod
    async def search_profiles(u: Dict, limit=5) -> List[Dict]:
        async with async_session_maker() as s:
            my_likes = await s.execute(select(Like.to_user_id).where(Like.from_user_id == u["id"]))
            my_dislikes = await s.execute(select(Dislike.to_user_id).where(Dislike.from_user_id == u["id"]))
            exc = set(r[0] for r in my_likes.fetchall()) | set(r[0] for r in my_dislikes.fetchall())
            exc.add(u["id"])
            results = []

            # ЭТАП 1: Кто лайкнул меня
            who_liked = await DB.get_who_liked_me(u["id"])
            priority_ids = [uid for uid in who_liked if uid not in exc]
            if priority_ids:
                pr = await s.execute(select(User).where(and_(
                    User.id.in_(priority_ids), User.is_active == True,
                    User.is_banned == False, User.is_profile_complete == True
                )))
                for p in pr.scalars().all():
                    d = DB._to_dict(p)
                    d["_priority"] = "liked_you"
                    d["_compat"] = Compatibility.calc_score(u, d)
                    results.append(d); exc.add(p.id)

            # ЭТАП 2: По городу
            remaining = limit - len(results)
            if remaining > 0:
                q = select(User).where(and_(
                    User.is_active == True, User.is_banned == False,
                    User.is_profile_complete == True, User.id.not_in(exc),
                    User.age >= u["age_from"], User.age <= u["age_to"],
                    User.city == u["city"],
                ))
                lf = u.get("looking_for", "both")
                if lf == "male": q = q.where(User.gender == Gender.MALE)
                elif lf == "female": q = q.where(User.gender == Gender.FEMALE)
                my_gender = u.get("gender")
                if my_gender:
                    q = q.where(or_(User.looking_for == LookingFor.BOTH, User.looking_for == LookingFor(my_gender)))
                q = q.order_by(User.boost_expires_at.desc().nullslast(), User.popularity_score.desc(), User.last_active_at.desc()).limit(remaining * 3)
                r = await s.execute(q)
                candidates = [DB._to_dict(p) for p in r.scalars().all()]
                for c in candidates:
                    c["_priority"] = "boosted" if DB.is_boosted(c) else "normal"
                    c["_compat"] = Compatibility.calc_score(u, c)
                candidates.sort(key=lambda x: (x["_priority"] == "boosted", x["_compat"] + random.uniform(0, 10)), reverse=True)
                results.extend(candidates[:remaining])

            # ЭТАП 3: Другие города
            if len(results) < limit:
                still_need = limit - len(results)
                all_exc = exc | set(r["id"] for r in results)
                q2 = select(User).where(and_(
                    User.is_active == True, User.is_banned == False,
                    User.is_profile_complete == True, User.id.not_in(all_exc),
                    User.city != u["city"], User.age >= u["age_from"], User.age <= u["age_to"],
                ))
                lf = u.get("looking_for", "both")
                if lf == "male": q2 = q2.where(User.gender == Gender.MALE)
                elif lf == "female": q2 = q2.where(User.gender == Gender.FEMALE)
                my_gender = u.get("gender")
                if my_gender:
                    q2 = q2.where(or_(User.looking_for == LookingFor.BOTH, User.looking_for == LookingFor(my_gender)))
                q2 = q2.order_by(User.popularity_score.desc()).limit(still_need)
                r2 = await s.execute(q2)
                for p in r2.scalars().all():
                    d = DB._to_dict(p); d["_priority"] = "other_city"; d["_compat"] = Compatibility.calc_score(u, d)
                    results.append(d)

            # Обновить popularity
            new_pop = Compatibility.calc_popularity(u)
            if abs(new_pop - u.get("popularity_score", 0)) > 1:
                await s.execute(update(User).where(User.id == u["id"]).values(popularity_score=new_pop))
                await s.commit()

            return results[:limit]

    @staticmethod
    async def add_like(fd: int, tid: int, is_super: bool = False) -> Dict:
        async with async_session_maker() as s:
            ex = await s.execute(select(Like).where(and_(Like.from_user_id == fd, Like.to_user_id == tid)))
            if ex.scalar_one_or_none():
                return {"is_match": False, "match_id": None, "compat": 0}
            s.add(Like(from_user_id=fd, to_user_id=tid, is_super_like=is_super))
            await s.execute(update(User).where(User.id == tid).values(likes_received_count=User.likes_received_count + 1))
            await s.execute(update(User).where(User.id == fd).values(likes_sent_count=User.likes_sent_count + 1))
            rev = await s.execute(select(Like).where(and_(Like.from_user_id == tid, Like.to_user_id == fd)))
            is_match = rev.scalar_one_or_none() is not None
            match_id = None; compat = 0.0
            if is_match:
                existing = await s.execute(select(Match).where(or_(
                    and_(Match.user1_id == fd, Match.user2_id == tid),
                    and_(Match.user1_id == tid, Match.user2_id == fd)
                )))
                if not existing.scalar_one_or_none():
                    u1r = await s.execute(select(User).where(User.id == fd))
                    u2r = await s.execute(select(User).where(User.id == tid))
                    u1, u2 = u1r.scalar_one_or_none(), u2r.scalar_one_or_none()
                    if u1 and u2: compat = Compatibility.calc_score(DB._to_dict(u1), DB._to_dict(u2))
                    m = Match(user1_id=min(fd, tid), user2_id=max(fd, tid), compatibility_score=compat)
                    s.add(m); await s.flush(); match_id = m.id
                    await s.execute(update(User).where(User.id.in_([fd, tid])).values(matches_count=User.matches_count + 1))
            await s.commit()
            # Обновить скрытые лайки для тизера
            await DB.update_hidden_likes(tid)
            return {"is_match": is_match, "match_id": match_id, "compat": compat}

    @staticmethod
    async def add_dislike(fd: int, tid: int):
        async with async_session_maker() as s:
            ex = await s.execute(select(Dislike).where(and_(Dislike.from_user_id == fd, Dislike.to_user_id == tid)))
            if not ex.scalar_one_or_none():
                s.add(Dislike(from_user_id=fd, to_user_id=tid)); await s.commit()

    @staticmethod
    async def reset_dislikes(uid: int) -> int:
        async with async_session_maker() as s:
            r = await s.execute(select(func.count(Dislike.id)).where(Dislike.from_user_id == uid))
            count = r.scalar() or 0
            await s.execute(delete(Dislike).where(Dislike.from_user_id == uid)); await s.commit()
            return count

    @staticmethod
    async def get_likes_received(uid: int, limit=20) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Like).where(Like.to_user_id == uid).order_by(Like.created_at.desc()).limit(limit))
            out = []
            for lk in r.scalars().all():
                u = await DB.get_user_by_id(lk.from_user_id)
                if u: u["is_super_like"] = lk.is_super_like; out.append(u)
            return out

    @staticmethod
    async def get_matches(uid: int) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(
                or_(Match.user1_id == uid, Match.user2_id == uid), Match.is_active == True
            )).order_by(Match.last_message_at.desc().nullslast(), Match.compatibility_score.desc()))
            out = []
            for m in r.scalars().all():
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                pr = await s.execute(select(User).where(User.id == pid))
                p = pr.scalar_one_or_none()
                if p:
                    unread = await s.execute(select(func.count(ChatMessage.id)).where(and_(
                        ChatMessage.match_id == m.id, ChatMessage.sender_id != uid, ChatMessage.is_read == False)))
                    out.append({
                        "match_id": m.id, "user_id": p.id, "telegram_id": p.telegram_id,
                        "name": p.name, "age": p.age, "photo": p.main_photo,
                        "compat": m.compatibility_score, "unread": unread.scalar() or 0, "last_msg": m.last_message_at
                    })
            return out

    @staticmethod
    async def unmatch(uid: int, match_id: int) -> bool:
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(Match.id == match_id, or_(Match.user1_id == uid, Match.user2_id == uid))))
            m = r.scalar_one_or_none()
            if not m: return False
            await s.execute(update(Match).where(Match.id == match_id).values(is_active=False))
            await s.execute(update(User).where(User.id.in_([m.user1_id, m.user2_id])).values(matches_count=func.greatest(User.matches_count - 1, 0)))
            await s.commit(); return True

    @staticmethod
    async def get_match_between(u1: int, u2: int) -> Optional[int]:
        async with async_session_maker() as s:
            r = await s.execute(select(Match.id).where(and_(
                or_(and_(Match.user1_id == u1, Match.user2_id == u2), and_(Match.user1_id == u2, Match.user2_id == u1)),
                Match.is_active == True)))
            row = r.first()
            return row[0] if row else None

    @staticmethod
    async def send_msg(mid: int, sid: int, txt: str):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=txt))
            await s.execute(update(Match).where(Match.id == mid).values(last_message_at=datetime.utcnow()))
            await s.commit()

    @staticmethod
    async def get_msgs(mid: int, limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(ChatMessage).where(ChatMessage.match_id == mid).order_by(ChatMessage.created_at.desc()).limit(limit))
            return [{"sender_id": m.sender_id, "text": m.text, "created_at": m.created_at} for m in reversed(r.scalars().all())]

    @staticmethod
    async def mark_read(mid: int, uid: int):
        async with async_session_maker() as s:
            await s.execute(update(ChatMessage).where(and_(ChatMessage.match_id == mid, ChatMessage.sender_id != uid, ChatMessage.is_read == False)).values(is_read=True))
            await s.commit()

    @staticmethod
    async def get_unread(uid: int) -> int:
        async with async_session_maker() as s:
            ms = await s.execute(select(Match.id).where(and_(or_(Match.user1_id == uid, Match.user2_id == uid), Match.is_active == True)))
            mids = [m[0] for m in ms.fetchall()]
            if not mids: return 0
            r = await s.execute(select(func.count(ChatMessage.id)).where(and_(ChatMessage.match_id.in_(mids), ChatMessage.sender_id != uid, ChatMessage.is_read == False)))
            return r.scalar() or 0

    @staticmethod
    async def add_guest(vid: int, uid: int):
        async with async_session_maker() as s:
            s.add(GuestVisit(visitor_id=vid, visited_user_id=uid))
            await s.execute(update(User).where(User.id == uid).values(views_count=User.views_count + 1))
            await s.commit()

    @staticmethod
    async def get_guests(uid: int, limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(GuestVisit.visitor_id).where(GuestVisit.visited_user_id == uid).order_by(GuestVisit.created_at.desc()).distinct().limit(limit))
            ids = [row[0] for row in r.fetchall()]
            if not ids: return []
            us = await s.execute(select(User).where(User.id.in_(ids)))
            return [DB._to_dict(u) for u in us.scalars().all()]

    @staticmethod
    async def dec_likes(tg_id: int):
        u = await DB.get_user(tg_id)
        if u and u.get("daily_likes_remaining", 0) > 9000: return  # безлимит
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(daily_likes_remaining=func.greatest(User.daily_likes_remaining - 1, 0)))
            await s.commit()

    @staticmethod
    async def dec_superlikes(tg_id: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(daily_superlikes_remaining=func.greatest(User.daily_superlikes_remaining - 1, 0)))
            await s.commit()

    @staticmethod
    async def dec_messages(tg_id: int):
        u = await DB.get_user(tg_id)
        if u and u.get("daily_messages_remaining", 0) > 9000: return
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(daily_messages_remaining=func.greatest(User.daily_messages_remaining - 1, 0)))
            await s.commit()

    @staticmethod
    async def use_boost(uid: int) -> bool:
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u or (u.boost_count or 0) <= 0: return False
            now = datetime.utcnow()
            ne = (u.boost_expires_at + timedelta(hours=24)) if u.boost_expires_at and u.boost_expires_at > now else now + timedelta(hours=24)
            await s.execute(update(User).where(User.id == uid).values(boost_count=User.boost_count - 1, boost_expires_at=ne))
            await s.commit(); return True

    @staticmethod
    async def add_boosts(uid: int, c: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.id == uid).values(boost_count=User.boost_count + c))
            await s.commit()

    @staticmethod
    async def activate_trial(uid: int) -> bool:
        """Активация пробного периода 3 дня VIP Light"""
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u or u.trial_used: return False
            exp = datetime.utcnow() + timedelta(days=3)
            await s.execute(update(User).where(User.id == uid).values(
                subscription_tier=SubscriptionTier.VIP_LIGHT,
                subscription_expires_at=exp, trial_used=True
            ))
            await s.commit(); return True

    @staticmethod
    async def process_referral(new_uid: int, ref_code: str):
        """Обработка реферала — бонус обоим"""
        async with async_session_maker() as s:
            r = await s.execute(select(User).where(User.referral_code == ref_code.upper()))
            referrer = r.scalar_one_or_none()
            if not referrer or referrer.id == new_uid: return
            # Бонус рефереру: 1 буст
            await s.execute(update(User).where(User.id == referrer.id).values(
                boost_count=User.boost_count + 1, referral_bonus_count=User.referral_bonus_count + 1
            ))
            # Бонус новому: 5 доп лайков
            await s.execute(update(User).where(User.id == new_uid).values(
                daily_likes_remaining=User.daily_likes_remaining + 5, referred_by=referrer.id
            ))
            await s.commit()

    @staticmethod
    async def create_report(rid, ruid, reason):
        async with async_session_maker() as s:
            s.add(Report(reporter_id=rid, reported_user_id=ruid, reason=reason)); await s.commit()

    @staticmethod
    async def get_stats() -> Dict:
        async with async_session_maker() as s:
            total = (await s.execute(select(func.count(User.id)))).scalar() or 0
            complete = (await s.execute(select(func.count(User.id)).where(User.is_profile_complete == True))).scalar() or 0
            now = datetime.utcnow()
            day_ago, week_ago, month_ago = now - timedelta(days=1), now - timedelta(days=7), now - timedelta(days=30)
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
            month_rev = (await s.execute(select(func.sum(Payment.amount)).where(and_(Payment.status == PaymentStatus.SUCCEEDED, Payment.paid_at > month_ago)))).scalar() or 0
            trials = (await s.execute(select(func.count(User.id)).where(User.trial_used == True))).scalar() or 0
            pending_reports = (await s.execute(select(func.count(Report.id)).where(Report.status == "pending"))).scalar() or 0
            return {
                "total": total, "complete": complete, "dau": dau, "wau": wau, "mau": mau,
                "vip": vip, "banned": banned, "today_reg": today_reg, "matches": total_matches,
                "messages": total_msgs, "likes": total_likes, "trials": trials,
                "revenue": revenue / 100 if revenue else 0, "month_revenue": month_rev / 100 if month_rev else 0,
                "pending_reports": pending_reports, "conversion": (vip / complete * 100) if complete > 0 else 0,
            }

    @staticmethod
    async def search_users(query: str) -> List[Dict]:
        async with async_session_maker() as s:
            if query.isdigit():
                r = await s.execute(select(User).where(or_(User.id == int(query), User.telegram_id == int(query))))
            else:
                r = await s.execute(select(User).where(or_(User.username.ilike(f"%{query}%"), User.name.ilike(f"%{query}%"))).limit(10))
            return [DB._to_dict(u) for u in r.scalars().all()]

    @staticmethod
    async def get_all_user_ids(ft="all") -> List[int]:
        async with async_session_maker() as s:
            q = select(User.telegram_id).where(and_(User.is_active == True, User.is_banned == False))
            if ft == "complete": q = q.where(User.is_profile_complete == True)
            elif ft == "vip": q = q.where(User.subscription_tier != SubscriptionTier.FREE)
            elif ft == "free": q = q.where(User.subscription_tier == SubscriptionTier.FREE)
            r = await s.execute(q)
            return [row[0] for row in r.fetchall()]

    @staticmethod
    async def get_pending_reports(limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Report).where(Report.status == "pending").order_by(Report.created_at.desc()).limit(limit))
            out = []
            for rep in r.scalars().all():
                reporter = await DB.get_user_by_id(rep.reporter_id)
                reported = await DB.get_user_by_id(rep.reported_user_id)
                out.append({"id": rep.id, "reason": rep.reason, "created_at": rep.created_at, "reporter": reporter, "reported": reported})
            return out

    @staticmethod
    async def resolve_report(rid, action, notes=""):
        async with async_session_maker() as s:
            await s.execute(update(Report).where(Report.id == rid).values(status=action, admin_notes=notes, resolved_at=datetime.utcnow()))
            await s.commit()

    @staticmethod
    async def activate_subscription_by_id(uid, tier, days):
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u: return
            te = SubscriptionTier(tier); now = datetime.utcnow()
            exp = None if te == SubscriptionTier.VIP_LIFETIME else (
                (u.subscription_expires_at + timedelta(days=days)) if u.subscription_expires_at and u.subscription_expires_at > now
                else now + timedelta(days=days))
            await s.execute(update(User).where(User.id == uid).values(subscription_tier=te, subscription_expires_at=exp))
            await s.commit()

    @staticmethod
    async def create_payment(uid, yid, amount, desc, ptype, ptier=None, pdur=None, pcount=None) -> int:
        async with async_session_maker() as s:
            p = Payment(user_id=uid, yookassa_payment_id=yid, amount=amount, description=desc, product_type=ptype, product_tier=ptier, product_duration=pdur, product_count=pcount)
            s.add(p); await s.commit(); await s.refresh(p); return p.id

    @staticmethod
    async def get_payment(pid) -> Optional[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Payment).where(Payment.id == pid))
            p = r.scalar_one_or_none()
            return {"id": p.id, "user_id": p.user_id, "yookassa_payment_id": p.yookassa_payment_id, "status": p.status.value, "product_type": p.product_type, "product_tier": p.product_tier, "product_duration": p.product_duration, "product_count": p.product_count} if p else None

    @staticmethod
    async def update_payment_status(pid, st):
        async with async_session_maker() as s:
            v = {"status": st}
            if st == PaymentStatus.SUCCEEDED: v["paid_at"] = datetime.utcnow()
            await s.execute(update(Payment).where(Payment.id == pid).values(**v)); await s.commit()

    @staticmethod
    async def create_promo(code, tier, days, max_uses):
        async with async_session_maker() as s:
            s.add(PromoCode(code=code.upper(), tier=tier, duration_days=days, max_uses=max_uses)); await s.commit()

    @staticmethod
    async def use_promo(user_id, code) -> Dict:
        async with async_session_maker() as s:
            r = await s.execute(select(PromoCode).where(and_(PromoCode.code == code.upper(), PromoCode.is_active == True)))
            promo = r.scalar_one_or_none()
            if not promo: return {"error": "Промокод не найден"}
            if promo.used_count >= promo.max_uses: return {"error": "Промокод исчерпан"}
            used = await s.execute(select(PromoUse).where(and_(PromoUse.promo_id == promo.id, PromoUse.user_id == user_id)))
            if used.scalar_one_or_none(): return {"error": "Уже использован"}
            s.add(PromoUse(promo_id=promo.id, user_id=user_id))
            await s.execute(update(PromoCode).where(PromoCode.id == promo.id).values(used_count=PromoCode.used_count + 1))
            await s.commit()
            await DB.activate_subscription_by_id(user_id, promo.tier, promo.duration_days)
            return {"success": True, "tier": promo.tier, "days": promo.duration_days}

    @staticmethod
    async def log_broadcast(admin_id, text, target, sent, failed):
        async with async_session_maker() as s:
            s.add(BroadcastLog(admin_id=admin_id, message_text=text, target_filter=target, sent_count=sent, failed_count=failed)); await s.commit()

# ═══════════════════════════════════════════════════════════════════════════════
# TEXTS
# ═══════════════════════════════════════════════════════════════════════════════

TIER_NAMES = {"free": "🍷 Бесплатный", "vip_light": "🥂 Винчик Light",
              "vip_standard": "🍾 Винчик Standard", "vip_pro": "👑 Винчик Pro", "vip_lifetime": "💎 Винчик Forever"}

class T:
    WELCOME_NEW = f"🍷 *Добро пожаловать в {BOT_NAME}!*\n\nНайди свою половинку за бокалом вина! 🥂\n\nДавай создадим анкету 👇"
    WELCOME_BACK = "🍷 *С возвращением, {name}!*\n\n{status}\n👁️ {views} · 💘 {matches} · 💬 {msgs}\n{extras}"
    ASK_NAME = "👤 Как тебя зовут?"
    ASK_AGE = "🎂 Сколько тебе лет? _(18-99)_"
    ASK_GENDER = "👫 Твой пол:"
    ASK_CITY = "🌍 Твой город:"
    ASK_PHOTO = "📸 Отправь фото или «Пропустить»:"
    ASK_BIO = "✍️ О себе _(до 500 симв.)_ или «Пропустить»:"
    ASK_INTERESTS = "🎯 *Выбери интересы* _(чем больше, тем лучше подбор)_:"
    ASK_LOOKING = "🔍 Кого ищешь?"
    ASK_AGE_RANGE = "🎯 *Возрастной диапазон?*\nНапиши через дефис: `18-30`"
    BAD_NAME = "⚠️ 2-50 символов:"
    BAD_AGE = "⚠️ 18-99:"
    REG_DONE = f"✅ *Готово!* Добро пожаловать в {BOT_NAME}! 🍷"
    NO_PROFILES = "😔 *Анкеты закончились!*\n\nПопробуй расширить поиск или зайди позже."
    NO_MATCHES = "😞 Пока нет мэтчей. Листай анкеты! 🍷"
    NO_PROFILE = "⚠️ Заполни профиль → /start"
    BANNED = "🚫 Заблокирован."
    NO_GUESTS = "😴 Пока нет гостей"
    NO_MSGS = "💤 Нет сообщений"
    NEW_MATCH = "💕 *Мэтч с {name}!* 🥂\n🎯 Совместимость: {compat}%"
    SUPERLIKE_RECEIVED = "⭐ *{name}* отправил тебе суперлайк!"

    SHOP = f"🛍️ *{BOT_NAME}* — Магазин\n\n🥂 VIP-подписки\n🚀 Буст анкеты\n📊 Сравнить тарифы\n🎁 Промокод"

    COMPARE = f"""
📊 *ТАРИФЫ · {BOT_NAME}*

🍷 *Бесплатный*
• 15 лайков/день · 5 сообщений
• 2 гостя · Базовый поиск

🥂 *Винчик Light — 149₽/мес*
• 100 лайков · ∞ сообщений
• 1 суперлайк⭐ · 10 гостей
• Приоритет · Без рекламы

🍾 *Винчик Standard — 349₽/мес* 🔥
• ∞ лайков и сообщений
• 2 суперлайка⭐ · Все гости
• *Кто тебя лайкнул* ❤️
• Невидимка · 1 буст/день
• Сброс пропущенных

👑 *Винчик Pro — 599₽/мес*
• Всё из Standard +
• 5 суперлайков⭐ · 3 буста
• Бейдж 👑 · Топ выдачи · 24/7

💎 *Винчик Forever — 2999₽*
• Всё из Pro НАВСЕГДА
• Бейдж 💎 · Все обновления
"""

    FAQ = f"""
❓ *FAQ · {BOT_NAME}*

*🍷 Как работает?* Ставь 👍/⭐ — если взаимно, открывается чат!
*⭐ Суперлайк?* Уведомляет человека, шанс мэтча ×3!
*🎯 Совместимость?* Интересы + возраст + город + предпочтения
*🚀 Буст?* Анкета в топе 24ч, +500% просмотров
*🔄 Сброс?* VIP может сбросить пропущенных
*🎁 Пробный период?* 3 дня VIP Light бесплатно!
"""

    ADMIN_STATS = """
📊 *{bot_name}*

👥 {total}/{complete} · DAU:{dau} WAU:{wau} MAU:{mau}
VIP:{vip}({conversion:.1f}%) · Trial:{trials} · Бан:{banned} · +{today_reg}

💬 ❤️{likes} 💘{matches} 💬{messages}
💰 {revenue:.0f}₽ · Мес:{month_revenue:.0f}₽ · 🚩{pending_reports}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════

class KB:
    @staticmethod
    def main(unread=0, hidden_likes=0):
        chat = f"💬 Чаты ({unread})" if unread else "💬 Чаты"
        likes = f"❤️ Симпатии ({hidden_likes})" if hidden_likes else "❤️ Симпатии"
        return ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="📋 Анкеты"), KeyboardButton(text=likes)],
            [KeyboardButton(text=chat), KeyboardButton(text="👀 Гости")],
            [KeyboardButton(text="🛍️ Магазин"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="❓ FAQ")],
        ], resize_keyboard=True)

    @staticmethod
    def gender():
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👨 М", callback_data="g:male"), InlineKeyboardButton(text="👩 Ж", callback_data="g:female")]])

    @staticmethod
    def looking():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 М", callback_data="l:male"), InlineKeyboardButton(text="👩 Ж", callback_data="l:female")],
            [InlineKeyboardButton(text="👫 Всех", callback_data="l:both")]])

    @staticmethod
    def interests(selected=None):
        if not selected: selected = set()
        rows = []
        for i in range(0, len(Compatibility.INTERESTS_LIST), 2):
            row = []
            for j in range(2):
                if i+j < len(Compatibility.INTERESTS_LIST):
                    item = Compatibility.INTERESTS_LIST[i+j]
                    row.append(InlineKeyboardButton(text=f"{'✅' if item in selected else ''}{item}", callback_data=f"int:{i+j}"))
            rows.append(row)
        rows.append([InlineKeyboardButton(text="✅ Готово", callback_data="int:done")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def skip():
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭️ Пропустить", callback_data="skip")]])

    @staticmethod
    def search(uid, show_sl=False):
        row = [InlineKeyboardButton(text="👍", callback_data=f"lk:{uid}"), InlineKeyboardButton(text="👎", callback_data=f"dl:{uid}")]
        if show_sl: row.insert(1, InlineKeyboardButton(text="⭐", callback_data=f"sl:{uid}"))
        return InlineKeyboardMarkup(inline_keyboard=[row, [InlineKeyboardButton(text="🚩", callback_data=f"rp:{uid}")]])

    @staticmethod
    def no_profiles(is_vip=False):
        b = [[InlineKeyboardButton(text="🌍 Другие города", callback_data="sr:expand")]]
        if is_vip: b.append([InlineKeyboardButton(text="🔄 Сбросить", callback_data="sr:reset")])
        else: b.append([InlineKeyboardButton(text="🔄 Сброс (VIP)", callback_data="sr:reset_locked")])
        b.append([InlineKeyboardButton(text="🔁 Ещё раз", callback_data="sr:retry")])
        b.append([InlineKeyboardButton(text="◀️", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def matches(ms):
        b = [[InlineKeyboardButton(text=f"💬 {m['name']},{m['age']}{' 🎯'+str(int(m['compat']))+'%' if m.get('compat') else ''}{' 🔴'+str(m['unread']) if m.get('unread') else ''}", callback_data=f"ch:{m['user_id']}")] for m in ms[:10]]
        b.append([InlineKeyboardButton(text="◀️", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def who_liked(users):
        b = [[InlineKeyboardButton(text=f"{'⭐' if u.get('is_super_like') else '❤️'} {u['name']},{u['age']}", callback_data=f"wl:{u['id']}")] for u in users[:10]]
        b.append([InlineKeyboardButton(text="◀️", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def who_liked_action(uid):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👍 Лайк", callback_data=f"lk:{uid}"), InlineKeyboardButton(text="👎 Нет", callback_data=f"dl:{uid}")],
            [InlineKeyboardButton(text="◀️", callback_data="likes:list")]])

    @staticmethod
    def chat_actions(mid, pid):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Мэтчи", callback_data="bm")],
            [InlineKeyboardButton(text="💔 Размэтч", callback_data=f"um:{mid}")]])

    @staticmethod
    def shop():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 VIP-подписки", callback_data="sh:subs")],
            [InlineKeyboardButton(text="🚀 Буст", callback_data="sh:boost")],
            [InlineKeyboardButton(text="📊 Сравнить", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🎁 Промокод", callback_data="sh:promo"),
             InlineKeyboardButton(text="🎁 Пробный", callback_data="trial:start")],
            [InlineKeyboardButton(text="👥 Пригласи друга", callback_data="referral:info")],
            [InlineKeyboardButton(text="◀️", callback_data="mn")]])

    @staticmethod
    def subs():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Light — 149₽", callback_data="tf:vip_light")],
            [InlineKeyboardButton(text="🍾 Standard — 349₽ 🔥", callback_data="tf:vip_standard")],
            [InlineKeyboardButton(text="👑 Pro — 599₽", callback_data="tf:vip_pro")],
            [InlineKeyboardButton(text="💎 Forever — 2999₽", callback_data="tf:vip_lifetime")],
            [InlineKeyboardButton(text="🎁 3 дня бесплатно!", callback_data="trial:start")],
            [InlineKeyboardButton(text="📊 Сравнить", callback_data="sh:compare")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:mn")]])

    @staticmethod
    def buy_light():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎁 3 дня бесплатно!", callback_data="trial:start")],
            [InlineKeyboardButton(text="💳 149₽/мес", callback_data="by:vip_light:30:14900")],
            [InlineKeyboardButton(text="💳 379₽/3мес -15%", callback_data="by:vip_light:90:37900")],
            [InlineKeyboardButton(text="💳 649₽/6мес -27%", callback_data="by:vip_light:180:64900")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:subs")]])

    @staticmethod
    def buy_standard():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 349₽/мес", callback_data="by:vip_standard:30:34900")],
            [InlineKeyboardButton(text="💳 849₽/3мес -19%", callback_data="by:vip_standard:90:84900")],
            [InlineKeyboardButton(text="💳 1449₽/6мес -31%", callback_data="by:vip_standard:180:144900")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:subs")]])

    @staticmethod
    def buy_pro():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 599₽/мес", callback_data="by:vip_pro:30:59900")],
            [InlineKeyboardButton(text="💳 1499₽/3мес -17%", callback_data="by:vip_pro:90:149900")],
            [InlineKeyboardButton(text="💳 2599₽/6мес -28%", callback_data="by:vip_pro:180:259900")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:subs")]])

    @staticmethod
    def buy_lifetime():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 2999₽ навсегда", callback_data="by:vip_lifetime:0:299900")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:subs")]])

    @staticmethod
    def boost_menu(has, act):
        b = []
        if has: b.append([InlineKeyboardButton(text="🚀 Активировать", callback_data="bo:act")])
        b += [[InlineKeyboardButton(text="1×39₽", callback_data="by:boost:1:3900"), InlineKeyboardButton(text="5×149₽", callback_data="by:boost:5:14900")],
              [InlineKeyboardButton(text="10×249₽ -36%", callback_data="by:boost:10:24900")],
              [InlineKeyboardButton(text="◀️", callback_data="sh:mn")]]
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def profile(is_vip=False, hidden_likes=0):
        b = [[InlineKeyboardButton(text="✏️ Ред.", callback_data="pe"), InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")],
             [InlineKeyboardButton(text="🎯 Интересы", callback_data="ed:interests"), InlineKeyboardButton(text="🎂 Диапазон", callback_data="ed:agerange")],
             [InlineKeyboardButton(text="🚀 Буст", callback_data="profile:boost")]]
        if is_vip:
            b.append([InlineKeyboardButton(text=f"❤️ Кто лайкнул ({hidden_likes})", callback_data="likes:list")])
        elif hidden_likes > 0:
            b.append([InlineKeyboardButton(text=f"🔒 {hidden_likes} скрытых лайков → VIP", callback_data="sh:subs")])
        b.append([InlineKeyboardButton(text="👥 Пригласи друга", callback_data="referral:info")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def edit():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Имя", callback_data="ed:name"), InlineKeyboardButton(text="🎂 Возраст", callback_data="ed:age")],
            [InlineKeyboardButton(text="🌍 Город", callback_data="ed:city"), InlineKeyboardButton(text="✍️ О себе", callback_data="ed:bio")],
            [InlineKeyboardButton(text="◀️ Профиль", callback_data="pv")]])

    @staticmethod
    def report_reasons():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚨 Спам", callback_data="rr:spam"), InlineKeyboardButton(text="😱 Фейк", callback_data="rr:fake")],
            [InlineKeyboardButton(text="🔞 18+", callback_data="rr:nsfw"), InlineKeyboardButton(text="😠 Оскорб", callback_data="rr:harass")],
            [InlineKeyboardButton(text="❌", callback_data="mn")]])

    @staticmethod
    def admin():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статы", callback_data="adm:stats")],
            [InlineKeyboardButton(text="🔍 Найти", callback_data="adm:search"), InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="🚩 Жалобы", callback_data="adm:reports"), InlineKeyboardButton(text="🎁 Промо", callback_data="adm:promo")],
            [InlineKeyboardButton(text="✖️", callback_data="mn")]])

    @staticmethod
    def admin_user(uid, banned):
        ban = InlineKeyboardButton(text="✅ Разбан", callback_data=f"au:unban:{uid}") if banned else InlineKeyboardButton(text="🚫 Бан", callback_data=f"au:ban:{uid}")
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✨ VIP", callback_data=f"au:givevip:{uid}"), InlineKeyboardButton(text="🚀 Бусты", callback_data=f"au:giveboost:{uid}")],
            [ban, InlineKeyboardButton(text="✅ Верифик", callback_data=f"au:verify:{uid}")],
            [InlineKeyboardButton(text="🛡️", callback_data="adm:main")]])

    @staticmethod
    def give_vip_tiers():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Light", callback_data="gv:vip_light"), InlineKeyboardButton(text="🍾 Std", callback_data="gv:vip_standard")],
            [InlineKeyboardButton(text="👑 Pro", callback_data="gv:vip_pro"), InlineKeyboardButton(text="💎 Forever", callback_data="gv:vip_lifetime")],
            [InlineKeyboardButton(text="❌", callback_data="adm:main")]])

    @staticmethod
    def back_admin():
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛡️", callback_data="adm:main")]])

    @staticmethod
    def broadcast_targets():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 Все", callback_data="bc:all"), InlineKeyboardButton(text="✅ Анкеты", callback_data="bc:complete")],
            [InlineKeyboardButton(text="✨ VIP", callback_data="bc:vip"), InlineKeyboardButton(text="Free", callback_data="bc:free")],
            [InlineKeyboardButton(text="❌", callback_data="adm:main")]])

    @staticmethod
    def broadcast_confirm():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Отправить", callback_data="bc:send")],
            [InlineKeyboardButton(text="❌", callback_data="adm:main")]])

# ═══════════════════════════════════════════════════════════════════════════════
# ANTI-SPAM & PAY
# ═══════════════════════════════════════════════════════════════════════════════

class AntiSpam:
    def __init__(self):
        self.u: Dict[str, List[float]] = {}
    async def check(self, uid, act, limit=5, tw=60) -> bool:
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
        desc = f"{TIER_NAMES.get(tier,'')}" if ptype == "subscription" else f"Буст×{count}"
        try:
            p = YooPayment.create({"amount": {"value": f"{amount/100:.2f}", "currency": "RUB"}, "confirmation": {"type": ConfirmationType.REDIRECT, "return_url": f"{config.DOMAIN}/ok"}, "capture": True, "description": desc, "metadata": {"user_id": user["id"], "type": ptype, "tier": tier, "dur": dur, "count": count}}, str(uuid.uuid4()))
            pid = await DB.create_payment(user["id"], p.id, amount, desc, ptype, tier, dur, count)
            return {"pid": pid, "url": p.confirmation.confirmation_url}
        except Exception as e: logger.error(f"Pay err: {e}"); return {"error": str(e)}

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
        except Exception as e: logger.error(f"Check err: {e}"); return {"status": "error"}

# ═══════════════════════════════════════════════════════════════════════════════
# MIDDLEWARE
# ═══════════════════════════════════════════════════════════════════════════════

class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        tg = event.from_user if isinstance(event, (Message, CallbackQuery)) else None
        u = None
        if tg:
            u = await DB.get_user(tg.id)
            if u:
                u = await DB.reset_limits(u)
                if u.get("is_banned"):
                    if isinstance(event, Message): await event.answer(T.BANNED)
                    return
        data["user"] = u
        return await handler(event, data)

# ═══════════════════════════════════════════════════════════════════════════════
# HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

rt = Router()

def build_profile_text(u):
    badge = DB.get_badge(u); role = DB.get_role_tag(u); sub = DB.get_tier_name(u)
    if u.get("subscription_expires_at") and u["subscription_tier"] not in ("free", "vip_lifetime"):
        sub += f" (до {u['subscription_expires_at'].strftime('%d.%m')})"
    bi = ""
    if DB.is_boosted(u): bi += f"\n🚀 Буст до {u['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if u.get("boost_count"): bi += f"\n📦 Бустов: {u['boost_count']}"
    interests = u.get("interests", "")
    tips = MonetizationEngine.get_profile_tips(u)
    tips_txt = "\n\n💡 " + "\n💡 ".join(tips) if tips else ""
    return (f"👤 *Профиль*\n\n{badge}*{u['name']}*, {u['age']}{role}\n🌍 {u['city']}\n"
            f"{u['bio'] or '_Нет описания_'}\n{'🎯 '+interests if interests else ''}\n\n"
            f"👁️ {u['views_count']} · ❤️ {u['likes_received_count']} · 💘 {u['matches_count']}\n"
            f"🔍 {u['age_from']}-{u['age_to']}\nСтатус: {sub}{bi}{tips_txt}")

def build_card_text(p, v):
    badge = DB.get_badge(p); boost = " 🚀" if DB.is_boosted(p) else ""
    lm = {"male": "👨", "female": "👩", "both": "👫"}
    compat = Compatibility.calc_score(v, p)
    interests = p.get("interests", "")
    priority = p.get("_priority", "")
    pb = ""
    if priority == "liked_you" and DB.is_vip(v): pb = "\n❤️ _Лайкнул тебя!_"
    elif priority == "liked_you": pb = "\n💡 _Может быть взаимно..._"
    elif priority == "other_city": pb = "\n🌍 _Другой город_"
    return (f"{badge}*{p['name']}*{boost}, {p['age']}\n🌍 {p['city']}{pb}\n"
            f"{p['bio'] or ''}\n{'🎯 '+interests if interests else ''}\n\n"
            f"🎯 Совместимость: *{compat:.0f}%*\n🔍 Ищет: {lm.get(p.get('looking_for','both'),'👫')}")

# ══════════════════════ START ══════════════════════

@rt.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    # Обработка реферальной ссылки
    ref_code = None
    if msg.text and len(msg.text.split()) > 1:
        ref_code = msg.text.split()[1]

    if user and user.get("is_profile_complete"):
        un = await DB.get_unread(user["id"])
        who = await DB.get_who_liked_me(user["id"])
        await DB.update_hidden_likes(user["id"])
        st = DB.get_tier_name(user)
        if DB.is_boosted(user): st += " · 🚀"
        extras = ""
        if who and not DB.is_vip(user):
            extras = f"\n🔒 *{len(who)} скрытых лайков* — открой с VIP!"
        elif who and DB.is_vip(user):
            extras = f"\n❤️ Тебя лайкнули: {len(who)}"
        await msg.answer(T.WELCOME_BACK.format(name=user["name"], status=st, views=user["views_count"], matches=user["matches_count"], msgs=un, extras=extras),
                        reply_markup=KB.main(un, len(who)), parse_mode=ParseMode.MARKDOWN)
        # Показать тизер скрытых лайков
        if who and not DB.is_vip(user) and MonetizationEngine.should_show_teaser(user, "hidden_likes"):
            txt, kb = MonetizationEngine.get_hidden_likes_msg(len(who))
            await msg.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
            await DB.update_user(user["telegram_id"], last_teaser_shown=datetime.utcnow())
    else:
        if not user:
            user = await DB.create_user(msg.from_user.id, msg.from_user.username)
            if ref_code:
                await DB.process_referral(user["id"], ref_code)
        await msg.answer(T.WELCOME_NEW, parse_mode=ParseMode.MARKDOWN)
        await msg.answer(T.ASK_NAME, reply_markup=ReplyKeyboardRemove())
        await state.set_state(RegStates.name)

# ══════════════════════ REGISTRATION ══════════════════════

@rt.message(RegStates.name)
async def reg_name(m, state):
    n = m.text.strip()
    if len(n)<2 or len(n)>50: return await m.answer(T.BAD_NAME)
    await state.update_data(name=n); await m.answer(T.ASK_AGE, parse_mode=ParseMode.MARKDOWN); await state.set_state(RegStates.age)

@rt.message(RegStates.age)
async def reg_age(m, state):
    try: a=int(m.text.strip()); assert 18<=a<=99
    except: return await m.answer(T.BAD_AGE)
    await state.update_data(age=a); await m.answer(T.ASK_GENDER, reply_markup=KB.gender()); await state.set_state(RegStates.gender)

@rt.callback_query(RegStates.gender, F.data.startswith("g:"))
async def reg_gender(cb, state):
    await state.update_data(gender=cb.data[2:]); await cb.message.edit_text(T.ASK_CITY); await state.set_state(RegStates.city); await cb.answer()

@rt.message(RegStates.city)
async def reg_city(m, state):
    c=m.text.strip().title()
    if len(c)<2: return await m.answer("🌍 Город!")
    await state.update_data(city=c); await m.answer(T.ASK_PHOTO, reply_markup=KB.skip()); await state.set_state(RegStates.photo)

@rt.message(RegStates.photo, F.photo)
async def reg_photo(m, state):
    await state.update_data(photo=m.photo[-1].file_id); await m.answer(T.ASK_BIO, reply_markup=KB.skip()); await state.set_state(RegStates.bio)

@rt.callback_query(RegStates.photo, F.data=="skip")
async def reg_skip_photo(cb, state):
    await state.update_data(photo=None); await cb.message.edit_text(T.ASK_BIO); await state.set_state(RegStates.bio); await cb.answer()

@rt.message(RegStates.bio)
async def reg_bio(m, state):
    await state.update_data(bio=m.text.strip()[:500]); await m.answer(T.ASK_INTERESTS, reply_markup=KB.interests()); await state.update_data(sel_int=set()); await state.set_state(RegStates.interests)

@rt.callback_query(RegStates.bio, F.data=="skip")
async def reg_skip_bio(cb, state):
    await state.update_data(bio=""); await cb.message.edit_text(T.ASK_INTERESTS, reply_markup=KB.interests()); await state.update_data(sel_int=set()); await state.set_state(RegStates.interests); await cb.answer()

@rt.callback_query(RegStates.interests, F.data.startswith("int:"))
async def reg_int(cb, state):
    v=cb.data[4:]
    if v=="done":
        d=await state.get_data(); await state.update_data(interests=",".join(d.get("sel_int",set())))
        await cb.message.edit_text(T.ASK_LOOKING, reply_markup=KB.looking()); await state.set_state(RegStates.looking_for)
    else:
        d=await state.get_data(); sel=d.get("sel_int",set()); item=Compatibility.INTERESTS_LIST[int(v)]
        sel.discard(item) if item in sel else sel.add(item)
        await state.update_data(sel_int=sel); await cb.message.edit_reply_markup(reply_markup=KB.interests(sel))
    await cb.answer()

@rt.callback_query(RegStates.looking_for, F.data.startswith("l:"))
async def reg_looking(cb, state):
    await state.update_data(looking_for=cb.data[2:]); await cb.message.edit_text(T.ASK_AGE_RANGE, parse_mode=ParseMode.MARKDOWN); await state.set_state(RegStates.age_range); await cb.answer()

@rt.message(RegStates.age_range)
async def reg_age_range(m, state):
    try:
        parts=m.text.strip().replace(" ","").split("-"); af,at=int(parts[0]),int(parts[1])
        assert 18<=af<=99 and 18<=at<=99 and af<=at
    except: return await m.answer("⚠️ Формат: `18-30`", parse_mode=ParseMode.MARKDOWN)
    d=await state.get_data()
    upd={"name":d["name"],"age":d["age"],"gender":Gender(d["gender"]),"city":d["city"],"bio":d.get("bio",""),
         "interests":d.get("interests",""),"looking_for":LookingFor(d["looking_for"]),"age_from":af,"age_to":at,"is_profile_complete":True}
    if d.get("photo"): upd["photos"]=d["photo"]; upd["main_photo"]=d["photo"]
    await DB.update_user(m.from_user.id, **upd); await state.clear()
    await m.answer(T.REG_DONE, parse_mode=ParseMode.MARKDOWN)
    # Предложить пробный период
    await m.answer(
        "🎁 *Специальное предложение!*\n\nПопробуй VIP Light *3 дня бесплатно*!\n• Безлимит лайков\n• Суперлайки\n• Без рекламы",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Попробовать бесплатно!", callback_data="trial:start")],
            [InlineKeyboardButton(text="⏭️ Позже", callback_data="mn")]
        ]), parse_mode=ParseMode.MARKDOWN)

# ══════════════════════ BROWSE ══════════════════════

@rt.message(F.text=="📋 Анкеты")
async def browse(m, state, user):
    if not user or not user.get("is_profile_complete"): return await m.answer(T.NO_PROFILE)
    await state.set_state(SearchStates.browsing)
    ps=await DB.search_profiles(user,5)
    if not ps: return await m.answer(T.NO_PROFILES, reply_markup=KB.no_profiles(DB.is_vip(user)), parse_mode=ParseMode.MARKDOWN)
    await state.update_data(sq=[p["id"] for p in ps[1:]])
    await show_card(m, ps[0], user)

async def show_card(msg, p, v):
    await DB.add_guest(v["id"], p["id"])
    txt=build_card_text(p, v)
    show_sl=v.get("daily_superlikes_remaining",0)>0
    kb=KB.search(p["id"], show_sl)
    if p.get("main_photo"): await msg.answer_photo(photo=p["main_photo"], caption=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else: await msg.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

async def next_card(cb_or_msg, state, user):
    d=await state.get_data(); q=d.get("sq",[])
    if q:
        nid=q.pop(0); await state.update_data(sq=q)
        p=await DB.get_user_by_id(nid)
        if p and p.get("is_active") and not p.get("is_banned"):
            p["_priority"]="normal"; p["_compat"]=Compatibility.calc_score(user,p)
            msg=cb_or_msg.message if isinstance(cb_or_msg,CallbackQuery) else cb_or_msg
            return await show_card(msg, p, user)
    ps=await DB.search_profiles(user,5)
    msg=cb_or_msg.message if isinstance(cb_or_msg,CallbackQuery) else cb_or_msg
    if ps:
        await state.update_data(sq=[p["id"] for p in ps[1:]])
        await show_card(msg, ps[0], user)
    else:
        await msg.answer(T.NO_PROFILES, reply_markup=KB.no_profiles(DB.is_vip(user)), parse_mode=ParseMode.MARKDOWN)
        # Тизер буста когда анкеты кончились
        if not DB.is_vip(user) and MonetizationEngine.should_show_teaser(user, "boost"):
            txt, kb = MonetizationEngine.get_boost_tease(user)
            await msg.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
            await DB.update_user(user["telegram_id"], last_teaser_shown=datetime.utcnow())

@rt.callback_query(F.data.startswith("lk:"))
async def handle_like(cb, state, user):
    if not user: return await cb.answer("❌")
    if not DB.is_vip(user) and user.get("daily_likes_remaining",0) <= 0:
        txt, kb = MonetizationEngine.get_likes_limit_msg(user)
        try: await cb.message.edit_caption(caption=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        except: await cb.message.edit_text(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        return
    if not await anti_spam.check(cb.from_user.id, "like"): return await cb.answer("⚠️ Не спеши!", show_alert=True)

    tid=int(cb.data[3:])
    result=await DB.add_like(user["id"], tid)
    await DB.dec_likes(user["telegram_id"])

    # Предупреждение о малом количестве лайков
    user = await DB.get_user(cb.from_user.id)
    if not DB.is_vip(user):
        warning = MonetizationEngine.get_low_likes_warning(user.get("daily_likes_remaining", 0))
        if warning:
            await cb.message.answer(warning, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🥂 Безлимит лайков", callback_data="sh:subs")]
            ]), parse_mode=ParseMode.MARKDOWN)

    if result["is_match"]:
        t=await DB.get_user_by_id(tid); tn=t["name"] if t else "?"
        compat=result.get("compat",0)
        try: await cb.message.edit_caption(caption=T.NEW_MATCH.format(name=tn,compat=compat), parse_mode=ParseMode.MARKDOWN)
        except: await cb.message.edit_text(T.NEW_MATCH.format(name=tn,compat=compat), parse_mode=ParseMode.MARKDOWN)
        if t:
            try: await cb.bot.send_message(t["telegram_id"], T.NEW_MATCH.format(name=user["name"],compat=compat), parse_mode=ParseMode.MARKDOWN)
            except: pass
        # Upsell после мэтча
        upsell = MonetizationEngine.get_match_upsell(user)
        if upsell:
            txt, kb = upsell
            await cb.message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else:
        await cb.answer("👍")

    await next_card(cb, state, user)
    await cb.answer()

@rt.callback_query(F.data.startswith("sl:"))
async def handle_superlike(cb, state, user):
    if not user: return await cb.answer("❌")
    if user.get("daily_superlikes_remaining",0)<=0:
        txt, kb = MonetizationEngine.get_superlike_tease()
        await cb.answer("❌ Суперлайки закончились!", show_alert=True)
        await cb.message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        return
    tid=int(cb.data[3:])
    result=await DB.add_like(user["id"], tid, is_super=True)
    await DB.dec_superlikes(user["telegram_id"])
    await DB.dec_likes(user["telegram_id"])
    t=await DB.get_user_by_id(tid)
    if result["is_match"]:
        tn=t["name"] if t else "?"; compat=result.get("compat",0)
        try: await cb.message.edit_caption(caption=T.NEW_MATCH.format(name=tn,compat=compat), parse_mode=ParseMode.MARKDOWN)
        except: await cb.message.edit_text(T.NEW_MATCH.format(name=tn,compat=compat), parse_mode=ParseMode.MARKDOWN)
        if t:
            try: await cb.bot.send_message(t["telegram_id"], T.NEW_MATCH.format(name=user["name"],compat=compat), parse_mode=ParseMode.MARKDOWN)
            except: pass
    else:
        await cb.answer("⭐ Суперлайк!")
        if t:
            try: await cb.bot.send_message(t["telegram_id"], T.SUPERLIKE_RECEIVED.format(name=user["name"]), parse_mode=ParseMode.MARKDOWN)
            except: pass
    user=await DB.get_user(cb.from_user.id)
    await next_card(cb, state, user); await cb.answer()

@rt.callback_query(F.data.startswith("dl:"))
async def handle_dislike(cb, state, user):
    if not user: return
    await DB.add_dislike(user["id"], int(cb.data[3:]))
    await next_card(cb, state, user); await cb.answer()

@rt.callback_query(F.data=="sr:expand")
async def search_expand(cb, state, user):
    if not user: return
    ps=await DB.search_profiles(user, 5)
    if ps:
        await state.update_data(sq=[p["id"] for p in ps[1:]]); await state.set_state(SearchStates.browsing)
        await show_card(cb.message, ps[0], user)
    else: await cb.message.edit_text("😔 Пока нет. Зайди позже!")
    await cb.answer()

@rt.callback_query(F.data=="sr:reset")
async def search_reset(cb, state, user):
    if not user: return
    if not DB.is_vip(user): return await cb.answer("🥂 Только VIP!", show_alert=True)
    count=await DB.reset_dislikes(user["id"])
    await cb.answer(f"🔄 Сброшено {count}!", show_alert=True)
    user=await DB.get_user(cb.from_user.id)
    ps=await DB.search_profiles(user,5)
    if ps:
        await state.update_data(sq=[p["id"] for p in ps[1:]]); await state.set_state(SearchStates.browsing)
        await show_card(cb.message, ps[0], user)

@rt.callback_query(F.data=="sr:reset_locked")
async def search_reset_locked(cb, user):
    """Попытка сброса без VIP — upsell"""
    txt = "🔒 *Сброс пропущенных — только для VIP!*\n\nС VIP ты снова увидишь всех, кого пропустил.\n\n🍾 Доступно от Винчик Standard (349₽/мес)"
    await cb.message.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🍾 Открыть Standard", callback_data="tf:vip_standard")],
        [InlineKeyboardButton(text="🎁 3 дня бесплатно", callback_data="trial:start")],
    ]), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data=="sr:retry")
async def search_retry(cb, state, user):
    if not user: return
    ps=await DB.search_profiles(user,5)
    if ps:
        await state.update_data(sq=[p["id"] for p in ps[1:]]); await state.set_state(SearchStates.browsing)
        await show_card(cb.message, ps[0], user)
    else: await cb.answer("😔 Пока нет", show_alert=True)
    await cb.answer()

# ══════════════════════ TRIAL ══════════════════════

@rt.callback_query(F.data=="trial:start")
async def trial_start(cb, user):
    if not user: return await cb.answer("❌")
    if DB.is_vip(user): return await cb.answer("✨ У тебя уже VIP!", show_alert=True)
    if user.get("trial_used"): return await cb.answer("🎁 Пробный период уже был. Выбери тариф!", show_alert=True)
    ok = await DB.activate_trial(user["id"])
    if ok:
        await cb.message.answer(
            "🎉 *VIP Light активирован на 3 дня!*\n\n"
            "✅ 100 лайков/день\n✅ Безлимит сообщений\n✅ 1 суперлайк/день\n✅ Приоритет в выдаче\n\n"
            "Через 3 дня подписка отключится автоматически.\n"
            "Понравится — продли за 149₽/мес! 🍷",
            reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)
    else:
        await cb.answer("❌ Пробный период уже использован", show_alert=True)
    await cb.answer()

# ══════════════════════ REFERRAL ══════════════════════

@rt.callback_query(F.data=="referral:info")
async def referral_info(cb, user):
    if not user: return
    code = user.get("referral_code", "???")
    bonus = user.get("referral_bonus_count", 0)
    link = f"https://t.me/{(await cb.bot.me()).username}?start={code}"
    txt = (
        f"👥 *Пригласи друга — получи бонус!*\n\n"
        f"Твоя ссылка:\n`{link}`\n\n"
        f"За каждого друга с заполненной анкетой:\n"
        f"• Тебе: 🚀 1 бесплатный буст\n"
        f"• Другу: +5 дополнительных лайков\n\n"
        f"Приглашено: *{bonus}* друзей"
    )
    await cb.message.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Поделиться", url=f"https://t.me/share/url?url={link}&text=Присоединяйся к {BOT_NAME}!")],
        [InlineKeyboardButton(text="◀️", callback_data="pv")]
    ]), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

# ══════════════════════ WHO LIKED ME ══════════════════════

@rt.callback_query(F.data=="likes:list")
async def who_liked_list(cb, user):
    if not user: return
    if not DB.is_vip(user):
        who=await DB.get_who_liked_me(user["id"])
        txt, kb = MonetizationEngine.get_hidden_likes_msg(len(who))
        await cb.message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        return await cb.answer()
    users=await DB.get_likes_received(user["id"],20)
    if not users: return await cb.answer("😔 Пока нет", show_alert=True)
    await cb.message.edit_text(f"❤️ *Лайкнули тебя ({len(users)}):*", reply_markup=KB.who_liked(users), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("wl:"))
async def who_liked_view(cb, user):
    if not user or not DB.is_vip(user): return
    p=await DB.get_user_by_id(int(cb.data[3:]));
    if not p: return await cb.answer("Не найден")
    txt=build_card_text(p, user)
    try: await cb.message.edit_text(txt, reply_markup=KB.who_liked_action(p["id"]), parse_mode=ParseMode.MARKDOWN)
    except: pass
    await cb.answer()

# ══════════════════════ MATCHES & CHAT ══════════════════════

@rt.message(F.text.startswith("❤️"))
async def show_matches(m, user):
    if not user or not user.get("is_profile_complete"): return await m.answer(T.NO_PROFILE)
    ms=await DB.get_matches(user["id"])
    if ms: await m.answer(f"❤️ *Мэтчи ({len(ms)}):*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else: await m.answer(T.NO_MATCHES)

@rt.callback_query(F.data.startswith("ch:"))
async def start_chat(cb, state, user):
    if not user: return
    pid=int(cb.data[3:]); p=await DB.get_user_by_id(pid)
    if not p: return await cb.answer("?")
    mid=await DB.get_match_between(user["id"], pid)
    if not mid: return await cb.answer("Нет мэтча")
    await DB.mark_read(mid, user["id"])
    msgs=await DB.get_msgs(mid,5)
    txt=f"💬 *Чат с {p['name']}*\n\n"
    for mg in msgs:
        sn="Вы" if mg["sender_id"]==user["id"] else p["name"]
        txt+=f"*{sn}:* {mg['text']}\n"
    if not msgs: txt+="_Напиши первым!_"
    await state.update_data(cp=pid,mi=mid); await state.set_state(ChatStates.chatting)
    await cb.message.edit_text(txt, reply_markup=KB.chat_actions(mid,pid), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.message(ChatStates.chatting)
async def send_chat_msg(m, state, user):
    if not user: return
    d=await state.get_data(); mid,pid=d.get("mi"),d.get("cp")
    if not mid: await state.clear(); return await m.answer("Чат закрыт", reply_markup=KB.main())

    # Лимит сообщений
    if not DB.is_vip(user) and user.get("daily_messages_remaining", 0) <= 0:
        p = await DB.get_user_by_id(pid)
        txt, kb = MonetizationEngine.get_msg_limit_msg(p["name"] if p else "Собеседник")
        await m.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        return

    await DB.send_msg(mid, user["id"], m.text)
    await DB.dec_messages(user["telegram_id"])
    p=await DB.get_user_by_id(pid)
    if p:
        try: await m.bot.send_message(p["telegram_id"], f"💬 *{user['name']}:* {m.text}", parse_mode=ParseMode.MARKDOWN)
        except: pass

    # Предупреждение о лимите сообщений
    user = await DB.get_user(m.from_user.id)
    if not DB.is_vip(user) and user.get("daily_messages_remaining", 0) == 2:
        await m.answer("⚠️ *Осталось 2 сообщения!* С VIP — безлимит 💬", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Безлимит", callback_data="sh:subs")]
        ]), parse_mode=ParseMode.MARKDOWN)
    else:
        await m.answer("✅")

@rt.callback_query(F.data.startswith("um:"))
async def unmatch_handler(cb, state, user):
    if not user: return
    await DB.unmatch(user["id"], int(cb.data[3:])); await state.clear()
    await cb.message.edit_text("💔 Мэтч удалён."); await cb.answer()

@rt.callback_query(F.data=="bm")
async def back_matches(cb, state, user):
    await state.clear()
    if not user: return
    ms=await DB.get_matches(user["id"])
    if ms: await cb.message.edit_text(f"❤️ *({len(ms)}):*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else: await cb.message.edit_text(T.NO_MATCHES)
    await cb.answer()

@rt.message(F.text.startswith("💬"))
async def show_chats(m, user):
    if not user or not user.get("is_profile_complete"): return await m.answer(T.NO_PROFILE)
    ms=await DB.get_matches(user["id"])
    if ms: await m.answer("💬 *Диалоги:*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else: await m.answer(T.NO_MSGS)

@rt.message(F.text=="👀 Гости")
async def show_guests(m, user):
    if not user or not user.get("is_profile_complete"): return await m.answer(T.NO_PROFILE)
    lim=20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE
    gs=await DB.get_guests(user["id"],lim)
    if not gs: return await m.answer(T.NO_GUESTS)
    txt="👀 *Гости:*\n\n"
    for i,g in enumerate(gs,1): txt+=f"{i}. {g['name']},{g['age']}—{g['city']}\n"
    total_guests = len(await DB.get_guests(user["id"], 100))
    if not DB.is_vip(user) and total_guests > lim:
        txt += f"\n🔒 _Ещё {total_guests-lim} скрытых гостей — открой с VIP!_"
    await m.answer(txt, parse_mode=ParseMode.MARKDOWN)

# ══════════════════════ PROFILE ══════════════════════

@rt.message(F.text=="👤 Профиль")
async def show_profile(m, user):
    if not user or not user.get("is_profile_complete"): return await m.answer(T.NO_PROFILE)
    await DB.update_hidden_likes(user["id"])
    user = await DB.get_user(m.from_user.id)
    txt=build_profile_text(user)
    hidden = user.get("hidden_likes_count", 0)
    kb=KB.profile(DB.is_vip(user), hidden)
    if user.get("main_photo"): await m.answer_photo(photo=user["main_photo"], caption=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else: await m.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data=="pe")
async def profile_edit(cb):
    try: await cb.message.edit_caption(caption="✏️ *Редактировать:*", reply_markup=KB.edit(), parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text("✏️ *Редактировать:*", reply_markup=KB.edit(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data=="ed:name")
async def edit_name(cb, state): await cb.message.answer("👤 Имя:"); await state.set_state(EditStates.edit_name); await cb.answer()

@rt.message(EditStates.edit_name)
async def save_name(m, state):
    n=m.text.strip()
    if len(n)<2: return await m.answer(T.BAD_NAME)
    await DB.update_user(m.from_user.id, name=n); await state.clear(); await m.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:age")
async def edit_age(cb, state): await cb.message.answer("🎂:"); await state.set_state(EditStates.edit_age); await cb.answer()

@rt.message(EditStates.edit_age)
async def save_age(m, state):
    try: a=int(m.text.strip()); assert 18<=a<=99
    except: return await m.answer(T.BAD_AGE)
    await DB.update_user(m.from_user.id, age=a); await state.clear(); await m.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:city")
async def edit_city(cb, state): await cb.message.answer("🌍:"); await state.set_state(EditStates.edit_city); await cb.answer()

@rt.message(EditStates.edit_city)
async def save_city(m, state): await DB.update_user(m.from_user.id, city=m.text.strip().title()); await state.clear(); await m.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:bio")
async def edit_bio(cb, state): await cb.message.answer("✍️:"); await state.set_state(EditStates.edit_bio); await cb.answer()

@rt.message(EditStates.edit_bio)
async def save_bio(m, state): await DB.update_user(m.from_user.id, bio=m.text.strip()[:500]); await state.clear(); await m.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:interests")
async def edit_interests(cb, state, user):
    if not user: return
    cur=set(i.strip() for i in (user.get("interests") or "").split(",") if i.strip())
    await state.update_data(sel_int=cur, editing_int=True)
    await cb.message.answer(T.ASK_INTERESTS, reply_markup=KB.interests(cur), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(EditStates.edit_interests); await cb.answer()

@rt.callback_query(EditStates.edit_interests, F.data.startswith("int:"))
async def save_interests(cb, state):
    v=cb.data[4:]
    if v=="done":
        d=await state.get_data(); sel=d.get("sel_int",set())
        await DB.update_user(cb.from_user.id, interests=",".join(sel)); await state.clear()
        await cb.message.edit_text("✅ Обновлено!"); await cb.message.answer("👋", reply_markup=KB.main())
    else:
        d=await state.get_data(); sel=d.get("sel_int",set()); item=Compatibility.INTERESTS_LIST[int(v)]
        sel.discard(item) if item in sel else sel.add(item)
        await state.update_data(sel_int=sel); await cb.message.edit_reply_markup(reply_markup=KB.interests(sel))
    await cb.answer()

@rt.callback_query(F.data=="ed:agerange")
async def edit_agerange(cb, state, user):
    if not user: return
    await cb.message.answer(f"🎯 Сейчас: {user['age_from']}-{user['age_to']}\nНовый: `18-30`", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(EditStates.edit_age_range); await cb.answer()

@rt.message(EditStates.edit_age_range)
async def save_agerange(m, state):
    try:
        parts=m.text.strip().replace(" ","").split("-"); af,at=int(parts[0]),int(parts[1])
        assert 18<=af<=99 and 18<=at<=99 and af<=at
    except: return await m.answer("⚠️ `18-30`", parse_mode=ParseMode.MARKDOWN)
    await DB.update_user(m.from_user.id, age_from=af, age_to=at); await state.clear()
    await m.answer(f"✅ {af}-{at}", reply_markup=KB.main())

@rt.callback_query(F.data=="ed:photo")
async def edit_photo(cb, state): await cb.message.answer("📸:"); await state.set_state(EditStates.add_photo); await cb.answer()

@rt.message(EditStates.add_photo, F.photo)
async def save_photo(m, state, user):
    if not user: return
    pid=m.photo[-1].file_id
    pl=[p for p in (user.get("photos","") or "").split(",") if p.strip()]
    if len(pl)>=5: await state.clear(); return await m.answer("⚠️ Макс 5!", reply_markup=KB.main())
    pl.append(pid)
    await DB.update_user(m.from_user.id, photos=",".join(pl), main_photo=pid); await state.clear()
    await m.answer("✅ Фото!", reply_markup=KB.main())

@rt.callback_query(F.data=="pv")
async def back_profile(cb, user):
    if not user: return
    user=await DB.get_user(cb.from_user.id)
    await DB.update_hidden_likes(user["id"])
    user=await DB.get_user(cb.from_user.id)
    txt=build_profile_text(user); hidden=user.get("hidden_likes_count",0)
    try: await cb.message.edit_caption(caption=txt, reply_markup=KB.profile(DB.is_vip(user), hidden), parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text(txt, reply_markup=KB.profile(DB.is_vip(user), hidden), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

# ══════════════════════ BOOST ══════════════════════

@rt.callback_query(F.data=="profile:boost")
async def profile_boost(cb, user):
    if not user: return
    has=user.get("boost_count",0)>0; act=DB.is_boosted(user)
    st=""
    if act: st+=f"\n\n🚀 До {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if has: st+=f"\n📦 {user['boost_count']}"
    if not has and not act: st="\n\n❌ Нет бустов"
    txt=f"🚀 *БУСТ АНКЕТЫ*\n\nТоп выдачи на 24ч!\n👁️ +500% · ❤️ +300%\n\n💡 Лучше вечером!{st}"
    bk = InlineKeyboardMarkup(inline_keyboard=(
        ([[InlineKeyboardButton(text="🚀 Активировать", callback_data="bo:act:profile")]] if has else []) +
        [[InlineKeyboardButton(text="1×39₽", callback_data="by:boost:1:3900"), InlineKeyboardButton(text="5×149₽", callback_data="by:boost:5:14900")],
         [InlineKeyboardButton(text="◀️ Профиль", callback_data="pv")]]))
    try: await cb.message.edit_caption(caption=txt, reply_markup=bk, parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text(txt, reply_markup=bk, parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("bo:act"))
async def activate_boost(cb, user):
    if not user or user.get("boost_count",0)<=0: return await cb.answer("❌ Нет!", show_alert=True)
    ok=await DB.use_boost(user["id"])
    if ok:
        u=await DB.get_user(cb.from_user.id)
        back = "pv" if ":profile" in cb.data else "sh:mn"
        try: await cb.message.edit_caption(caption=f"🚀 *Активирован!*\nДо {u['boost_expires_at'].strftime('%d.%m %H:%M')}\n📦 {u['boost_count']}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️", callback_data=back)]]), parse_mode=ParseMode.MARKDOWN)
        except: await cb.message.edit_text(f"🚀 *Активирован!*\nДо {u['boost_expires_at'].strftime('%d.%m %H:%M')}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️", callback_data=back)]]), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

# ══════════════════════ SHOP ══════════════════════

@rt.message(F.text=="🛍️ Магазин")
async def shop_menu(m):
    await m.answer(T.SHOP, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data=="sh:mn")
async def shop_main(cb): await cb.message.edit_text(T.SHOP, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data=="sh:compare")
async def shop_compare(cb): await cb.message.edit_text(T.COMPARE, reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data=="sh:subs")
async def shop_subs(cb): await cb.message.edit_text("🥂 *Тарифы:*", reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data=="tf:vip_light")
async def tf_light(cb):
    txt = "🥂 *ВИНЧИК LIGHT*\n\n• 100 лайков · ∞ сообщений\n• 1 суперлайк⭐ · 10 гостей\n• Приоритет · Без рекламы\n\n_Попробуй 3 дня бесплатно!_"
    await cb.message.edit_text(txt, reply_markup=KB.buy_light(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data=="tf:vip_standard")
async def tf_std(cb):
    txt = "🍾 *ВИНЧИК STANDARD* 🔥\n\n• ∞ лайков · ∞ сообщений\n• 2 суперлайка⭐ · Все гости\n• *Кто тебя лайкнул* ❤️\n• Невидимка · 1 буст/день · Сброс 🔄"
    await cb.message.edit_text(txt, reply_markup=KB.buy_standard(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data=="tf:vip_pro")
async def tf_pro(cb):
    txt = "👑 *ВИНЧИК PRO* 💫\n\nВсё из Standard +\n• 5 суперлайков⭐ · 3 буста\n• Бейдж 👑 · Топ выдачи · 24/7"
    await cb.message.edit_text(txt, reply_markup=KB.buy_pro(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data=="tf:vip_lifetime")
async def tf_life(cb):
    txt = "💎 *ВИНЧИК FOREVER*\n\nВсё из Pro НАВСЕГДА!\n💎 Бейдж · 🎁 Обновления · 💬 Чат с командой\n\n*2999₽ — один раз!*"
    await cb.message.edit_text(txt, reply_markup=KB.buy_lifetime(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data=="sh:boost")
async def shop_boost(cb, user):
    if not user: return
    has=user.get("boost_count",0)>0; act=DB.is_boosted(user)
    st=""
    if act: st+=f"\n🚀 До {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if has: st+=f"\n📦 {user['boost_count']}"
    if not has and not act: st="\n❌ Нет бустов"
    await cb.message.edit_text(f"🚀 *БУСТ*\nТоп 24ч · +500% просмотров{st}", reply_markup=KB.boost_menu(has,act), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data.startswith("by:"))
async def handle_buy(cb, user):
    if not user: return await cb.answer("❌")
    parts=cb.data.split(":"); prod,param,amt=parts[1],int(parts[2]),int(parts[3])
    res = await Pay.create(user, "boost", count=param, amount=amt) if prod=="boost" else await Pay.create(user, "subscription", tier=prod, dur=param, amount=amt)
    if "error" in res: return await cb.answer(f"❌ {res['error']}", show_alert=True)
    await cb.message.edit_text(f"💳 *{amt/100:.0f}₽*\n\n1️⃣ Оплати → 2️⃣ Проверь",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить", url=res["url"])],
            [InlineKeyboardButton(text="✅ Проверить", callback_data=f"ck:{res['pid']}")],
            [InlineKeyboardButton(text="❌", callback_data="sh:mn")]
        ]), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data.startswith("ck:"))
async def check_payment(cb):
    res=await Pay.check(int(cb.data[3:]))
    if res["status"]=="succeeded":
        txt=f"✅ *{res.get('count',1)} бустов!*" if res.get("type")=="boost" else "✅ *Подписка активирована!* 🍷"
        await cb.message.edit_text(txt, parse_mode=ParseMode.MARKDOWN); await cb.message.answer("👋", reply_markup=KB.main())
    elif res["status"]=="pending": await cb.answer("⏳...", show_alert=True)
    else: await cb.answer("❌", show_alert=True)
    await cb.answer()

# ══════════════════════ PROMO ══════════════════════

@rt.callback_query(F.data=="sh:promo")
async def promo_input(cb, state):
    await cb.message.edit_text("🎁 *Промокод:*", parse_mode=ParseMode.MARKDOWN)
    await state.update_data(promo_user_mode=True); await state.set_state(AdminStates.promo_code); await cb.answer()

@rt.message(AdminStates.promo_code)
async def promo_code_input(m, state, user):
    d=await state.get_data()
    if d.get("promo_user_mode"):
        code=m.text.strip().upper(); await state.clear()
        if not user: return await m.answer("❌")
        result=await DB.use_promo(user["id"], code)
        if "error" in result: await m.answer(f"❌ {result['error']}", reply_markup=KB.main())
        else: await m.answer(f"✅ *{TIER_NAMES.get(result['tier'],'VIP')}* на {result['days']}дн! 🍷", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)
        return
    if not user or not DB.is_admin(user): return
    await state.update_data(pc_code=m.text.strip().upper())
    await m.answer("✨ *Тариф:*", reply_markup=KB.give_vip_tiers(), parse_mode=ParseMode.MARKDOWN); await state.set_state(AdminStates.promo_tier)

@rt.callback_query(AdminStates.promo_tier, F.data.startswith("gv:"))
async def promo_tier(cb, state, user):
    if not user or not DB.is_admin(user): return
    await state.update_data(pc_tier=cb.data[3:]); await cb.message.edit_text("⏰ *Дней?*", parse_mode=ParseMode.MARKDOWN); await state.set_state(AdminStates.promo_duration); await cb.answer()

@rt.message(AdminStates.promo_duration)
async def promo_dur(m, state, user):
    if not user or not DB.is_admin(user): return
    try: days=int(m.text.strip())
    except: return await m.answer("⚠️ Число!")
    await state.update_data(pc_days=days); await m.answer("🔢 *Лимит?*"); await state.set_state(AdminStates.promo_uses)

@rt.message(AdminStates.promo_uses)
async def promo_uses(m, state, user):
    if not user or not DB.is_admin(user): return
    try: uses=int(m.text.strip())
    except: return await m.answer("⚠️!")
    d=await state.get_data()
    await DB.create_promo(d["pc_code"], d["pc_tier"], d["pc_days"], uses); await state.clear()
    await m.answer(f"✅ `{d['pc_code']}` · {d['pc_days']}дн · ×{uses}", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

# ══════════════════════ FAQ & COMMON ══════════════════════

@rt.message(F.text=="❓ FAQ")
async def show_faq(m): await m.answer(T.FAQ, parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("rp:"))
async def start_report(cb, state):
    await state.update_data(rp_id=int(cb.data[3:]))
    try: await cb.message.edit_caption(caption="🚩 *Причина:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text("🚩 *Причина:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("rr:"))
async def save_report(cb, state, user):
    if not user: return
    d=await state.get_data(); rid=d.get("rp_id")
    if rid: await DB.create_report(user["id"], rid, cb.data[3:])
    await state.clear()
    try: await cb.message.edit_caption(caption="✅ Отправлено!")
    except: await cb.message.edit_text("✅ Отправлено!")
    await next_card(cb, state, user); await cb.answer()

@rt.callback_query(F.data=="mn")
async def back_menu(cb, state):
    await state.clear()
    try: await cb.message.delete()
    except: pass
    await cb.message.answer("👋", reply_markup=KB.main()); await cb.answer()

# ══════════════════════ ADMIN ══════════════════════

def is_adm(u): return u and u.get("telegram_id") in config.ADMIN_IDS

@rt.message(Command("admin"))
async def admin_cmd(m, user):
    if not is_adm(user): return
    await m.answer(f"🛡️ *Админка · {BOT_NAME}*\n*{user['name']}*", reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data=="adm:main")
async def admin_main(cb, state, user):
    if not is_adm(user): return; await state.clear()
    await cb.message.edit_text(f"🛡️ *Админка*\n*{user['name']}*", reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data=="adm:stats")
async def admin_stats(cb, user):
    if not is_adm(user): return
    s=await DB.get_stats()
    await cb.message.edit_text(T.ADMIN_STATS.format(bot_name=BOT_NAME, **s), reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data=="adm:search")
async def admin_search(cb, state, user):
    if not is_adm(user): return
    await cb.message.edit_text("🔍 *Запрос:*", parse_mode=ParseMode.MARKDOWN); await state.set_state(AdminStates.search_user); await cb.answer()

@rt.message(AdminStates.search_user)
async def admin_search_result(m, state, user):
    if not is_adm(user): return
    results=await DB.search_users(m.text.strip()); await state.clear()
    if not results: return await m.answer("😔", reply_markup=KB.back_admin())
    u=results[0]
    txt=f"ID:`{u['id']}` TG:`{u['telegram_id']}` @{u.get('username') or '-'}\n{DB.get_badge(u)}*{u['name']}*,{u['age']} · {u['city']}\n{TIER_NAMES.get(u['subscription_tier'],'')} · 👁️{u['views_count']} ❤️{u['likes_received_count']} 💘{u['matches_count']}"
    await m.answer(txt, reply_markup=KB.admin_user(u["id"], u["is_banned"]), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("au:ban:"))
async def admin_ban(cb, user):
    if not is_adm(user): return
    uid=int(cb.data.split(":")[2]); u=await DB.get_user_by_id(uid)
    if u: await DB.update_user(u["telegram_id"], is_banned=True); await cb.message.edit_text(f"🚫 Забанен!", reply_markup=KB.back_admin())
    await cb.answer()

@rt.callback_query(F.data.startswith("au:unban:"))
async def admin_unban(cb, user):
    if not is_adm(user): return
    uid=int(cb.data.split(":")[2]); u=await DB.get_user_by_id(uid)
    if u: await DB.update_user(u["telegram_id"], is_banned=False); await cb.message.edit_text("✅ Разбанен!", reply_markup=KB.back_admin())
    await cb.answer()

@rt.callback_query(F.data.startswith("au:verify:"))
async def admin_verify(cb, user):
    if not is_adm(user): return
    uid=int(cb.data.split(":")[2]); u=await DB.get_user_by_id(uid)
    if u: await DB.update_user(u["telegram_id"], is_verified=True); await cb.message.edit_text("✅!", reply_markup=KB.back_admin())
    await cb.answer()

@rt.callback_query(F.data.startswith("au:givevip:"))
async def admin_give_vip(cb, state, user):
    if not is_adm(user): return
    await state.update_data(target_uid=int(cb.data.split(":")[2]))
    await cb.message.edit_text("✨ *Тариф:*", reply_markup=KB.give_vip_tiers(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data.startswith("gv:"))
async def admin_gv(cb, state, user):
    if not is_adm(user): return
    tier=cb.data[3:]
    if tier=="vip_lifetime":
        d=await state.get_data()
        if d.get("target_uid"): await DB.activate_subscription_by_id(d["target_uid"], tier, 0)
        await state.clear(); await cb.message.edit_text("✅ 💎!", reply_markup=KB.back_admin())
    else:
        await state.update_data(give_tier=tier); await cb.message.edit_text("⏰ *Дней?*", parse_mode=ParseMode.MARKDOWN); await state.set_state(AdminStates.give_vip_duration)
    await cb.answer()

@rt.message(AdminStates.give_vip_duration)
async def admin_gv_days(m, state, user):
    if not is_adm(user): return
    try: days=int(m.text.strip())
    except: return await m.answer("⚠️!")
    d=await state.get_data()
    await DB.activate_subscription_by_id(d["target_uid"], d["give_tier"], days); await state.clear()
    await m.answer(f"✅ {days}дн!", reply_markup=KB.main())

@rt.callback_query(F.data.startswith("au:giveboost:"))
async def admin_give_boost(cb, state, user):
    if not is_adm(user): return
    await state.update_data(target_uid=int(cb.data.split(":")[2]))
    await cb.message.edit_text("🚀 *Сколько?*", parse_mode=ParseMode.MARKDOWN); await state.set_state(AdminStates.give_boost_count); await cb.answer()

@rt.message(AdminStates.give_boost_count)
async def admin_gb(m, state, user):
    if not is_adm(user): return
    try: c=int(m.text.strip())
    except: return await m.answer("⚠️!")
    d=await state.get_data(); await DB.add_boosts(d["target_uid"], c); await state.clear()
    await m.answer(f"✅ {c}!", reply_markup=KB.main())

@rt.callback_query(F.data=="adm:reports")
async def admin_reports(cb, user):
    if not is_adm(user): return
    reps=await DB.get_pending_reports(5)
    if not reps: await cb.message.edit_text("✅ Нет!", reply_markup=KB.back_admin()); await cb.answer(); return
    rep=reps[0]; rn=rep["reporter"]["name"] if rep["reporter"] else "?"; rdn=rep["reported"]["name"] if rep["reported"] else "?"; rid=rep["reported"]["id"] if rep["reported"] else 0
    await cb.message.edit_text(f"🚩 *#{rep['id']}*\n{rdn}(ID:{rid}) ← {rn}\n{rep['reason']}\nВсего:{len(reps)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Бан", callback_data=f"ar:ban:{rep['id']}:{rid}"), InlineKeyboardButton(text="⚠️ Варн", callback_data=f"ar:warn:{rep['id']}:{rid}")],
            [InlineKeyboardButton(text="❌ Откл", callback_data=f"ar:dismiss:{rep['id']}:{rid}")],
            [InlineKeyboardButton(text="➡️", callback_data="adm:reports")]
        ]), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data.startswith("ar:"))
async def admin_report_action(cb, user):
    if not is_adm(user): return
    parts=cb.data.split(":"); action,rid,ruid=parts[1],int(parts[2]),int(parts[3])
    if action=="ban":
        u=await DB.get_user_by_id(ruid)
        if u: await DB.update_user(u["telegram_id"], is_banned=True)
        await DB.resolve_report(rid, "banned"); await cb.message.edit_text("🚫", reply_markup=KB.back_admin())
    elif action=="warn":
        u=await DB.get_user_by_id(ruid)
        if u:
            try: await cb.bot.send_message(u["telegram_id"], "⚠️ Предупреждение!")
            except: pass
        await DB.resolve_report(rid, "warned"); await cb.message.edit_text("⚠️", reply_markup=KB.back_admin())
    elif action=="dismiss":
        await DB.resolve_report(rid, "dismissed"); await cb.message.edit_text("❌", reply_markup=KB.back_admin())
    await cb.answer()

@rt.callback_query(F.data=="adm:broadcast")
async def admin_broadcast(cb, state, user):
    if not is_adm(user): return
    await cb.message.edit_text("📢 *Текст:*", parse_mode=ParseMode.MARKDOWN); await state.set_state(AdminStates.broadcast_text); await cb.answer()

@rt.message(AdminStates.broadcast_text)
async def admin_bc_text(m, state, user):
    if not is_adm(user): return
    await state.update_data(bc_text=m.text); await m.answer("👥 *Кому?*", reply_markup=KB.broadcast_targets(), parse_mode=ParseMode.MARKDOWN); await state.set_state(AdminStates.broadcast_confirm)

@rt.callback_query(AdminStates.broadcast_confirm, F.data.startswith("bc:"))
async def admin_bc(cb, state, user):
    if not is_adm(user): return
    target=cb.data[3:]
    if target=="send":
        d=await state.get_data(); txt=d["bc_text"]; tgt=d.get("bc_target","all")
        ids=await DB.get_all_user_ids(tgt); await state.clear()
        await cb.message.edit_text(f"📢 *{len(ids)}...*", parse_mode=ParseMode.MARKDOWN)
        sent=failed=0
        for tid in ids:
            try: await cb.bot.send_message(tid, txt, parse_mode=ParseMode.MARKDOWN); sent+=1
            except: failed+=1
            if sent%25==0: await asyncio.sleep(1)
        await DB.log_broadcast(user["telegram_id"], txt, tgt, sent, failed)
        await cb.message.answer(f"✅ {sent} · ❌ {failed}", reply_markup=KB.back_admin())
    else:
        await state.update_data(bc_target=target); d=await state.get_data()
        names={"all":"Все","complete":"Анкеты","vip":"VIP","free":"Free"}
        ids=await DB.get_all_user_ids(target)
        await cb.message.edit_text(f"📢 *Рассылка*\n\n{d['bc_text'][:200]}\n\n*{names.get(target,target)}* · {len(ids)}",
            reply_markup=KB.broadcast_confirm(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data=="adm:promo")
async def admin_promo_start(cb, state, user):
    if not is_adm(user): return
    await cb.message.edit_text("🎁 *Код:*", parse_mode=ParseMode.MARKDOWN)
    await state.update_data(promo_user_mode=False); await state.set_state(AdminStates.promo_code); await cb.answer()

# ═════════════════════════════════════════════════════════════════════════════════
# BACKGROUND TASKS — периодические уведомления
# ═════════════════════════════════════════════════════════════════════════════════

async def send_hidden_likes_reminders(bot: Bot):
    """Раз в 4 часа уведомлять о скрытых лайках"""
    while True:
        await asyncio.sleep(14400)  # 4 часа
        try:
            async with async_session_maker() as s:
                users = await s.execute(
                    select(User).where(and_(
                        User.is_active == True,
                        User.is_banned == False,
                        User.is_profile_complete == True,
                        User.subscription_tier == SubscriptionTier.FREE,
                        User.hidden_likes_count > 0,
                        User.last_active_at > datetime.utcnow() - timedelta(days=3),
                    )).limit(50)
                )
                for u in users.scalars().all():
                    if u.hidden_likes_count > 0:
                        try:
                            txt, kb = MonetizationEngine.get_hidden_likes_msg(u.hidden_likes_count)
                            await bot.send_message(u.telegram_id, txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                        except: pass
                        await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Reminder error: {e}")

async def send_boost_suggestions(bot: Bot):
    """Раз в день вечером предлагать буст"""
    while True:
        now = datetime.utcnow()
        # Ждём 19:00 UTC (22:00 МСК)
        target = now.replace(hour=19, minute=0, second=0)
        if now > target:
            target += timedelta(days=1)
        wait = (target - now).total_seconds()
        await asyncio.sleep(wait)

        try:
            async with async_session_maker() as s:
                users = await s.execute(
                    select(User).where(and_(
                        User.is_active == True, User.is_banned == False,
                        User.is_profile_complete == True,
                        User.last_active_at > datetime.utcnow() - timedelta(days=2),
                    )).limit(100)
                )
                for u in users.scalars().all():
                    if not u.boost_expires_at or u.boost_expires_at < datetime.utcnow():
                        try:
                            ud = DB._to_dict(u)
                            txt, kb = MonetizationEngine.get_boost_tease(ud)
                            await bot.send_message(u.telegram_id, txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                        except: pass
                        await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Boost suggest error: {e}")

# ═════════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════════

async def main():
    await init_db()
    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(rt)
    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())

    # Запуск фоновых задач
    asyncio.create_task(send_hidden_likes_reminders(bot))
    asyncio.create_task(send_boost_suggestions(bot))

    logger.info(f"🚀 {BOT_NAME} запущен")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
