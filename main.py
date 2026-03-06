"""
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🍷 ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v4.1
🍷 + ПОЛНАЯ СИСТЕМА УВЕДОМЛЕНИЙ
🍷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import os
import uuid
import logging
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
    Text, ForeignKey, Enum as SQLEnum, Float, JSON,
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

    # Notification settings
    NOTIFY_BATCH_SIZE: int = 25
    NOTIFY_BATCH_DELAY: float = 1.0
    INACTIVE_REMINDER_HOURS: int = 24
    SUB_EXPIRY_WARN_DAYS: int = 3

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
    WINE_GLASS = "wine_glass"
    WINE_BOTTLE = "wine_bottle"
    SOMMELIER = "sommelier"
    WINE_CELLAR = "wine_cellar"

class PaymentStatus(str, Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    CANCELED = "canceled"

class NotificationType(str, Enum):
    LIKE_RECEIVED = "like_received"
    SUPER_LIKE_RECEIVED = "super_like_received"
    NEW_MATCH = "new_match"
    NEW_MESSAGE = "new_message"
    PROFILE_VIEWED = "profile_viewed"
    DAILY_REMINDER = "daily_reminder"
    LIMITS_RESTORED = "limits_restored"
    SUB_EXPIRING = "sub_expiring"
    SUB_EXPIRED = "sub_expired"
    BOOST_EXPIRED = "boost_expired"
    NEW_USERS_IN_CITY = "new_users_in_city"
    WEEKLY_STATS = "weekly_stats"
    UNREAD_MESSAGES = "unread_messages"


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

    interests = Column(Text, default="")

    # ═══ NOTIFICATION SETTINGS ═══
    notify_likes = Column(Boolean, default=True)
    notify_matches = Column(Boolean, default=True)
    notify_messages = Column(Boolean, default=True)
    notify_guests = Column(Boolean, default=True)
    notify_reminders = Column(Boolean, default=True)
    notify_sub_events = Column(Boolean, default=True)
    quiet_hours_start = Column(Integer, nullable=True)  # 0-23 часов
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
    is_notified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class GuestVisit(Base):
    __tablename__ = "guest_visits"
    id = Column(Integer, primary_key=True)
    visitor_id = Column(Integer, ForeignKey("users.id"))
    visited_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    is_notified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class NotificationLog(Base):
    """Лог отправленных уведомлений (для дедупликации и статистики)"""
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

class NotifySettingsState(StatesGroup):
    quiet_start = State()
    quiet_end = State()

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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#             COMPATIBILITY ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CompatibilityEngine:
    @staticmethod
    def calculate(user1: Dict, user2: Dict) -> float:
        score = 0.0
        max_score = 0.0

        max_score += 30
        if (user1.get("city") or "").lower() == (user2.get("city") or "").lower():
            score += 30

        max_score += 25
        age1 = user1.get("age", 25)
        age2 = user2.get("age", 25)
        age_diff = abs(age1 - age2)
        if age_diff <= 2: score += 25
        elif age_diff <= 5: score += 20
        elif age_diff <= 10: score += 12
        elif age_diff <= 15: score += 5

        max_score += 20
        lf1 = user1.get("looking_for", "both")
        lf2 = user2.get("looking_for", "both")
        g1 = user1.get("gender")
        g2 = user2.get("gender")
        mutual = True
        if lf1 != "both" and lf1 != g2: mutual = False
        if lf2 != "both" and lf2 != g1: mutual = False
        if mutual: score += 20

        max_score += 15
        la = user2.get("last_active_at")
        if la:
            hours_ago = (datetime.utcnow() - la).total_seconds() / 3600
            if hours_ago < 1: score += 15
            elif hours_ago < 6: score += 12
            elif hours_ago < 24: score += 8
            elif hours_ago < 72: score += 3

        max_score += 10
        if user2.get("bio"): score += 5
        if user2.get("main_photo"): score += 5

        return round((score / max_score) * 100, 1) if max_score > 0 else 0.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#               ANIMATIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class Anim:
    WINE_POUR = ["🍷", "🍷🍷", "🍷🍷🍷", "🥂 Наливаем..."]
    HEARTS = ["💕", "💕💕", "💕💕💕", "💞 Это мэтч!"]
    SEARCH = ["🔍", "🔍👀", "🔍👀✨", "🍷 Ищем анкеты..."]

    @staticmethod
    async def animate(message: Message, frames: List[str], delay: float = 0.4) -> Message:
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
        frames = ["💕", "💕💕💕", "🎉💕🎉💕🎉",
                  f"🍷✨ *Взаимная симпатия!* ✨🍷\n\n💕 Вы с *{name}* понравились друг другу!"]
        msg = await message.answer(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(0.5)
            try: await msg.edit_text(frame, parse_mode=ParseMode.MARKDOWN)
            except Exception: pass

    @staticmethod
    async def boost_animation(message: Message) -> Message:
        frames = ["🚀", "🚀✨", "🚀✨🍷", "🚀✨🍷 *Буст активирован!*"]
        msg = await message.answer(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(0.4)
            try: await msg.edit_text(frame, parse_mode=ParseMode.MARKDOWN)
            except Exception: pass
        return msg

    @staticmethod
    async def payment_success_animation(message: Message) -> Message:
        frames = ["💳", "💳 ✅", "💳 ✅ 🎉", "🍷 *Оплата прошла успешно!* 🎉"]
        msg = await message.answer(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(0.4)
            try: await msg.edit_text(frame, parse_mode=ParseMode.MARKDOWN)
            except Exception: pass
        return msg

    @staticmethod
    def compatibility_bar(score: float) -> str:
        filled = int(score / 10)
        empty = 10 - filled
        bar = "🟣" * filled + "⚪" * empty
        if score >= 80: emoji = "🔥"
        elif score >= 60: emoji = "💕"
        elif score >= 40: emoji = "👍"
        else: emoji = "🤔"
        return f"{emoji} {bar} {score:.0f}%"

    @staticmethod
    def get_wine_emoji() -> str:
        return random.choice(["🍷", "🥂", "🍇", "🍾", "🏆"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#           NOTIFICATION SERVICE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class NotificationService:
    """
    Полная система уведомлений:
    - Мгновенные (лайк, мэтч, сообщение, гость, суперлайк)
    - Фоновые (напоминания, статистика, истечение подписки, лимиты)
    - Настройки пользователя (вкл/выкл, тихие часы)
    - Логирование и дедупликация
    """

    def __init__(self, bot: Bot):
        self.bot = bot
        self._running = False

    # ═══ ПРОВЕРКА НАСТРОЕК ═══

    @staticmethod
    def _can_notify(user: Dict, ntype: str) -> bool:
        """Проверяет, можно ли отправить уведомление пользователю"""
        if not user or user.get("is_banned"):
            return False

        # Проверка по типу
        type_settings = {
            "like_received": "notify_likes",
            "super_like_received": "notify_likes",
            "new_match": "notify_matches",
            "new_message": "notify_messages",
            "profile_viewed": "notify_guests",
            "daily_reminder": "notify_reminders",
            "limits_restored": "notify_reminders",
            "sub_expiring": "notify_sub_events",
            "sub_expired": "notify_sub_events",
            "boost_expired": "notify_sub_events",
            "new_users_in_city": "notify_reminders",
            "weekly_stats": "notify_reminders",
            "unread_messages": "notify_messages",
        }

        setting_key = type_settings.get(ntype, "notify_reminders")
        if not user.get(setting_key, True):
            return False

        # Проверка тихих часов
        qs = user.get("quiet_hours_start")
        qe = user.get("quiet_hours_end")
        if qs is not None and qe is not None:
            now_hour = datetime.utcnow().hour
            if qs < qe:
                if qs <= now_hour < qe:
                    return False
            else:  # Через полночь (напр. 23-7)
                if now_hour >= qs or now_hour < qe:
                    return False

        return True

    # ═══ ОТПРАВКА ═══

    async def _send(self, user: Dict, text: str, ntype: str,
                    reply_markup=None, log: bool = True) -> bool:
        """Базовая отправка с логированием"""
        if not self._can_notify(user, ntype):
            return False

        delivered = False
        error = None
        try:
            await self.bot.send_message(
                user["telegram_id"], text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
            delivered = True
        except Exception as e:
            error = str(e)[:200]
            logger.debug(f"Notify failed {user['telegram_id']}: {error}")

        if log:
            await self._log(user["id"], ntype, text[:200], delivered, error)

        return delivered

    @staticmethod
    async def _log(user_id: int, ntype: str, text: str,
                   delivered: bool, error: str = None):
        try:
            async with async_session_maker() as s:
                s.add(NotificationLog(
                    user_id=user_id,
                    notification_type=ntype,
                    message_text=text,
                    is_delivered=delivered,
                    error_text=error
                ))
                await s.commit()
        except Exception as e:
            logger.error(f"Notify log error: {e}")

    # ═══ МГНОВЕННЫЕ УВЕДОМЛЕНИЯ ═══

    async def notify_like_received(self, liker: Dict, target: Dict, is_super: bool = False):
        """🔔 Кто-то поставил лайк"""
        if is_super:
            text = (
                f"⚡ *Суперлайк!*\n\n"
                f"💜 *{liker['name']}*, {liker['age']} из {liker['city']} "
                f"поставил(а) тебе суперлайк!\n\n"
                f"_Ты точно ей/ему понравился!_ 🍷"
            )
            ntype = "super_like_received"
        else:
            # VIP видят кто лайкнул, бесплатные — нет
            if DB.is_vip(target):
                text = (
                    f"❤️ *Новый лайк!*\n\n"
                    f"*{liker['name']}*, {liker['age']} из {liker['city']} "
                    f"оценил(а) твой профиль!\n\n"
                    f"_Листай анкеты — может это взаимно!_ 💕"
                )
            else:
                text = (
                    f"❤️ *Кто-то оценил твой профиль!*\n\n"
                    f"У тебя {target.get('likes_received_count', 0) + 1} лайков!\n"
                    f"🔒 _Подпишись чтобы узнать кто_ 🍷"
                )
            ntype = "like_received"

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍷 Смотреть анкеты", callback_data="go:browse")],
        ])
        await self._send(target, text, ntype, reply_markup=kb)

    async def notify_new_match(self, user1: Dict, user2: Dict, compatibility: float):
        """🔔 Новый мэтч с совместимостью"""
        compat_bar = Anim.compatibility_bar(compatibility)

        for me, partner in [(user1, user2), (user2, user1)]:
            text = (
                f"🍷✨ *Взаимная симпатия!* ✨🍷\n\n"
                f"💕 Вы с *{partner['name']}* понравились друг другу!\n\n"
                f"📊 Совместимость: {compat_bar}\n\n"
                f"_Напишите друг другу прямо сейчас!_ 💬"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"💬 Написать {partner['name']}",
                    callback_data=f"ch:{partner['id']}"
                )],
                [InlineKeyboardButton(text="💕 Все симпатии", callback_data="go:matches")],
            ])
            await self._send(me, text, "new_match", reply_markup=kb)

    async def notify_new_message(self, sender: Dict, recipient: Dict, text_preview: str):
        """🔔 Новое сообщение"""
        preview = text_preview[:50] + "..." if len(text_preview) > 50 else text_preview
        badge = DB.get_badge(sender)

        text = (
            f"💬 *Новое сообщение!*\n\n"
            f"{badge}*{sender['name']}:*\n"
            f"_{preview}_"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"💬 Ответить {sender['name']}",
                callback_data=f"ch:{sender['id']}"
            )],
        ])
        await self._send(recipient, text, "new_message", reply_markup=kb)

    async def notify_profile_viewed(self, visitor: Dict, target: Dict):
        """🔔 Кто-то посмотрел профиль (батчевое — раз в час)"""
        la = target.get("last_notified_at")
        if la and (datetime.utcnow() - la).total_seconds() < 3600:
            return  # Не чаще раза в час

        if DB.is_vip(target):
            text = (
                f"👻 *{visitor['name']}*, {visitor['age']} "
                f"заходил(а) на твой профиль!\n\n"
                f"🍷 _Полный список в разделе «Гости»_"
            )
        else:
            text = (
                f"👻 *Кто-то смотрел твой профиль!*\n\n"
                f"👁️ Всего просмотров: {target.get('views_count', 0) + 1}\n"
                f"🔒 _Подпишись чтобы видеть всех гостей_ 🍷"
            )

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👻 Гости", callback_data="go:guests")],
        ])
        ok = await self._send(target, text, "profile_viewed", reply_markup=kb)
        if ok:
            await DB.update_user(target["telegram_id"], last_notified_at=datetime.utcnow())

    async def notify_sub_expiring(self, user: Dict, days_left: int):
        """🔔 Подписка скоро истекает"""
        tier = TIER_NAMES.get(user["subscription_tier"], "VIP")
        text = (
            f"⏰ *Подписка истекает!*\n\n"
            f"🍷 {tier} заканчивается через *{days_left}* дн.\n\n"
            f"🔄 Продли чтобы не потерять привилегии!"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍷 Продлить", callback_data="sh:subs")],
        ])
        await self._send(user, text, "sub_expiring", reply_markup=kb)

    async def notify_sub_expired(self, user: Dict):
        """🔔 Подписка истекла"""
        tier = TIER_NAMES.get(user["subscription_tier"], "VIP")
        text = (
            f"😔 *Подписка {tier} истекла*\n\n"
            f"Ты снова на бесплатном тарифе:\n"
            f"• 30 лайков/день\n"
            f"• 10 сообщений/день\n"
            f"• 3 гостя\n\n"
            f"🍷 _Верни свои привилегии!_"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍷 Вернуть подписку", callback_data="sh:subs")],
        ])
        await self._send(user, text, "sub_expired", reply_markup=kb)

    async def notify_boost_expired(self, user: Dict):
        """🔔 Буст закончился"""
        text = (
            f"🚀 *Буст закончился!*\n\n"
            f"📉 Твоя анкета вернулась в обычную выдачу.\n"
            f"За время буста: +{random.randint(15, 50)} просмотров!\n\n"
            f"🔄 _Активируй новый буст!_"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Новый буст", callback_data="sh:boost")],
        ])
        await self._send(user, text, "boost_expired", reply_markup=kb)

    async def notify_limits_restored(self, user: Dict):
        """🔔 Лимиты восстановлены"""
        tier = user.get("subscription_tier", "free")
        limits = DB.get_tier_limits(tier)
        text = (
            f"🌅 *Доброе утро!*\n\n"
            f"❤️ Лайков: {limits['likes'] if limits['likes'] < 999999 else '♾️'}\n"
            f"💬 Сообщений: {'♾️' if limits['messages'] > 999 else limits['messages']}\n\n"
            f"🍷 _Время искать свою половинку!_"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍷 Смотреть анкеты", callback_data="go:browse")],
        ])
        await self._send(user, text, "limits_restored", reply_markup=kb)

    async def notify_daily_reminder(self, user: Dict):
        """🔔 Дневное напоминание для неактивных"""
        variants = [
            (
                f"🍷 *{user['name']}, давно тебя не видели!*\n\n"
                f"💕 Появились новые анкеты в {user.get('city', 'твоём городе')}!\n"
                f"_Может сегодня тот самый день?_ ✨"
            ),
            (
                f"👋 *{user['name']}, скучаем!*\n\n"
                f"🍷 Пока тебя не было, {random.randint(3, 15)} "
                f"человек искали знакомства рядом!\n\n"
                f"_Загляни — вдруг кто-то ждёт именно тебя_ 💕"
            ),
            (
                f"🥂 *{user['name']}, бокальчик вина?*\n\n"
                f"У тебя {user.get('likes_received_count', 0)} лайков!\n"
                f"Не упусти свой шанс 🍷"
            ),
        ]
        text = random.choice(variants)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍷 Смотреть анкеты", callback_data="go:browse")],
        ])
        await self._send(user, text, "daily_reminder", reply_markup=kb)

    async def notify_unread_messages(self, user: Dict, unread_count: int):
        """🔔 Напоминание о непрочитанных сообщениях"""
        text = (
            f"💬 *У тебя {unread_count} непрочитанных сообщений!*\n\n"
            f"_Не заставляй ждать!_ 🍷"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Читать", callback_data="go:matches")],
        ])
        await self._send(user, text, "unread_messages", reply_markup=kb)

    async def notify_weekly_stats(self, user: Dict, stats: Dict):
        """🔔 Еженедельная статистика"""
        text = (
            f"📊 *Твоя неделя на Винчике:*\n\n"
            f"👁️ Просмотров: {stats.get('views', 0)}\n"
            f"❤️ Лайков: {stats.get('likes', 0)}\n"
            f"💕 Новых мэтчей: {stats.get('matches', 0)}\n"
            f"💬 Сообщений: {stats.get('messages', 0)}\n\n"
        )
        if stats.get('likes', 0) > 0:
            text += f"🔥 _Ты в топе! Продолжай в том же духе!_ 🍷"
        else:
            text += f"💡 _Добавь фото и описание для больше лайков!_ 🍷"

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍷 К анкетам", callback_data="go:browse")],
        ])
        await self._send(user, text, "weekly_stats", reply_markup=kb)

    async def notify_new_users_in_city(self, user: Dict, count: int):
        """🔔 Новые пользователи в городе"""
        text = (
            f"✨ *Новые люди рядом!*\n\n"
            f"🏙️ В *{user.get('city', 'твоём городе')}* "
            f"появилось *{count}* новых анкет!\n\n"
            f"🍷 _Успей познакомиться первым!_"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍷 Смотреть", callback_data="go:browse")],
        ])
        await self._send(user, text, "new_users_in_city", reply_markup=kb)

    # ═══ ФОНОВЫЙ ПЛАНИРОВЩИК ═══

    async def start_scheduler(self):
        """Запускает фоновые задачи"""
        self._running = True
        logger.info("🔔 Notification scheduler started")
        asyncio.create_task(self._run_periodic())

    async def stop_scheduler(self):
        self._running = False
        logger.info("🔔 Notification scheduler stopped")

    async def _run_periodic(self):
        """Главный цикл фоновых уведомлений"""
        while self._running:
            try:
                await self._check_sub_expiry()
                await self._check_boost_expiry()
                await self._check_inactive_users()
                await self._check_unread_messages()
                await self._check_new_users_in_cities()
                await self._check_limits_reset()
                await self._check_weekly_stats()
            except Exception as e:
                logger.error(f"🔔 Scheduler error: {e}")

            # Проверяем каждые 15 минут
            await asyncio.sleep(900)

    async def _check_sub_expiry(self):
        """Проверяет истекающие и истёкшие подписки"""
        async with async_session_maker() as s:
            now = datetime.utcnow()
            warn_date = now + timedelta(days=config.SUB_EXPIRY_WARN_DAYS)

            # Скоро истекающие
            expiring = await s.execute(
                select(User).where(and_(
                    User.subscription_tier != SubscriptionTier.FREE,
                    User.subscription_tier != SubscriptionTier.WINE_CELLAR,
                    User.subscription_expires_at.isnot(None),
                    User.subscription_expires_at > now,
                    User.subscription_expires_at <= warn_date,
                    User.is_active == True,
                    User.is_banned == False
                ))
            )
            for u in expiring.scalars().all():
                ud = DB._to_dict(u)
                days_left = max(1, (u.subscription_expires_at - now).days)
                # Не спамим — проверяем лог
                last_warn = await self._last_notification(u.id, "sub_expiring")
                if not last_warn or (now - last_warn).days >= 1:
                    await self.notify_sub_expiring(ud, days_left)

            # Уже истёкшие
            expired = await s.execute(
                select(User).where(and_(
                    User.subscription_tier != SubscriptionTier.FREE,
                    User.subscription_tier != SubscriptionTier.WINE_CELLAR,
                    User.subscription_expires_at.isnot(None),
                    User.subscription_expires_at <= now,
                    User.is_active == True
                ))
            )
            for u in expired.scalars().all():
                ud = DB._to_dict(u)
                last_exp = await self._last_notification(u.id, "sub_expired")
                if not last_exp or (now - last_exp).days >= 3:
                    await self.notify_sub_expired(ud)
                    # Сбрасываем подписку
                    await s.execute(
                        update(User).where(User.id == u.id)
                        .values(subscription_tier=SubscriptionTier.FREE)
                    )
            await s.commit()

    async def _check_boost_expiry(self):
        """Проверяет истёкшие бусты"""
        async with async_session_maker() as s:
            now = datetime.utcnow()
            check_from = now - timedelta(minutes=30)

            expired = await s.execute(
                select(User).where(and_(
                    User.boost_expires_at.isnot(None),
                    User.boost_expires_at <= now,
                    User.boost_expires_at >= check_from,
                    User.is_active == True
                ))
            )
            for u in expired.scalars().all():
                ud = DB._to_dict(u)
                last_n = await self._last_notification(u.id, "boost_expired")
                if not last_n or (now - last_n).total_seconds() > 3600:
                    await self.notify_boost_expired(ud)

    async def _check_inactive_users(self):
        """Отправляет напоминания неактивным пользователям"""
        async with async_session_maker() as s:
            now = datetime.utcnow()
            threshold = now - timedelta(hours=config.INACTIVE_REMINDER_HOURS)

            inactive = await s.execute(
                select(User).where(and_(
                    User.is_active == True,
                    User.is_banned == False,
                    User.is_profile_complete == True,
                    User.last_active_at < threshold,
                    or_(
                        User.last_reminder_at.is_(None),
                        User.last_reminder_at < threshold
                    )
                )).limit(50)
            )
            for u in inactive.scalars().all():
                ud = DB._to_dict(u)
                ok = await self.notify_daily_reminder(ud)
                if ok:
                    await s.execute(
                        update(User).where(User.id == u.id)
                        .values(last_reminder_at=now)
                    )
            await s.commit()

    async def _check_unread_messages(self):
        """Напоминает о непрочитанных сообщениях"""
        async with async_session_maker() as s:
            now = datetime.utcnow()
            hour_ago = now - timedelta(hours=1)

            # Находим пользователей с непрочитанными сообщениями старше 1ч
            users_with_unread = await s.execute(
                select(
                    User.id,
                    func.count(ChatMessage.id).label("unread_count")
                )
                .join(
                    Match,
                    or_(Match.user1_id == User.id, Match.user2_id == User.id)
                )
                .join(
                    ChatMessage,
                    and_(
                        ChatMessage.match_id == Match.id,
                        ChatMessage.sender_id != User.id,
                        ChatMessage.is_read == False,
                        ChatMessage.is_notified == False,
                        ChatMessage.created_at < hour_ago
                    )
                )
                .where(and_(
                    User.is_active == True,
                    User.is_banned == False,
                ))
                .group_by(User.id)
                .limit(50)
            )

            for row in users_with_unread.fetchall():
                uid, count = row[0], row[1]
                u = await DB.get_user_by_id(uid)
                if u:
                    last_n = await self._last_notification(uid, "unread_messages")
                    if not last_n or (now - last_n).total_seconds() > 7200:
                        await self.notify_unread_messages(u, count)

                    # Помечаем как notified
                    await s.execute(
                        update(ChatMessage).where(and_(
                            ChatMessage.is_read == False,
                            ChatMessage.is_notified == False,
                            ChatMessage.sender_id != uid
                        )).values(is_notified=True)
                    )
            await s.commit()

    async def _check_new_users_in_cities(self):
        """Уведомляет о новых пользователях в городе"""
        async with async_session_maker() as s:
            now = datetime.utcnow()
            day_ago = now - timedelta(days=1)

            # Города с новыми пользователями
            new_by_city = await s.execute(
                select(User.city, func.count(User.id).label("cnt"))
                .where(and_(
                    User.is_profile_complete == True,
                    User.created_at > day_ago,
                    User.city.isnot(None)
                ))
                .group_by(User.city)
                .having(func.count(User.id) >= 2)
            )

            for row in new_by_city.fetchall():
                city, count = row[0], row[1]
                if not city:
                    continue

                # Активные пользователи этого города (не новые)
                users_in_city = await s.execute(
                    select(User).where(and_(
                        User.city == city,
                        User.is_active == True,
                        User.is_profile_complete == True,
                        User.created_at < day_ago,
                        or_(
                            User.last_reminder_at.is_(None),
                            User.last_reminder_at < day_ago
                        )
                    )).limit(20)
                )
                for u in users_in_city.scalars().all():
                    ud = DB._to_dict(u)
                    await self.notify_new_users_in_city(ud, count)

    async def _check_limits_reset(self):
        """Уведомляет о восстановлении лимитов (утро)"""
        now = datetime.utcnow()
        # Отправляем только в 8-9 утра UTC
        if now.hour not in (8, 9):
            return

        async with async_session_maker() as s:
            users = await s.execute(
                select(User).where(and_(
                    User.is_active == True,
                    User.is_banned == False,
                    User.is_profile_complete == True,
                    User.subscription_tier == SubscriptionTier.FREE,
                    User.daily_likes_remaining <= 0,
                    User.last_active_at > now - timedelta(days=3),
                )).limit(50)
            )
            for u in users.scalars().all():
                ud = DB._to_dict(u)
                last_n = await self._last_notification(u.id, "limits_restored")
                if not last_n or (now - last_n).days >= 1:
                    await self.notify_limits_restored(ud)

    async def _check_weekly_stats(self):
        """Еженедельная статистика (по понедельникам)"""
        now = datetime.utcnow()
        if now.weekday() != 0 or now.hour != 10:
            return

        async with async_session_maker() as s:
            week_ago = now - timedelta(days=7)
            users = await s.execute(
                select(User).where(and_(
                    User.is_active == True,
                    User.is_profile_complete == True,
                    or_(
                        User.last_weekly_stats_at.is_(None),
                        User.last_weekly_stats_at < week_ago
                    )
                )).limit(100)
            )
            for u in users.scalars().all():
                # Считаем статистику за неделю
                views = await s.execute(
                    select(func.count(GuestVisit.id)).where(and_(
                        GuestVisit.visited_user_id == u.id,
                        GuestVisit.created_at > week_ago
                    ))
                )
                likes = await s.execute(
                    select(func.count(Like.id)).where(and_(
                        Like.to_user_id == u.id,
                        Like.created_at > week_ago
                    ))
                )
                matches = await s.execute(
                    select(func.count(Match.id)).where(and_(
                        or_(Match.user1_id == u.id, Match.user2_id == u.id),
                        Match.created_at > week_ago
                    ))
                )
                msgs = await s.execute(
                    select(func.count(ChatMessage.id)).where(and_(
                        ChatMessage.sender_id == u.id,
                        ChatMessage.created_at > week_ago
                    ))
                )

                stats = {
                    "views": views.scalar() or 0,
                    "likes": likes.scalar() or 0,
                    "matches": matches.scalar() or 0,
                    "messages": msgs.scalar() or 0,
                }

                ud = DB._to_dict(u)
                await self.notify_weekly_stats(ud, stats)

                await s.execute(
                    update(User).where(User.id == u.id)
                    .values(last_weekly_stats_at=now)
                )
            await s.commit()

    async def _last_notification(self, user_id: int, ntype: str) -> Optional[datetime]:
        """Получает время последнего уведомления данного типа"""
        async with async_session_maker() as s:
            r = await s.execute(
                select(NotificationLog.created_at)
                .where(and_(
                    NotificationLog.user_id == user_id,
                    NotificationLog.notification_type == ntype,
                    NotificationLog.is_delivered == True
                ))
                .order_by(NotificationLog.created_at.desc())
                .limit(1)
            )
            row = r.first()
            return row[0] if row else None

    # ═══ СТАТИСТИКА УВЕДОМЛЕНИЙ (для админки) ═══

    @staticmethod
    async def get_stats() -> Dict:
        async with async_session_maker() as s:
            now = datetime.utcnow()
            day_ago = now - timedelta(days=1)

            total = (await s.execute(
                select(func.count(NotificationLog.id))
            )).scalar() or 0

            today = (await s.execute(
                select(func.count(NotificationLog.id))
                .where(NotificationLog.created_at > day_ago)
            )).scalar() or 0

            delivered = (await s.execute(
                select(func.count(NotificationLog.id))
                .where(and_(
                    NotificationLog.is_delivered == True,
                    NotificationLog.created_at > day_ago
                ))
            )).scalar() or 0

            failed = today - delivered

            # По типам за сутки
            by_type = await s.execute(
                select(
                    NotificationLog.notification_type,
                    func.count(NotificationLog.id)
                )
                .where(NotificationLog.created_at > day_ago)
                .group_by(NotificationLog.notification_type)
            )
            types = {row[0]: row[1] for row in by_type.fetchall()}

            return {
                "total": total,
                "today": today,
                "delivered": delivered,
                "failed": failed,
                "delivery_rate": (delivered / today * 100) if today > 0 else 0,
                "by_type": types
            }


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
            "age_from": u.age_from, "age_to": u.age_to,
            "photos": u.photos or "", "main_photo": u.main_photo,
            "is_active": u.is_active, "is_banned": u.is_banned,
            "is_verified": u.is_verified, "is_profile_complete": u.is_profile_complete,
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
            "notify_likes": u.notify_likes if u.notify_likes is not None else True,
            "notify_matches": u.notify_matches if u.notify_matches is not None else True,
            "notify_messages": u.notify_messages if u.notify_messages is not None else True,
            "notify_guests": u.notify_guests if u.notify_guests is not None else True,
            "notify_reminders": u.notify_reminders if u.notify_reminders is not None else True,
            "notify_sub_events": u.notify_sub_events if u.notify_sub_events is not None else True,
            "quiet_hours_start": u.quiet_hours_start,
            "quiet_hours_end": u.quiet_hours_end,
            "last_notified_at": u.last_notified_at,
            "last_reminder_at": u.last_reminder_at,
            "created_at": u.created_at,
            "last_active_at": u.last_active_at,
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
        tier = u.get("subscription_tier", "free")
        if tier == "wine_cellar": return "🏆 "
        if tier == "sommelier": return "🎖️ "
        if DB.is_vip(u): return "🍷 "
        if u.get("is_verified"): return "✅ "
        return ""

    @staticmethod
    def get_role_tag(u: Dict) -> str:
        if DB.is_creator(u): return " · 👑 Создатель"
        if DB.is_admin(u): return " · 🛡️ Админ"
        return ""

    @staticmethod
    def get_tier_limits(tier: str) -> Dict:
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
                telegram_id=tg_id, username=username,
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
                last_limits_reset=now, last_active_at=now
            )
        await DB.update_user(u["telegram_id"], last_active_at=now)
        return u

    @staticmethod
    async def search_profiles(u: Dict, limit: int = 1) -> List[Dict]:
        async with async_session_maker() as s:
            liked = await s.execute(
                select(Like.to_user_id).where(Like.from_user_id == u["id"])
            )
            exc = [r[0] for r in liked.fetchall()] + [u["id"]]

            q = select(User).where(and_(
                User.is_active == True, User.is_banned == False,
                User.is_profile_complete == True,
                User.id.not_in(exc),
                User.age >= u["age_from"], User.age <= u["age_to"]
            ))

            lf = u.get("looking_for", "both")
            if lf == "male": q = q.where(User.gender == Gender.MALE)
            elif lf == "female": q = q.where(User.gender == Gender.FEMALE)

            q = q.order_by(
                (User.city == u["city"]).desc(),
                User.boost_expires_at.desc().nullslast(),
                User.last_active_at.desc()
            ).limit(limit * 3)

            r = await s.execute(q)
            candidates = [DB._to_dict(x) for x in r.scalars().all()]
            for c in candidates:
                c["_compat"] = CompatibilityEngine.calculate(u, c)
            candidates.sort(key=lambda x: x["_compat"], reverse=True)
            return candidates[:limit]

    @staticmethod
    async def add_like(fid: int, tid: int, is_super: bool = False) -> Dict:
        async with async_session_maker() as s:
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

            rev = await s.execute(
                select(Like).where(and_(Like.from_user_id == tid, Like.to_user_id == fid))
            )
            is_match = rev.scalar_one_or_none() is not None

            match_id = None
            compatibility = 0.0

            if is_match:
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
            return {"is_match": is_match, "match_id": match_id, "compatibility": compatibility,
                    "is_super": is_super}

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
                    unread_r = await s.execute(
                        select(func.count(ChatMessage.id)).where(and_(
                            ChatMessage.match_id == m.id,
                            ChatMessage.sender_id != uid,
                            ChatMessage.is_read == False
                        ))
                    )
                    unread = unread_r.scalar() or 0
                    out.append({
                        "match_id": m.id, "user_id": p.id,
                        "telegram_id": p.telegram_id,
                        "name": p.name, "age": p.age, "photo": p.main_photo,
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
            if not mids: return 0
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
        if vid == uid: return
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
                select(GuestVisit.visitor_id, func.max(GuestVisit.created_at).label("lv"))
                .where(GuestVisit.visited_user_id == uid)
                .group_by(GuestVisit.visitor_id)
                .order_by(desc("lv")).limit(limit)
            )
            rows = r.fetchall()
            if not rows: return []
            ids = [row[0] for row in rows]
            us = await s.execute(select(User).where(User.id.in_(ids)))
            um = {u.id: DB._to_dict(u) for u in us.scalars().all()}
            return [um[uid] for uid in ids if uid in um]

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
            if not u or (u.boost_count or 0) <= 0: return False
            now = datetime.utcnow()
            ne = (u.boost_expires_at + timedelta(hours=24)
                  if u.boost_expires_at and u.boost_expires_at > now
                  else now + timedelta(hours=24))
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
        async with async_session_maker() as s:
            r = await s.execute(
                select(Match).where(and_(
                    Match.id == match_id, Match.is_active == True,
                    or_(Match.user1_id == uid, Match.user2_id == uid)
                ))
            )
            m = r.scalar_one_or_none()
            if not m: return False
            await s.execute(
                update(Match).where(Match.id == match_id).values(is_active=False)
            )
            await s.execute(
                update(User).where(User.id.in_([m.user1_id, m.user2_id]))
                .values(matches_count=func.greatest(User.matches_count - 1, 0))
            )
            await s.commit()
            return True

    @staticmethod
    async def get_stats() -> Dict:
        async with async_session_maker() as s:
            total = (await s.execute(select(func.count(User.id)))).scalar() or 0
            complete = (await s.execute(
                select(func.count(User.id)).where(User.is_profile_complete == True)
            )).scalar() or 0
            now = datetime.utcnow()
            da, wa, ma = now - timedelta(days=1), now - timedelta(days=7), now - timedelta(days=30)
            dau = (await s.execute(select(func.count(User.id)).where(User.last_active_at > da))).scalar() or 0
            wau = (await s.execute(select(func.count(User.id)).where(User.last_active_at > wa))).scalar() or 0
            mau = (await s.execute(select(func.count(User.id)).where(User.last_active_at > ma))).scalar() or 0
            vip = (await s.execute(select(func.count(User.id)).where(User.subscription_tier != SubscriptionTier.FREE))).scalar() or 0
            banned = (await s.execute(select(func.count(User.id)).where(User.is_banned == True))).scalar() or 0
            today_reg = (await s.execute(select(func.count(User.id)).where(User.created_at > da))).scalar() or 0
            total_matches = (await s.execute(select(func.count(Match.id)))).scalar() or 0
            total_msgs = (await s.execute(select(func.count(ChatMessage.id)))).scalar() or 0
            total_likes = (await s.execute(select(func.count(Like.id)))).scalar() or 0
            revenue = (await s.execute(select(func.sum(Payment.amount)).where(Payment.status == PaymentStatus.SUCCEEDED))).scalar() or 0
            month_rev = (await s.execute(select(func.sum(Payment.amount)).where(and_(Payment.status == PaymentStatus.SUCCEEDED, Payment.paid_at > ma)))).scalar() or 0
            pending_reports = (await s.execute(select(func.count(Report.id)).where(Report.status == "pending"))).scalar() or 0
            avg_compat = (await s.execute(select(func.avg(Match.compatibility_score)).where(Match.compatibility_score > 0))).scalar() or 0

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
                r = await s.execute(select(User).where(or_(User.id == int(query), User.telegram_id == int(query))))
            else:
                q = query.lstrip("@")
                r = await s.execute(select(User).where(or_(User.username.ilike(f"%{q}%"), User.name.ilike(f"%{q}%"))).limit(10))
            return [DB._to_dict(u) for u in r.scalars().all()]

    @staticmethod
    async def get_all_user_ids(filter_type: str = "all") -> List[int]:
        async with async_session_maker() as s:
            q = select(User.telegram_id).where(and_(User.is_active == True, User.is_banned == False))
            if filter_type == "complete": q = q.where(User.is_profile_complete == True)
            elif filter_type == "vip": q = q.where(User.subscription_tier != SubscriptionTier.FREE)
            elif filter_type == "free": q = q.where(User.subscription_tier == SubscriptionTier.FREE)
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
                out.append({"id": rep.id, "reason": rep.reason, "created_at": rep.created_at, "reporter": reporter, "reported": reported})
            return out

    @staticmethod
    async def resolve_report(report_id: int, action: str, notes: str = ""):
        async with async_session_maker() as s:
            await s.execute(update(Report).where(Report.id == report_id).values(status=action, admin_notes=notes, resolved_at=datetime.utcnow()))
            await s.commit()

    @staticmethod
    async def get_recent_payments(limit: int = 10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Payment).order_by(Payment.created_at.desc()).limit(limit))
            out = []
            for p in r.scalars().all():
                u = await DB.get_user_by_id(p.user_id)
                out.append({"id": p.id, "amount": p.amount / 100, "status": p.status.value, "description": p.description, "created_at": p.created_at, "user_name": u["name"] if u else "?", "user_tg": u["telegram_id"] if u else 0})
            return out

    @staticmethod
    async def create_promo(code, tier, days, max_uses):
        async with async_session_maker() as s:
            s.add(PromoCode(code=code.upper(), tier=tier, duration_days=days, max_uses=max_uses))
            await s.commit()

    @staticmethod
    async def use_promo(user_id: int, code: str) -> Dict:
        async with async_session_maker() as s:
            r = await s.execute(select(PromoCode).where(and_(PromoCode.code == code.upper(), PromoCode.is_active == True)))
            promo = r.scalar_one_or_none()
            if not promo: return {"error": "❌ Промокод не найден"}
            if promo.used_count >= promo.max_uses: return {"error": "❌ Промокод исчерпан"}
            used = await s.execute(select(PromoUse).where(and_(PromoUse.promo_id == promo.id, PromoUse.user_id == user_id)))
            if used.scalar_one_or_none(): return {"error": "❌ Ты уже использовал этот промокод"}
            s.add(PromoUse(promo_id=promo.id, user_id=user_id))
            await s.execute(update(PromoCode).where(PromoCode.id == promo.id).values(used_count=PromoCode.used_count + 1))
            await s.commit()
            await DB.activate_subscription_by_id(user_id, promo.tier, promo.duration_days)
            return {"success": True, "tier": promo.tier, "days": promo.duration_days}

    @staticmethod
    async def activate_subscription_by_id(uid: int, tier: str, days: int):
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u: return
            te = SubscriptionTier(tier)
            now = datetime.utcnow()
            if te == SubscriptionTier.WINE_CELLAR:
                exp = None
            elif u.subscription_expires_at and u.subscription_expires_at > now:
                exp = u.subscription_expires_at + timedelta(days=days)
            else:
                exp = now + timedelta(days=days)
            await s.execute(update(User).where(User.id == uid).values(subscription_tier=te, subscription_expires_at=exp))
            await s.commit()

    @staticmethod
    async def get_total_users() -> int:
        async with async_session_maker() as s:
            r = await s.execute(select(func.count(User.id)).where(User.is_profile_complete == True))
            return r.scalar() or 0

    @staticmethod
    async def create_payment(uid, yid, amount, desc, ptype, ptier=None, pdur=None, pcount=None) -> int:
        async with async_session_maker() as s:
            p = Payment(user_id=uid, yookassa_payment_id=yid, amount=amount, description=desc, product_type=ptype, product_tier=ptier, product_duration=pdur, product_count=pcount)
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
                return {"id": p.id, "user_id": p.user_id, "yookassa_payment_id": p.yookassa_payment_id, "status": p.status.value, "product_type": p.product_type, "product_tier": p.product_tier, "product_duration": p.product_duration, "product_count": p.product_count}
            return None

    @staticmethod
    async def update_payment_status(pid: int, st: PaymentStatus):
        async with async_session_maker() as s:
            v = {"status": st}
            if st == PaymentStatus.SUCCEEDED: v["paid_at"] = datetime.utcnow()
            await s.execute(update(Payment).where(Payment.id == pid).values(**v))
            await s.commit()

    @staticmethod
    async def log_broadcast(admin_id, text, target, sent, failed):
        async with async_session_maker() as s:
            s.add(BroadcastLog(admin_id=admin_id, message_text=text, target_filter=target, sent_count=sent, failed_count=failed))
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


class T:
    NO_PROFILE = "📝 Сначала заполни профиль → /start"
    BANNED = "🚫 Аккаунт заблокирован."
    NO_PROFILES = "😔 *Анкеты закончились.*\n\n🍷 Попробуй позже!"
    LIKES_LIMIT = "⚠️ *Лимит лайков!*\n\n🥂 *Бокал Вина* — 100 лайков!\n🍾 *Бутылка Вина* — безлимит!"
    NO_MATCHES = "😔 Пока нет взаимных симпатий\n\n🍷 Листай анкеты!"
    NO_GUESTS = "👻 Пока никто не заходил\n\n💡 _Поставь буст!_"
    NO_MSGS = "💬 Нет активных диалогов"

    NOTIFY_SETTINGS = """
🔔 *Настройки уведомлений*

{status}

🕐 Тихие часы: {quiet}

_Выбери что настроить:_
"""

    NOTIFY_HELP = """
🔔 *Какие уведомления есть:*

❤️ *Лайки* — кто-то оценил твой профиль
💕 *Мэтчи* — взаимная симпатия!
💬 *Сообщения* — новые сообщения в чатах
👻 *Гости* — кто смотрел твой профиль
🔄 *Напоминания* — лимиты, новые анкеты
🍷 *Подписка* — истечение, буст
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
            [KeyboardButton(text="🔔 Уведомления"), KeyboardButton(text="❓ FAQ")],
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
    def confirm_unmatch(match_id: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да", callback_data=f"um_yes:{match_id}"),
             InlineKeyboardButton(text="❌ Отмена", callback_data="bm")],
        ])

    @staticmethod
    def back_matches():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💕 К симпатиям", callback_data="bm")],
        ])

    @staticmethod
    def shop():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Подписки", callback_data="sh:subs")],
            [InlineKeyboardButton(text="🚀 Буст анкеты", callback_data="sh:boost")],
            [InlineKeyboardButton(text="📊 Сравнить тарифы", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🎁 Промокод", callback_data="sh:promo")],
            [InlineKeyboardButton(text="🍷 Меню", callback_data="mn")],
        ])

    @staticmethod
    def subs():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Бокал Вина", callback_data="tf:wine_glass")],
            [InlineKeyboardButton(text="🍾 Бутылка Вина 🔥", callback_data="tf:wine_bottle")],
            [InlineKeyboardButton(text="🎖️ Сомелье", callback_data="tf:sommelier")],
            [InlineKeyboardButton(text="🏆 Винный Погреб", callback_data="tf:wine_cellar")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sh:mn")],
        ])

    @staticmethod
    def buy_wine_glass():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 299₽/неделя", callback_data="by:wine_glass:7:29900")],
            [InlineKeyboardButton(text="🥂 799₽/месяц 🔥", callback_data="by:wine_glass:30:79900")],
            [InlineKeyboardButton(text="⬅️", callback_data="sh:subs")],
        ])

    @staticmethod
    def buy_wine_bottle():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍾 499₽/мес", callback_data="by:wine_bottle:30:49900")],
            [InlineKeyboardButton(text="🍾 1199₽/3мес 🔥", callback_data="by:wine_bottle:90:119900")],
            [InlineKeyboardButton(text="⬅️", callback_data="sh:subs")],
        ])

    @staticmethod
    def buy_sommelier():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎖️ 799₽/мес", callback_data="by:sommelier:30:79900")],
            [InlineKeyboardButton(text="🎖️ 1999₽/3мес 🔥", callback_data="by:sommelier:90:199900")],
            [InlineKeyboardButton(text="🎖️ 3499₽/6мес 💰", callback_data="by:sommelier:180:349900")],
            [InlineKeyboardButton(text="⬅️", callback_data="sh:subs")],
        ])

    @staticmethod
    def buy_wine_cellar():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏆 4999₽ навсегда 💎", callback_data="by:wine_cellar:0:499900")],
            [InlineKeyboardButton(text="⬅️", callback_data="sh:subs")],
        ])

    @staticmethod
    def boost_menu(has_boosts: bool, is_active: bool):
        b = []
        if has_boosts:
            b.append([InlineKeyboardButton(text="🚀 Активировать", callback_data="bo:act")])
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
            [InlineKeyboardButton(text="🔄 Проверить", callback_data=f"ck:{pid}")],
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
            [InlineKeyboardButton(text="⬅️", callback_data="pv")],
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

    # ═══ NOTIFICATION SETTINGS KB ═══

    @staticmethod
    def notify_settings(user: Dict):
        def icon(val): return "✅" if val else "❌"

        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"{icon(user.get('notify_likes', True))} Лайки",
                callback_data="ns:likes"
            ),
             InlineKeyboardButton(
                 text=f"{icon(user.get('notify_matches', True))} Мэтчи",
                 callback_data="ns:matches"
             )],
            [InlineKeyboardButton(
                text=f"{icon(user.get('notify_messages', True))} Сообщения",
                callback_data="ns:messages"
            ),
             InlineKeyboardButton(
                 text=f"{icon(user.get('notify_guests', True))} Гости",
                 callback_data="ns:guests"
             )],
            [InlineKeyboardButton(
                text=f"{icon(user.get('notify_reminders', True))} Напоминания",
                callback_data="ns:reminders"
            ),
             InlineKeyboardButton(
                 text=f"{icon(user.get('notify_sub_events', True))} Подписка",
                 callback_data="ns:sub_events"
             )],
            [InlineKeyboardButton(text="🕐 Тихие часы", callback_data="ns:quiet")],
            [InlineKeyboardButton(text="✅ Включить всё", callback_data="ns:all_on"),
             InlineKeyboardButton(text="❌ Выключить всё", callback_data="ns:all_off")],
            [InlineKeyboardButton(text="❓ Что это?", callback_data="ns:help")],
            [InlineKeyboardButton(text="🍷 Меню", callback_data="mn")],
        ])

    @staticmethod
    def quiet_hours():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌙 22:00 — 08:00", callback_data="qh:22:8")],
            [InlineKeyboardButton(text="🌙 23:00 — 07:00", callback_data="qh:23:7")],
            [InlineKeyboardButton(text="🌙 00:00 — 09:00", callback_data="qh:0:9")],
            [InlineKeyboardButton(text="🔔 Отключить тихие часы", callback_data="qh:off")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="ns:back")],
        ])

    # ═══ ADMIN KB ═══

    @staticmethod
    def admin():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
            [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="adm:search")],
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="⚠️ Жалобы", callback_data="adm:reports")],
            [InlineKeyboardButton(text="💰 Платежи", callback_data="adm:payments")],
            [InlineKeyboardButton(text="🔔 Стат. уведомлений", callback_data="adm:notify_stats")],
            [InlineKeyboardButton(text="🎁 Промокод", callback_data="adm:promo")],
            [InlineKeyboardButton(text="🏆 Топ", callback_data="adm:top")],
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
            [InlineKeyboardButton(text="🍷 VIP", callback_data=f"au:givevip:{uid}"),
             InlineKeyboardButton(text="🚀 Бусты", callback_data=f"au:giveboost:{uid}")],
            [ban_btn, InlineKeyboardButton(text="✅ Верифицировать", callback_data=f"au:verify:{uid}")],
            [InlineKeyboardButton(text="🛡️ Админка", callback_data="adm:main")],
        ])

    @staticmethod
    def admin_report(rid: int, ruid: int):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔒 Бан", callback_data=f"ar:ban:{rid}:{ruid}"),
             InlineKeyboardButton(text="⚠️ Предупредить", callback_data=f"ar:warn:{rid}:{ruid}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ar:dismiss:{rid}:{ruid}")],
            [InlineKeyboardButton(text="➡️ Далее", callback_data="adm:reports")],
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
    async def create(user: Dict, ptype: str, tier: str = None, dur: int = None, count: int = None, amount: int = 0) -> Dict:
        if not YOOKASSA_AVAILABLE or not config.YOOKASSA_SHOP_ID:
            return {"error": "ЮKassa не настроена"}
        desc = f"Подписка «{TIER_NAMES.get(tier, 'VIP')}» · {BOT_NAME}" if ptype == "subscription" else f"Буст ({count}шт) · {BOT_NAME}"
        try:
            p = YooPayment.create({
                "amount": {"value": f"{amount / 100:.2f}", "currency": "RUB"},
                "confirmation": {"type": ConfirmationType.REDIRECT, "return_url": f"{config.DOMAIN}/ok"},
                "capture": True, "description": desc,
                "metadata": {"user_id": user["id"], "type": ptype, "tier": tier, "dur": dur, "count": count}
            }, str(uuid.uuid4()))
            pid = await DB.create_payment(user["id"], p.id, amount, desc, ptype, tier, dur, count)
            return {"pid": pid, "url": p.confirmation.confirmation_url}
        except Exception as e:
            logger.error(f"Payment error: {e}")
            return {"error": str(e)}

    @staticmethod
    async def check(pid: int) -> Dict:
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
                    if isinstance(event, Message): await event.answer(T.BANNED)
                    elif isinstance(event, CallbackQuery): await event.answer(T.BANNED, show_alert=True)
                    return
        data["user"] = u
        return await handler(event, data)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#               HANDLERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

rt = Router()

# Глобальная ссылка на NotificationService
notifier: Optional[NotificationService] = None


# ═══ QUICK NAV CALLBACKS ═══

@rt.callback_query(F.data == "go:browse")
async def go_browse(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user or not user.get("is_profile_complete"):
        await callback.answer("📝 Заполни анкету!", show_alert=True)
        return
    try: await callback.message.delete()
    except: pass
    ps = await DB.search_profiles(user, 1)
    if ps:
        await show_card(callback.message, ps[0], user)
    else:
        await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.callback_query(F.data == "go:matches")
async def go_matches(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user: return
    ms = await DB.get_matches(user["id"])
    if ms:
        await callback.message.edit_text(
            f"💕 *Симпатии ({len(ms)}):*",
            reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN
        )
    else:
        await callback.message.edit_text(T.NO_MATCHES, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.callback_query(F.data == "go:guests")
async def go_guests(callback: CallbackQuery, user: Optional[Dict]):
    if not user: return
    try: await callback.message.delete()
    except: pass
    lim = 20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE
    gs = await DB.get_guests(user["id"], lim)
    if not gs:
        await callback.message.answer(T.NO_GUESTS, parse_mode=ParseMode.MARKDOWN)
    else:
        txt = "👻 *Кто смотрел:*\n\n"
        for i, g in enumerate(gs, 1):
            txt += f"{i}. {DB.get_badge(g)}{g['name']}, {g['age']} — 🏙️ {g['city']}\n"
        await callback.message.answer(txt, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


# ═══ START ═══

@rt.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if user and user.get("is_profile_complete"):
        un = await DB.get_unread(user["id"])
        st = TIER_NAMES.get(user["subscription_tier"], "🆓")
        if DB.is_boosted(user): st += " · 🚀 Буст"
        st += DB.get_role_tag(user)
        await message.answer(
            f"🍷 *С возвращением, {user['name']}!* 🥂\n\n"
            f"{st}\n"
            f"📊 👁️ {user['views_count']} · 💕 {user['matches_count']} · ✉️ {un}",
            reply_markup=KB.main(un), parse_mode=ParseMode.MARKDOWN
        )
    else:
        if not user:
            await DB.create_user(message.from_user.id, message.from_user.username)
        await Anim.animate(message, Anim.WINE_POUR, 0.5)
        await asyncio.sleep(0.3)
        await message.answer(
            f"🍷 *Добро пожаловать в {BOT_NAME}!*\n\n"
            f"Давай создадим анкету 📝",
            parse_mode=ParseMode.MARKDOWN
        )
        await message.answer("✏️ Как тебя зовут?", reply_markup=ReplyKeyboardRemove())
        await state.set_state(RegStates.name)


# ═══ REGISTRATION ═══

@rt.message(RegStates.name)
async def reg_name(message: Message, state: FSMContext):
    n = message.text.strip()
    if len(n) < 2 or len(n) > 50:
        await message.answer("⚠️ 2-50 символов:")
        return
    await state.update_data(name=n)
    await message.answer(f"Приятно, *{n}*! 🍷\n\n🎂 Сколько лет? _(18-99)_", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.age)

@rt.message(RegStates.age)
async def reg_age(message: Message, state: FSMContext):
    try:
        a = int(message.text.strip())
        if not 18 <= a <= 99: raise ValueError
    except: await message.answer("⚠️ 18-99:"); return
    await state.update_data(age=a)
    await message.answer("🚻 Твой пол:", reply_markup=KB.gender())
    await state.set_state(RegStates.gender)

@rt.callback_query(RegStates.gender, F.data.startswith("g:"))
async def reg_gender(callback: CallbackQuery, state: FSMContext):
    await state.update_data(gender=callback.data[2:])
    await callback.message.edit_text("✅")
    await callback.message.answer("🏙️ Твой город:")
    await state.set_state(RegStates.city)
    await callback.answer()

@rt.message(RegStates.city)
async def reg_city(message: Message, state: FSMContext):
    c = message.text.strip().title()
    if len(c) < 2: await message.answer("🏙️ Город:"); return
    await state.update_data(city=c)
    await message.answer("📸 Фото или «Пропустить»:", reply_markup=KB.skip())
    await state.set_state(RegStates.photo)

@rt.message(RegStates.photo, F.photo)
async def reg_photo(message: Message, state: FSMContext):
    await state.update_data(photo=message.photo[-1].file_id)
    await message.answer("📸 ✅\n\n📝 О себе _(до 500)_ или «Пропустить»:", reply_markup=KB.skip(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.bio)

@rt.callback_query(RegStates.photo, F.data == "skip")
async def reg_skip_photo(callback: CallbackQuery, state: FSMContext):
    await state.update_data(photo=None)
    await callback.message.edit_text("📸 Пропущено")
    await callback.message.answer("📝 О себе или «Пропустить»:", reply_markup=KB.skip())
    await state.set_state(RegStates.bio)
    await callback.answer()

@rt.message(RegStates.bio)
async def reg_bio(message: Message, state: FSMContext):
    await state.update_data(bio=message.text.strip()[:500])
    await message.answer("🔍 Кого ищешь?", reply_markup=KB.looking())
    await state.set_state(RegStates.looking_for)

@rt.callback_query(RegStates.bio, F.data == "skip")
async def reg_skip_bio(callback: CallbackQuery, state: FSMContext):
    await state.update_data(bio="")
    await callback.message.edit_text("🔍 Кого ищешь?", reply_markup=KB.looking())
    await state.set_state(RegStates.looking_for)
    await callback.answer()

@rt.callback_query(RegStates.looking_for, F.data.startswith("l:"))
async def reg_looking(callback: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    upd = {
        "name": d["name"], "age": d["age"], "gender": Gender(d["gender"]),
        "city": d["city"], "bio": d.get("bio", ""),
        "looking_for": LookingFor(callback.data[2:]),
        "is_profile_complete": True,
    }
    if d.get("photo"):
        upd["photos"] = d["photo"]
        upd["main_photo"] = d["photo"]
    await DB.update_user(callback.from_user.id, **upd)
    await state.clear()
    await callback.message.edit_text("⏳ Создаём анкету...")
    await asyncio.sleep(0.5)
    await callback.message.edit_text("🎉 *Анкета готова!*", parse_mode=ParseMode.MARKDOWN)
    total = await DB.get_total_users()
    await callback.message.answer(
        f"🍷 *{total}* человек уже здесь!\nЖми «🍷 Анкеты»!",
        reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


# ═══ BROWSE ═══

@rt.message(F.text == "🍷 Анкеты")
async def browse(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE); return
    await state.clear()
    msg = await message.answer("🔍 Ищем анкеты...")
    await asyncio.sleep(0.5)
    ps = await DB.search_profiles(user, 1)
    if not ps: await msg.edit_text(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN); return
    await msg.delete()
    await show_card(message, ps[0], user)


async def show_card(message: Message, p: Dict, v: Dict):
    await DB.add_guest(v["id"], p["id"])

    # Уведомляем о просмотре
    global notifier
    if notifier:
        asyncio.create_task(notifier.notify_profile_viewed(v, p))

    lm = {"male": "👨 Мужчин", "female": "👩 Женщин", "both": "👫 Всех"}
    badge = DB.get_badge(p)
    boost = " 🚀" if DB.is_boosted(p) else ""
    compat = CompatibilityEngine.calculate(v, p)

    txt = (
        f"{badge}*{p['name']}*{boost}, {p['age']}\n"
        f"🏙️ {p['city']}\n\n"
        f"{p['bio'] or '_Нет описания_'}\n\n"
        f"🔍 Ищет: {lm.get(p.get('looking_for', 'both'), '👫 Всех')}\n"
        f"💕 {Anim.compatibility_bar(compat)}"
    )
    kb = KB.search(p["id"], compat)
    try:
        if p.get("main_photo"):
            await message.answer_photo(photo=p["main_photo"], caption=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        else:
            await message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except:
        await message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("lk:"))
async def handle_like(callback: CallbackQuery, user: Optional[Dict]):
    if not user: await callback.answer("❗"); return

    if not DB.is_vip(user) and user.get("daily_likes_remaining", 0) <= 0:
        try: await callback.message.edit_caption(caption=T.LIKES_LIMIT, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)
        except: await callback.message.edit_text(T.LIKES_LIMIT, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)
        await callback.answer(); return

    tid = int(callback.data[3:])
    result = await DB.add_like(user["id"], tid)
    if not DB.is_vip(user): await DB.dec_likes(user["telegram_id"])

    global notifier
    target = await DB.get_user_by_id(tid)

    if result["is_match"]:
        compat_bar = Anim.compatibility_bar(result["compatibility"])
        tn = target["name"] if target else "?"

        match_text = f"🍷✨ *Взаимная симпатия с {tn}!* ✨🍷\n\n💕 {compat_bar}"
        try: await callback.message.edit_caption(caption=match_text, parse_mode=ParseMode.MARKDOWN)
        except: await callback.message.edit_text(match_text, parse_mode=ParseMode.MARKDOWN)

        # Уведомление о мэтче обоим
        if notifier and target:
            await notifier.notify_new_match(user, target, result["compatibility"])

        await callback.answer("🍷✨ Мэтч! 💕")
    else:
        # Уведомление о лайке получателю
        if notifier and target:
            asyncio.create_task(
                notifier.notify_like_received(user, target, result.get("is_super", False))
            )
        await callback.answer("❤️ Нравится!")

    user = await DB.get_user(callback.from_user.id)
    ps = await DB.search_profiles(user, 1)
    if ps:
        await show_card(callback.message, ps[0], user)
    else:
        await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("dl:"))
async def handle_dislike(callback: CallbackQuery, user: Optional[Dict]):
    if not user: return
    await callback.answer("👋")
    ps = await DB.search_profiles(user, 1)
    if ps:
        await show_card(callback.message, ps[0], user)
    else:
        try: await callback.message.edit_caption(caption=T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)
        except: await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)


# ═══ MATCHES & CHAT ═══

@rt.message(F.text == "💕 Симпатии")
async def show_matches(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): await message.answer(T.NO_PROFILE); return
    await state.clear()
    ms = await DB.get_matches(user["id"])
    if ms:
        total_unread = sum(m.get("unread", 0) for m in ms)
        header = f"💕 *Симпатии ({len(ms)})*"
        if total_unread > 0: header += f" · 🔴 {total_unread}"
        await message.answer(header, reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer(T.NO_MATCHES, parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data.startswith("ch:"))
async def start_chat(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    pid = int(callback.data[3:])
    p = await DB.get_user_by_id(pid)
    if not p: await callback.answer("❌"); return
    mid = await DB.get_match_between(user["id"], pid)
    if not mid: await callback.answer("❌ Нет мэтча"); return
    await DB.mark_messages_read(mid, user["id"])
    msgs = await DB.get_msgs(mid, 8)
    txt = f"💬 *Чат с {DB.get_badge(p)}{p['name']}*\n\n"
    for mg in msgs:
        sn = "📤 Вы" if mg["sender_id"] == user["id"] else f"📩 {p['name']}"
        ts = mg["created_at"].strftime("%H:%M") if mg.get("created_at") else ""
        txt += f"*{sn}:* {mg['text']} _{ts}_\n"
    if not msgs: txt += f"_Напишите первым!_ {Anim.get_wine_emoji()}"
    await state.update_data(cp=pid, mi=mid)
    await state.set_state(ChatStates.chatting)
    await callback.message.edit_text(txt, reply_markup=KB.chat_actions(mid, pid), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@rt.message(ChatStates.chatting)
async def send_chat_msg(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d = await state.get_data()
    mid, pid = d.get("mi"), d.get("cp")
    if not mid: await state.clear(); await message.answer("💬 Чат закрыт", reply_markup=KB.main()); return

    await DB.send_msg(mid, user["id"], message.text)

    p = await DB.get_user_by_id(pid)
    if p:
        # Уведомление о сообщении
        global notifier
        if notifier:
            asyncio.create_task(
                notifier.notify_new_message(user, p, message.text)
            )

    await message.answer("✅")


@rt.callback_query(F.data == "bm")
async def back_to_matches(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user: return
    ms = await DB.get_matches(user["id"])
    if ms:
        await callback.message.edit_text(f"💕 *Симпатии ({len(ms)}):*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else:
        await callback.message.edit_text(T.NO_MATCHES, parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("um:"))
async def unmatch_confirm(callback: CallbackQuery, state: FSMContext):
    mid = int(callback.data[3:])
    await callback.message.edit_text("💔 *Точно отвязать?*", reply_markup=KB.confirm_unmatch(mid), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data.startswith("um_yes:"))
async def unmatch_do(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    await state.clear()
    mid = int(callback.data[7:])
    ok = await DB.unmatch(user["id"], mid)
    if ok: await callback.message.edit_text("💔 Отвязано.", reply_markup=KB.back_matches())
    else: await callback.answer("❌", show_alert=True)


# ═══ CHATS & GUESTS ═══

@rt.message(F.text.startswith("💬 Чаты"))
async def show_chats(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): await message.answer(T.NO_PROFILE); return
    await state.clear()
    ms = await DB.get_matches(user["id"])
    if ms: await message.answer("💬 *Диалоги:*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else: await message.answer(T.NO_MSGS, parse_mode=ParseMode.MARKDOWN)

@rt.message(F.text == "👻 Гости")
async def show_guests(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): await message.answer(T.NO_PROFILE); return
    lim = 20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE
    gs = await DB.get_guests(user["id"], lim)
    if not gs: await message.answer(T.NO_GUESTS, parse_mode=ParseMode.MARKDOWN); return
    txt = "👻 *Кто смотрел:*\n\n"
    for i, g in enumerate(gs, 1):
        txt += f"{i}. {DB.get_badge(g)}{g['name']}, {g['age']} — 🏙️ {g['city']}\n"
    if not DB.is_vip(user):
        hidden = max(0, user.get("views_count", 0) - lim)
        if hidden > 0: txt += f"\n🔒 _Ещё {hidden} скрыто_"
        txt += "\n\n🍷 _Подписка — все гости!_"
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN)


# ═══ PROFILE ═══

@rt.message(F.text == "👤 Профиль")
async def show_profile(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): await message.answer(T.NO_PROFILE); return
    badge = DB.get_badge(user)
    sub = TIER_NAMES.get(user["subscription_tier"], "🆓")
    if user.get("subscription_expires_at") and user["subscription_tier"] not in ("free", "wine_cellar"):
        sub += f" (до {user['subscription_expires_at'].strftime('%d.%m.%Y')})"
    bi = ""
    if DB.is_boosted(user): bi += f"\n🚀 Буст до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if user.get("boost_count", 0) > 0: bi += f"\n🚀 Запас: {user['boost_count']}"
    limits = DB.get_tier_limits(user["subscription_tier"])
    likes_str = "♾️" if limits["likes"] >= 999999 else f"{user.get('daily_likes_remaining', 0)}/{limits['likes']}"

    txt = (
        f"👤 *Профиль*\n\n{badge}*{user['name']}*, {user['age']}{DB.get_role_tag(user)}\n"
        f"🏙️ {user['city']}\n\n{user['bio'] or '_—_'}\n\n"
        f"📊 👁️ {user['views_count']} · ❤️ {user['likes_received_count']} · 💕 {user['matches_count']}\n"
        f"❤️ Лайков: {likes_str}\n\n🍷 {sub}{bi}"
    )
    try:
        if user.get("main_photo"):
            await message.answer_photo(photo=user["main_photo"], caption=txt, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)
        else: await message.answer(txt, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)
    except: await message.answer(txt, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)


@rt.callback_query(F.data == "pe")
async def profile_edit_menu(callback: CallbackQuery):
    try: await callback.message.edit_caption(caption="✏️ *Что изменить?*", reply_markup=KB.edit(), parse_mode=ParseMode.MARKDOWN)
    except: await callback.message.edit_text("✏️ *Что изменить?*", reply_markup=KB.edit(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data == "pv")
async def back_to_profile(callback: CallbackQuery, user: Optional[Dict]):
    if user:
        try: await callback.message.delete()
        except: pass
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
    if len(n) < 2 or len(n) > 50: await message.answer("⚠️ 2-50 символов:"); return
    await DB.update_user(message.from_user.id, name=n)
    await state.clear()
    await message.answer(f"✅ Имя: *{n}*", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data == "ed:age")
async def edit_age(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("🎂 Возраст:")
    await state.set_state(EditStates.edit_age)
    await callback.answer()

@rt.message(EditStates.edit_age)
async def save_age(message: Message, state: FSMContext):
    try:
        a = int(message.text.strip())
        if not 18 <= a <= 99: raise ValueError
    except: await message.answer("⚠️ 18-99:"); return
    await DB.update_user(message.from_user.id, age=a)
    await state.clear()
    await message.answer(f"✅ Возраст: *{a}*", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data == "ed:city")
async def edit_city(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("🏙️ Город:")
    await state.set_state(EditStates.edit_city)
    await callback.answer()

@rt.message(EditStates.edit_city)
async def save_city(message: Message, state: FSMContext):
    await DB.update_user(message.from_user.id, city=message.text.strip().title())
    await state.clear()
    await message.answer("✅ Город обновлён!", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:bio")
async def edit_bio(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("📝 О себе:")
    await state.set_state(EditStates.edit_bio)
    await callback.answer()

@rt.message(EditStates.edit_bio)
async def save_bio(message: Message, state: FSMContext):
    await DB.update_user(message.from_user.id, bio=message.text.strip()[:500])
    await state.clear()
    await message.answer("✅ Описание обновлено!", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:photo")
async def edit_photo(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("📸 Новое фото:")
    await state.set_state(EditStates.add_photo)
    await callback.answer()

@rt.message(EditStates.add_photo, F.photo)
async def save_photo(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    pid = message.photo[-1].file_id
    ph = user.get("photos", "")
    ph = (ph + "," + pid) if ph else pid
    await DB.update_user(message.from_user.id, photos=ph, main_photo=pid)
    await state.clear()
    await message.answer("📸 ✅", reply_markup=KB.main())


# ═══ NOTIFICATION SETTINGS ═══

@rt.message(F.text == "🔔 Уведомления")
async def notify_settings_menu(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return
    await state.clear()
    await _show_notify_settings(message, user)


async def _show_notify_settings(message: Message, user: Dict):
    """Показывает меню настроек уведомлений"""
    def s(v): return "✅ Вкл" if v else "❌ Выкл"

    status = (
        f"❤️ Лайки: {s(user.get('notify_likes', True))}\n"
        f"💕 Мэтчи: {s(user.get('notify_matches', True))}\n"
        f"💬 Сообщения: {s(user.get('notify_messages', True))}\n"
        f"👻 Гости: {s(user.get('notify_guests', True))}\n"
        f"🔄 Напоминания: {s(user.get('notify_reminders', True))}\n"
        f"🍷 Подписка: {s(user.get('notify_sub_events', True))}"
    )

    qs = user.get("quiet_hours_start")
    qe = user.get("quiet_hours_end")
    if qs is not None and qe is not None:
        quiet = f"🌙 {qs:02d}:00 — {qe:02d}:00"
    else:
        quiet = "🔔 Отключены"

    await message.answer(
        T.NOTIFY_SETTINGS.format(status=status, quiet=quiet),
        reply_markup=KB.notify_settings(user),
        parse_mode=ParseMode.MARKDOWN
    )


@rt.callback_query(F.data.startswith("ns:"))
async def handle_notify_setting(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    action = callback.data[3:]

    # Toggle individual settings
    toggle_map = {
        "likes": "notify_likes",
        "matches": "notify_matches",
        "messages": "notify_messages",
        "guests": "notify_guests",
        "reminders": "notify_reminders",
        "sub_events": "notify_sub_events",
    }

    if action in toggle_map:
        field_name = toggle_map[action]
        current = user.get(field_name, True)
        new_val = not current
        user = await DB.update_user(user["telegram_id"], **{field_name: new_val})

        emoji = "✅" if new_val else "❌"
        names = {"likes": "Лайки", "matches": "Мэтчи", "messages": "Сообщения",
                 "guests": "Гости", "reminders": "Напоминания", "sub_events": "Подписка"}
        await callback.answer(f"{emoji} {names.get(action, action)}")

        # Обновляем клавиатуру
        await callback.message.edit_reply_markup(
            reply_markup=KB.notify_settings(user)
        )

    elif action == "all_on":
        user = await DB.update_user(user["telegram_id"],
            notify_likes=True, notify_matches=True,
            notify_messages=True, notify_guests=True,
            notify_reminders=True, notify_sub_events=True
        )
        await callback.answer("✅ Все уведомления включены!")
        await callback.message.edit_reply_markup(reply_markup=KB.notify_settings(user))

    elif action == "all_off":
        user = await DB.update_user(user["telegram_id"],
            notify_likes=False, notify_matches=False,
            notify_messages=False, notify_guests=False,
            notify_reminders=False, notify_sub_events=False
        )
        await callback.answer("❌ Все уведомления выключены")
        await callback.message.edit_reply_markup(reply_markup=KB.notify_settings(user))

    elif action == "quiet":
        await callback.message.edit_text(
            "🕐 *Тихие часы*\n\n"
            "В это время уведомления не будут отправляться.\n"
            "_Время указано в UTC_",
            reply_markup=KB.quiet_hours(),
            parse_mode=ParseMode.MARKDOWN
        )
        await callback.answer()

    elif action == "help":
        await callback.answer()
        await callback.message.edit_text(
            T.NOTIFY_HELP,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="ns:back")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )

    elif action == "back":
        user = await DB.get_user(callback.from_user.id)
        def s(v): return "✅ Вкл" if v else "❌ Выкл"
        status = (
            f"❤️ Лайки: {s(user.get('notify_likes', True))}\n"
            f"💕 Мэтчи: {s(user.get('notify_matches', True))}\n"
            f"💬 Сообщения: {s(user.get('notify_messages', True))}\n"
            f"👻 Гости: {s(user.get('notify_guests', True))}\n"
            f"🔄 Напоминания: {s(user.get('notify_reminders', True))}\n"
            f"🍷 Подписка: {s(user.get('notify_sub_events', True))}"
        )
        qs = user.get("quiet_hours_start")
        qe = user.get("quiet_hours_end")
        quiet = f"🌙 {qs:02d}:00 — {qe:02d}:00" if qs is not None and qe is not None else "🔔 Отключены"
        await callback.message.edit_text(
            T.NOTIFY_SETTINGS.format(status=status, quiet=quiet),
            reply_markup=KB.notify_settings(user),
            parse_mode=ParseMode.MARKDOWN
        )
        await callback.answer()


@rt.callback_query(F.data.startswith("qh:"))
async def handle_quiet_hours(callback: CallbackQuery, user: Optional[Dict]):
    if not user: return
    data = callback.data[3:]

    if data == "off":
        await DB.update_user(user["telegram_id"], quiet_hours_start=None, quiet_hours_end=None)
        await callback.answer("🔔 Тихие часы отключены")
    else:
        parts = data.split(":")
        start = int(parts[0])
        end = int(parts[1])
        await DB.update_user(user["telegram_id"], quiet_hours_start=start, quiet_hours_end=end)
        await callback.answer(f"🌙 Тихие часы: {start:02d}:00 — {end:02d}:00")

    # Возвращаемся к настройкам
    user = await DB.get_user(callback.from_user.id)
    def s(v): return "✅ Вкл" if v else "❌ Выкл"
    status = (
        f"❤️ Лайки: {s(user.get('notify_likes', True))}\n"
        f"💕 Мэтчи: {s(user.get('notify_matches', True))}\n"
        f"💬 Сообщения: {s(user.get('notify_messages', True))}\n"
        f"👻 Гости: {s(user.get('notify_guests', True))}\n"
        f"🔄 Напоминания: {s(user.get('notify_reminders', True))}\n"
        f"🍷 Подписка: {s(user.get('notify_sub_events', True))}"
    )
    qs = user.get("quiet_hours_start")
    qe = user.get("quiet_hours_end")
    quiet = f"🌙 {qs:02d}:00 — {qe:02d}:00" if qs is not None and qe is not None else "🔔 Отключены"
    await callback.message.edit_text(
        T.NOTIFY_SETTINGS.format(status=status, quiet=quiet),
        reply_markup=KB.notify_settings(user),
        parse_mode=ParseMode.MARKDOWN
    )


# ═══ SHOP ═══

@rt.message(F.text == "🛒 Винная Карта")
async def shop_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        f"🍷 *Винная Карта · {BOT_NAME}*\n\n"
        f"🥂 Подписки · 🚀 Буст · 📊 Сравнить · 🎁 Промокод",
        reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN
    )

@rt.callback_query(F.data == "sh:mn")
async def shop_main(callback: CallbackQuery):
    await callback.message.edit_text(
        f"🍷 *Винная Карта*", reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()

@rt.callback_query(F.data == "sh:compare")
async def shop_compare(callback: CallbackQuery):
    await callback.message.edit_text(
        "📊 *ВИННАЯ КАРТА ТАРИФОВ*\n\n"
        "🆓 *Бесплатный*\n30 лайков · 10 сообщений · 3 гостя\n\n"
        "🥂 *Бокал Вина*\n100 лайков · ∞ сообщений · 10 гостей\n\n"
        "🍾 *Бутылка Вина* 🔥\n∞ лайков · Все гости · Приоритет · Невидимка · 1 буст\n\n"
        "🎖️ *Сомелье*\nВсё + 3 буста · Суперлайки · VIP-бейдж\n\n"
        "🏆 *Винный Погреб*\nВсё навсегда · Бейдж Основатель 👑",
        reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()

@rt.callback_query(F.data == "sh:subs")
async def shop_subs(callback: CallbackQuery):
    await callback.message.edit_text("🍷 *Выбери тариф:*", reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data == "tf:wine_glass")
async def tf1(cb: CallbackQuery):
    await cb.message.edit_text("🥂 *БОКАЛ ВИНА*\n\n✨ 100 лайков · 💬 ∞ сообщений · 👻 10 гостей\n\n• 299₽/нед · 799₽/мес 🔥", reply_markup=KB.buy_wine_glass(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "tf:wine_bottle")
async def tf2(cb: CallbackQuery):
    await cb.message.edit_text("🍾 *БУТЫЛКА ВИНА* 🔥\n\n♾️ Лайки · Все гости · Приоритет · Невидимка · 1 буст\n\n• 499₽/мес · 1199₽/3мес -20%", reply_markup=KB.buy_wine_bottle(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "tf:sommelier")
async def tf3(cb: CallbackQuery):
    await cb.message.edit_text("🎖️ *СОМЕЛЬЕ*\n\nВсё + 3 буста · Суперлайки · VIP-бейдж · 24/7 поддержка\n\n• 799₽/мес · 1999₽/3мес · 3499₽/6мес", reply_markup=KB.buy_sommelier(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "tf:wine_cellar")
async def tf4(cb: CallbackQuery):
    await cb.message.edit_text("🏆 *ВИННЫЙ ПОГРЕБ*\n\nВсё из Сомелье навсегда · Бейдж «Основатель» 👑\n\n💎 4999₽ один раз", reply_markup=KB.buy_wine_cellar(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "sh:boost")
async def shop_boost(callback: CallbackQuery, user: Optional[Dict]):
    if not user: return
    has = user.get("boost_count", 0) > 0
    act = DB.is_boosted(user)
    st = ""
    if act: st += f"\n🚀 Буст до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if has: st += f"\n🚀 Запас: {user['boost_count']}"
    if not has and not act: st = "\n📦 Нет бустов"
    await callback.message.edit_text(f"🚀 *БУСТ АНКЕТЫ*\n\n+500% просмотров на 24ч!{st}", reply_markup=KB.boost_menu(has, act), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data == "bo:act")
async def activate_boost(callback: CallbackQuery, user: Optional[Dict]):
    if not user or user.get("boost_count", 0) <= 0:
        await callback.answer("🚫 Нет бустов!", show_alert=True); return
    ok = await DB.use_boost(user["id"])
    if ok:
        await Anim.boost_animation(callback.message)
        u = await DB.get_user(callback.from_user.id)
        await callback.message.answer(f"🚀 *До {u['boost_expires_at'].strftime('%d.%m %H:%M')}* · Осталось: {u['boost_count']}", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)
    else: await callback.answer("❌", show_alert=True)

@rt.callback_query(F.data.startswith("by:"))
async def handle_buy(callback: CallbackQuery, user: Optional[Dict]):
    if not user: return
    parts = callback.data.split(":")
    prod, param, amt = parts[1], int(parts[2]), int(parts[3])
    if prod == "boost": res = await Pay.create(user, "boost", count=param, amount=amt)
    else: res = await Pay.create(user, "subscription", tier=prod, dur=param, amount=amt)
    if "error" in res: await callback.answer(f"❌ {res['error']}", show_alert=True); return
    await callback.message.edit_text(f"💳 *{amt / 100:.0f}₽*\n\n1️⃣ Оплати → 2️⃣ Проверь", reply_markup=KB.pay(res["url"], res["pid"]), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data.startswith("ck:"))
async def check_payment(callback: CallbackQuery):
    pid = int(callback.data[3:])
    res = await Pay.check(pid)
    if res["status"] == "succeeded":
        await Anim.payment_success_animation(callback.message)
        if res.get("type") == "boost":
            await callback.message.answer(f"🚀 *{res.get('count', 1)} бустов!*", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)
        else:
            await callback.message.answer("🍷 *Подписка активна!* ✨", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)
    elif res["status"] == "pending":
        await callback.answer("⏳ Обрабатывается...", show_alert=True)
    else:
        await callback.answer("❌ Не найдена", show_alert=True)


# ═══ PROMO ═══

@rt.callback_query(F.data == "sh:promo")
async def promo_input(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🎁 *Промокод:*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(PromoInputState.waiting_code)
    await callback.answer()

@rt.message(PromoInputState.waiting_code)
async def promo_activate(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user: await message.answer("❗", reply_markup=KB.main()); return
    result = await DB.use_promo(user["id"], message.text.strip().upper())
    if "error" in result:
        await message.answer(result['error'], reply_markup=KB.main())
    else:
        tn = TIER_NAMES.get(result["tier"], "VIP")
        msg = await message.answer("🎁 Проверяем...")
        await asyncio.sleep(0.5)
        await msg.edit_text(f"🎉 *{tn}* на {result['days']} дней! 🍷", parse_mode=ParseMode.MARKDOWN)
        await message.answer("🍷", reply_markup=KB.main())


# ═══ FAQ & REPORTS ═══

@rt.message(F.text == "❓ FAQ")
async def show_faq(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        f"❓ *FAQ · {BOT_NAME}*\n\n"
        f"*🍷 Симпатии* — ставь ❤️, при взаимности — мэтч!\n"
        f"*🚀 Буст* — 24ч в топе, +500% просмотров\n"
        f"*🥂 Подписки* — больше лайков, гости, приоритет\n"
        f"*💕 Совместимость* — % при мэтче\n"
        f"*🔔 Уведомления* — настрой в меню",
        parse_mode=ParseMode.MARKDOWN
    )

@rt.callback_query(F.data.startswith("rp:"))
async def start_report(callback: CallbackQuery, state: FSMContext):
    await state.update_data(rp_id=int(callback.data[3:]))
    try: await callback.message.edit_caption(caption="⚠️ *Причина:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    except: await callback.message.edit_text("⚠️ *Причина:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data.startswith("rr:"))
async def save_report(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d = await state.get_data()
    rid = d.get("rp_id")
    if rid: await DB.create_report(user["id"], rid, callback.data[3:])
    await state.clear()
    try: await callback.message.edit_caption(caption="✅ Жалоба отправлена 🍷")
    except: await callback.message.edit_text("✅ Жалоба отправлена 🍷")
    await asyncio.sleep(1)
    ps = await DB.search_profiles(user, 1)
    if ps: await show_card(callback.message, ps[0], user)
    await callback.answer()

@rt.callback_query(F.data == "mn")
async def back_to_menu(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    try: await callback.message.delete()
    except: pass
    unread = await DB.get_unread(user["id"]) if user else 0
    await callback.message.answer("🍷", reply_markup=KB.main(unread))
    await callback.answer()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#              ADMIN PANEL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def is_adm(user): return user and user.get("telegram_id") in config.ADMIN_IDS

@rt.message(Command("admin"))
async def admin_cmd(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.clear()
    role = "👑 Создатель" if DB.is_creator(user) else "🛡️ Админ"
    await message.answer(f"🛡️ *Админ-панель · {BOT_NAME}*\n\n👤 *{user['name']}* {role}", reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data == "adm:main")
async def admin_main(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.clear()
    role = "👑 Создатель" if DB.is_creator(user) else "🛡️ Админ"
    await callback.message.edit_text(f"🛡️ *Админ-панель*\n\n👤 *{user['name']}* {role}", reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data == "adm:stats")
async def admin_stats(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    s = await DB.get_stats()
    await callback.message.edit_text(
        f"📊 *Статистика*\n\n"
        f"👥 Всего: {s['total']} · С анкетой: {s['complete']}\n"
        f"📈 DAU: {s['dau']} · WAU: {s['wau']} · MAU: {s['mau']}\n"
        f"🍷 VIP: {s['vip']} ({s['conversion']:.1f}%) · 🚫 Бан: {s['banned']}\n"
        f"📅 Сегодня: +{s['today_reg']}\n\n"
        f"❤️ {s['likes']} · 💕 {s['matches']} · 💬 {s['messages']}\n"
        f"💕 Ср. совместимость: {s['avg_compatibility']:.0f}%\n\n"
        f"💰 Всего: {s['revenue']:.0f}₽ · Месяц: {s['month_revenue']:.0f}₽\n"
        f"⚠️ Жалоб: {s['pending_reports']}",
        reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


# ═══ ADMIN: NOTIFICATION STATS ═══

@rt.callback_query(F.data == "adm:notify_stats")
async def admin_notify_stats(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return

    ns = await NotificationService.get_stats()

    type_names = {
        "like_received": "❤️ Лайки",
        "super_like_received": "⚡ Суперлайки",
        "new_match": "💕 Мэтчи",
        "new_message": "💬 Сообщения",
        "profile_viewed": "👻 Гости",
        "daily_reminder": "🔄 Напоминания",
        "limits_restored": "🌅 Лимиты",
        "sub_expiring": "⏰ Истечение VIP",
        "sub_expired": "😔 VIP истёк",
        "boost_expired": "🚀 Буст истёк",
        "new_users_in_city": "✨ Новые рядом",
        "weekly_stats": "📊 Недельная стат.",
        "unread_messages": "💬 Непрочитанные",
    }

    txt = (
        f"🔔 *Статистика уведомлений*\n\n"
        f"📊 Всего за всё время: {ns['total']}\n"
        f"📅 За сутки: {ns['today']}\n"
        f"✅ Доставлено: {ns['delivered']}\n"
        f"❌ Ошибок: {ns['failed']}\n"
        f"📈 Доставляемость: {ns['delivery_rate']:.1f}%\n\n"
        f"📋 *По типам (за сутки):*\n"
    )
    for ntype, count in sorted(ns["by_type"].items(), key=lambda x: x[1], reverse=True):
        name = type_names.get(ntype, ntype)
        txt += f"  {name}: {count}\n"

    if not ns["by_type"]:
        txt += "  _Нет данных_\n"

    await callback.message.edit_text(
        txt, reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


# ═══ ADMIN: SEARCH, BAN, VIP, BOOST, REPORTS, PAYMENTS, TOP ═══

@rt.callback_query(F.data == "adm:search")
async def admin_search(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await callback.message.edit_text("🔍 *ID / @username / имя:*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.search_user)
    await callback.answer()

@rt.message(AdminStates.search_user)
async def admin_search_result(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    results = await DB.search_users(message.text.strip())
    await state.clear()
    if not results: await message.answer("❌ Не найдено", reply_markup=KB.back_admin()); return
    u = results[0]
    badge = DB.get_badge(u)
    await message.answer(
        f"👤 *{badge}{u['name']}*, {u['age']}\n"
        f"🆔 `{u['id']}` · TG: `{u['telegram_id']}`\n"
        f"🏙️ {u['city']} · @{u.get('username') or '-'}\n"
        f"🍷 {TIER_NAMES.get(u['subscription_tier'], '🆓')}\n"
        f"👁️ {u['views_count']} · ❤️ {u['likes_received_count']} · 💕 {u['matches_count']}\n"
        f"🚫 Бан: {'Да' if u['is_banned'] else 'Нет'}",
        reply_markup=KB.admin_user(u["id"], u["is_banned"]),
        parse_mode=ParseMode.MARKDOWN
    )

@rt.callback_query(F.data.startswith("au:ban:"))
async def admin_ban(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    uid = int(callback.data.split(":")[2])
    u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], is_banned=True)
        await callback.message.edit_text(f"🔒 *{u['name']}* забанен!", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
        try: await callback.bot.send_message(u["telegram_id"], T.BANNED)
        except: pass
    await callback.answer()

@rt.callback_query(F.data.startswith("au:unban:"))
async def admin_unban(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    uid = int(callback.data.split(":")[2])
    u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], is_banned=False)
        await callback.message.edit_text(f"🔓 *{u['name']}* разбанен!", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data.startswith("au:verify:"))
async def admin_verify(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    uid = int(callback.data.split(":")[2])
    u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], is_verified=True)
        await callback.message.edit_text(f"✅ *{u['name']}* верифицирован!", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data.startswith("au:givevip:"))
async def admin_give_vip(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.update_data(target_uid=int(callback.data.split(":")[2]))
    await callback.message.edit_text("🍷 *Тариф:*", reply_markup=KB.give_vip_tiers(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data.startswith("gv:"))
async def admin_gv_tier(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    tier = callback.data[3:]
    if tier == "wine_cellar":
        d = await state.get_data()
        await DB.activate_subscription_by_id(d["target_uid"], tier, 0)
        await state.clear()
        await callback.message.edit_text("🏆 Навсегда!", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
    else:
        await state.update_data(give_tier=tier)
        await callback.message.edit_text("📅 *Дней:*", parse_mode=ParseMode.MARKDOWN)
        await state.set_state(AdminStates.give_vip_duration)
    await callback.answer()

@rt.message(AdminStates.give_vip_duration)
async def admin_gv_days(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    try: days = int(message.text.strip())
    except: await message.answer("❗ Число!"); return
    d = await state.get_data()
    await DB.activate_subscription_by_id(d["target_uid"], d["give_tier"], days)
    await state.clear()
    await message.answer(f"🍷 *{TIER_NAMES.get(d['give_tier'])}* на {days}дн!", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("au:giveboost:"))
async def admin_give_boost(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.update_data(target_uid=int(callback.data.split(":")[2]))
    await callback.message.edit_text("🚀 *Сколько бустов?*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.give_boost_count)
    await callback.answer()

@rt.message(AdminStates.give_boost_count)
async def admin_gb_count(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    try: count = int(message.text.strip())
    except: await message.answer("❗ Число!"); return
    d = await state.get_data()
    await DB.add_boosts(d["target_uid"], count)
    await state.clear()
    await message.answer(f"🚀 *{count} бустов* выдано!", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data == "adm:reports")
async def admin_reports(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    reps = await DB.get_pending_reports(5)
    if not reps:
        await callback.message.edit_text("✅ *Нет жалоб!*", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
        await callback.answer(); return
    rep = reps[0]
    rn = rep["reporter"]["name"] if rep["reporter"] else "?"
    rdn = rep["reported"]["name"] if rep["reported"] else "?"
    rid = rep["reported"]["id"] if rep["reported"] else 0
    await callback.message.edit_text(
        f"⚠️ *Жалоба #{rep['id']}*\n\nНа: *{rdn}* · От: *{rn}* · 📝 {rep['reason']}\n📊 Всего: {len(reps)}",
        reply_markup=KB.admin_report(rep["id"], rid), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()

@rt.callback_query(F.data.startswith("ar:"))
async def admin_report_action(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    parts = callback.data.split(":")
    action, rid, ruid = parts[1], int(parts[2]), int(parts[3])
    if action == "ban":
        u = await DB.get_user_by_id(ruid)
        if u: await DB.update_user(u["telegram_id"], is_banned=True)
        await DB.resolve_report(rid, "banned")
        await callback.message.edit_text("🔒 Забанен", reply_markup=KB.back_admin())
    elif action == "warn":
        u = await DB.get_user_by_id(ruid)
        if u:
            try: await callback.bot.send_message(u["telegram_id"], "⚠️ *Предупреждение!*\n\nВаш профиль получил жалобу.", parse_mode=ParseMode.MARKDOWN)
            except: pass
        await DB.resolve_report(rid, "warned")
        await callback.message.edit_text("⚠️ Предупреждён", reply_markup=KB.back_admin())
    elif action == "dismiss":
        await DB.resolve_report(rid, "dismissed")
        await callback.message.edit_text("❌ Отклонено", reply_markup=KB.back_admin())
    await callback.answer()

@rt.callback_query(F.data == "adm:payments")
async def admin_payments(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    pays = await DB.get_recent_payments(10)
    if not pays:
        await callback.message.edit_text("💰 *Нет платежей*", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
        await callback.answer(); return
    txt = "💰 *Платежи:*\n\n"
    for p in pays:
        st = {"pending": "⏳", "succeeded": "✅", "canceled": "❌"}.get(p["status"], "?")
        txt += f"{st} {p['amount']:.0f}₽ · {p['user_name']}\n"
    await callback.message.edit_text(txt, reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data == "adm:top")
async def admin_top(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    async with async_session_maker() as s:
        tl = await s.execute(select(User).where(User.is_profile_complete == True).order_by(desc(User.likes_received_count)).limit(5))
        txt = "🏆 *Топ по лайкам:*\n"
        for i, u in enumerate(tl.scalars().all(), 1): txt += f"{i}. {u.name} — ❤️ {u.likes_received_count}\n"
    await callback.message.edit_text(txt, reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

# ═══ BROADCAST ═══

@rt.callback_query(F.data == "adm:broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await callback.message.edit_text("📢 *Текст рассылки:*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.broadcast_text)
    await callback.answer()

@rt.message(AdminStates.broadcast_text)
async def admin_bc_text(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.update_data(bc_text=message.text)
    await message.answer("👥 *Аудитория:*", reply_markup=KB.broadcast_targets(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.broadcast_confirm)

@rt.callback_query(AdminStates.broadcast_confirm, F.data.startswith("bc:"))
async def admin_bc_target(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    target = callback.data[3:]
    if target == "send":
        d = await state.get_data()
        txt, tgt = d["bc_text"], d.get("bc_target", "all")
        ids = await DB.get_all_user_ids(tgt)
        await state.clear()
        await callback.message.edit_text(f"📤 *Отправка {len(ids)}...*", parse_mode=ParseMode.MARKDOWN)
        sent, failed = 0, 0
        for tid in ids:
            try: await callback.bot.send_message(tid, txt, parse_mode=ParseMode.MARKDOWN); sent += 1
            except: failed += 1
            if sent % 25 == 0: await asyncio.sleep(1)
        await DB.log_broadcast(user["telegram_id"], txt, tgt, sent, failed)
        await callback.message.answer(f"✅ Отправлено: {sent} · ❌ Ошибок: {failed}", reply_markup=KB.back_admin())
    else:
        await state.update_data(bc_target=target)
        d = await state.get_data()
        names = {"all": "Все", "complete": "С анкетой", "vip": "VIP", "free": "Бесплатные"}
        ids = await DB.get_all_user_ids(target)
        await callback.message.edit_text(f"📢 *Рассылка*\n\n📝 {d['bc_text'][:200]}\n\n👥 {names.get(target)} · 📊 {len(ids)}\n\nОтправить?", reply_markup=KB.broadcast_confirm(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

# ═══ ADMIN: PROMO ═══

@rt.callback_query(F.data == "adm:promo")
async def admin_promo(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await callback.message.edit_text("🎁 *Код:*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.promo_code)
    await callback.answer()

@rt.message(AdminStates.promo_code)
async def admin_pc(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.update_data(pc_code=message.text.strip().upper())
    await message.answer("🍷 *Тариф:*", reply_markup=KB.give_vip_tiers(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.promo_tier)

@rt.callback_query(AdminStates.promo_tier, F.data.startswith("gv:"))
async def admin_pt(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.update_data(pc_tier=callback.data[3:])
    await callback.message.edit_text("📅 *Дней?*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.promo_duration)
    await callback.answer()

@rt.message(AdminStates.promo_duration)
async def admin_pd(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    try: days = int(message.text.strip())
    except: await message.answer("❗"); return
    await state.update_data(pc_days=days)
    await message.answer("🔢 *Лимит использований?*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.promo_uses)

@rt.message(AdminStates.promo_uses)
async def admin_pu(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    try: uses = int(message.text.strip())
    except: await message.answer("❗"); return
    d = await state.get_data()
    await DB.create_promo(d["pc_code"], d["pc_tier"], d["pc_days"], uses)
    await state.clear()
    await message.answer(f"🎁 *Промокод создан!*\n\n🔑 `{d['pc_code']}`\n🍷 {TIER_NAMES.get(d['pc_tier'])} · {d['pc_days']}дн · {uses}шт", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                  MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def main():
    global notifier

    await init_db()

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())

    dp.include_router(rt)

    # Инициализируем систему уведомлений
    notifier = NotificationService(bot)
    await notifier.start_scheduler()

    logger.info(f"🍷 {BOT_NAME} v4.1 starting...")
    logger.info(f"🔔 Notification system: ACTIVE")
    logger.info(f"🔔 Scheduler: checking every 15 min")
    logger.info(f"🔔 Types: likes, matches, messages, guests, reminders, sub events, weekly stats")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await notifier.stop_scheduler()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
