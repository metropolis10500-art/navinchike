"""
ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v3.5 (IMPROVED SEARCH & MATCHING)

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
    func, and_, or_, desc, asc, case, text as sa_text
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

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING & CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
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
    FREE_DAILY_MESSAGES: int = 10
    FREE_GUESTS_VISIBLE: int = 3
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

class SwipeAction(str, Enum):
    LIKE = "like"
    DISLIKE = "dislike"
    SUPERLIKE = "superlike"

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
    daily_likes_remaining = Column(Integer, default=30)
    daily_messages_remaining = Column(Integer, default=10)
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
    last_search_no_results = Column(DateTime, nullable=True)
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

class MutualCandidate(Base):
    """Кэш кандидатов, которые лайкнули пользователя (для приоритета)"""
    __tablename__ = "mutual_candidates"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    candidate_id = Column(Integer, ForeignKey("users.id"))
    shown = Column(Boolean, default=False)
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
# FSM STATES
# ═══════════════════════════════════════════════════════════════════════════════

class RegStates(StatesGroup):
    name = State()
    age = State()
    gender = State()
    city = State()
    photo = State()
    bio = State()
    interests = State()
    looking_for = State()
    age_range = State()

class EditStates(StatesGroup):
    edit_name = State()
    edit_age = State()
    edit_city = State()
    edit_bio = State()
    edit_interests = State()
    edit_age_range = State()
    add_photo = State()

class ChatStates(StatesGroup):
    chatting = State()

class SearchStates(StatesGroup):
    browsing = State()

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

# ═══════════════════════════════════════════════════════════════════════════════
# COMPATIBILITY ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class Compatibility:
    """Движок совместимости — вычисляет score между двумя профилями"""

    INTERESTS_LIST = [
        "🎵 Музыка", "🎬 Кино", "📚 Книги", "🏃 Спорт", "✈️ Путешествия",
        "🍳 Кулинария", "🎮 Игры", "📷 Фото", "🎨 Искусство", "💻 IT",
        "🐾 Животные", "🧘 Йога", "🏕️ Природа", "🍷 Вино", "💃 Танцы",
        "🎸 Концерты", "🏋️ Фитнес", "📺 Сериалы", "🏖️ Пляж", "☕ Кофе",
    ]

    @staticmethod
    def calc_score(user1: Dict, user2: Dict) -> float:
        score = 0.0

        # 1. Совпадение интересов (0-40 баллов)
        i1 = set((user1.get("interests") or "").split(","))
        i2 = set((user2.get("interests") or "").split(","))
        i1.discard("")
        i2.discard("")
        if i1 and i2:
            common = len(i1 & i2)
            total = len(i1 | i2)
            score += (common / total) * 40 if total > 0 else 0

        # 2. Возрастная близость (0-20 баллов)
        a1 = user1.get("age") or 25
        a2 = user2.get("age") or 25
        age_diff = abs(a1 - a2)
        if age_diff <= 2:
            score += 20
        elif age_diff <= 5:
            score += 15
        elif age_diff <= 10:
            score += 10
        elif age_diff <= 15:
            score += 5

        # 3. Город совпадает (0-15 баллов)
        if (user1.get("city") or "").lower() == (user2.get("city") or "").lower():
            score += 15

        # 4. Взаимный looking_for (0-15 баллов)
        g1 = user1.get("gender")
        g2 = user2.get("gender")
        lf1 = user1.get("looking_for", "both")
        lf2 = user2.get("looking_for", "both")
        mutual_match = True
        if lf1 != "both" and lf1 != g2:
            mutual_match = False
        if lf2 != "both" and lf2 != g1:
            mutual_match = False
        if mutual_match:
            score += 15

        # 5. Оба активны (0-10 баллов)
        now = datetime.utcnow()
        la1 = user1.get("last_active_at") or now
        la2 = user2.get("last_active_at") or now
        if (now - la1).days <= 1 and (now - la2).days <= 1:
            score += 10
        elif (now - la1).days <= 3 and (now - la2).days <= 3:
            score += 5

        return round(score, 1)

    @staticmethod
    def calc_popularity(user: Dict) -> float:
        likes = user.get("likes_received_count", 0)
        views = user.get("views_count", 0)
        matches = user.get("matches_count", 0)
        ratio = likes / max(views, 1)
        return round((likes * 0.4 + matches * 2 + ratio * 50), 1)

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
            "city": u.city, "bio": u.bio,
            "interests": u.interests or "",
            "looking_for": u.looking_for.value if u.looking_for else "both",
            "age_from": u.age_from, "age_to": u.age_to,
            "photos": u.photos or "", "main_photo": u.main_photo,
            "is_active": u.is_active, "is_banned": u.is_banned,
            "is_verified": u.is_verified,
            "is_profile_complete": u.is_profile_complete,
            "subscription_tier": u.subscription_tier.value if u.subscription_tier else "free",
            "subscription_expires_at": u.subscription_expires_at,
            "daily_likes_remaining": u.daily_likes_remaining or 30,
            "daily_messages_remaining": u.daily_messages_remaining or 10,
            "daily_superlikes_remaining": u.daily_superlikes_remaining or 0,
            "last_limits_reset": u.last_limits_reset,
            "boost_expires_at": u.boost_expires_at,
            "boost_count": u.boost_count or 0,
            "views_count": u.views_count or 0,
            "likes_received_count": u.likes_received_count or 0,
            "likes_sent_count": u.likes_sent_count or 0,
            "matches_count": u.matches_count or 0,
            "popularity_score": u.popularity_score or 0.0,
            "search_expand_city": u.search_expand_city or False,
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
        if DB.is_creator(u): return " · 👑 Создатель"
        if DB.is_admin(u): return " · 🛡️ Админ"
        return ""

    @staticmethod
    def get_vip_tier_name(u: Dict) -> str:
        return {
            "free": "🍷 Бесплатный", "vip_light": "🥂 Винчик Light",
            "vip_standard": "🍾 Винчик Standard", "vip_pro": "👑 Винчик Pro",
            "vip_lifetime": "💎 Винчик Forever",
        }.get(u.get("subscription_tier", "free"), "🍷 Бесплатный")

    @staticmethod
    def get_superlikes_limit(u: Dict) -> int:
        t = u.get("subscription_tier", "free")
        if t in ("vip_pro", "vip_lifetime"): return 5
        if t == "vip_standard": return 2
        if t == "vip_light": return 1
        return 0

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
            u = User(telegram_id=tg_id, username=username,
                     referral_code=str(uuid.uuid4())[:8].upper(),
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
            return await DB.update_user(
                u["telegram_id"],
                daily_likes_remaining=config.FREE_DAILY_LIKES,
                daily_messages_remaining=config.FREE_DAILY_MESSAGES,
                daily_superlikes_remaining=sl,
                last_limits_reset=now, last_active_at=now
            )
        await DB.update_user(u["telegram_id"], last_active_at=now)
        return u

    # ═══════════════════════════════════════════════════════
    # УЛУЧШЕННАЯ СИСТЕМА ПОИСКА
    # ═══════════════════════════════════════════════════════

    @staticmethod
    async def get_who_liked_me(uid: int) -> List[int]:
        """Получить ID тех, кто лайкнул пользователя, но пользователь ещё не видел"""
        async with async_session_maker() as s:
            # Кто лайкнул меня
            liked_me = await s.execute(
                select(Like.from_user_id).where(Like.to_user_id == uid)
            )
            liked_me_ids = set(r[0] for r in liked_me.fetchall())

            # Кого я уже лайкнул/дизлайкнул
            my_likes = await s.execute(
                select(Like.to_user_id).where(Like.from_user_id == uid)
            )
            my_dislikes = await s.execute(
                select(Dislike.to_user_id).where(Dislike.from_user_id == uid)
            )
            seen = set(r[0] for r in my_likes.fetchall()) | set(r[0] for r in my_dislikes.fetchall())

            return list(liked_me_ids - seen)

    @staticmethod
    async def search_profiles(u: Dict, limit=5) -> List[Dict]:
        """
        Улучшенный поиск с приоритетами:
        1. Те, кто лайкнул тебя (VIP видит кто, остальные — просто приоритет)
        2. Бустнутые профили
        3. По совместимости и популярности
        4. Расширение географии если нет результатов
        """
        async with async_session_maker() as s:
            # Собираем исключения: кого уже лайкнули/дизлайкнули + себя
            my_likes = await s.execute(
                select(Like.to_user_id).where(Like.from_user_id == u["id"])
            )
            my_dislikes = await s.execute(
                select(Dislike.to_user_id).where(Dislike.from_user_id == u["id"])
            )
            exc = set(r[0] for r in my_likes.fetchall())
            exc |= set(r[0] for r in my_dislikes.fetchall())
            exc.add(u["id"])

            results = []

            # ─── ЭТАП 1: Те кто лайкнул меня (приоритет) ───
            who_liked = await DB.get_who_liked_me(u["id"])
            if who_liked:
                priority_ids = [uid for uid in who_liked if uid not in exc]
                if priority_ids:
                    pr = await s.execute(
                        select(User).where(and_(
                            User.id.in_(priority_ids),
                            User.is_active == True,
                            User.is_banned == False,
                            User.is_profile_complete == True,
                        ))
                    )
                    for p in pr.scalars().all():
                        d = DB._to_dict(p)
                        d["_priority"] = "liked_you"
                        d["_compat"] = Compatibility.calc_score(u, d)
                        results.append(d)
                        exc.add(p.id)

            # ─── ЭТАП 2: Основной поиск по городу ───
            remaining = limit - len(results)
            if remaining > 0:
                q = select(User).where(and_(
                    User.is_active == True,
                    User.is_banned == False,
                    User.is_profile_complete == True,
                    User.id.not_in(exc),
                    User.age >= u["age_from"],
                    User.age <= u["age_to"],
                ))

                # Фильтр по городу
                q = q.where(User.city == u["city"])

                # Фильтр по полу
                lf = u.get("looking_for", "both")
                if lf == "male":
                    q = q.where(User.gender == Gender.MALE)
                elif lf == "female":
                    q = q.where(User.gender == Gender.FEMALE)

                # Взаимный фильтр: показываем только тех, кто тоже ищет наш пол
                my_gender = u.get("gender")
                if my_gender:
                    q = q.where(or_(
                        User.looking_for == LookingFor.BOTH,
                        User.looking_for == LookingFor(my_gender),
                    ))

                # Сортировка: бусты > популярность > активность
                q = q.order_by(
                    User.boost_expires_at.desc().nullslast(),
                    User.popularity_score.desc(),
                    User.last_active_at.desc(),
                ).limit(remaining * 3)  # берём с запасом для ранжирования

                r = await s.execute(q)
                candidates = []
                for p in r.scalars().all():
                    d = DB._to_dict(p)
                    d["_priority"] = "boosted" if DB.is_boosted(d) else "normal"
                    d["_compat"] = Compatibility.calc_score(u, d)
                    candidates.append(d)

                # Ранжируем по совместимости с элементом случайности
                candidates.sort(key=lambda x: (
                    x["_priority"] == "boosted",
                    x["_compat"] + random.uniform(0, 10),
                ), reverse=True)

                results.extend(candidates[:remaining])

            # ─── ЭТАП 3: Расширение географии если мало результатов ───
            if len(results) < limit and config.SEARCH_EXPAND_RADIUS:
                still_need = limit - len(results)
                all_exc = exc | set(r["id"] for r in results)

                q2 = select(User).where(and_(
                    User.is_active == True,
                    User.is_banned == False,
                    User.is_profile_complete == True,
                    User.id.not_in(all_exc),
                    User.city != u["city"],  # другие города
                    User.age >= u["age_from"],
                    User.age <= u["age_to"],
                ))

                lf = u.get("looking_for", "both")
                if lf == "male":
                    q2 = q2.where(User.gender == Gender.MALE)
                elif lf == "female":
                    q2 = q2.where(User.gender == Gender.FEMALE)

                my_gender = u.get("gender")
                if my_gender:
                    q2 = q2.where(or_(
                        User.looking_for == LookingFor.BOTH,
                        User.looking_for == LookingFor(my_gender),
                    ))

                q2 = q2.order_by(
                    User.popularity_score.desc(),
                    User.last_active_at.desc(),
                ).limit(still_need)

                r2 = await s.execute(q2)
                for p in r2.scalars().all():
                    d = DB._to_dict(p)
                    d["_priority"] = "other_city"
                    d["_compat"] = Compatibility.calc_score(u, d)
                    results.append(d)

            # Пересчитаем popularity для текущего пользователя
            new_pop = Compatibility.calc_popularity(u)
            if abs(new_pop - u.get("popularity_score", 0)) > 1:
                await s.execute(
                    update(User).where(User.id == u["id"]).values(popularity_score=new_pop)
                )
                await s.commit()

            return results[:limit]

    @staticmethod
    async def add_like(fd: int, tid: int, is_super: bool = False) -> Dict:
        """
        Добавить лайк. Возвращает:
        {"is_match": bool, "match_id": int|None, "compat": float}
        """
        async with async_session_maker() as s:
            # Проверка дублей
            ex = await s.execute(
                select(Like).where(and_(Like.from_user_id == fd, Like.to_user_id == tid))
            )
            if ex.scalar_one_or_none():
                return {"is_match": False, "match_id": None, "compat": 0}

            s.add(Like(from_user_id=fd, to_user_id=tid, is_super_like=is_super))

            # Обновляем счётчики
            await s.execute(update(User).where(User.id == tid).values(
                likes_received_count=User.likes_received_count + 1
            ))
            await s.execute(update(User).where(User.id == fd).values(
                likes_sent_count=User.likes_sent_count + 1
            ))

            # Проверяем взаимность
            rev = await s.execute(
                select(Like).where(and_(Like.from_user_id == tid, Like.to_user_id == fd))
            )
            is_match = rev.scalar_one_or_none() is not None
            match_id = None
            compat = 0.0

            if is_match:
                existing = await s.execute(select(Match).where(or_(
                    and_(Match.user1_id == fd, Match.user2_id == tid),
                    and_(Match.user1_id == tid, Match.user2_id == fd)
                )))
                if not existing.scalar_one_or_none():
                    # Считаем совместимость
                    u1r = await s.execute(select(User).where(User.id == fd))
                    u2r = await s.execute(select(User).where(User.id == tid))
                    u1 = u1r.scalar_one_or_none()
                    u2 = u2r.scalar_one_or_none()
                    if u1 and u2:
                        compat = Compatibility.calc_score(DB._to_dict(u1), DB._to_dict(u2))

                    m = Match(
                        user1_id=min(fd, tid), user2_id=max(fd, tid),
                        compatibility_score=compat
                    )
                    s.add(m)
                    await s.flush()
                    match_id = m.id

                    await s.execute(update(User).where(User.id.in_([fd, tid])).values(
                        matches_count=User.matches_count + 1
                    ))

            await s.commit()
            return {"is_match": is_match, "match_id": match_id, "compat": compat}

    @staticmethod
    async def add_dislike(fd: int, tid: int):
        """Добавить дизлайк (чтобы не показывать повторно)"""
        async with async_session_maker() as s:
            ex = await s.execute(
                select(Dislike).where(and_(Dislike.from_user_id == fd, Dislike.to_user_id == tid))
            )
            if not ex.scalar_one_or_none():
                s.add(Dislike(from_user_id=fd, to_user_id=tid))
                await s.commit()

    @staticmethod
    async def reset_dislikes(uid: int) -> int:
        """Сброс дизлайков (VIP фича). Возвращает кол-во сброшенных."""
        async with async_session_maker() as s:
            r = await s.execute(
                select(func.count(Dislike.id)).where(Dislike.from_user_id == uid)
            )
            count = r.scalar() or 0
            await s.execute(delete(Dislike).where(Dislike.from_user_id == uid))
            await s.commit()
            return count

    @staticmethod
    async def get_likes_received(uid: int, limit=20) -> List[Dict]:
        """Кто лайкнул меня (для VIP)"""
        async with async_session_maker() as s:
            r = await s.execute(
                select(Like).where(Like.to_user_id == uid)
                .order_by(Like.created_at.desc()).limit(limit)
            )
            out = []
            for lk in r.scalars().all():
                u = await DB.get_user_by_id(lk.from_user_id)
                if u:
                    u["is_super_like"] = lk.is_super_like
                    u["liked_at"] = lk.created_at
                    out.append(u)
            return out

    @staticmethod
    async def get_matches(uid: int) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(Match).where(and_(
                    or_(Match.user1_id == uid, Match.user2_id == uid),
                    Match.is_active == True
                )).order_by(Match.last_message_at.desc().nullslast(), Match.compatibility_score.desc())
            )
            out = []
            for m in r.scalars().all():
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                pr = await s.execute(select(User).where(User.id == pid))
                p = pr.scalar_one_or_none()
                if p:
                    # Непрочитанные сообщения для этого мэтча
                    unread = await s.execute(
                        select(func.count(ChatMessage.id)).where(and_(
                            ChatMessage.match_id == m.id,
                            ChatMessage.sender_id != uid,
                            ChatMessage.is_read == False
                        ))
                    )
                    unread_count = unread.scalar() or 0

                    out.append({
                        "match_id": m.id, "user_id": p.id,
                        "telegram_id": p.telegram_id, "name": p.name,
                        "age": p.age, "photo": p.main_photo,
                        "compat": m.compatibility_score,
                        "unread": unread_count,
                        "last_msg": m.last_message_at,
                    })
            return out

    @staticmethod
    async def unmatch(uid: int, match_id: int) -> bool:
        """Размэтчить (удалить мэтч)"""
        async with async_session_maker() as s:
            r = await s.execute(select(Match).where(and_(
                Match.id == match_id,
                or_(Match.user1_id == uid, Match.user2_id == uid)
            )))
            m = r.scalar_one_or_none()
            if not m:
                return False
            await s.execute(
                update(Match).where(Match.id == match_id).values(is_active=False)
            )
            await s.execute(
                update(User).where(User.id.in_([m.user1_id, m.user2_id])).values(
                    matches_count=func.greatest(User.matches_count - 1, 0)
                )
            )
            await s.commit()
            return True

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
    async def send_msg(mid: int, sid: int, txt: str):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=txt))
            await s.execute(update(Match).where(Match.id == mid).values(last_message_at=datetime.utcnow()))
            await s.commit()

    @staticmethod
    async def get_msgs(mid: int, limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(ChatMessage).where(ChatMessage.match_id == mid)
                .order_by(ChatMessage.created_at.desc()).limit(limit)
            )
            return [{"sender_id": m.sender_id, "text": m.text, "created_at": m.created_at}
                    for m in reversed(r.scalars().all())]

    @staticmethod
    async def mark_read(mid: int, uid: int):
        """Пометить сообщения прочитанными"""
        async with async_session_maker() as s:
            await s.execute(
                update(ChatMessage).where(and_(
                    ChatMessage.match_id == mid,
                    ChatMessage.sender_id != uid,
                    ChatMessage.is_read == False
                )).values(is_read=True)
            )
            await s.commit()

    @staticmethod
    async def get_unread(uid: int) -> int:
        async with async_session_maker() as s:
            ms = await s.execute(select(Match.id).where(
                and_(or_(Match.user1_id == uid, Match.user2_id == uid), Match.is_active == True)
            ))
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
    async def add_guest(vid: int, uid: int):
        async with async_session_maker() as s:
            s.add(GuestVisit(visitor_id=vid, visited_user_id=uid))
            await s.execute(update(User).where(User.id == uid).values(views_count=User.views_count + 1))
            await s.commit()

    @staticmethod
    async def get_guests(uid: int, limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(GuestVisit.visitor_id)
                .where(GuestVisit.visited_user_id == uid)
                .order_by(GuestVisit.created_at.desc())
                .distinct().limit(limit)
            )
            ids = [row[0] for row in r.fetchall()]
            if not ids:
                return []
            us = await s.execute(select(User).where(User.id.in_(ids)))
            return [DB._to_dict(u) for u in us.scalars().all()]

    @staticmethod
    async def dec_likes(tg_id: int):
        u = await DB.get_user(tg_id)
        if u and DB.is_vip(u):
            return
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(
                daily_likes_remaining=func.greatest(User.daily_likes_remaining - 1, 0)
            ))
            await s.commit()

    @staticmethod
    async def dec_superlikes(tg_id: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.telegram_id == tg_id).values(
                daily_superlikes_remaining=func.greatest(User.daily_superlikes_remaining - 1, 0)
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
            ne = (u.boost_expires_at + timedelta(hours=24)) if u.boost_expires_at and u.boost_expires_at > now else now + timedelta(hours=24)
            await s.execute(update(User).where(User.id == uid).values(
                boost_count=User.boost_count - 1, boost_expires_at=ne
            ))
            await s.commit()
            return True

    @staticmethod
    async def add_boosts(uid: int, c: int):
        async with async_session_maker() as s:
            await s.execute(update(User).where(User.id == uid).values(boost_count=User.boost_count + c))
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
            pending_reports = (await s.execute(select(func.count(Report.id)).where(Report.status == "pending"))).scalar() or 0
            return {
                "total": total, "complete": complete, "dau": dau, "wau": wau, "mau": mau,
                "vip": vip, "banned": banned, "today_reg": today_reg, "matches": total_matches,
                "messages": total_msgs, "likes": total_likes,
                "revenue": revenue / 100 if revenue else 0,
                "month_revenue": month_rev / 100 if month_rev else 0,
                "pending_reports": pending_reports,
                "conversion": (vip / complete * 100) if complete > 0 else 0,
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
    async def get_all_user_ids(filter_type: str = "all") -> List[int]:
        async with async_session_maker() as s:
            q = select(User.telegram_id).where(and_(User.is_active == True, User.is_banned == False))
            if filter_type == "complete": q = q.where(User.is_profile_complete == True)
            elif filter_type == "vip": q = q.where(User.subscription_tier != SubscriptionTier.FREE)
            elif filter_type == "free": q = q.where(User.subscription_tier == SubscriptionTier.FREE)
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
    async def resolve_report(report_id: int, action: str, notes: str = ""):
        async with async_session_maker() as s:
            await s.execute(update(Report).where(Report.id == report_id).values(status=action, admin_notes=notes, resolved_at=datetime.utcnow()))
            await s.commit()

    @staticmethod
    async def get_recent_payments(limit=10) -> List[Dict]:
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
    async def use_promo(user_id, code) -> Dict:
        async with async_session_maker() as s:
            r = await s.execute(select(PromoCode).where(and_(PromoCode.code == code.upper(), PromoCode.is_active == True)))
            promo = r.scalar_one_or_none()
            if not promo: return {"error": "Промокод не найден"}
            if promo.used_count >= promo.max_uses: return {"error": "Промокод исчерпан"}
            used = await s.execute(select(PromoUse).where(and_(PromoUse.promo_id == promo.id, PromoUse.user_id == user_id)))
            if used.scalar_one_or_none(): return {"error": "Ты уже использовал этот промокод"}
            s.add(PromoUse(promo_id=promo.id, user_id=user_id))
            await s.execute(update(PromoCode).where(PromoCode.id == promo.id).values(used_count=PromoCode.used_count + 1))
            await s.commit()
            await DB.activate_subscription_by_id(user_id, promo.tier, promo.duration_days)
            return {"success": True, "tier": promo.tier, "days": promo.duration_days}

    @staticmethod
    async def activate_subscription_by_id(uid, tier, days):
        async with async_session_maker() as s:
            ur = await s.execute(select(User).where(User.id == uid))
            u = ur.scalar_one_or_none()
            if not u: return
            te = SubscriptionTier(tier)
            now = datetime.utcnow()
            exp = None if te == SubscriptionTier.VIP_LIFETIME else (
                (u.subscription_expires_at + timedelta(days=days)) if u.subscription_expires_at and u.subscription_expires_at > now
                else now + timedelta(days=days)
            )
            await s.execute(update(User).where(User.id == uid).values(subscription_tier=te, subscription_expires_at=exp))
            await s.commit()

    @staticmethod
    async def create_payment(uid, yid, amount, desc, ptype, ptier=None, pdur=None, pcount=None) -> int:
        async with async_session_maker() as s:
            p = Payment(user_id=uid, yookassa_payment_id=yid, amount=amount, description=desc, product_type=ptype, product_tier=ptier, product_duration=pdur, product_count=pcount)
            s.add(p)
            await s.commit()
            await s.refresh(p)
            return p.id

    @staticmethod
    async def get_payment(pid) -> Optional[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(select(Payment).where(Payment.id == pid))
            p = r.scalar_one_or_none()
            if p:
                return {"id": p.id, "user_id": p.user_id, "yookassa_payment_id": p.yookassa_payment_id, "status": p.status.value, "product_type": p.product_type, "product_tier": p.product_tier, "product_duration": p.product_duration, "product_count": p.product_count}
            return None

    @staticmethod
    async def update_payment_status(pid, st):
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

# ═══════════════════════════════════════════════════════════════════════════════
# TEXTS
# ═══════════════════════════════════════════════════════════════════════════════

TIER_NAMES = {
    "free": "🍷 Бесплатный", "vip_light": "🥂 Винчик Light",
    "vip_standard": "🍾 Винчик Standard", "vip_pro": "👑 Винчик Pro",
    "vip_lifetime": "💎 Винчик Forever",
}

class T:
    WELCOME_NEW = f"""
🍷 *Добро пожаловать в {BOT_NAME}!*

Найди свою половинку за бокалом вина! 🥂
Тысячи людей по всей России уже здесь.

Давай создадим анкету 👇
"""
    WELCOME_BACK = """
🍷 *С возвращением, {name}!*

{status}
👁️ {views} · 💘 {matches} · 💬 {msgs}
{likes_info}
"""
    ASK_NAME = "👤 Как тебя зовут?"
    ASK_AGE = "🎂 Сколько тебе лет? _(18-99)_"
    ASK_GENDER = "👫 Твой пол:"
    ASK_CITY = "🌍 Твой город:"
    ASK_PHOTO = "📸 Отправь своё фото или нажми «Пропустить»:"
    ASK_BIO = "✍️ Расскажи о себе _(до 500 симв.)_ или «Пропустить»:"
    ASK_INTERESTS = "🎯 *Выбери свои интересы* _(чем больше, тем точнее подбор)_:"
    ASK_LOOKING = "🔍 Кого ищешь?"
    ASK_AGE_RANGE = "🎯 *Возрастной диапазон?*\nВведи через дефис, например: `18-30`"
    BAD_NAME = "⚠️ Имя должно быть 2-50 символов:"
    BAD_AGE = "⚠️ Возраст должен быть 18-99:"
    REG_DONE = f"✅ *Анкета готова!* Добро пожаловать в {BOT_NAME}! 🍷"
    NO_PROFILES = """
😔 *Анкеты в твоём городе закончились!*

Попробуй:
• 🔄 Сбросить пропущенных
• 🌍 Расширить поиск на другие города
• ⏰ Зайти позже — появятся новые люди
"""
    LIKES_LIMIT = "❌ *Лимит лайков на сегодня!*\n🥂 Винчик VIP — безлимитные лайки!"
    NEW_MATCH = "💕 *Взаимная симпатия с {name}!* 🥂\n\n🎯 Совместимость: {compat}%\n\nНапиши первым 👇"
    NEW_MATCH_SHORT = "💕 *Мэтч с {name}!* 🥂"
    SUPERLIKE_RECEIVED = "⭐ *{name}* поставил тебе суперлайк! Ты ему очень понравился(ась)!"
    NO_MATCHES = "😞 Пока нет взаимных симпатий. Листай анкеты! 🍷"
    NO_PROFILE = "⚠️ Заполни профиль 👉 /start"
    BANNED = "🚫 Аккаунт заблокирован."
    NO_GUESTS = "😴 Пока никто не заглядывал к тебе"
    NO_MSGS = "💤 Нет сообщений"

    SHOP = f"""
🛍️ *{BOT_NAME}* — Винный магазин

🥂 *VIP-подписки* — разблокируй возможности
🚀 *Буст анкеты* — поднимись в топ
📊 *Сравнить тарифы*
🎁 *Промокод* — активируй подарок
"""

    FAQ = f"""
❓ *FAQ · {BOT_NAME}*

*🍷 Как работают симпатии?*
Ставь 👍 или ⭐ — если взаимно, откроется чат!

*⭐ Что такое суперлайк?*
Суперлайк уведомляет человека, что ты его выделил. Шанс мэтча ×3!

*🎯 Как работает совместимость?*
Алгоритм анализирует интересы, возраст, город и предпочтения.

*🚀 Что такое буст?*
Твоя анкета поднимается в топ выдачи на 24 часа.

*🔄 Что значит «Сбросить пропущенных»?*
Люди, которых ты пропустил, снова появятся в выдаче.

*🥂 Что даёт VIP?*
Безлимит, суперлайки, кто лайкнул тебя, невидимка и другое.
"""

    BOOST_INFO = """
🚀 *БУСТ АНКЕТЫ · {bot_name}*

Поднимает профиль в топ на 24ч!
👁️ +500% просмотров · ❤️ +300% лайков

💡 Лучшее время — вечер 18:00-22:00

{status}
"""

    COMPARE = f"""
📊 *ТАРИФЫ · {BOT_NAME}*

🍷 *Бесплатный*
• 30 лайков/день
• 10 сообщений/день
• 3 гостя/день

🥂 *Винчик Light (149₽/мес)*
• 100 лайков/день
• ∞ сообщений
• 1 суперлайк/день ⭐
• 10 гостей/день
• Приоритет в выдаче

🍾 *Винчик Standard (349₽/мес)*
• ∞ лайков и сообщений
• 2 суперлайка/день ⭐
• Все гости
• Кто тебя лайкнул ❤️
• Невидимка
• 1 буст/день
• Сброс пропущенных

👑 *Винчик Pro (599₽/мес)*
• Всё из Standard +
• 5 суперлайков/день ⭐
• 3 буста/день
• VIP-бейдж 👑
• В топе выдачи
• Приоритетная поддержка

💎 *Винчик Forever (2999₽)*
• Всё из Pro НАВСЕГДА
• Бейдж 💎
• Все будущие обновления
"""

    LIGHT = f"""
🥂 *ВИНЧИК LIGHT*

• 100 лайков/день
• ∞ сообщений
• 1 суперлайк/день ⭐
• 10 гостей/день
• Приоритет в выдаче

📍 149₽/мес · 379₽/3мес (-15%) · 649₽/6мес (-27%)
"""
    STANDARD = f"""
🍾 *ВИНЧИК STANDARD* 🔥

• ∞ лайков и сообщений
• 2 суперлайка/день ⭐
• Все гости
• Кто тебя лайкнул ❤️
• Невидимка · 1 буст/день
• Сброс пропущенных 🔄

📍 349₽/мес · 849₽/3мес (-19%) · 1449₽/6мес (-31%)
"""
    PRO = f"""
👑 *ВИНЧИК PRO* 💫

Всё из Standard + :
• 5 суперлайков/день ⭐
• 3 буста/день
• VIP-бейдж 👑
• В топе выдачи
• Поддержка 24/7

📍 599₽/мес · 1499₽/3мес (-17%) · 2599₽/6мес (-28%)
"""
    LIFETIME = f"""
💎 *ВИНЧИК FOREVER*

Всё из Pro НАВСЕГДА!
💎 Бейдж · 🎁 Все обновления · 💬 Чат с командой

Окупается за 5 месяцев!
*2999₽ — один раз навсегда*
"""

    ADMIN_MAIN = """
🛡️ *Админ-панель · {bot_name}*
*{admin_name}* {role}
"""
    ADMIN_STATS = """
📊 *Статистика · {bot_name}*

👥 Всего: {total} · С анкетой: {complete}
DAU: {dau} · WAU: {wau} · MAU: {mau}
VIP: {vip} ({conversion:.1f}%) · Бан: {banned} · Сегодня: +{today_reg}

💬 Лайков: {likes} · Мэтчей: {matches} · Сообщений: {messages}
💰 Всего: {revenue:.0f}₽ · Месяц: {month_revenue:.0f}₽ · Жалоб: {pending_reports}
"""
    ADMIN_USER_CARD = """
*Карточка*
ID: `{id}` · TG: `{telegram_id}` · @{username}
{badge}*{name}*, {age} · 🌍 {city}
_{bio}_
{tier} · 👁️{views} ❤️{likes} 💘{matches} 🚀{boosts}
Рег: {created} · Бан: {banned}
"""
    BROADCAST_CONFIRM = """
📢 *Рассылка*
{text}
Аудитория: *{target}* · Получателей: *{count}*
"""

# ═══════════════════════════════════════════════════════════════════════════════
# KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════

class KB:
    @staticmethod
    def main(unread=0):
        chat_text = f"💬 Чаты ({unread})" if unread > 0 else "💬 Чаты"
        return ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="📋 Анкеты"), KeyboardButton(text="❤️ Симпатии")],
            [KeyboardButton(text=chat_text), KeyboardButton(text="👀 Гости")],
            [KeyboardButton(text="🛍️ Магазин"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="❓ FAQ")],
        ], resize_keyboard=True)

    @staticmethod
    def gender():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужской", callback_data="g:male"),
             InlineKeyboardButton(text="👩 Женский", callback_data="g:female")]
        ])

    @staticmethod
    def looking():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужчин", callback_data="l:male"),
             InlineKeyboardButton(text="👩 Женщин", callback_data="l:female")],
            [InlineKeyboardButton(text="👫 Всех", callback_data="l:both")]
        ])

    @staticmethod
    def interests(selected: set = None):
        """Клавиатура выбора интересов (toggle)"""
        if selected is None:
            selected = set()
        rows = []
        items = Compatibility.INTERESTS_LIST
        for i in range(0, len(items), 2):
            row = []
            for j in range(2):
                if i + j < len(items):
                    item = items[i + j]
                    check = "✅ " if item in selected else ""
                    row.append(InlineKeyboardButton(
                        text=f"{check}{item}",
                        callback_data=f"int:{i+j}"
                    ))
            rows.append(row)
        rows.append([InlineKeyboardButton(text="✅ Готово", callback_data="int:done")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def skip():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭️ Пропустить", callback_data="skip")]
        ])

    @staticmethod
    def search(uid, show_superlike=False):
        row1 = [
            InlineKeyboardButton(text="👍", callback_data=f"lk:{uid}"),
            InlineKeyboardButton(text="👎", callback_data=f"dl:{uid}"),
        ]
        if show_superlike:
            row1.insert(1, InlineKeyboardButton(text="⭐", callback_data=f"sl:{uid}"))
        return InlineKeyboardMarkup(inline_keyboard=[
            row1,
            [InlineKeyboardButton(text="🚩 Жалоба", callback_data=f"rp:{uid}")]
        ])

    @staticmethod
    def no_profiles(is_vip=False):
        b = [
            [InlineKeyboardButton(text="🌍 Искать в других городах", callback_data="sr:expand")],
        ]
        if is_vip:
            b.append([InlineKeyboardButton(text="🔄 Сбросить пропущенных", callback_data="sr:reset")])
        b.append([InlineKeyboardButton(text="🔁 Попробовать снова", callback_data="sr:retry")])
        b.append([InlineKeyboardButton(text="◀️ Меню", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def matches(ms):
        b = []
        for m in ms[:10]:
            unread_badge = f" 🔴{m['unread']}" if m.get("unread", 0) > 0 else ""
            compat_badge = f" 🎯{m['compat']:.0f}%" if m.get("compat", 0) > 0 else ""
            b.append([InlineKeyboardButton(
                text=f"💬 {m['name']}, {m['age']}{compat_badge}{unread_badge}",
                callback_data=f"ch:{m['user_id']}"
            )])
        b.append([InlineKeyboardButton(text="◀️ Назад", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def who_liked(users):
        b = []
        for u in users[:10]:
            sl = " ⭐" if u.get("is_super_like") else ""
            b.append([InlineKeyboardButton(
                text=f"❤️ {u['name']}, {u['age']}{sl}",
                callback_data=f"wl:{u['id']}"
            )])
        b.append([InlineKeyboardButton(text="◀️ Назад", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def who_liked_action(uid):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👍 Лайкнуть в ответ", callback_data=f"lk:{uid}"),
             InlineKeyboardButton(text="👎 Пропустить", callback_data=f"dl:{uid}")],
            [InlineKeyboardButton(text="◀️ К списку", callback_data="likes:list")]
        ])

    @staticmethod
    def back_matches():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Симпатии", callback_data="bm")]
        ])

    @staticmethod
    def chat_actions(match_id, partner_id):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Симпатии", callback_data="bm")],
            [InlineKeyboardButton(text="💔 Размэтчить", callback_data=f"um:{match_id}")]
        ])

    @staticmethod
    def shop():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 VIP-подписки", callback_data="sh:subs")],
            [InlineKeyboardButton(text="🚀 Буст анкеты", callback_data="sh:boost")],
            [InlineKeyboardButton(text="📊 Сравнить тарифы", callback_data="sh:compare")],
            [InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="sh:promo")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="mn")]
        ])

    @staticmethod
    def subs():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Винчик Light", callback_data="tf:vip_light")],
            [InlineKeyboardButton(text="🍾 Винчик Standard", callback_data="tf:vip_standard")],
            [InlineKeyboardButton(text="👑 Винчик Pro", callback_data="tf:vip_pro")],
            [InlineKeyboardButton(text="💎 Винчик Forever", callback_data="tf:vip_lifetime")],
            [InlineKeyboardButton(text="📊 Сравнить", callback_data="sh:compare")],
            [InlineKeyboardButton(text="◀️ Магазин", callback_data="sh:mn")]
        ])

    @staticmethod
    def buy_light():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 149₽/мес", callback_data="by:vip_light:30:14900")],
            [InlineKeyboardButton(text="💳 379₽/3мес (-15%)", callback_data="by:vip_light:90:37900")],
            [InlineKeyboardButton(text="💳 649₽/6мес (-27%)", callback_data="by:vip_light:180:64900")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:subs")]
        ])

    @staticmethod
    def buy_standard():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 349₽/мес", callback_data="by:vip_standard:30:34900")],
            [InlineKeyboardButton(text="💳 849₽/3мес (-19%)", callback_data="by:vip_standard:90:84900")],
            [InlineKeyboardButton(text="💳 1449₽/6мес (-31%)", callback_data="by:vip_standard:180:144900")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:subs")]
        ])

    @staticmethod
    def buy_pro():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 599₽/мес", callback_data="by:vip_pro:30:59900")],
            [InlineKeyboardButton(text="💳 1499₽/3мес (-17%)", callback_data="by:vip_pro:90:149900")],
            [InlineKeyboardButton(text="💳 2599₽/6мес (-28%)", callback_data="by:vip_pro:180:259900")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:subs")]
        ])

    @staticmethod
    def buy_lifetime():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 2999₽ навсегда", callback_data="by:vip_lifetime:0:299900")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:subs")]
        ])

    @staticmethod
    def boost_menu(has, active):
        b = []
        if has: b.append([InlineKeyboardButton(text="🚀 Активировать", callback_data="bo:act")])
        b += [
            [InlineKeyboardButton(text="💳 1×39₽", callback_data="by:boost:1:3900"),
             InlineKeyboardButton(text="💳 5×149₽", callback_data="by:boost:5:14900")],
            [InlineKeyboardButton(text="💳 10×249₽", callback_data="by:boost:10:24900")],
            [InlineKeyboardButton(text="◀️", callback_data="sh:mn")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def boost_from_profile(has, active):
        b = []
        if has: b.append([InlineKeyboardButton(text="🚀 Активировать", callback_data="bo:act:profile")])
        b += [
            [InlineKeyboardButton(text="💳 1×39₽", callback_data="by:boost:1:3900"),
             InlineKeyboardButton(text="💳 5×149₽", callback_data="by:boost:5:14900")],
            [InlineKeyboardButton(text="◀️ Профиль", callback_data="pv")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def pay(url, pid):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить", url=url)],
            [InlineKeyboardButton(text="✅ Проверить", callback_data=f"ck:{pid}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="sh:mn")]
        ])

    @staticmethod
    def profile(is_vip=False):
        b = [
            [InlineKeyboardButton(text="✏️ Редактировать", callback_data="pe"),
             InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")],
            [InlineKeyboardButton(text="🎯 Интересы", callback_data="ed:interests"),
             InlineKeyboardButton(text="🎂 Диапазон", callback_data="ed:agerange")],
            [InlineKeyboardButton(text="🚀 Буст", callback_data="profile:boost")],
        ]
        if is_vip:
            b.append([InlineKeyboardButton(text="❤️ Кто меня лайкнул", callback_data="likes:list")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def edit():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Имя", callback_data="ed:name"),
             InlineKeyboardButton(text="🎂 Возраст", callback_data="ed:age")],
            [InlineKeyboardButton(text="🌍 Город", callback_data="ed:city"),
             InlineKeyboardButton(text="✍️ О себе", callback_data="ed:bio")],
            [InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")],
            [InlineKeyboardButton(text="◀️ Профиль", callback_data="pv")]
        ])

    @staticmethod
    def report_reasons():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚨 Спам", callback_data="rr:spam"),
             InlineKeyboardButton(text="😱 Фейк", callback_data="rr:fake")],
            [InlineKeyboardButton(text="🔞 18+", callback_data="rr:nsfw"),
             InlineKeyboardButton(text="😠 Оскорбления", callback_data="rr:harass")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="mn")]
        ])

    @staticmethod
    def admin():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
            [InlineKeyboardButton(text="🔍 Найти", callback_data="adm:search"),
             InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="🚩 Жалобы", callback_data="adm:reports"),
             InlineKeyboardButton(text="💳 Платежи", callback_data="adm:payments")],
            [InlineKeyboardButton(text="🎁 Промокод", callback_data="adm:promo"),
             InlineKeyboardButton(text="⭐ Топ", callback_data="adm:top")],
            [InlineKeyboardButton(text="✖️ Закрыть", callback_data="mn")]
        ])

    @staticmethod
    def admin_user(uid, is_banned):
        ban_btn = InlineKeyboardButton(text="✅ Разбан", callback_data=f"au:unban:{uid}") if is_banned else InlineKeyboardButton(text="🚫 Бан", callback_data=f"au:ban:{uid}")
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✨ VIP", callback_data=f"au:givevip:{uid}"),
             InlineKeyboardButton(text="🚀 Бусты", callback_data=f"au:giveboost:{uid}")],
            [ban_btn, InlineKeyboardButton(text="✅ Верифик", callback_data=f"au:verify:{uid}")],
            [InlineKeyboardButton(text="🛡️ Админка", callback_data="adm:main")]
        ])

    @staticmethod
    def admin_report(rid, ruid):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Бан", callback_data=f"ar:ban:{rid}:{ruid}"),
             InlineKeyboardButton(text="⚠️ Варн", callback_data=f"ar:warn:{rid}:{ruid}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ar:dismiss:{rid}:{ruid}")],
            [InlineKeyboardButton(text="➡️ Далее", callback_data="adm:reports")]
        ])

    @staticmethod
    def broadcast_targets():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 Все", callback_data="bc:all"),
             InlineKeyboardButton(text="✅ С анкетой", callback_data="bc:complete")],
            [InlineKeyboardButton(text="✨ VIP", callback_data="bc:vip"),
             InlineKeyboardButton(text="💬 Free", callback_data="bc:free")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:main")]
        ])

    @staticmethod
    def broadcast_confirm():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Отправить", callback_data="bc:send")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:main")]
        ])

    @staticmethod
    def give_vip_tiers():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🥂 Light", callback_data="gv:vip_light"),
             InlineKeyboardButton(text="🍾 Standard", callback_data="gv:vip_standard")],
            [InlineKeyboardButton(text="👑 Pro", callback_data="gv:vip_pro"),
             InlineKeyboardButton(text="💎 Forever", callback_data="gv:vip_lifetime")],
            [InlineKeyboardButton(text="❌", callback_data="adm:main")]
        ])

    @staticmethod
    def back_admin():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛡️ Админка", callback_data="adm:main")]
        ])

# ═══════════════════════════════════════════════════════════════════════════════
# ANTI-SPAM
# ═══════════════════════════════════════════════════════════════════════════════

class AntiSpam:
    def __init__(self):
        self.users: Dict[str, List[float]] = {}
    async def check(self, user_id, action, limit=5, time_window=60) -> bool:
        now = time.time()
        key = f"{user_id}:{action}"
        if key not in self.users: self.users[key] = []
        self.users[key] = [t for t in self.users[key] if (now - t) < time_window]
        if len(self.users[key]) >= limit: return False
        self.users[key].append(now)
        return True

anti_spam = AntiSpam()

# ═══════════════════════════════════════════════════════════════════════════════
# PAYMENT SERVICE
# ═══════════════════════════════════════════════════════════════════════════════

class Pay:
    @staticmethod
    async def create(user, ptype, tier=None, dur=None, count=None, amount=0):
        if not YOOKASSA_AVAILABLE or not config.YOOKASSA_SHOP_ID:
            return {"error": "ЮKassa не настроена"}
        desc = f"Подписка {TIER_NAMES.get(tier, '')}" if ptype == "subscription" else f"Буст ({count}шт)"
        try:
            p = YooPayment.create({
                "amount": {"value": f"{amount/100:.2f}", "currency": "RUB"},
                "confirmation": {"type": ConfirmationType.REDIRECT, "return_url": f"{config.DOMAIN}/ok"},
                "capture": True, "description": desc,
                "metadata": {"user_id": user["id"], "type": ptype, "tier": tier, "dur": dur, "count": count},
            }, str(uuid.uuid4()))
            pid = await DB.create_payment(user["id"], p.id, amount, desc, ptype, tier, dur, count)
            return {"pid": pid, "url": p.confirmation.confirmation_url}
        except Exception as e:
            logger.error(f"Payment error: {e}")
            return {"error": str(e)}

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
        except Exception as e:
            logger.error(f"Payment check error: {e}")
            return {"status": "error"}

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

# ══════════════════════ HELPERS ══════════════════════

def build_profile_text(user: Dict) -> str:
    badge = DB.get_badge(user)
    role = DB.get_role_tag(user)
    sub = DB.get_vip_tier_name(user)
    if user.get("subscription_expires_at") and user["subscription_tier"] not in ("free", "vip_lifetime"):
        sub += f" (до {user['subscription_expires_at'].strftime('%d.%m.%Y')})"
    bi = ""
    if DB.is_boosted(user):
        bi += f"\n🚀 Буст до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if user.get("boost_count", 0) > 0:
        bi += f"\n📦 Бустов: {user['boost_count']}"
    interests = user.get("interests", "")
    int_text = f"\n🎯 {interests}" if interests else ""
    return (
        f"👤 *Мой профиль*\n\n"
        f"{badge}*{user['name']}*, {user['age']}{role}\n"
        f"🌍 {user['city']}\n"
        f"{user['bio'] or '_Нет описания_'}{int_text}\n\n"
        f"👁️ {user['views_count']} · ❤️ {user['likes_received_count']} · 💘 {user['matches_count']}\n"
        f"🔍 Возраст: {user['age_from']}-{user['age_to']}\n\n"
        f"Статус: {sub}{bi}"
    )

def build_card_text(p: Dict, viewer: Dict) -> str:
    badge = DB.get_badge(p)
    role = DB.get_role_tag(p)
    boost = " 🚀" if DB.is_boosted(p) else ""
    lm = {"male": "👨", "female": "👩", "both": "👫"}
    compat = Compatibility.calc_score(viewer, p)
    interests = p.get("interests", "")
    int_text = f"\n🎯 {interests}" if interests else ""
    priority = p.get("_priority", "")
    priority_badge = ""
    if priority == "liked_you":
        priority_badge = "\n❤️ _Этот человек лайкнул тебя!_\n" if DB.is_vip(viewer) else ""
    elif priority == "other_city":
        priority_badge = f"\n🌍 _Другой город_\n"

    return (
        f"{badge}*{p['name']}*{boost}, {p['age']}{role}\n"
        f"🌍 {p['city']}\n"
        f"{priority_badge}"
        f"{p['bio'] or '_Нет описания_'}{int_text}\n\n"
        f"🎯 Совместимость: *{compat:.0f}%*\n"
        f"🔍 Ищет: {lm.get(p.get('looking_for', 'both'), '👫')}"
    )

# ══════════════════════ START ══════════════════════

@rt.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if user and user.get("is_profile_complete"):
        un = await DB.get_unread(user["id"])
        st = DB.get_vip_tier_name(user)
        if DB.is_boosted(user): st += " · 🚀"
        st += DB.get_role_tag(user)
        who_liked = await DB.get_who_liked_me(user["id"])
        li = f"\n❤️ Тебя лайкнули: {len(who_liked)} чел." if who_liked else ""
        await message.answer(
            T.WELCOME_BACK.format(name=user["name"], status=st, views=user["views_count"],
                                  matches=user["matches_count"], msgs=un, likes_info=li),
            reply_markup=KB.main(un), parse_mode=ParseMode.MARKDOWN)
    else:
        if not user:
            await DB.create_user(message.from_user.id, message.from_user.username)
        await message.answer(T.WELCOME_NEW, parse_mode=ParseMode.MARKDOWN)
        await message.answer(T.ASK_NAME, reply_markup=ReplyKeyboardRemove())
        await state.set_state(RegStates.name)

# ══════════════════════ REGISTRATION ══════════════════════

@rt.message(RegStates.name)
async def reg_name(msg: Message, state: FSMContext):
    n = msg.text.strip()
    if len(n) < 2 or len(n) > 50: return await msg.answer(T.BAD_NAME)
    await state.update_data(name=n)
    await msg.answer(T.ASK_AGE, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.age)

@rt.message(RegStates.age)
async def reg_age(msg: Message, state: FSMContext):
    try:
        a = int(msg.text.strip())
        assert 18 <= a <= 99
    except: return await msg.answer(T.BAD_AGE)
    await state.update_data(age=a)
    await msg.answer(T.ASK_GENDER, reply_markup=KB.gender())
    await state.set_state(RegStates.gender)

@rt.callback_query(RegStates.gender, F.data.startswith("g:"))
async def reg_gender(cb: CallbackQuery, state: FSMContext):
    await state.update_data(gender=cb.data[2:])
    await cb.message.edit_text(T.ASK_CITY)
    await state.set_state(RegStates.city)
    await cb.answer()

@rt.message(RegStates.city)
async def reg_city(msg: Message, state: FSMContext):
    c = msg.text.strip().title()
    if len(c) < 2: return await msg.answer("🌍 Введи город!")
    await state.update_data(city=c)
    await msg.answer(T.ASK_PHOTO, reply_markup=KB.skip())
    await state.set_state(RegStates.photo)

@rt.message(RegStates.photo, F.photo)
async def reg_photo(msg: Message, state: FSMContext):
    await state.update_data(photo=msg.photo[-1].file_id)
    await msg.answer(T.ASK_BIO, reply_markup=KB.skip())
    await state.set_state(RegStates.bio)

@rt.callback_query(RegStates.photo, F.data == "skip")
async def reg_skip_photo(cb: CallbackQuery, state: FSMContext):
    await state.update_data(photo=None)
    await cb.message.edit_text(T.ASK_BIO)
    await state.set_state(RegStates.bio)
    await cb.answer()

@rt.message(RegStates.bio)
async def reg_bio(msg: Message, state: FSMContext):
    await state.update_data(bio=msg.text.strip()[:500])
    await msg.answer(T.ASK_INTERESTS, reply_markup=KB.interests())
    await state.update_data(selected_interests=set())
    await state.set_state(RegStates.interests)

@rt.callback_query(RegStates.bio, F.data == "skip")
async def reg_skip_bio(cb: CallbackQuery, state: FSMContext):
    await state.update_data(bio="")
    await cb.message.edit_text(T.ASK_INTERESTS, reply_markup=KB.interests())
    await state.update_data(selected_interests=set())
    await state.set_state(RegStates.interests)
    await cb.answer()

@rt.callback_query(RegStates.interests, F.data.startswith("int:"))
async def reg_interests(cb: CallbackQuery, state: FSMContext):
    val = cb.data[4:]
    if val == "done":
        d = await state.get_data()
        sel = d.get("selected_interests", set())
        await state.update_data(interests=",".join(sel))
        await cb.message.edit_text(T.ASK_LOOKING, reply_markup=KB.looking())
        await state.set_state(RegStates.looking_for)
    else:
        idx = int(val)
        d = await state.get_data()
        sel = d.get("selected_interests", set())
        item = Compatibility.INTERESTS_LIST[idx]
        if item in sel:
            sel.discard(item)
        else:
            sel.add(item)
        await state.update_data(selected_interests=sel)
        await cb.message.edit_reply_markup(reply_markup=KB.interests(sel))
    await cb.answer()

@rt.callback_query(RegStates.looking_for, F.data.startswith("l:"))
async def reg_looking(cb: CallbackQuery, state: FSMContext):
    await state.update_data(looking_for=cb.data[2:])
    await cb.message.edit_text(T.ASK_AGE_RANGE, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.age_range)
    await cb.answer()

@rt.message(RegStates.age_range)
async def reg_age_range(msg: Message, state: FSMContext):
    txt = msg.text.strip().replace(" ", "")
    try:
        parts = txt.split("-")
        af, at = int(parts[0]), int(parts[1])
        assert 18 <= af <= 99 and 18 <= at <= 99 and af <= at
    except:
        return await msg.answer("⚠️ Формат: `18-30`", parse_mode=ParseMode.MARKDOWN)

    d = await state.get_data()
    upd = {
        "name": d["name"], "age": d["age"], "gender": Gender(d["gender"]),
        "city": d["city"], "bio": d.get("bio", ""),
        "interests": d.get("interests", ""),
        "looking_for": LookingFor(d["looking_for"]),
        "age_from": af, "age_to": at,
        "is_profile_complete": True,
    }
    if d.get("photo"):
        upd["photos"] = d["photo"]
        upd["main_photo"] = d["photo"]
    await DB.update_user(msg.from_user.id, **upd)
    await state.clear()
    await msg.answer(T.REG_DONE, parse_mode=ParseMode.MARKDOWN)
    await msg.answer("👋 Выбери действие:", reply_markup=KB.main())

# ══════════════════════ BROWSE / SEARCH ══════════════════════

@rt.message(F.text == "📋 Анкеты")
async def browse(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        return await msg.answer(T.NO_PROFILE)
    await state.set_state(SearchStates.browsing)
    ps = await DB.search_profiles(user, 5)
    if not ps:
        return await msg.answer(T.NO_PROFILES, reply_markup=KB.no_profiles(DB.is_vip(user)), parse_mode=ParseMode.MARKDOWN)
    await state.update_data(search_queue=[p["id"] for p in ps[1:]])
    await show_card(msg, ps[0], user)

async def show_card(msg: Message, p: Dict, v: Dict):
    await DB.add_guest(v["id"], p["id"])
    txt = build_card_text(p, v)
    show_sl = v.get("daily_superlikes_remaining", 0) > 0
    kb = KB.search(p["id"], show_superlike=show_sl)
    if p.get("main_photo"):
        await msg.answer_photo(photo=p["main_photo"], caption=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

async def next_card(cb_or_msg, state: FSMContext, user: Dict):
    """Показать следующую анкету из очереди или загрузить новые"""
    d = await state.get_data()
    queue = d.get("search_queue", [])

    if queue:
        next_id = queue.pop(0)
        await state.update_data(search_queue=queue)
        p = await DB.get_user_by_id(next_id)
        if p and p.get("is_active") and not p.get("is_banned"):
            p["_priority"] = "normal"
            p["_compat"] = Compatibility.calc_score(user, p)
            msg = cb_or_msg.message if isinstance(cb_or_msg, CallbackQuery) else cb_or_msg
            await show_card(msg, p, user)
            return

    # Подгрузить новые
    ps = await DB.search_profiles(user, 5)
    if ps:
        await state.update_data(search_queue=[p["id"] for p in ps[1:]])
        msg = cb_or_msg.message if isinstance(cb_or_msg, CallbackQuery) else cb_or_msg
        await show_card(msg, ps[0], user)
    else:
        msg = cb_or_msg.message if isinstance(cb_or_msg, CallbackQuery) else cb_or_msg
        await msg.answer(T.NO_PROFILES, reply_markup=KB.no_profiles(DB.is_vip(user)), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("lk:"))
async def handle_like(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return await cb.answer("❌")
    if not DB.is_vip(user) and user.get("daily_likes_remaining", 0) <= 0:
        try: await cb.message.edit_caption(caption=T.LIKES_LIMIT, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)
        except: await cb.message.edit_text(T.LIKES_LIMIT, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)
        return
    if not await anti_spam.check(cb.from_user.id, "like"): return await cb.answer("⚠️ Не спешите!", show_alert=True)

    tid = int(cb.data[3:])
    result = await DB.add_like(user["id"], tid)
    if not DB.is_vip(user): await DB.dec_likes(user["telegram_id"])

    if result["is_match"]:
        t = await DB.get_user_by_id(tid)
        tn = t["name"] if t else "?"
        compat = result.get("compat", 0)
        try: await cb.message.edit_caption(caption=T.NEW_MATCH.format(name=tn, compat=compat), parse_mode=ParseMode.MARKDOWN)
        except: await cb.message.edit_text(T.NEW_MATCH.format(name=tn, compat=compat), parse_mode=ParseMode.MARKDOWN)
        if t:
            try: await cb.bot.send_message(t["telegram_id"], T.NEW_MATCH.format(name=user["name"], compat=compat), parse_mode=ParseMode.MARKDOWN)
            except: pass
    else:
        await cb.answer("👍")

    user = await DB.get_user(cb.from_user.id)
    await next_card(cb, state, user)
    await cb.answer()

@rt.callback_query(F.data.startswith("sl:"))
async def handle_superlike(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return await cb.answer("❌")
    if user.get("daily_superlikes_remaining", 0) <= 0:
        return await cb.answer("❌ Суперлайки закончились! VIP даёт больше ⭐", show_alert=True)
    if not await anti_spam.check(cb.from_user.id, "superlike", limit=3): return await cb.answer("⚠️ Не спешите!", show_alert=True)

    tid = int(cb.data[3:])
    result = await DB.add_like(user["id"], tid, is_super=True)
    await DB.dec_superlikes(user["telegram_id"])
    if not DB.is_vip(user): await DB.dec_likes(user["telegram_id"])

    t = await DB.get_user_by_id(tid)
    if result["is_match"]:
        tn = t["name"] if t else "?"
        compat = result.get("compat", 0)
        try: await cb.message.edit_caption(caption=T.NEW_MATCH.format(name=tn, compat=compat), parse_mode=ParseMode.MARKDOWN)
        except: await cb.message.edit_text(T.NEW_MATCH.format(name=tn, compat=compat), parse_mode=ParseMode.MARKDOWN)
        if t:
            try: await cb.bot.send_message(t["telegram_id"], T.NEW_MATCH.format(name=user["name"], compat=compat), parse_mode=ParseMode.MARKDOWN)
            except: pass
    else:
        await cb.answer("⭐ Суперлайк!")
        if t:
            try: await cb.bot.send_message(t["telegram_id"], T.SUPERLIKE_RECEIVED.format(name=user["name"]), parse_mode=ParseMode.MARKDOWN)
            except: pass

    user = await DB.get_user(cb.from_user.id)
    await next_card(cb, state, user)
    await cb.answer()

@rt.callback_query(F.data.startswith("dl:"))
async def handle_dislike(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    tid = int(cb.data[3:])
    await DB.add_dislike(user["id"], tid)
    await next_card(cb, state, user)
    await cb.answer()

@rt.callback_query(F.data == "sr:expand")
async def search_expand(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    await DB.update_user(user["telegram_id"], search_expand_city=True)
    user = await DB.get_user(cb.from_user.id)
    ps = await DB.search_profiles(user, 5)
    if ps:
        await state.update_data(search_queue=[p["id"] for p in ps[1:]])
        await state.set_state(SearchStates.browsing)
        await show_card(cb.message, ps[0], user)
    else:
        await cb.message.edit_text("😔 Увы, пока нет новых анкет. Зайди позже!")
    await cb.answer()

@rt.callback_query(F.data == "sr:reset")
async def search_reset(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    if not DB.is_vip(user):
        return await cb.answer("🥂 Только для VIP!", show_alert=True)
    count = await DB.reset_dislikes(user["id"])
    await cb.answer(f"🔄 Сброшено {count} пропущенных!", show_alert=True)
    user = await DB.get_user(cb.from_user.id)
    ps = await DB.search_profiles(user, 5)
    if ps:
        await state.update_data(search_queue=[p["id"] for p in ps[1:]])
        await state.set_state(SearchStates.browsing)
        await show_card(cb.message, ps[0], user)
    else:
        await cb.message.edit_text("😔 Всё ещё нет новых анкет.")

@rt.callback_query(F.data == "sr:retry")
async def search_retry(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    ps = await DB.search_profiles(user, 5)
    if ps:
        await state.update_data(search_queue=[p["id"] for p in ps[1:]])
        await state.set_state(SearchStates.browsing)
        await show_card(cb.message, ps[0], user)
    else:
        await cb.answer("😔 Пока нет новых анкет", show_alert=True)
    await cb.answer()

# ══════════════════════ WHO LIKED ME ══════════════════════

@rt.callback_query(F.data == "likes:list")
async def who_liked_list(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return
    if not DB.is_vip(user):
        who = await DB.get_who_liked_me(user["id"])
        return await cb.answer(f"❤️ Тебя лайкнули {len(who)} чел. Открой VIP чтобы увидеть!", show_alert=True)
    users = await DB.get_likes_received(user["id"], 20)
    if not users:
        return await cb.answer("😔 Пока никто не лайкнул", show_alert=True)
    await cb.message.edit_text(
        f"❤️ *Тебя лайкнули ({len(users)}):*",
        reply_markup=KB.who_liked(users), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("wl:"))
async def who_liked_view(cb: CallbackQuery, user: Optional[Dict]):
    if not user or not DB.is_vip(user): return
    uid = int(cb.data[3:])
    p = await DB.get_user_by_id(uid)
    if not p: return await cb.answer("Не найден")
    txt = build_card_text(p, user)
    try: await cb.message.edit_text(txt, reply_markup=KB.who_liked_action(uid), parse_mode=ParseMode.MARKDOWN)
    except: pass
    await cb.answer()

# ══════════════════════ MATCHES & CHAT ══════════════════════

@rt.message(F.text.startswith("❤️ Симпатии"))
async def show_matches(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return await msg.answer(T.NO_PROFILE)
    ms = await DB.get_matches(user["id"])
    if ms:
        await msg.answer(f"❤️ *Симпатии ({len(ms)}):*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer(T.NO_MATCHES)

@rt.callback_query(F.data.startswith("ch:"))
async def start_chat(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    pid = int(cb.data[3:])
    p = await DB.get_user_by_id(pid)
    if not p: return await cb.answer("Не найден")
    mid = await DB.get_match_between(user["id"], pid)
    if not mid: return await cb.answer("Нет мэтча")
    await DB.mark_read(mid, user["id"])
    msgs = await DB.get_msgs(mid, 5)
    txt = f"💬 *Чат с {p['name']}*\n\n"
    for mg in msgs:
        sn = "Вы" if mg["sender_id"] == user["id"] else p["name"]
        txt += f"*{sn}:* {mg['text']}\n"
    if not msgs: txt += "_Напиши первым!_"
    await state.update_data(cp=pid, mi=mid)
    await state.set_state(ChatStates.chatting)
    await cb.message.edit_text(txt, reply_markup=KB.chat_actions(mid, pid), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.message(ChatStates.chatting)
async def send_chat_msg(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d = await state.get_data()
    mid, pid = d.get("mi"), d.get("cp")
    if not mid:
        await state.clear()
        return await msg.answer("Чат закрыт", reply_markup=KB.main())
    await DB.send_msg(mid, user["id"], msg.text)
    p = await DB.get_user_by_id(pid)
    if p:
        try: await msg.bot.send_message(p["telegram_id"], f"💬 *{user['name']}:* {msg.text}", parse_mode=ParseMode.MARKDOWN)
        except: pass
    await msg.answer("✅")

@rt.callback_query(F.data.startswith("um:"))
async def unmatch_handler(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    mid = int(cb.data[3:])
    ok = await DB.unmatch(user["id"], mid)
    await state.clear()
    if ok:
        await cb.message.edit_text("💔 Мэтч удалён.")
    else:
        await cb.message.edit_text("❌ Ошибка")
    await cb.answer()

@rt.callback_query(F.data == "bm")
async def back_to_matches(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user: return
    ms = await DB.get_matches(user["id"])
    if ms:
        await cb.message.edit_text(f"❤️ *({len(ms)}):*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else:
        await cb.message.edit_text(T.NO_MATCHES)
    await cb.answer()

@rt.message(F.text.startswith("💬 Чат"))
async def show_chats(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return await msg.answer(T.NO_PROFILE)
    ms = await DB.get_matches(user["id"])
    if ms:
        await msg.answer("💬 *Диалоги:*", reply_markup=KB.matches(ms), parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer(T.NO_MSGS)

@rt.message(F.text == "👀 Гости")
async def show_guests(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return await msg.answer(T.NO_PROFILE)
    lim = 20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE
    gs = await DB.get_guests(user["id"], lim)
    if not gs: return await msg.answer(T.NO_GUESTS)
    txt = "👀 *Гости:*\n\n"
    for i, g in enumerate(gs, 1): txt += f"{i}. {g['name']}, {g['age']} — {g['city']}\n"
    if not DB.is_vip(user): txt += "\n_🥂 VIP — все гости!_"
    await msg.answer(txt, parse_mode=ParseMode.MARKDOWN)

# ══════════════════════ PROFILE ══════════════════════

@rt.message(F.text == "👤 Профиль")
async def show_profile(msg: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"): return await msg.answer(T.NO_PROFILE)
    txt = build_profile_text(user)
    kb = KB.profile(is_vip=DB.is_vip(user))
    if user.get("main_photo"):
        await msg.answer_photo(photo=user["main_photo"], caption=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data == "pe")
async def profile_edit_menu(cb: CallbackQuery):
    try: await cb.message.edit_caption(caption="✏️ *Редактировать:*", reply_markup=KB.edit(), parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text("✏️ *Редактировать:*", reply_markup=KB.edit(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "ed:name")
async def edit_name(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("👤 Новое имя:")
    await state.set_state(EditStates.edit_name)
    await cb.answer()

@rt.message(EditStates.edit_name)
async def save_name(msg: Message, state: FSMContext):
    n = msg.text.strip()
    if len(n) < 2 or len(n) > 50: return await msg.answer(T.BAD_NAME)
    await DB.update_user(msg.from_user.id, name=n)
    await state.clear()
    await msg.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:age")
async def edit_age(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("🎂 Возраст:")
    await state.set_state(EditStates.edit_age)
    await cb.answer()

@rt.message(EditStates.edit_age)
async def save_age(msg: Message, state: FSMContext):
    try:
        a = int(msg.text.strip())
        assert 18 <= a <= 99
    except: return await msg.answer(T.BAD_AGE)
    await DB.update_user(msg.from_user.id, age=a)
    await state.clear()
    await msg.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:city")
async def edit_city(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("🌍 Город:")
    await state.set_state(EditStates.edit_city)
    await cb.answer()

@rt.message(EditStates.edit_city)
async def save_city(msg: Message, state: FSMContext):
    await DB.update_user(msg.from_user.id, city=msg.text.strip().title())
    await state.clear()
    await msg.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:bio")
async def edit_bio(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("✍️ О себе:")
    await state.set_state(EditStates.edit_bio)
    await cb.answer()

@rt.message(EditStates.edit_bio)
async def save_bio(msg: Message, state: FSMContext):
    await DB.update_user(msg.from_user.id, bio=msg.text.strip()[:500])
    await state.clear()
    await msg.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:interests")
async def edit_interests(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    current = set(i.strip() for i in (user.get("interests") or "").split(",") if i.strip())
    await state.update_data(selected_interests=current, editing_interests=True)
    await cb.message.answer(T.ASK_INTERESTS, reply_markup=KB.interests(current), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(EditStates.edit_interests)
    await cb.answer()

@rt.callback_query(EditStates.edit_interests, F.data.startswith("int:"))
async def save_interests(cb: CallbackQuery, state: FSMContext):
    val = cb.data[4:]
    if val == "done":
        d = await state.get_data()
        sel = d.get("selected_interests", set())
        await DB.update_user(cb.from_user.id, interests=",".join(sel))
        await state.clear()
        await cb.message.edit_text("✅ Интересы обновлены!")
        await cb.message.answer("👋", reply_markup=KB.main())
    else:
        idx = int(val)
        d = await state.get_data()
        sel = d.get("selected_interests", set())
        item = Compatibility.INTERESTS_LIST[idx]
        if item in sel: sel.discard(item)
        else: sel.add(item)
        await state.update_data(selected_interests=sel)
        await cb.message.edit_reply_markup(reply_markup=KB.interests(sel))
    await cb.answer()

@rt.callback_query(F.data == "ed:agerange")
async def edit_age_range(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    await cb.message.answer(f"🎯 Текущий: {user['age_from']}-{user['age_to']}\n\nВведи новый через дефис: `18-30`", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(EditStates.edit_age_range)
    await cb.answer()

@rt.message(EditStates.edit_age_range)
async def save_age_range(msg: Message, state: FSMContext):
    txt = msg.text.strip().replace(" ", "")
    try:
        parts = txt.split("-")
        af, at = int(parts[0]), int(parts[1])
        assert 18 <= af <= 99 and 18 <= at <= 99 and af <= at
    except: return await msg.answer("⚠️ Формат: `18-30`", parse_mode=ParseMode.MARKDOWN)
    await DB.update_user(msg.from_user.id, age_from=af, age_to=at)
    await state.clear()
    await msg.answer(f"✅ Диапазон: {af}-{at}", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:photo")
async def edit_photo(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("📸 Фото:")
    await state.set_state(EditStates.add_photo)
    await cb.answer()

@rt.message(EditStates.add_photo, F.photo)
async def save_photo(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user: return
    pid = msg.photo[-1].file_id
    photos_list = [p for p in (user.get("photos", "") or "").split(",") if p.strip()]
    if len(photos_list) >= 5:
        await state.clear()
        return await msg.answer("⚠️ Макс 5 фото!", reply_markup=KB.main())
    photos_list.append(pid)
    await DB.update_user(msg.from_user.id, photos=",".join(photos_list), main_photo=pid)
    await state.clear()
    await msg.answer("✅ Фото добавлено!", reply_markup=KB.main())

@rt.callback_query(F.data == "pv")
async def back_to_profile(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return
    user = await DB.get_user(cb.from_user.id)
    txt = build_profile_text(user)
    kb = KB.profile(is_vip=DB.is_vip(user))
    try: await cb.message.edit_caption(caption=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

# ══════════════════════ BOOST ══════════════════════

@rt.callback_query(F.data == "profile:boost")
async def profile_boost(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return await cb.answer("❌")
    has = user.get("boost_count", 0) > 0
    act = DB.is_boosted(user)
    st = ""
    if act: st += f"\n\n🚀 Буст до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if has: st += f"\n📦 Бустов: {user['boost_count']}"
    if not has and not act: st = "\n\n❌ Нет бустов"
    txt = T.BOOST_INFO.format(bot_name=BOT_NAME, status=st)
    try: await cb.message.edit_caption(caption=txt, reply_markup=KB.boost_from_profile(has, act), parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text(txt, reply_markup=KB.boost_from_profile(has, act), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("bo:act"))
async def activate_boost(cb: CallbackQuery, user: Optional[Dict]):
    if not user or user.get("boost_count", 0) <= 0:
        return await cb.answer("❌ Нет бустов!", show_alert=True)
    ok = await DB.use_boost(user["id"])
    if ok:
        u = await DB.get_user(cb.from_user.id)
        back_cb = "pv" if ":profile" in cb.data else "sh:mn"
        back_text = "◀️ Профиль" if ":profile" in cb.data else "◀️ Магазин"
        try: await cb.message.edit_caption(
            caption=f"🚀 *Буст активирован!*\nДо {u['boost_expires_at'].strftime('%d.%m %H:%M')}\n📦 Осталось: {u['boost_count']}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=back_text, callback_data=back_cb)]]),
            parse_mode=ParseMode.MARKDOWN)
        except: await cb.message.edit_text(
            f"🚀 *Буст активирован!*\nДо {u['boost_expires_at'].strftime('%d.%m %H:%M')}\n📦 Осталось: {u['boost_count']}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=back_text, callback_data=back_cb)]]),
            parse_mode=ParseMode.MARKDOWN)
    else:
        await cb.answer("❌ Ошибка", show_alert=True)
    await cb.answer()

# ══════════════════════ SHOP ══════════════════════

@rt.message(F.text == "🛍️ Магазин")
async def shop_menu(msg: Message):
    await msg.answer(T.SHOP, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data == "sh:mn")
async def shop_main(cb: CallbackQuery):
    await cb.message.edit_text(T.SHOP, reply_markup=KB.shop(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "sh:compare")
async def shop_compare(cb: CallbackQuery):
    await cb.message.edit_text(T.COMPARE, reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "sh:subs")
async def shop_subs(cb: CallbackQuery):
    await cb.message.edit_text("🥂 *Выбери тариф:*", reply_markup=KB.subs(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "tf:vip_light")
async def tf_light(cb: CallbackQuery):
    await cb.message.edit_text(T.LIGHT, reply_markup=KB.buy_light(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data == "tf:vip_standard")
async def tf_standard(cb: CallbackQuery):
    await cb.message.edit_text(T.STANDARD, reply_markup=KB.buy_standard(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data == "tf:vip_pro")
async def tf_pro(cb: CallbackQuery):
    await cb.message.edit_text(T.PRO, reply_markup=KB.buy_pro(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data == "tf:vip_lifetime")
async def tf_lifetime(cb: CallbackQuery):
    await cb.message.edit_text(T.LIFETIME, reply_markup=KB.buy_lifetime(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data == "sh:boost")
async def shop_boost(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return await cb.answer("❌")
    has = user.get("boost_count", 0) > 0; act = DB.is_boosted(user)
    st = ""
    if act: st += f"\n\n🚀 До {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if has: st += f"\n📦 {user['boost_count']}"
    if not has and not act: st = "\n\n❌ Нет бустов"
    await cb.message.edit_text(T.BOOST_INFO.format(bot_name=BOT_NAME, status=st), reply_markup=KB.boost_menu(has, act), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("by:"))
async def handle_buy(cb: CallbackQuery, user: Optional[Dict]):
    if not user: return await cb.answer("❌")
    parts = cb.data.split(":"); prod, param, amt = parts[1], int(parts[2]), int(parts[3])
    res = await Pay.create(user, "boost", count=param, amount=amt) if prod == "boost" else await Pay.create(user, "subscription", tier=prod, dur=param, amount=amt)
    if "error" in res: return await cb.answer(f"❌ {res['error']}", show_alert=True)
    await cb.message.edit_text(f"💳 *Покупка · {amt/100:.0f}₽*\n\n1️⃣ Оплати → 2️⃣ Проверь", reply_markup=KB.pay(res["url"], res["pid"]), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("ck:"))
async def check_payment(cb: CallbackQuery):
    pid = int(cb.data[3:]); res = await Pay.check(pid)
    if res["status"] == "succeeded":
        txt = f"✅ *{res.get('count',1)} бустов!*" if res.get("type") == "boost" else "✅ *Подписка активирована!* 🍷"
        await cb.message.edit_text(txt, parse_mode=ParseMode.MARKDOWN)
        await cb.message.answer("👋", reply_markup=KB.main())
    elif res["status"] == "pending": await cb.answer("⏳ Обрабатывается...", show_alert=True)
    else: await cb.answer("❌ Не найдено", show_alert=True)
    await cb.answer()

# ══════════════════════ PROMO ══════════════════════

@rt.callback_query(F.data == "sh:promo")
async def promo_input(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_text("🎁 *Введи промокод:*", parse_mode=ParseMode.MARKDOWN)
    await state.update_data(promo_user_mode=True)
    await state.set_state(AdminStates.promo_code)
    await cb.answer()

@rt.message(AdminStates.promo_code)
async def promo_code_input(msg: Message, state: FSMContext, user: Optional[Dict]):
    d = await state.get_data()
    if d.get("promo_user_mode"):
        code = msg.text.strip().upper(); await state.clear()
        if not user: return await msg.answer("❌")
        result = await DB.use_promo(user["id"], code)
        if "error" in result: await msg.answer(f"❌ {result['error']}", reply_markup=KB.main())
        else: await msg.answer(f"✅ *Промокод активирован!*\n{TIER_NAMES.get(result['tier'],'VIP')} на {result['days']}дн!", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)
        return
    if not user or not DB.is_admin(user): return
    await state.update_data(pc_code=msg.text.strip().upper())
    await msg.answer("✨ *Тариф:*", reply_markup=KB.give_vip_tiers(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.promo_tier)

@rt.callback_query(AdminStates.promo_tier, F.data.startswith("gv:"))
async def promo_tier(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user or not DB.is_admin(user): return
    await state.update_data(pc_tier=cb.data[3:])
    await cb.message.edit_text("⏰ *Дней?*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.promo_duration); await cb.answer()

@rt.message(AdminStates.promo_duration)
async def promo_dur(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not DB.is_admin(user): return
    try: days = int(msg.text.strip())
    except: return await msg.answer("⚠️ Число!")
    await state.update_data(pc_days=days)
    await msg.answer("🔢 *Лимит?*"); await state.set_state(AdminStates.promo_uses)

@rt.message(AdminStates.promo_uses)
async def promo_uses(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not user or not DB.is_admin(user): return
    try: uses = int(msg.text.strip())
    except: return await msg.answer("⚠️ Число!")
    d = await state.get_data()
    await DB.create_promo(d["pc_code"], d["pc_tier"], d["pc_days"], uses); await state.clear()
    await msg.answer(f"✅ `{d['pc_code']}` · {TIER_NAMES.get(d['pc_tier'],'VIP')} · {d['pc_days']}дн · Лимит:{uses}", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

# ══════════════════════ FAQ & REPORTS & MENU ══════════════════════

@rt.message(F.text == "❓ FAQ")
async def show_faq(msg: Message):
    await msg.answer(T.FAQ, parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("rp:"))
async def start_report(cb: CallbackQuery, state: FSMContext):
    await state.update_data(rp_id=int(cb.data[3:]))
    try: await cb.message.edit_caption(caption="🚩 *Причина:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    except: await cb.message.edit_text("🚩 *Причина:*", reply_markup=KB.report_reasons(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("rr:"))
async def save_report(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user: return
    d = await state.get_data(); rid = d.get("rp_id")
    if rid: await DB.create_report(user["id"], rid, cb.data[3:])
    await state.clear()
    try: await cb.message.edit_caption(caption="✅ Жалоба отправлена!")
    except: await cb.message.edit_text("✅ Жалоба отправлена!")
    user = await DB.get_user(cb.from_user.id)
    await next_card(cb, state, user)
    await cb.answer()

@rt.callback_query(F.data == "mn")
async def back_to_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    try: await cb.message.delete()
    except: pass
    await cb.message.answer("👋", reply_markup=KB.main()); await cb.answer()

# ══════════════════════ ADMIN ══════════════════════

def is_adm(user): return user and user.get("telegram_id") in config.ADMIN_IDS

@rt.message(Command("admin"))
async def admin_cmd(msg: Message, user: Optional[Dict]):
    if not is_adm(user): return
    role = "👑 Создатель" if DB.is_creator(user) else "🛡️ Админ"
    await msg.answer(T.ADMIN_MAIN.format(bot_name=BOT_NAME, admin_name=user["name"] or "Admin", role=role), reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data == "adm:main")
async def admin_main(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return; await state.clear()
    role = "👑 Создатель" if DB.is_creator(user) else "🛡️ Админ"
    await cb.message.edit_text(T.ADMIN_MAIN.format(bot_name=BOT_NAME, admin_name=user["name"] or "Admin", role=role), reply_markup=KB.admin(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data == "adm:stats")
async def admin_stats(cb: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    s = await DB.get_stats()
    await cb.message.edit_text(T.ADMIN_STATS.format(bot_name=BOT_NAME, **s), reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data == "adm:search")
async def admin_search(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await cb.message.edit_text("🔍 *Запрос:*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.search_user); await cb.answer()

@rt.message(AdminStates.search_user)
async def admin_search_result(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    results = await DB.search_users(msg.text.strip()); await state.clear()
    if not results: return await msg.answer("😔 Не найдено", reply_markup=KB.back_admin())
    u = results[0]; badge = DB.get_badge(u)
    txt = T.ADMIN_USER_CARD.format(id=u["id"], telegram_id=u["telegram_id"], username=u.get("username") or "-", badge=badge, name=u["name"] or "-", age=u["age"] or "-", city=u["city"] or "-", bio=u["bio"] or "-", tier=TIER_NAMES.get(u["subscription_tier"],""), views=u["views_count"], likes=u["likes_received_count"], matches=u["matches_count"], boosts=u.get("boost_count",0), created=u["created_at"].strftime("%d.%m.%Y") if u["created_at"] else "-", banned="Да🚫" if u["is_banned"] else "Нет")
    await msg.answer(txt, reply_markup=KB.admin_user(u["id"], u["is_banned"]), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("au:ban:"))
async def admin_ban(cb: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    uid = int(cb.data.split(":")[2]); u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], is_banned=True)
        await cb.message.edit_text(f"🚫 *{u['name']}* забанен!", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
        try: await cb.bot.send_message(u["telegram_id"], "🚫 Аккаунт заблокирован.")
        except: pass
    await cb.answer()

@rt.callback_query(F.data.startswith("au:unban:"))
async def admin_unban(cb: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    uid = int(cb.data.split(":")[2]); u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], is_banned=False)
        await cb.message.edit_text(f"✅ *{u['name']}* разбанен!", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("au:verify:"))
async def admin_verify(cb: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    uid = int(cb.data.split(":")[2]); u = await DB.get_user_by_id(uid)
    if u:
        await DB.update_user(u["telegram_id"], is_verified=True)
        await cb.message.edit_text(f"✅ *{u['name']}* верифицирован!", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data.startswith("au:givevip:"))
async def admin_give_vip(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    uid = int(cb.data.split(":")[2]); await state.update_data(target_uid=uid)
    await cb.message.edit_text("✨ *Тариф:*", reply_markup=KB.give_vip_tiers(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data.startswith("gv:"))
async def admin_gv_tier(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    tier = cb.data[3:]
    if tier == "vip_lifetime":
        d = await state.get_data()
        if d.get("target_uid"): await DB.activate_subscription_by_id(d["target_uid"], tier, 0)
        await state.clear()
        await cb.message.edit_text("✅ *Forever выдан!*💎", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN)
    else:
        await state.update_data(give_tier=tier)
        await cb.message.edit_text("⏰ *Дней?*", parse_mode=ParseMode.MARKDOWN)
        await state.set_state(AdminStates.give_vip_duration)
    await cb.answer()

@rt.message(AdminStates.give_vip_duration)
async def admin_gv_days(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    try: days = int(msg.text.strip())
    except: return await msg.answer("⚠️ Число!")
    d = await state.get_data()
    await DB.activate_subscription_by_id(d["target_uid"], d["give_tier"], days); await state.clear()
    await msg.answer(f"✅ {TIER_NAMES.get(d['give_tier'],'VIP')} на {days}дн!", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("au:giveboost:"))
async def admin_give_boost(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    uid = int(cb.data.split(":")[2]); await state.update_data(target_uid=uid)
    await cb.message.edit_text("🚀 *Сколько?*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.give_boost_count); await cb.answer()

@rt.message(AdminStates.give_boost_count)
async def admin_gb_count(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    try: count = int(msg.text.strip())
    except: return await msg.answer("⚠️ Число!")
    d = await state.get_data()
    await DB.add_boosts(d["target_uid"], count); await state.clear()
    await msg.answer(f"✅ {count} бустов!", reply_markup=KB.main(), parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data == "adm:reports")
async def admin_reports(cb: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    reps = await DB.get_pending_reports(5)
    if not reps:
        await cb.message.edit_text("✅ *Нет жалоб!*", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN); await cb.answer(); return
    rep = reps[0]; rn = rep["reporter"]["name"] if rep["reporter"] else "?"; rdn = rep["reported"]["name"] if rep["reported"] else "?"; rid = rep["reported"]["id"] if rep["reported"] else 0
    await cb.message.edit_text(f"🚩 *#{rep['id']}*\nНа: *{rdn}*(ID:{rid})\nОт: *{rn}*\nПричина: *{rep['reason']}*\nВсего: {len(reps)}", reply_markup=KB.admin_report(rep["id"], rid), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data.startswith("ar:"))
async def admin_report_action(cb: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    parts = cb.data.split(":"); action, rid, ruid = parts[1], int(parts[2]), int(parts[3])
    if action == "ban":
        u = await DB.get_user_by_id(ruid)
        if u: await DB.update_user(u["telegram_id"], is_banned=True)
        await DB.resolve_report(rid, "banned"); await cb.message.edit_text("🚫 Забанен", reply_markup=KB.back_admin())
    elif action == "warn":
        u = await DB.get_user_by_id(ruid)
        if u:
            try: await cb.bot.send_message(u["telegram_id"], "⚠️ Предупреждение от модерации!")
            except: pass
        await DB.resolve_report(rid, "warned"); await cb.message.edit_text("⚠️ Предупреждён", reply_markup=KB.back_admin())
    elif action == "dismiss":
        await DB.resolve_report(rid, "dismissed"); await cb.message.edit_text("❌ Отклонено", reply_markup=KB.back_admin())
    await cb.answer()

@rt.callback_query(F.data == "adm:payments")
async def admin_payments(cb: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    pays = await DB.get_recent_payments(10)
    if not pays: await cb.message.edit_text("💬 *Нет*", reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN); await cb.answer(); return
    txt = "💳 *Платежи:*\n\n"
    for p in pays:
        st = {"pending":"⏳","succeeded":"✅","canceled":"❌"}.get(p["status"],"?")
        txt += f"{st} {p['amount']:.0f}₽ · {p['user_name']}\n"
    await cb.message.edit_text(txt, reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data == "adm:top")
async def admin_top(cb: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user): return
    async with async_session_maker() as s:
        tl = await s.execute(select(User).where(User.is_profile_complete == True).order_by(desc(User.likes_received_count)).limit(5))
        txt = "❤️ *Топ лайков:*\n"
        for i, u in enumerate(tl.scalars().all(), 1): txt += f"{i}. {u.name} — {u.likes_received_count}\n"
    await cb.message.edit_text(txt, reply_markup=KB.back_admin(), parse_mode=ParseMode.MARKDOWN); await cb.answer()

@rt.callback_query(F.data == "adm:broadcast")
async def admin_broadcast(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await cb.message.edit_text("📢 *Текст:*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.broadcast_text); await cb.answer()

@rt.message(AdminStates.broadcast_text)
async def admin_bc_text(msg: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await state.update_data(bc_text=msg.text)
    await msg.answer("👥 *Аудитория:*", reply_markup=KB.broadcast_targets(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.broadcast_confirm)

@rt.callback_query(AdminStates.broadcast_confirm, F.data.startswith("bc:"))
async def admin_bc_target(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    target = cb.data[3:]
    if target == "send":
        d = await state.get_data(); txt = d["bc_text"]; tgt = d.get("bc_target","all")
        ids = await DB.get_all_user_ids(tgt); await state.clear()
        await cb.message.edit_text(f"📢 *Отправка {len(ids)}...*", parse_mode=ParseMode.MARKDOWN)
        sent = failed = 0
        for tid in ids:
            try: await cb.bot.send_message(tid, txt, parse_mode=ParseMode.MARKDOWN); sent += 1
            except: failed += 1
            if sent % 25 == 0: await asyncio.sleep(1)
        await DB.log_broadcast(user["telegram_id"], txt, tgt, sent, failed)
        await cb.message.answer(f"✅ Отправлено: {sent} · Ошибок: {failed}", reply_markup=KB.back_admin())
    else:
        await state.update_data(bc_target=target); d = await state.get_data()
        names = {"all":"Все","complete":"С анкетой","vip":"VIP","free":"Free"}
        ids = await DB.get_all_user_ids(target)
        await cb.message.edit_text(T.BROADCAST_CONFIRM.format(text=d["bc_text"][:200], target=names.get(target,target), count=len(ids)), reply_markup=KB.broadcast_confirm(), parse_mode=ParseMode.MARKDOWN)
    await cb.answer()

@rt.callback_query(F.data == "adm:promo")
async def admin_promo_start(cb: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user): return
    await cb.message.edit_text("🎁 *Код:*", parse_mode=ParseMode.MARKDOWN)
    await state.update_data(promo_user_mode=False); await state.set_state(AdminStates.promo_code); await cb.answer()

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
    logger.info(f"🚀 {BOT_NAME} запущен")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
