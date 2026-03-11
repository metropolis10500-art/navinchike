"""
ЗНАКОМСТВА НА ВИНЧИКЕ — Telegram Dating Bot v3.3 (FULLY FIXED)
https://github.com/yourname/dating-bot

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
    Text, ForeignKey, Enum as SQLEnum, select, update, func, and_, or_, desc
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

BOT_NAME = "♥️ Знакомства на Винчике"

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
    referral_code = Column(String(20), unique=True, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    last_active_at = Column(DateTime, default=datetime.utcnow)

class Like(Base):
    __tablename__ = "likes"

    id = Column(Integer, primary_key=True)
    from_user_id = Column(Integer, ForeignKey("users.id"))
    to_user_id = Column(Integer, ForeignKey("users.id"))
    is_super_like = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True)
    user1_id = Column(Integer, ForeignKey("users.id"))
    user2_id = Column(Integer, ForeignKey("users.id"))
    is_active = Column(Boolean, default=True)
    last_message_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class ChatMessage(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey("matches.id"))
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
async_session_maker = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

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
    looking_for = State()

class EditStates(StatesGroup):
    edit_name = State()
    edit_age = State()
    edit_city = State()
    edit_bio = State()
    add_photo = State()

class ChatStates(StatesGroup):
    chatting = State()

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
            "last_limits_reset": u.last_limits_reset,
            "boost_expires_at": u.boost_expires_at,
            "boost_count": u.boost_count or 0,
            "views_count": u.views_count or 0,
            "likes_received_count": u.likes_received_count or 0,
            "matches_count": u.matches_count or 0,
            "created_at": u.created_at, "last_active_at": u.last_active_at,
        }

    @staticmethod
    def is_vip(u: Dict) -> bool:
        t = u.get("subscription_tier", "free")
        if t == "vip_lifetime":
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
            await s.execute(
                update(User).where(User.telegram_id == tg_id).values(**kw)
            )
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
    async def search_profiles(u: Dict, limit=1) -> List[Dict]:
        async with async_session_maker() as s:
            liked = await s.execute(
                select(Like.to_user_id).where(Like.from_user_id == u["id"])
            )
            exc = [r[0] for r in liked.fetchall()] + [u["id"]]

            q = select(User).where(
                and_(
                    User.is_active == True,
                    User.is_banned == False,
                    User.is_profile_complete == True,
                    User.id.not_in(exc),
                    User.city == u["city"],
                    User.age >= u["age_from"],
                    User.age <= u["age_to"]
                )
            )

            lf = u.get("looking_for", "both")
            if lf == "male":
                q = q.where(User.gender == Gender.MALE)
            elif lf == "female":
                q = q.where(User.gender == Gender.FEMALE)

            q = q.order_by(
                User.boost_expires_at.desc().nullslast(),
                User.last_active_at.desc()
            ).limit(limit)

            r = await s.execute(q)
            return [DB._to_dict(x) for x in r.scalars().all()]

    @staticmethod
    async def add_like(fd: int, tid: int) -> bool:
        """Returns True if match was created"""
        async with async_session_maker() as s:
            ex = await s.execute(
                select(Like).where(
                    and_(Like.from_user_id == fd, Like.to_user_id == tid)
                )
            )
            if ex.scalar_one_or_none():
                return False

            s.add(Like(from_user_id=fd, to_user_id=tid))
            await s.execute(
                update(User).where(User.id == tid).values(
                    likes_received_count=User.likes_received_count + 1
                )
            )

            rev = await s.execute(
                select(Like).where(
                    and_(Like.from_user_id == tid, Like.to_user_id == fd)
                )
            )
            is_match = rev.scalar_one_or_none() is not None

            if is_match:
                existing = await s.execute(
                    select(Match).where(
                        or_(
                            and_(Match.user1_id == fd, Match.user2_id == tid),
                            and_(Match.user1_id == tid, Match.user2_id == fd)
                        )
                    )
                )
                if not existing.scalar_one_or_none():
                    s.add(Match(user1_id=min(fd, tid), user2_id=max(fd, tid)))
                    await s.execute(
                        update(User).where(User.id.in_([fd, tid])).values(
                            matches_count=User.matches_count + 1
                        )
                    )

            await s.commit()
            return is_match

    @staticmethod
    async def get_matches(uid: int) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(Match).where(
                    and_(
                        or_(Match.user1_id == uid, Match.user2_id == uid),
                        Match.is_active == True
                    )
                ).order_by(Match.last_message_at.desc().nullslast())
            )
            out = []
            for m in r.scalars().all():
                pid = m.user2_id if m.user1_id == uid else m.user1_id
                pr = await s.execute(select(User).where(User.id == pid))
                p = pr.scalar_one_or_none()
                if p:
                    out.append({
                        "match_id": m.id,
                        "user_id": p.id,
                        "telegram_id": p.telegram_id,
                        "name": p.name,
                        "age": p.age,
                        "photo": p.main_photo
                    })
            return out

    @staticmethod
    async def get_match_between(u1: int, u2: int) -> Optional[int]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(Match.id).where(
                    or_(
                        and_(Match.user1_id == u1, Match.user2_id == u2),
                        and_(Match.user1_id == u2, Match.user2_id == u1)
                    )
                )
            )
            row = r.first()
            return row[0] if row else None

    @staticmethod
    async def send_msg(mid: int, sid: int, txt: str):
        async with async_session_maker() as s:
            s.add(ChatMessage(match_id=mid, sender_id=sid, text=txt))
            await s.execute(
                update(Match).where(Match.id == mid).values(
                    last_message_at=datetime.utcnow()
                )
            )
            await s.commit()

    @staticmethod
    async def get_msgs(mid: int, limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(ChatMessage).where(ChatMessage.match_id == mid)
                .order_by(ChatMessage.created_at.desc()).limit(limit)
            )
            return [
                {
                    "sender_id": m.sender_id,
                    "text": m.text,
                    "created_at": m.created_at
                }
                for m in reversed(r.scalars().all())
            ]

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
                select(func.count(ChatMessage.id)).where(
                    and_(
                        ChatMessage.match_id.in_(mids),
                        ChatMessage.sender_id != uid,
                        ChatMessage.is_read == False
                    )
                )
            )
            return r.scalar() or 0

    @staticmethod
    async def add_guest(vid: int, uid: int):
        async with async_session_maker() as s:
            s.add(GuestVisit(visitor_id=vid, visited_user_id=uid))
            await s.execute(
                update(User).where(User.id == uid).values(
                    views_count=User.views_count + 1
                )
            )
            await s.commit()

    @staticmethod
    async def get_guests(uid: int, limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(GuestVisit.visitor_id)
                .where(GuestVisit.visited_user_id == uid)
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
    async def dec_likes(tg_id: int):
        u = await DB.get_user(tg_id)
        if u and DB.is_vip(u):
            return
        async with async_session_maker() as s:
            await s.execute(
                update(User).where(User.telegram_id == tg_id).values(
                    daily_likes_remaining=User.daily_likes_remaining - 1
                )
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
            ne = (u.boost_expires_at + timedelta(hours=24)) if u.boost_expires_at and u.boost_expires_at > now else now + timedelta(hours=24)
            await s.execute(
                update(User).where(User.id == uid).values(
                    boost_count=User.boost_count - 1,
                    boost_expires_at=ne
                )
            )
            await s.commit()
            return True

    @staticmethod
    async def add_boosts(uid: int, c: int):
        async with async_session_maker() as s:
            await s.execute(
                update(User).where(User.id == uid).values(
                    boost_count=User.boost_count + c
                )
            )
            await s.commit()

    @staticmethod
    async def create_report(rid: int, ruid: int, reason: str):
        async with async_session_maker() as s:
            s.add(Report(reporter_id=rid, reported_user_id=ruid, reason=reason))
            await s.commit()

    # ADMIN DB METHODS
    @staticmethod
    async def get_stats() -> Dict:
        async with async_session_maker() as s:
            total = (await s.execute(select(func.count(User.id)))).scalar() or 0
            complete = (
                await s.execute(
                    select(func.count(User.id)).where(User.is_profile_complete == True)
                )
            ).scalar() or 0
            now = datetime.utcnow()
            day_ago = now - timedelta(days=1)
            week_ago = now - timedelta(days=7)
            month_ago = now - timedelta(days=30)
            dau = (
                await s.execute(
                    select(func.count(User.id)).where(User.last_active_at > day_ago)
                )
            ).scalar() or 0
            wau = (
                await s.execute(
                    select(func.count(User.id)).where(User.last_active_at > week_ago)
                )
            ).scalar() or 0
            mau = (
                await s.execute(
                    select(func.count(User.id)).where(User.last_active_at > month_ago)
                )
            ).scalar() or 0
            vip = (
                await s.execute(
                    select(func.count(User.id)).where(
                        User.subscription_tier != SubscriptionTier.FREE
                    )
                )
            ).scalar() or 0
            banned = (
                await s.execute(select(func.count(User.id)).where(User.is_banned == True))
            ).scalar() or 0
            today_reg = (
                await s.execute(
                    select(func.count(User.id)).where(User.created_at > day_ago)
                )
            ).scalar() or 0
            total_matches = (await s.execute(select(func.count(Match.id)))).scalar() or 0
            total_msgs = (
                await s.execute(select(func.count(ChatMessage.id)))
            ).scalar() or 0
            total_likes = (await s.execute(select(func.count(Like.id)))).scalar() or 0
            revenue = (
                await s.execute(
                    select(func.sum(Payment.amount)).where(
                        Payment.status == PaymentStatus.SUCCEEDED
                    )
                )
            ).scalar() or 0
            month_rev = (
                await s.execute(
                    select(func.sum(Payment.amount)).where(
                        and_(
                            Payment.status == PaymentStatus.SUCCEEDED,
                            Payment.paid_at > month_ago
                        )
                    )
                )
            ).scalar() or 0
            pending_reports = (
                await s.execute(
                    select(func.count(Report.id)).where(Report.status == "pending")
                )
            ).scalar() or 0

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
                "revenue": revenue / 100 if revenue else 0,
                "month_revenue": month_rev / 100 if month_rev else 0,
                "pending_reports": pending_reports,
                "conversion": (vip / complete * 100) if complete > 0 else 0,
            }

    @staticmethod
    async def search_users(query: str) -> List[Dict]:
        async with async_session_maker() as s:
            if query.isdigit():
                r = await s.execute(
                    select(User).where(
                        or_(
                            User.id == int(query),
                            User.telegram_id == int(query)
                        )
                    )
                )
            else:
                r = await s.execute(
                    select(User).where(
                        or_(
                            User.username.ilike(f"%{query}%"),
                            User.name.ilike(f"%{query}%")
                        )
                    ).limit(10)
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
    async def get_pending_reports(limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(Report)
                .where(Report.status == "pending")
                .order_by(Report.created_at.desc())
                .limit(limit)
            )
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
            await s.execute(
                update(Report).where(Report.id == report_id).values(
                    status=action,
                    admin_notes=notes,
                    resolved_at=datetime.utcnow()
                )
            )
            await s.commit()

    @staticmethod
    async def get_recent_payments(limit=10) -> List[Dict]:
        async with async_session_maker() as s:
            r = await s.execute(
                select(Payment).order_by(Payment.created_at.desc()).limit(limit)
            )
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
            s.add(
                PromoCode(
                    code=code.upper(),
                    tier=tier,
                    duration_days=days,
                    max_uses=max_uses
                )
            )
            await s.commit()

    @staticmethod
    async def use_promo(user_id: int, code: str) -> Dict:
        async with async_session_maker() as s:
            r = await s.execute(
                select(PromoCode).where(
                    and_(
                        PromoCode.code == code.upper(),
                        PromoCode.is_active == True
                    )
                )
            )
            promo = r.scalar_one_or_none()
            if not promo:
                return {"error": "Промокод не найден"}
            if promo.used_count >= promo.max_uses:
                return {"error": "Промокод исчерпан"}

            used = await s.execute(
                select(PromoUse).where(
                    and_(PromoUse.promo_id == promo.id, PromoUse.user_id == user_id)
                )
            )
            if used.scalar_one_or_none():
                return {"error": "Ты уже использовал этот промокод"}

            s.add(PromoUse(promo_id=promo.id, user_id=user_id))
            await s.execute(
                update(PromoCode).where(PromoCode.id == promo.id).values(
                    used_count=PromoCode.used_count + 1
                )
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

            if te == SubscriptionTier.VIP_LIFETIME:
                exp = None
            elif u.subscription_expires_at and u.subscription_expires_at > now:
                exp = u.subscription_expires_at + timedelta(days=days)
            else:
                exp = now + timedelta(days=days)

            await s.execute(
                update(User).where(User.id == uid).values(
                    subscription_tier=te,
                    subscription_expires_at=exp
                )
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
                    "product_count": p.product_count
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
            s.add(
                BroadcastLog(
                    admin_id=admin_id,
                    message_text=text,
                    target_filter=target,
                    sent_count=sent,
                    failed_count=failed
                )
            )
            await s.commit()

# ═══════════════════════════════════════════════════════════════════════════════
# TEXTS
# ═══════════════════════════════════════════════════════════════════════════════

TIER_NAMES = {
    "free": "💬 Бесплатный",
    "vip_light": "✨ VIP Light",
    "vip_standard": "⭐ VIP Standard",
    "vip_pro": "👑 VIP Pro",
    "vip_lifetime": "💎 VIP Lifetime",
}

class T:
    WELCOME_NEW = f"""
♥️ *Добро пожаловать в {BOT_NAME}!*

Здесь ты найдёшь свою половинку среди тысяч людей по всей России! 🇷🇺

Давай создадим анкету 👇
"""

    WELCOME_BACK = """
♥️ *С возвращением, {name}!*

{status}
👁️ Просмотров: {views} · 💘 Мэтчей: {matches} · 💬 Сообщений: {msgs}
"""

    ASK_NAME = "👤 Как тебя зовут?"
    ASK_AGE = "🎂 Сколько тебе лет? _(18-99)_"
    ASK_GENDER = "👫 Твой пол:"
    ASK_CITY = "🌍 Твой город:"
    ASK_PHOTO = "📸 Отправь фото или «Пропустить»:"
    ASK_BIO = "✍️ О себе _(до 500 симв.)_ или «Пропустить»:"
    ASK_LOOKING = "🔍 Кого ищешь?"
    BAD_NAME = "⚠️ Имя должно быть 2-50 символов:"
    BAD_AGE = "⚠️ Возраст должен быть 18-99:"
    REG_DONE = f"✅ *Анкета готова!* Добро пожаловать в {BOT_NAME}!"
    NO_PROFILES = "😔 *Анкеты закончились.* Попробуй позже!"
    LIKES_LIMIT = "❌ *Лимит лайков!*\n✨ VIP — безлимитные лайки!"
    NEW_MATCH = "💕 *Взаимная симпатия с {name}!* 🎉"
    NO_MATCHES = "😞 Пока нет взаимных симпатий"
    NO_PROFILE = "⚠️ Заполни профиль 👉 /start"
    BANNED = "🚫 Аккаунт заблокирован."
    NO_GUESTS = "😴 Пока нет гостей"
    NO_MSGS = "💤 Нет сообщений"
    PAY_PENDING = "⏳ Нажми «Оплатить» для перехода:"

    SHOP = f"""
🛍️ *{BOT_NAME}* — Магазин

✨ *VIP-подписки* — все возможности
🚀 *Буст анкеты* — в топ выдачи
📊 *Сравнить тарифы*
🎁 *Промокод* — активировать
"""

    FAQ = f"""
❓ *FAQ · {BOT_NAME}*

*❤️ Как работают симпатии?*
Ставь 👍 — если взаимно, общайтесь в чате!

*🚀 Что такое буст?*
Твоя анкета поднимается в топ на 24 часа.

*✨ VIP — что даёт?*
Безлимит лайков, все гости, приоритет, невидимка и многое другое.
Жми «Сравнить тарифы» в магазине.
"""

    BOOST_INFO = """
🚀 *БУСТ АНКЕТЫ*

Поднимает профиль в топ на 24ч.
👁️ +500% просмотров · ❤️ +300% лайков

💡 Лучше активировать вечером 18:00-22:00

{status}
"""

    COMPARE = """
📊 *СРАВНЕНИЕ ТАРИФОВ*

💬 *Бесплатный*
• 30 лайков/день
• 10 сообщений/день
• 3 гостя/день
• Базовые фильтры

✨ *VIP Light (199₽/мес)*
• 100 лайков/день (+3x)
• ∞ сообщений
• 10 гостей/день
• Без рекламы
• Приоритет в выдаче

⭐ *VIP Standard (499₽/мес)*
• ∞ лайков
• ∞ сообщений
• Все гости
• Невидимка (скрытое лайкание)
• 1 буст/день
• Фильтры по интересам
• Приоритет +50%

👑 *VIP Pro (899₽/мес)*
• Всё из Standard +
• 3 буста/день
• 5 суперлайков/день
• VIP-бейдж 👑
• Эксклюзивные подарки
• 24/7 Приоритетная поддержка
• В топе выдачи

💎 *VIP Lifetime (4999₽)*
• Всё из Pro НАВСЕГДА
• Бейдж Основателя 💎
• Все будущие обновления
• Приватный чат с командой
"""

    LIGHT = """
✨ *VIP LIGHT*

👁️ 100 лайков/день (×3 от бесплатного)
💬 Безлимитные сообщения
👥 10 гостей/день
🚫 Без рекламы
⭐ Приоритет в выдаче

_Лучший выбор для новичков_

📍 Тариф доступен:
• 199₽/месяц (6.6₽/день)
• 499₽/3 месяца (5.5₽/день) — *экономия 25%*
• 899₽/6 месяцев (5₽/день) — *экономия 36%*
"""

    STANDARD = """
⭐ *VIP STANDARD* — Самый популярный

❤️ Безлимитные лайки и сообщения
👥 Все гости (без лимита)
👻 Невидимка (скрытое лайкание)
🚀 1 буст/день (топ на 24ч)
⭐ Приоритет в выдаче +50%
🔍 Расширенные фильтры
🎯 Поиск по интересам

_×3 мэтчей по сравнению с бесплатным_

📍 Стоимость:
• 499₽/месяц (16.6₽/день)
• 1199₽/3 месяца (13.3₽/день) — *скидка 20%* ⭐
• 1999₽/6 месяцев (11.1₽/день) — *скидка 33%*
"""

    PRO = """
👑 *VIP PRO* — Максимум возможностей

Всё из VIP Standard +

🚀 3 буста/день (топ выдачи)
⭐ 5 суперлайков/день (выделены)
👑 VIP-бейдж (видно всем)
🎁 Эксклюзивные подарки
🏆 В топе выдачи всегда
💬 24/7 Приоритетная поддержка
🎭 Функция "Суперлайк" (выделенный лайк)

_×5 мэтчей по сравнению с бесплатным_

📍 Стоимость:
• 899₽/месяц (30₽/день)
• 2299₽/3 месяца (25.4₽/день) — *скидка 15%*
• 3999₽/6 месяцев (22.2₽/день) — *скидка 33%*
"""

    LIFETIME = """
💎 *VIP LIFETIME* — Навсегда

Всё из VIP Pro НАВСЕГДА +

💎 Бейдж "Основателя" (уникальный)
🎁 Все будущие обновления бесплатно
💬 Личный чат с командой разработчиков
🏅 Приоритет поддержки
🎊 Эксклюзивные события

*Один платёж — вечный доступ*

💰 Сравнение цен:
• VIP Pro на 6 месяцев = 3999₽
• VIP Pro на год = 7998₽
• VIP Lifetime = 4999₽

✅ *Окупается менее чем за год!*
✅ Потом навсегда бесплатно!

*4999₽ — один раз на всю жизнь*
"""

    ADMIN_MAIN = """
🛡️ *Админ-панель · {bot_name}*

*{admin_name}* {role}

Быстрые действия 👇
"""

    ADMIN_STATS = """
📊 *Статистика · {bot_name}*

*👥 Пользователи:*
Всего: {total}
С анкетой: {complete}
DAU: {dau} · WAU: {wau} · MAU: {mau}
VIP: {vip} ({conversion:.1f}%)
Забанено: {banned}
Сегодня: {today_reg}

*💬 Активность:*
Лайков: {likes}
Мэтчей: {matches}
Сообщений: {messages}

*💰 Финансы:*
Всего: {revenue:.0f}₽
За месяц: {month_revenue:.0f}₽
Жалоб: {pending_reports}
"""

    ADMIN_USER_CARD = """
*Карточка пользователя*

ID: `{id}` · TG: `{telegram_id}`
@{username}

{badge}*{name}*, {age}
🌍 {city}

_{bio}_

Статус: {tier}
👁️ {views} · ❤️ {likes} · 💘 {matches}
🚀 Бустов: {boosts}
📅 Рег: {created} · Актив: {active}
🚫 Бан: {banned}
"""

    BROADCAST_CONFIRM = """
📢 *Рассылка*

{text}

Аудитория: *{target}*
Получателей: *{count}*

Отправить?
"""

# ═══════════════════════════════════════════════════════════════════════════════
# KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════

class KB:
    @staticmethod
    def main():
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📋 Анкеты"), KeyboardButton(text="❤️ Симпатии")],
                [KeyboardButton(text="💬 Чаты"), KeyboardButton(text="👀 Гости")],
                [KeyboardButton(text="🛍️ Магазин"), KeyboardButton(text="👤 Профиль")],
                [KeyboardButton(text="❓ FAQ")],
            ],
            resize_keyboard=True
        )

    @staticmethod
    def gender():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="👨 Мужской", callback_data="g:male"),
                    InlineKeyboardButton(text="👩 Женский", callback_data="g:female")
                ]
            ]
        )

    @staticmethod
    def looking():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="👨 Мужчин", callback_data="l:male"),
                    InlineKeyboardButton(text="👩 Женщин", callback_data="l:female")
                ],
                [InlineKeyboardButton(text="👫 Всех", callback_data="l:both")]
            ]
        )

    @staticmethod
    def skip():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⏭️ Пропустить", callback_data="skip")]
            ]
        )

    @staticmethod
    def search(uid):
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="👍", callback_data=f"lk:{uid}"),
                    InlineKeyboardButton(text="👎", callback_data=f"dl:{uid}")
                ],
                [InlineKeyboardButton(text="🚩 Жалоба", callback_data=f"rp:{uid}")]
            ]
        )

    @staticmethod
    def matches(ms):
        b = [
            [InlineKeyboardButton(
                text=f"💬 {m['name']}, {m['age']}",
                callback_data=f"ch:{m['user_id']}"
            )]
            for m in ms[:10]
        ]
        b.append([InlineKeyboardButton(text="◀️ Назад", callback_data="mn")])
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def back_matches():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Симпатии", callback_data="bm")]
            ]
        )

    @staticmethod
    def shop():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✨ VIP-подписки", callback_data="sh:subs")],
                [InlineKeyboardButton(text="🚀 Буст анкеты", callback_data="sh:boost")],
                [InlineKeyboardButton(text="📊 Сравнить тарифы", callback_data="sh:compare")],
                [InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="sh:promo")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="mn")]
            ]
        )

    @staticmethod
    def subs():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✨ VIP Light", callback_data="tf:vip_light")],
                [InlineKeyboardButton(text="⭐ VIP Standard", callback_data="tf:vip_standard")],
                [InlineKeyboardButton(text="👑 VIP Pro", callback_data="tf:vip_pro")],
                [InlineKeyboardButton(text="💎 VIP Lifetime", callback_data="tf:vip_lifetime")],
                [InlineKeyboardButton(text="📊 Сравнить", callback_data="sh:compare")],
                [InlineKeyboardButton(text="◀️ Магазин", callback_data="sh:mn")]
            ]
        )

    @staticmethod
    def buy_light():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💳 199₽ / месяц", callback_data="by:vip_light:30:19900")],
                [InlineKeyboardButton(text="💳 499₽ / 3 месяца (-25%)", callback_data="by:vip_light:90:49900")],
                [InlineKeyboardButton(text="💳 899₽ / 6 месяцев (-36%)", callback_data="by:vip_light:180:89900")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="sh:subs")]
            ]
        )

    @staticmethod
    def buy_standard():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💳 499₽ / месяц", callback_data="by:vip_standard:30:49900")],
                [InlineKeyboardButton(text="💳 1199₽ / 3 месяца (-20%)", callback_data="by:vip_standard:90:119900")],
                [InlineKeyboardButton(text="💳 1999₽ / 6 месяцев (-33%)", callback_data="by:vip_standard:180:199900")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="sh:subs")]
            ]
        )

    @staticmethod
    def buy_pro():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💳 899₽ / месяц", callback_data="by:vip_pro:30:89900")],
                [InlineKeyboardButton(text="💳 2299₽ / 3 месяца (-15%)", callback_data="by:vip_pro:90:229900")],
                [InlineKeyboardButton(text="💳 3999₽ / 6 месяцев (-33%)", callback_data="by:vip_pro:180:399900")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="sh:subs")]
            ]
        )

    @staticmethod
    def buy_lifetime():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💳 4999₽ навсегда", callback_data="by:vip_lifetime:0:499900")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="sh:subs")]
            ]
        )

    @staticmethod
    def boost_menu(has, active):
        b = []
        if has:
            b.append([InlineKeyboardButton(text="🚀 Активировать буст", callback_data="bo:act")])
        b += [
            [InlineKeyboardButton(text="💳 1шт — 49₽", callback_data="by:boost:1:4900")],
            [InlineKeyboardButton(text="💳 5шт — 199₽ (-18%)", callback_data="by:boost:5:19900")],
            [InlineKeyboardButton(text="💳 10шт — 349₽ (-29%)", callback_data="by:boost:10:34900")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="sh:mn")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=b)

    @staticmethod
    def pay(url, pid):
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить", url=url)],
                [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"ck:{pid}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="sh:mn")]
            ]
        )

    @staticmethod
    def profile():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✏️ Редактировать", callback_data="pe"),
                    InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")
                ],
                [InlineKeyboardButton(text="🚀 Буст", callback_data="sh:boost")]
            ]
        )

    @staticmethod
    def edit():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="👤 Имя", callback_data="ed:name"),
                    InlineKeyboardButton(text="🎂 Возраст", callback_data="ed:age")
                ],
                [
                    InlineKeyboardButton(text="🌍 Город", callback_data="ed:city"),
                    InlineKeyboardButton(text="✍️ О себе", callback_data="ed:bio")
                ],
                [InlineKeyboardButton(text="📸 Фото", callback_data="ed:photo")],
                [InlineKeyboardButton(text="◀️ Профиль", callback_data="pv")]
            ]
        )

    @staticmethod
    def report_reasons():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🚨 Спам", callback_data="rr:spam"),
                    InlineKeyboardButton(text="😱 Фейк", callback_data="rr:fake")
                ],
                [
                    InlineKeyboardButton(text="🔞 18+", callback_data="rr:nsfw"),
                    InlineKeyboardButton(text="😠 Оскорбления", callback_data="rr:harass")
                ],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="mn")]
            ]
        )

    @staticmethod
    def admin():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
                [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="adm:search")],
                [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:broadcast")],
                [InlineKeyboardButton(text="🚩 Жалобы", callback_data="adm:reports")],
                [InlineKeyboardButton(text="💳 Платежи", callback_data="adm:payments")],
                [InlineKeyboardButton(text="🎁 Создать промокод", callback_data="adm:promo")],
                [InlineKeyboardButton(text="⭐ Топ пользователей", callback_data="adm:top")],
                [InlineKeyboardButton(text="✖️ Закрыть", callback_data="mn")]
            ]
        )

    @staticmethod
    def admin_user(uid, is_banned):
        ban_btn = (
            InlineKeyboardButton(text="✅ Разбанить", callback_data=f"au:unban:{uid}")
            if is_banned
            else InlineKeyboardButton(text="🚫 Забанить", callback_data=f"au:ban:{uid}")
        )
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✨ Выдать VIP", callback_data=f"au:givevip:{uid}"),
                    InlineKeyboardButton(text="🚀 Бусты", callback_data=f"au:giveboost:{uid}")
                ],
                [
                    ban_btn,
                    InlineKeyboardButton(text="🗑️ Удалить фото", callback_data=f"au:delphotos:{uid}")
                ],
                [InlineKeyboardButton(text="✅ Верифицировать", callback_data=f"au:verify:{uid}")],
                [InlineKeyboardButton(text="🛡️ Админка", callback_data="adm:main")]
            ]
        )

    @staticmethod
    def admin_report(rid, ruid):
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🚫 Забанить", callback_data=f"ar:ban:{rid}:{ruid}"),
                    InlineKeyboardButton(text="⚠️ Предупредить", callback_data=f"ar:warn:{rid}:{ruid}")
                ],
                [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ar:dismiss:{rid}:{ruid}")],
                [InlineKeyboardButton(text="➡️ Следующая", callback_data="adm:reports")]
            ]
        )

    @staticmethod
    def broadcast_targets():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="👥 Все", callback_data="bc:all")],
                [InlineKeyboardButton(text="✅ С анкетой", callback_data="bc:complete")],
                [InlineKeyboardButton(text="✨ VIP", callback_data="bc:vip")],
                [InlineKeyboardButton(text="💬 Бесплатные", callback_data="bc:free")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:main")]
            ]
        )

    @staticmethod
    def broadcast_confirm():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Отправить", callback_data="bc:send")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:main")]
            ]
        )

    @staticmethod
    def give_vip_tiers():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✨ Light", callback_data="gv:vip_light"),
                    InlineKeyboardButton(text="⭐ Standard", callback_data="gv:vip_standard")
                ],
                [
                    InlineKeyboardButton(text="👑 Pro", callback_data="gv:vip_pro"),
                    InlineKeyboardButton(text="💎 Lifetime", callback_data="gv:vip_lifetime")
                ],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:main")]
            ]
        )

    @staticmethod
    def back_admin():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🛡️ Админка", callback_data="adm:main")]
            ]
        )

# ═══════════════════════════════════════════════════════════════════════════════
# ANTI-SPAM
# ═══════════════════════════════════════════════════════════════════════════════

class AntiSpam:
    def __init__(self):
        self.users: Dict[str, List[float]] = {}

    async def check(self, user_id: int, action: str, limit: int = 5, time_window: int = 60) -> bool:
        now = time.time()
        key = f"{user_id}:{action}"

        if key not in self.users:
            self.users[key] = []

        self.users[key] = [t for t in self.users[key] if (now - t) < time_window]

        if len(self.users[key]) >= limit:
            return False

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

        desc = (
            f"Подписка {TIER_NAMES.get(tier, '')}"
            if ptype == "subscription"
            else f"Буст ({count}шт)"
        )

        try:
            p = YooPayment.create(
                {
                    "amount": {"value": f"{amount/100:.2f}", "currency": "RUB"},
                    "confirmation": {
                        "type": ConfirmationType.REDIRECT,
                        "return_url": f"{config.DOMAIN}/ok",
                    },
                    "capture": True,
                    "description": desc,
                    "metadata": {
                        "user_id": user["id"],
                        "type": ptype,
                        "tier": tier,
                        "dur": dur,
                        "count": count,
                    },
                },
                str(uuid.uuid4()),
            )
            pid = await DB.create_payment(user["id"], p.id, amount, desc, ptype, tier, dur, count)
            return {"pid": pid, "url": p.confirmation.confirmation_url}
        except Exception as e:
            logger.error(f"Payment creation error: {e}")
            return {"error": str(e)}

    @staticmethod
    async def check(pid):
        p = await DB.get_payment(pid)
        if not p:
            return {"status": "not_found"}

        try:
            y = YooPayment.find_one(p["yookassa_payment_id"])
            if y.status == "succeeded" and p["status"] != "succeeded":
                await DB.update_payment_status(pid, PaymentStatus.SUCCEEDED)
                if p["product_type"] == "subscription":
                    await DB.activate_subscription_by_id(
                        p["user_id"],
                        p["product_tier"],
                        p["product_duration"] or 30,
                    )
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
                    if isinstance(event, Message):
                        await event.answer(T.BANNED)
                    return

        data["user"] = u
        return await handler(event, data)

# ═══════════════════════════════════════════════════════════════════════════════
# HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

rt = Router()

# ══════════════════════ START ══════════════════════

@rt.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, user: Optional[Dict]):
    await state.clear()

    if user and user.get("is_profile_complete"):
        un = await DB.get_unread(user["id"])
        st = TIER_NAMES.get(user["subscription_tier"], "")
        if DB.is_boosted(user):
            st += " · 🚀 Буст"
        st += DB.get_role_tag(user)

        await message.answer(
            T.WELCOME_BACK.format(
                name=user["name"],
                status=st,
                views=user["views_count"],
                matches=user["matches_count"],
                msgs=un,
            ),
            reply_markup=KB.main(),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        if not user:
            await DB.create_user(message.from_user.id, message.from_user.username)
        await message.answer(T.WELCOME_NEW, parse_mode=ParseMode.MARKDOWN)
        await message.answer(T.ASK_NAME, reply_markup=ReplyKeyboardRemove())
        await state.set_state(RegStates.name)

# ══════════════════════ REGISTRATION ══════════════════════

@rt.message(RegStates.name)
async def reg_name(message: Message, state: FSMContext):
    n = message.text.strip()
    if len(n) < 2 or len(n) > 50:
        await message.answer(T.BAD_NAME)
        return
    await state.update_data(name=n)
    await message.answer(T.ASK_AGE, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(RegStates.age)

@rt.message(RegStates.age)
async def reg_age(message: Message, state: FSMContext):
    try:
        a = int(message.text.strip())
        if not 18 <= a <= 99:
            raise ValueError
    except:
        await message.answer(T.BAD_AGE)
        return
    await state.update_data(age=a)
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
    c = message.text.strip().title()
    if len(c) < 2:
        await message.answer("🌍 Введи город!")
        return
    await state.update_data(city=c)
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
    await state.update_data(bio=message.text.strip()[:500])
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
    await callback.message.edit_text(T.REG_DONE, parse_mode=ParseMode.MARKDOWN)
    await callback.message.answer("👋 Выбери действие:", reply_markup=KB.main())
    await callback.answer()

# ══════════════════════ BROWSE ══════════════════════

@rt.message(F.text == "📋 Анкеты")
async def browse(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    ps = await DB.search_profiles(user, 1)
    if not ps:
        await message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)
        return

    await show_card(message, ps[0], user)

async def show_card(message: Message, p: Dict, v: Dict):
    await DB.add_guest(v["id"], p["id"])
    lm = {"male": "👨", "female": "👩", "both": "👫"}
    badge = DB.get_badge(p)
    role = DB.get_role_tag(p)
    boost = " 🚀" if DB.is_boosted(p) else ""

    txt = (
        f"{badge}*{p['name']}*{boost}, {p['age']}{role}\n"
        f"🌍 {p['city']}\n\n"
        f"{p['bio'] or '_Нет описания_'}\n\n"
        f"🔍 Ищет: {lm.get(p.get('looking_for', 'both'), '')}"
    )

    kb = KB.search(p["id"])

    if p.get("main_photo"):
        await message.answer_photo(
            photo=p["main_photo"],
            caption=txt,
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await message.answer(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("lk:"))
async def handle_like(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        await callback.answer("❌")
        return

    if not DB.is_vip(user) and user.get("daily_likes_remaining", 0) <= 0:
        try:
            await callback.message.edit_caption(
                caption=T.LIKES_LIMIT,
                reply_markup=KB.shop(),
                parse_mode=ParseMode.MARKDOWN,
            )
        except:
            await callback.message.edit_text(
                T.LIKES_LIMIT,
                reply_markup=KB.shop(),
                parse_mode=ParseMode.MARKDOWN,
            )
        return

    if not await anti_spam.check(callback.from_user.id, "like"):
        await callback.answer("⚠️ Не спешите!", show_alert=True)
        return

    tid = int(callback.data[3:])
    ism = await DB.add_like(user["id"], tid)

    if not DB.is_vip(user):
        await DB.dec_likes(user["telegram_id"])

    if ism:
        t = await DB.get_user_by_id(tid)
        tn = t["name"] if t else "?"
        try:
            await callback.message.edit_caption(
                caption=T.NEW_MATCH.format(name=tn),
                parse_mode=ParseMode.MARKDOWN,
            )
        except:
            await callback.message.edit_text(
                T.NEW_MATCH.format(name=tn),
                parse_mode=ParseMode.MARKDOWN,
            )

        if t:
            try:
                await callback.bot.send_message(
                    t["telegram_id"],
                    T.NEW_MATCH.format(name=user["name"]),
                    parse_mode=ParseMode.MARKDOWN,
                )
            except:
                pass
    else:
        await callback.answer("👍")

    user = await DB.get_user(callback.from_user.id)
    ps = await DB.search_profiles(user, 1)
    if ps:
        await show_card(callback.message, ps[0], user)
    else:
        await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)

    await callback.answer()

@rt.callback_query(F.data.startswith("dl:"))
async def handle_dislike(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    ps = await DB.search_profiles(user, 1)
    if ps:
        await show_card(callback.message, ps[0], user)
    else:
        try:
            await callback.message.edit_caption(
                caption=T.NO_PROFILES,
                parse_mode=ParseMode.MARKDOWN,
            )
        except:
            await callback.message.answer(T.NO_PROFILES, parse_mode=ParseMode.MARKDOWN)

    await callback.answer()

# ══════════════════════ MATCHES & CHAT ══════════════════════

@rt.message(F.text == "❤️ Симпатии")
async def show_matches(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    ms = await DB.get_matches(user["id"])
    if ms:
        await message.answer(
            f"❤️ *Симпатии ({len(ms)}):*",
            reply_markup=KB.matches(ms),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await message.answer(T.NO_MATCHES)

@rt.callback_query(F.data.startswith("ch:"))
async def start_chat(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    pid = int(callback.data[3:])
    p = await DB.get_user_by_id(pid)
    if not p:
        await callback.answer("Не найден")
        return

    mid = await DB.get_match_between(user["id"], pid)
    if not mid:
        await callback.answer("Нет симпатии")
        return

    msgs = await DB.get_msgs(mid, 5)
    txt = f"💬 *Чат с {p['name']}*\n\n"
    for mg in msgs:
        sn = "Вы" if mg["sender_id"] == user["id"] else p["name"]
        txt += f"*{sn}:* {mg['text']}\n"
    if not msgs:
        txt += "_Напиши первым!_"

    await state.update_data(cp=pid, mi=mid)
    await state.set_state(ChatStates.chatting)
    await callback.message.edit_text(
        txt,
        reply_markup=KB.back_matches(),
        parse_mode=ParseMode.MARKDOWN,
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
        await message.answer("Чат закрыт", reply_markup=KB.main())
        return

    await DB.send_msg(mid, user["id"], message.text)
    p = await DB.get_user_by_id(pid)

    if p:
        try:
            await message.bot.send_message(
                p["telegram_id"],
                f"💬 *{user['name']}:* {message.text}",
                parse_mode=ParseMode.MARKDOWN,
            )
        except:
            pass

    await message.answer("✅")

@rt.callback_query(F.data == "bm")
async def back_to_matches(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    await state.clear()
    if not user:
        return

    ms = await DB.get_matches(user["id"])
    if ms:
        await callback.message.edit_text(
            f"❤️ *({len(ms)}):*",
            reply_markup=KB.matches(ms),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await callback.message.edit_text(T.NO_MATCHES)

    await callback.answer()

@rt.message(F.text == "💬 Чаты")
async def show_chats(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    ms = await DB.get_matches(user["id"])
    if ms:
        await message.answer(
            "💬 *Диалоги:*",
            reply_markup=KB.matches(ms),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await message.answer(T.NO_MSGS)

@rt.message(F.text == "👀 Гости")
async def show_guests(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    lim = 20 if DB.is_vip(user) else config.FREE_GUESTS_VISIBLE
    gs = await DB.get_guests(user["id"], lim)

    if not gs:
        await message.answer(T.NO_GUESTS)
        return

    txt = "👀 *Гости:*\n\n"
    for i, g in enumerate(gs, 1):
        txt += f"{i}. {g['name']}, {g['age']} — {g['city']}\n"

    if not DB.is_vip(user):
        txt += "\n_✨ VIP — все гости!_"

    await message.answer(txt, parse_mode=ParseMode.MARKDOWN)

# ══════════════════════ PROFILE ══════════════════════

@rt.message(F.text == "👤 Профиль")
async def show_profile(message: Message, user: Optional[Dict]):
    if not user or not user.get("is_profile_complete"):
        await message.answer(T.NO_PROFILE)
        return

    badge = DB.get_badge(user)
    role = DB.get_role_tag(user)
    sub = TIER_NAMES.get(user["subscription_tier"], "")

    if user.get("subscription_expires_at") and user["subscription_tier"] not in ("free", "vip_lifetime"):
        sub += f" (до {user['subscription_expires_at'].strftime('%d.%m.%Y')})"

    bi = ""
    if DB.is_boosted(user):
        bi += f"\n🚀 Буст активен до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if user.get("boost_count", 0) > 0:
        bi += f"\n📦 Запас бустов: {user['boost_count']}"

    txt = (
        f"👤 *Мой профиль*\n\n"
        f"{badge}*{user['name']}*, {user['age']}{role}\n"
        f"🌍 {user['city']}\n\n"
        f"{user['bio'] or '_Не указано_'}\n\n"
        f"👁️ {user['views_count']} просмотров\n"
        f"❤️ {user['likes_received_count']} лайков\n"
        f"💘 {user['matches_count']} мэтчей\n\n"
        f"Статус: {sub}{bi}"
    )

    if user.get("main_photo"):
        await message.answer_photo(
            photo=user["main_photo"],
            caption=txt,
            reply_markup=KB.profile(),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await message.answer(
            txt,
            reply_markup=KB.profile(),
            parse_mode=ParseMode.MARKDOWN,
        )

@rt.callback_query(F.data == "pe")
async def profile_edit_menu(callback: CallbackQuery):
    try:
        await callback.message.edit_caption(
            caption="✏️ *Редактировать профиль:*",
            reply_markup=KB.edit(),
            parse_mode=ParseMode.MARKDOWN,
        )
    except:
        await callback.message.edit_text(
            "✏️ *Редактировать профиль:*",
            reply_markup=KB.edit(),
            parse_mode=ParseMode.MARKDOWN,
        )
    await callback.answer()

@rt.callback_query(F.data == "ed:name")
async def edit_name(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("👤 Новое имя:")
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
    await message.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:age")
async def edit_age(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("🎂 Возраст:")
    await state.set_state(EditStates.edit_age)
    await callback.answer()

@rt.message(EditStates.edit_age)
async def save_age(message: Message, state: FSMContext):
    try:
        a = int(message.text.strip())
        if not 18 <= a <= 99:
            raise ValueError
    except:
        await message.answer(T.BAD_AGE)
        return
    await DB.update_user(message.from_user.id, age=a)
    await state.clear()
    await message.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:city")
async def edit_city(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("🌍 Город:")
    await state.set_state(EditStates.edit_city)
    await callback.answer()

@rt.message(EditStates.edit_city)
async def save_city(message: Message, state: FSMContext):
    await DB.update_user(message.from_user.id, city=message.text.strip().title())
    await state.clear()
    await message.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:bio")
async def edit_bio(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("✍️ О себе:")
    await state.set_state(EditStates.edit_bio)
    await callback.answer()

@rt.message(EditStates.edit_bio)
async def save_bio(message: Message, state: FSMContext):
    await DB.update_user(message.from_user.id, bio=message.text.strip()[:500])
    await state.clear()
    await message.answer("✅", reply_markup=KB.main())

@rt.callback_query(F.data == "ed:photo")
async def edit_photo(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("📸 Фото:")
    await state.set_state(EditStates.add_photo)
    await callback.answer()

@rt.message(EditStates.add_photo, F.photo)
async def save_photo(message: Message, state: FSMContext, user: Optional[Dict]):
    if not user:
        return

    pid = message.photo[-1].file_id
    existing_photos = user.get("photos", "").strip()

    photos_list = [p for p in existing_photos.split(",") if p.strip()]
    if len(photos_list) >= 5:
        await message.answer("⚠️ Максимум 5 фото!", reply_markup=KB.main())
        await state.clear()
        return

    photos_list.append(pid)
    new_photos = ",".join(photos_list)
    main_photo = pid

    await DB.update_user(
        message.from_user.id,
        photos=new_photos,
        main_photo=main_photo
    )
    await state.clear()
    await message.answer("✅ Фото добавлено!", reply_markup=KB.main())

# ══════════════════════ SHOP ══════════════════════

@rt.message(F.text == "🛍️ Магазин")
async def shop_menu(message: Message):
    await message.answer(
        T.SHOP,
        reply_markup=KB.shop(),
        parse_mode=ParseMode.MARKDOWN,
    )

@rt.callback_query(F.data == "sh:mn")
async def shop_main(callback: CallbackQuery):
    await callback.message.edit_text(
        T.SHOP,
        reply_markup=KB.shop(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()

@rt.callback_query(F.data == "sh:compare")
async def shop_compare(callback: CallbackQuery):
    await callback.message.edit_text(
        T.COMPARE,
        reply_markup=KB.subs(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()

@rt.callback_query(F.data == "sh:subs")
async def shop_subs(callback: CallbackQuery):
    await callback.message.edit_text(
        "✨ *Выбери тариф:*",
        reply_markup=KB.subs(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()

@rt.callback_query(F.data == "tf:vip_light")
async def tf_light(callback: CallbackQuery):
    await callback.message.edit_text(T.LIGHT, reply_markup=KB.buy_light(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data == "tf:vip_standard")
async def tf_standard(callback: CallbackQuery):
    await callback.message.edit_text(T.STANDARD, reply_markup=KB.buy_standard(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data == "tf:vip_pro")
async def tf_pro(callback: CallbackQuery):
    await callback.message.edit_text(T.PRO, reply_markup=KB.buy_pro(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data == "tf:vip_lifetime")
async def tf_lifetime(callback: CallbackQuery):
    await callback.message.edit_text(T.LIFETIME, reply_markup=KB.buy_lifetime(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@rt.callback_query(F.data == "sh:boost")
async def shop_boost(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        await callback.answer("❌")
        return

    has = user.get("boost_count", 0) > 0
    act = DB.is_boosted(user)
    st = ""

    if act:
        st += f"\n\n🚀 Буст до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if has:
        st += f"\n📦 Запас: {user['boost_count']}"
    if not has and not act:
        st = "\n\n❌ Нет бустов"

    await callback.message.edit_text(
        T.BOOST_INFO.format(status=st),
        reply_markup=KB.boost_menu(has, act),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()

@rt.callback_query(F.data == "bo:act")
async def activate_boost(callback: CallbackQuery, user: Optional[Dict]):
    if not user or user.get("boost_count", 0) <= 0:
        await callback.answer("❌ Нет бустов!", show_alert=True)
        return

    ok = await DB.use_boost(user["id"])
    if ok:
        u = await DB.get_user(callback.from_user.id)
        await callback.message.edit_text(
            f"🚀 *Буст активирован!*\n"
            f"До {u['boost_expires_at'].strftime('%d.%m %H:%M')}\n"
            f"Запас: {u['boost_count']}",
            reply_markup=KB.shop(),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await callback.answer("❌ Ошибка", show_alert=True)

    await callback.answer()

@rt.callback_query(F.data.startswith("by:"))
async def handle_buy(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        await callback.answer("❌")
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
        f"💰 {amt/100:.0f}₽\n\n"
        f"1️⃣ Оплати\n"
        f"2️⃣ Нажми «Проверить»"
    )

    await callback.message.edit_text(
        txt,
        reply_markup=KB.pay(res["url"], res["pid"]),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()

@rt.callback_query(F.data.startswith("ck:"))
async def check_payment(callback: CallbackQuery):
    pid = int(callback.data[3:])
    res = await Pay.check(pid)

    if res["status"] == "succeeded":
        if res.get("type") == "boost":
            await callback.message.edit_text(
                f"✅ *{res.get('count', 1)} бустов добавлено!*",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await callback.message.edit_text(
                "✅ *Подписка активирована!*",
                parse_mode=ParseMode.MARKDOWN,
            )
        await callback.message.answer("👋", reply_markup=KB.main())
    elif res["status"] == "pending":
        await callback.answer("⏳ Обрабатывается...", show_alert=True)
    else:
        await callback.answer("❌ Ошибка", show_alert=True)

    await callback.answer()

# ══════════════════════ PROMO ══════════════════════

@rt.callback_query(F.data == "sh:promo")
async def promo_input(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🎁 *Введи промокод:*",
        parse_mode=ParseMode.MARKDOWN,
    )
    await state.update_data(promo_user_mode=True)
    await state.set_state(AdminStates.promo_code)
    await callback.answer()

@rt.message(AdminStates.promo_code)
async def promo_code_input(message: Message, state: FSMContext, user: Optional[Dict]):
    d = await state.get_data()

    if d.get("promo_user_mode"):
        code = message.text.strip().upper()
        await state.clear()

        if not user:
            await message.answer("❌")
            return

        result = await DB.use_promo(user["id"], code)

        if "error" in result:
            await message.answer(f"❌ {result['error']}", reply_markup=KB.main())
        else:
            tn = TIER_NAMES.get(result["tier"], "VIP")
            await message.answer(
                f"✅ *Промокод активирован!*\n"
                f"{tn} на {result['days']} дней!",
                reply_markup=KB.main(),
                parse_mode=ParseMode.MARKDOWN,
            )
        return

    if not DB.is_admin(user):
        return

    await state.update_data(pc_code=message.text.strip().upper())
    await message.answer("✨ *Тариф:*", reply_markup=KB.give_vip_tiers(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.promo_tier)

@rt.callback_query(AdminStates.promo_tier, F.data.startswith("gv:"))
async def promo_tier(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not DB.is_admin(user):
        return

    await state.update_data(pc_tier=callback.data[3:])
    await callback.message.edit_text("⏰ *Дней VIP?*", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminStates.promo_duration)
    await callback.answer()

@rt.message(AdminStates.promo_duration)
async def promo_dur(message: Message, state: FSMContext, user: Optional[Dict]):
    if not DB.is_admin(user):
        return

    try:
        days = int(message.text.strip())
    except:
        await message.answer("⚠️ Число!")
        return

    await state.update_data(pc_days=days)
    await message.answer("🔢 *Лимит использований?*")
    await state.set_state(AdminStates.promo_uses)

@rt.message(AdminStates.promo_uses)
async def promo_uses(message: Message, state: FSMContext, user: Optional[Dict]):
    if not DB.is_admin(user):
        return

    try:
        uses = int(message.text.strip())
    except:
        await message.answer("⚠️ Число!")
        return

    d = await state.get_data()
    await DB.create_promo(d["pc_code"], d["pc_tier"], d["pc_days"], uses)
    await state.clear()

    tn = TIER_NAMES.get(d["pc_tier"], "VIP")
    await message.answer(
        f"✅ *Промокод создан!*\n\n"
        f"`{d['pc_code']}`\n"
        f"{tn} · {d['pc_days']}дн\n"
        f"Лимит: {uses}",
        reply_markup=KB.main(),
        parse_mode=ParseMode.MARKDOWN,
    )

# ══════════════════════ FAQ & REPORTS ══════════════════════

@rt.message(F.text == "❓ FAQ")
async def show_faq(message: Message):
    await message.answer(T.FAQ, parse_mode=ParseMode.MARKDOWN)

@rt.callback_query(F.data.startswith("rp:"))
async def start_report(callback: CallbackQuery, state: FSMContext):
    await state.update_data(rp_id=int(callback.data[3:]))
    try:
        await callback.message.edit_caption(
            caption="🚩 *Причина:*",
            reply_markup=KB.report_reasons(),
            parse_mode=ParseMode.MARKDOWN,
        )
    except:
        await callback.message.edit_text(
            "🚩 *Причина:*",
            reply_markup=KB.report_reasons(),
            parse_mode=ParseMode.MARKDOWN,
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
        await callback.message.edit_caption(caption="✅ Жалоба отправлена!")
    except:
        await callback.message.edit_text("✅ Жалоба отправлена!")

    ps = await DB.search_profiles(user, 1)
    if ps:
        await show_card(callback.message, ps[0], user)

    await callback.answer()

@rt.callback_query(F.data == "mn")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer("👋", reply_markup=KB.main())
    await callback.answer()

@rt.callback_query(F.data == "pv")
async def back_to_profile(callback: CallbackQuery, user: Optional[Dict]):
    if not user:
        return

    badge = DB.get_badge(user)
    role = DB.get_role_tag(user)
    sub = TIER_NAMES.get(user["subscription_tier"], "")

    if user.get("subscription_expires_at") and user["subscription_tier"] not in ("free", "vip_lifetime"):
        sub += f" (до {user['subscription_expires_at'].strftime('%d.%m.%Y')})"

    bi = ""
    if DB.is_boosted(user):
        bi += f"\n🚀 Буст активен до {user['boost_expires_at'].strftime('%d.%m %H:%M')}"
    if user.get("boost_count", 0) > 0:
        bi += f"\n📦 Запас бустов: {user['boost_count']}"

    txt = (
        f"👤 *Мой профиль*\n\n"
        f"{badge}*{user['name']}*, {user['age']}{role}\n"
        f"🌍 {user['city']}\n\n"
        f"{user['bio'] or '_Не указано_'}\n\n"
        f"👁️ {user['views_count']} просмотров\n"
        f"❤️ {user['likes_received_count']} лайков\n"
        f"💘 {user['matches_count']} мэтчей\n\n"
        f"Статус: {sub}{bi}"
    )

    try:
        await callback.message.edit_caption(caption=txt, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)
    except:
        await callback.message.edit_text(txt, reply_markup=KB.profile(), parse_mode=ParseMode.MARKDOWN)

    await callback.answer()

# ══════════════════════ ADMIN PANEL ══════════════════════

def is_adm(user: Optional[Dict]) -> bool:
    return user is not None and user.get("telegram_id") in config.ADMIN_IDS

@rt.message(Command("admin"))
async def admin_cmd(message: Message, user: Optional[Dict]):
    if not is_adm(user):
        return

    role = "👑 Создатель" if DB.is_creator(user) else "🛡️ Админ"
    await message.answer(
        T.ADMIN_MAIN.format(
            bot_name=BOT_NAME,
            admin_name=user["name"] or "Admin",
            role=role,
        ),
        reply_markup=KB.admin(),
        parse_mode=ParseMode.MARKDOWN,
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
            role=role,
        ),
        reply_markup=KB.admin(),
        parse_mode=ParseMode.MARKDOWN,
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
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()

@rt.callback_query(F.data == "adm:search")
async def admin_search(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return

    await callback.message.edit_text(
        "🔍 *ID, telegram_id, @username или имя:*",
        parse_mode=ParseMode.MARKDOWN,
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
        await message.answer("😔 Не найдено", reply_markup=KB.back_admin())
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
        tier=TIER_NAMES.get(u["subscription_tier"], ""),
        views=u["views_count"],
        likes=u["likes_received_count"],
        matches=u["matches_count"],
        boosts=u.get("boost_count", 0),
        created=u["created_at"].strftime("%d.%m.%Y") if u["created_at"] else "-",
        active=u["last_active_at"].strftime("%d.%m.%Y %H:%M") if u["last_active_at"] else "-",
        banned="Да 🚫" if u["is_banned"] else "Нет",
    )

    await message.answer(
        txt,
        reply_markup=KB.admin_user(u["id"], u["is_banned"]),
        parse_mode=ParseMode.MARKDOWN,
    )

@rt.callback_query(F.data.startswith("au:ban:"))
async def admin_ban(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return

    uid = int(callback.data.split(":")[2])
    u = await DB.get_user_by_id(uid)

    if u:
        await DB.update_user(u["telegram_id"], is_banned=True)
        await callback.message.edit_text(
            f"🚫 *{u['name']}* забанен!",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN,
        )
        try:
            await callback.bot.send_message(u["telegram_id"], "Ваш аккаунт заблокирован администратором.")
        except:
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
            f"✅ *{u['name']}* разбанен!",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN,
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
            parse_mode=ParseMode.MARKDOWN,
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
            parse_mode=ParseMode.MARKDOWN,
        )

    await callback.answer()

@rt.callback_query(F.data.startswith("au:givevip:"))
async def admin_give_vip(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return

    uid = int(callback.data.split(":")[2])
    await state.update_data(target_uid=uid)
    await callback.message.edit_text(
        "✨ *Тариф:*",
        reply_markup=KB.give_vip_tiers(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()

@rt.callback_query(F.data.startswith("gv:"))
async def admin_gv_tier(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return

    tier = callback.data[3:]

    if tier == "vip_lifetime":
        d = await state.get_data()
        await DB.activate_subscription_by_id(d["target_uid"], tier, 0)
        await state.clear()
        await callback.message.edit_text(
            "✅ *VIP Lifetime выдан!*",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await state.update_data(give_tier=tier)
        await callback.message.edit_text(
            "⏰ *Дней? Введи число:*",
            parse_mode=ParseMode.MARKDOWN,
        )
        await state.set_state(AdminStates.give_vip_duration)

    await callback.answer()

@rt.message(AdminStates.give_vip_duration)
async def admin_gv_days(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return

    try:
        days = int(message.text.strip())
    except:
        await message.answer("⚠️ Число!")
        return

    d = await state.get_data()
    await DB.activate_subscription_by_id(d["target_uid"], d["give_tier"], days)
    await state.clear()

    await message.answer(
        f"✅ *{TIER_NAMES.get(d['give_tier'], 'VIP')}* на {days}дн выдан!",
        reply_markup=KB.main(),
        parse_mode=ParseMode.MARKDOWN,
    )

@rt.callback_query(F.data.startswith("au:giveboost:"))
async def admin_give_boost(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return

    uid = int(callback.data.split(":")[2])
    await state.update_data(target_uid=uid)
    await callback.message.edit_text(
        "🚀 *Сколько бустов?*",
        parse_mode=ParseMode.MARKDOWN,
    )
    await state.set_state(AdminStates.give_boost_count)
    await callback.answer()

@rt.message(AdminStates.give_boost_count)
async def admin_gb_count(message: Message, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return

    try:
        count = int(message.text.strip())
    except:
        await message.answer("⚠️ Число!")
        return

    d = await state.get_data()
    await DB.add_boosts(d["target_uid"], count)
    await state.clear()

    await message.answer(
        f"✅ *{count} бустов* выдано!",
        reply_markup=KB.main(),
        parse_mode=ParseMode.MARKDOWN,
    )

@rt.callback_query(F.data == "adm:reports")
async def admin_reports(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return

    reps = await DB.get_pending_reports(5)

    if not reps:
        await callback.message.edit_text(
            "✅ *Нет жалоб!*",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN,
        )
        await callback.answer()
        return

    rep = reps[0]
    rn = rep["reporter"]["name"] if rep["reporter"] else "?"
    rdn = rep["reported"]["name"] if rep["reported"] else "?"
    rid = rep["reported"]["id"] if rep["reported"] else 0

    txt = (
        f"🚩 *Жалоба #{rep['id']}*\n\n"
        f"На: *{rdn}* (ID:{rid})\n"
        f"От: *{rn}*\n"
        f"Причина: *{rep['reason']}*\n"
        f"📅 {rep['created_at'].strftime('%d.%m %H:%M')}\n\n"
        f"Всего: {len(reps)}"
    )

    await callback.message.edit_text(
        txt,
        reply_markup=KB.admin_report(rep["id"], rid),
        parse_mode=ParseMode.MARKDOWN,
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
        await callback.message.edit_text("🚫 Забанен", reply_markup=KB.back_admin())

    elif action == "warn":
        u = await DB.get_user_by_id(ruid)
        if u:
            try:
                await callback.bot.send_message(
                    u["telegram_id"],
                    "⚠️ Предупреждение от модерации!",
                )
            except:
                pass
        await DB.resolve_report(rid, "warned")
        await callback.message.edit_text("⚠️ Предупреждён", reply_markup=KB.back_admin())

    elif action == "dismiss":
        await DB.resolve_report(rid, "dismissed")
        await callback.message.edit_text("❌ Отклонено", reply_markup=KB.back_admin())

    await callback.answer()

@rt.callback_query(F.data == "adm:payments")
async def admin_payments(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return

    pays = await DB.get_recent_payments(10)

    if not pays:
        await callback.message.edit_text(
            "💬 *Нет платежей*",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN,
        )
        await callback.answer()
        return

    txt = "💳 *Платежи:*\n\n"
    for p in pays:
        st = {"pending": "⏳", "succeeded": "✅", "canceled": "❌"}.get(p["status"], "?")
        txt += f"{st} {p['amount']:.0f}₽ · {p['user_name']} · {p['description'] or '-'}\n"

    await callback.message.edit_text(
        txt,
        reply_markup=KB.back_admin(),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()

@rt.callback_query(F.data == "adm:top")
async def admin_top(callback: CallbackQuery, user: Optional[Dict]):
    if not is_adm(user):
        return

    async with async_session_maker() as s:
        tl = await s.execute(
            select(User)
            .where(User.is_profile_complete == True)
            .order_by(desc(User.likes_received_count))
            .limit(5)
        )
        tv = await s.execute(
            select(User)
            .where(User.is_profile_complete == True)
            .order_by(desc(User.views_count))
            .limit(5)
        )

        txt = "❤️ *Топ по лайкам:*\n"
        for i, u in enumerate(tl.scalars().all(), 1):
            txt += f"{i}. {u.name} — {u.likes_received_count}\n"

        txt += "\n👁️ *Топ по просмотрам:*\n"
        for i, u in enumerate(tv.scalars().all(), 1):
            txt += f"{i}. {u.name} — {u.views_count}\n"

        await callback.message.edit_text(
            txt,
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN,
        )

    await callback.answer()

@rt.callback_query(F.data == "adm:broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return

    await callback.message.edit_text(
        "📢 *Текст рассылки:*\n_Markdown поддерживается_",
        parse_mode=ParseMode.MARKDOWN,
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
        parse_mode=ParseMode.MARKDOWN,
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
            f"📢 *Отправка {len(ids)}...*",
            parse_mode=ParseMode.MARKDOWN,
        )

        sent = 0
        failed = 0

        for tid in ids:
            try:
                await callback.bot.send_message(
                    tid,
                    txt,
                    parse_mode=ParseMode.MARKDOWN,
                )
                sent += 1
            except:
                failed += 1

            if sent % 25 == 0:
                await asyncio.sleep(1)

        await DB.log_broadcast(user["telegram_id"], txt, tgt, sent, failed)
        await callback.message.answer(
            f"✅ *Готово!*\n✅ {sent}\n❌ {failed}",
            reply_markup=KB.back_admin(),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await state.update_data(bc_target=target)
        d = await state.get_data()
        names = {
            "all": "Все",
            "complete": "С анкетой",
            "vip": "VIP",
            "free": "Бесплатные",
        }
        ids = await DB.get_all_user_ids(target)

        await callback.message.edit_text(
            T.BROADCAST_CONFIRM.format(
                text=d["bc_text"][:200],
                target=names.get(target, target),
                count=len(ids),
            ),
            reply_markup=KB.broadcast_confirm(),
            parse_mode=ParseMode.MARKDOWN,
        )

    await callback.answer()

@rt.callback_query(F.data == "adm:promo")
async def admin_promo_start(callback: CallbackQuery, state: FSMContext, user: Optional[Dict]):
    if not is_adm(user):
        return

    await callback.message.edit_text(
        "🎁 *Введи код промокода:*",
        parse_mode=ParseMode.MARKDOWN,
    )
    await state.update_data(promo_user_mode=False)
    await state.set_state(AdminStates.promo_code)
    await callback.answer()

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
