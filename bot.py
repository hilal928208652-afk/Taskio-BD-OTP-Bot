"""
╔══════════════════════════════════════════════════════════════════╗
║         TASKIO BD PRO — BOT ENGINE v8.0 (SUPABASE CLOUD)        ║
║     Enterprise Distributed Engine • Ultra Secure PostgreSQL      ║
╚══════════════════════════════════════════════════════════════════╝
"""

import re
import os
import asyncpg
import logging
import asyncio
import hashlib
import time
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any

import httpx

# TELEGRAM CORE IMPORTS
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError

# ══════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════
BOT_TOKEN = "8679974247:AAFQD5ozSf3D5oXay02_R1UEu41VYrYUzBI"
MAPIKEY   = "ZNX_AA17NFTNRW9LZOETYOOB2P9Z"
ADMIN_IDS = {6488766623}

EARN_PER_OTP    = 0.40
OTP_REFRESH_SEC = 5
OTP_TIMEOUT_SEC = 600
MIN_WITHDRAW    = 50.00

API_BASE      = "https://api.zenexnetwork.com/v1"
SUPPORT_USER  = "taskiobd_admin_1"
SUPPORT_URL   = f"https://t.me/{SUPPORT_USER}"
WEB_STORE_URL = "https://taskiobd.top/insta_v2.php"

# ⚠️ আপনার Supabase Project Settings > Database থেকে Connection String (URI) এখানে বসান
SUPABASE_DATABASE_URL = "postgresql://postgres.awagkwgskbdmwrlgbwgs:%211%402%233%244%255Qa%40@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres?sslmode=require"

# ══════════════════════════════════════════════════════════════════
#  LOGGING & UTILS
# ══════════════════════════════════════════════════════════════════
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("TaskioBD.v8")

API_HEADERS = {"mapikey": MAPIKEY, "Content-Type": "application/json"}
DIV  = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
DIV2 = "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰"

# ══════════════════════════════════════════════════════════════════
#  SUPABASE/POSTGRESQL DB WRAPPERS (CONCURRENCY SAFE)
# ══════════════════════════════════════════════════════════════════
_db_pool: Optional[asyncpg.Pool] = None

def _convert_row(row) -> Optional[dict]:
    if not row:
        return None
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, Decimal):
            d[k] = float(v)
    return d

async def db_write(query: str, params: tuple = ()) -> Any:
    global _db_pool
    async with _db_pool.acquire() as conn:
        if "INSERT INTO otp_history" in query:
            query += " RETURNING id"
            return await conn.fetchval(query, *params)
        if "INSERT INTO support_messages" in query:
            query += " RETURNING id"
            return await conn.fetchval(query, *params)
        return await conn.execute(query, *params)

async def db_read_one(query: str, params: tuple = ()) -> Optional[dict]:
    global _db_pool
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(query, *params)
        return _convert_row(row)

async def db_read_all(query: str, params: tuple = ()) -> List[dict]:
    global _db_pool
    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
        return [_convert_row(r) for r in rows]

async def init_db(pool: asyncpg.Pool):
    """Initializes schema directly on Supabase PostgreSQL instance."""
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username VARCHAR(100),
            first_name VARCHAR(100),
            joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            account_status VARCHAR(20) DEFAULT 'active',
            balance NUMERIC DEFAULT 0.0,
            total_earned NUMERIC DEFAULT 0.0,
            successful_otps INT DEFAULT 0,
            withdrawn_amount NUMERIC DEFAULT 0.0,
            pending_withdraw NUMERIC DEFAULT 0.0,
            total_withdraw_amount NUMERIC DEFAULT 0.0,
            ban_reason TEXT,
            notes TEXT,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            last_active TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS active_sessions (
            user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            phone_number VARCHAR(20),
            service VARCHAR(50),
            range_code VARCHAR(50),
            history_id VARCHAR(50),
            started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS otp_hashes (
            hash VARCHAR(64) PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            phone VARCHAR(20),
            otp_code VARCHAR(20),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS otp_history (
            id SERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            phone_number VARCHAR(20),
            service VARCHAR(50),
            range_code VARCHAR(50),
            otp_code VARCHAR(20),
            otp_hash VARCHAR(64),
            full_message TEXT,
            earned NUMERIC DEFAULT 0.0,
            status VARCHAR(20) DEFAULT 'Pending',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            completed_at TIMESTAMP WITH TIME ZONE
        );
        CREATE TABLE IF NOT EXISTS withdraw_ledger (
            id VARCHAR(50) PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            username VARCHAR(100),
            amount NUMERIC DEFAULT 0.0,
            status VARCHAR(20) DEFAULT 'Pending',
            admin_note TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            resolved_at TIMESTAMP WITH TIME ZONE
        );
        CREATE TABLE IF NOT EXISTS support_messages (
            id SERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            username VARCHAR(100),
            message TEXT,
            status VARCHAR(20) DEFAULT 'open',
            reply TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            resolved_at TIMESTAMP WITH TIME ZONE
        );
        CREATE TABLE IF NOT EXISTS admin_audit_log (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT,
            action VARCHAR(50),
            target_id BIGINT,
            details TEXT,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)

# ══════════════════════════════════════════════════════════════════
#  CONCURRENCY / RATE LIMITING
# ══════════════════════════════════════════════════════════════════
_user_locks:   Dict[int, asyncio.Lock]       = {}
_active_tasks: Dict[int, List[asyncio.Task]] = {}
_rate_cache:   Dict[int, Dict]               = {}
RATE_WINDOW = 10
RATE_LIMIT  = 5

def get_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]

def register_task(user_id: int, task: asyncio.Task) -> None:
    _active_tasks.setdefault(user_id, [])
    _active_tasks[user_id] = [t for t in _active_tasks[user_id] if not t.done()]
    _active_tasks[user_id].append(task)

def check_rate_limit(user_id: int) -> bool:
    now   = time.time()
    entry = _rate_cache.get(user_id)
    if not entry or (now - entry["ts"]) > RATE_WINDOW:
        _rate_cache[user_id] = {"ts": now, "count": 1}
        return True
    if entry["count"] >= RATE_LIMIT:
        return False
    entry["count"] += 1
    return True

# ══════════════════════════════════════════════════════════════════
#  LOGIC ENGINE TRANSLATION LAYER
# ══════════════════════════════════════════════════════════════════
async def ensure_user(client: httpx.AsyncClient, user_id: int, username: str, first_name: str = "") -> None:
    query = """
        INSERT INTO users (user_id, username, first_name, updated_at, last_active)
        VALUES ($1, $2, $3, NOW(), NOW())
        ON CONFLICT (user_id) DO UPDATE
        SET username = EXCLUDED.username, first_name = EXCLUDED.first_name, updated_at = NOW(), last_active = NOW();
    """
    await db_write(query, (user_id, (username or "Unknown")[:64], (first_name or "")[:64]))

async def get_user(client: httpx.AsyncClient, user_id: int) -> Optional[dict]:
    u = await db_read_one("SELECT * FROM users WHERE user_id = $1", (user_id,))
    if not u:
        return None
    # UI এর পুরাতন কি (Keys) গুলোর সাথে ম্যাপিং কম্প্যাটিবিলিটি বজায় রাখা
    u["is_banned"] = 1 if u["account_status"] == "banned" else 0
    u["created_at"] = u["joined_at"].isoformat() if u["joined_at"] else ""
    u["last_active"] = u["last_active"].isoformat() if u["last_active"] else ""
    u["total_earnings"] = u["total_earned"]
    u["otp_count"] = u["successful_otps"]
    u["total_withdrawn"] = u["withdrawn_amount"]
    
    cnt_res = await db_read_one("SELECT COUNT(*) as cnt FROM withdraw_ledger WHERE user_id = $1", (user_id,))
    u["withdraw_count"] = cnt_res["cnt"] if cnt_res else 0
    return u

async def is_banned(client: httpx.AsyncClient, user_id: int) -> tuple[bool, str]:
    data = await db_read_one("SELECT account_status, ban_reason FROM users WHERE user_id = $1", (user_id,))
    if data and data["account_status"] == "banned":
        return True, data["ban_reason"] or ""
    return False, ""

async def credit_otp(
    client: httpx.AsyncClient,
    user_id: int, phone: str, service: str,
    otp_code: str, full_msg: str, h: str,
    history_doc_id: Optional[str] = None,
) -> bool:
    dup = await db_read_one("SELECT hash FROM otp_hashes WHERE hash = $1", (h,))
    if dup:
        return False
        
    user_data = await db_read_one("SELECT balance, total_earned FROM users WHERE user_id = $1", (user_id,))
    if not user_data:
        return False
        
    earn = EARN_PER_OTP
    new_bal = round(user_data["balance"] + earn, 4)
    new_earn = round(user_data["total_earned"] + earn, 4)
    
    await db_write("INSERT INTO otp_hashes (hash, user_id, phone, otp_code) VALUES ($1, $2, $3, $4)",
                   (h, user_id, phone, otp_code))
                   
    await db_write(
        """UPDATE users 
           SET balance = $1, total_earned = $2, successful_otps = successful_otps + 1, updated_at = NOW() 
           WHERE user_id = $3""",
        (new_bal, new_earn, user_id)
    )
    
    if history_doc_id:
        await db_write(
            """UPDATE otp_history 
               SET otp_code = $1, otp_hash = $2, full_message = $3, earned = $4, status = 'Completed', completed_at = NOW() 
               WHERE id = $5""",
            (otp_code, h, full_msg, earn, int(history_doc_id))
        )
    logger.info("OTP credited on Supabase Cloud | user=%d otp=%s +৳%.2f", user_id, otp_code, earn)
    return True

async def cancel_user_session(client: httpx.AsyncClient, user_id: int) -> None:
    if user_id in _active_tasks:
        for t in _active_tasks[user_id]:
            if not t.done():
                t.cancel()
        del _active_tasks[user_id]
        
    await db_write("UPDATE otp_history SET status = 'Cancelled' WHERE user_id = $1 AND status = 'Pending'", (user_id,))
    await db_write("DELETE FROM active_sessions WHERE user_id = $1", (user_id,))

async def log_admin_action(client: httpx.AsyncClient, admin_id: int, action: str, target_id: int, details: str) -> None:
    await db_write(
        "INSERT INTO admin_audit_log (admin_id, action, target_id, details) VALUES ($1, $2, $3, $4)",
        (admin_id, action, target_id, details)
    )

async def notify_admins(bot, client: httpx.AsyncClient, text: str, markup: InlineKeyboardMarkup = None) -> None:
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(chat_id=aid, text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
        except TelegramError as e:
            logger.warning("Admin notify failed %d: %s", aid, e)

# ══════════════════════════════════════════════════════════════════
#  ZENEX API HELPERS
# ══════════════════════════════════════════════════════════════════
async def api_get_ranges(client: httpx.AsyncClient) -> Optional[list]:
    try:
        r = await client.get(f"{API_BASE}/active-ranges", timeout=12)
        r.raise_for_status()
        res = r.json()
        if res.get("success") and "data" in res:
            return res["data"].get("active_ranges", [])
    except Exception as e:
        logger.error("api_get_ranges: %s", e)
    return None

async def api_get_number(client: httpx.AsyncClient, range_code: str) -> Optional[dict]:
    try:
        r = await client.post(
            f"{API_BASE}/getnum",
            json={"range": str(range_code), "is_national": False, "remove_plus": False},
            timeout=12,
        )
        r.raise_for_status()
        res = r.json()
        if "data" in res and res["data"] and "number" in res["data"]:
            return res["data"]
    except Exception as e:
        logger.error("api_get_number: %s", e)
    return None

async def api_fetch_sms(client: httpx.AsyncClient, phone: str) -> Optional[dict]:
    try:
        r = await client.get(f"{API_BASE}/numsuccess/info", timeout=10)
        r.raise_for_status()
        clean = phone.lstrip("+").strip()
        for item in r.json().get("data", {}).get("otps", []):
            if str(item.get("number", "")).lstrip("+").strip() == clean:
                return {"otp": item.get("otp", ""), "full_message": item.get("otp", "")}
    except Exception as e:
        logger.error("api_fetch_sms: %s", e)
    return None

def make_otp_hash(phone: str, otp: str) -> str:
    return hashlib.sha256(f"{phone}:{otp}".encode()).hexdigest()

def extract_otp(text: str) -> str:
    if not text:
        return ""
    m = re.search(r'\b(\d{3,4}[-\s]?\d{3,4})\b', text)
    if m:
        return m.group(1).replace(" ", "").replace("-", "")
    m2 = re.search(r'\b(\d{4,8})\b', text)
    return m2.group(1) if m2 else ""

def md(text: Any) -> str:
    return re.sub(r'([_*`\[])', r'\\\1', str(text or ""))

# ══════════════════════════════════════════════════════════════════
#  UI HELPERS
# ══════════════════════════════════════════════════════════════════
def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([
        [KeyboardButton("📱 Get Number")],
        [KeyboardButton("💸 Withdraw"),    KeyboardButton("💰 Balance")],
        [KeyboardButton("🛒 Shop"),         KeyboardButton("📜 History")],
        [KeyboardButton("👤 Profile"),      KeyboardButton("❔ Support")],
    ], resize_keyboard=True, is_persistent=True, input_field_placeholder="Taskio BD — Main Menu")

async def build_services_kb(client: httpx.AsyncClient) -> Optional[InlineKeyboardMarkup]:
    ranges = await api_get_ranges(client)
    if not ranges:
        return None
    ICONS = ["📱","🌐","💬","📡","⚡","🔷","🔵","🟢","🟡","🔴"]
    rows, row = [], []
    for idx, r in enumerate(ranges):
        if not isinstance(r, dict):
            continue
        rv  = str(r.get("range",   "")).strip()
        svc = str(r.get("service", "Unknown")).strip()[:20]
        tag = str(r.get("tag",     "")).strip()
        hits = r.get("hits", 0)
        if not rv:
            continue
        label = f"{ICONS[idx % len(ICONS)]} {svc}"
        if tag:  label += f" [{tag}]"
        if hits: label += f" •{hits}"
        row.append(InlineKeyboardButton(label, callback_data=f"srv:{rv}:{svc}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("🔄 রিফ্রেশ", callback_data="refresh_services")])
    return InlineKeyboardMarkup(rows) if rows else None

def _get_client(ctx: ContextTypes.DEFAULT_TYPE) -> httpx.AsyncClient:
    return ctx.application.bot_data["http_client"]

# ══════════════════════════════════════════════════════════════════
#  BACKGROUND OTP WORKER
# ══════════════════════════════════════════════════════════════════
async def otp_worker(
    bot, client: httpx.AsyncClient,
    user_id: int, phone: str, service: str, history_doc_id: str,
) -> None:
    start     = asyncio.get_event_loop().time()
    otp_found = False
    last_hash = ""
    logger.info("OTP worker start | user=%d phone=%s", user_id, phone)
    try:
        while (asyncio.get_event_loop().time() - start) < OTP_TIMEOUT_SEC:
            await asyncio.sleep(OTP_REFRESH_SEC)
            sms = await api_fetch_sms(client, phone)
            if not (sms and sms.get("otp")):
                continue
            full_msg = sms["full_message"]
            otp_code = extract_otp(full_msg)
            if not otp_code:
                continue
            h = make_otp_hash(phone, otp_code)
            if h == last_hash:
                continue
            last_hash = h
            async with get_lock(user_id):
                credited = await credit_otp(client, user_id, phone, service, otp_code, full_msg, h, history_doc_id)
            if not credited:
                continue
            otp_found = True
            u = await get_user(client, user_id)
            new_bal = u["balance"] if u else 0.0
            earn    = EARN_PER_OTP
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"🎉 *OTP পাওয়া গেছে\\!*\n"
                        f"{DIV}\n"
                        f"📱 *সার্ভিস:* {md(service)}\n"
                        f"📞 *নাম্বার:* `{phone}`\n"
                        f"🔐 *OTP কোড:* `{otp_code}`\n\n"
                        f"💬 *পূর্ণ মেসেজ:*\n{md(full_msg)}\n"
                        f"{DIV}\n"
                        f"💰 *\\+৳{earn:.2f}  ক্রেডিট হয়েছে*\n"
                        f"💳 *ব্যালেন্স:* `৳{new_bal:.2f}`"
                    ),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(f"📋 OTP কপি ({otp_code})", callback_data=f"copy:{otp_code}")
                    ]]),
                )
            except TelegramError as te:
                logger.warning("Notify failed user=%d: %s", user_id, te)
            break

        await db_write("DELETE FROM active_sessions WHERE user_id = $1", (user_id,))
        if not otp_found:
            await db_write("UPDATE otp_history SET status = 'Timeout' WHERE id = $1", (int(history_doc_id),))
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"⏰ *সময়সীমা শেষ\\!*\n"
                        f"{DIV}\n"
                        f"`{phone}` 에 {OTP_TIMEOUT_SEC // 60} মিনিটে OTP আসেনি।\n"
                        f"নতুন নাম্বারের জন্য *📱 Get Number* চাপুন।"
                    ),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=main_keyboard(),
                )
            except TelegramError:
                pass
    except asyncio.CancelledError:
        logger.info("OTP worker cancelled | user=%d", user_id)
        await db_write("DELETE FROM active_sessions WHERE user_id = $1", (user_id,))
        try:
            await db_write("UPDATE otp_history SET status = 'Cancelled' WHERE id = $1", (int(history_doc_id),))
        except Exception:
            pass
    except Exception as e:
        logger.exception("OTP worker crash | user=%d: %s", user_id, e)
        await db_write("DELETE FROM active_sessions WHERE user_id = $1", (user_id,))

# ══════════════════════════════════════════════════════════════════
#  NUMBER ALLOCATION ENGINE
# ══════════════════════════════════════════════════════════════════
async def allocate_number(
    bot, client: httpx.AsyncClient,
    user_id: int, range_code: str, service: str,
) -> None:
    await cancel_user_session(client, user_id)
    num_res = await api_get_number(client, range_code)
    if not num_res or not num_res.get("number"):
        await bot.send_message(
            chat_id=user_id,
            text=(
                f"❌ *নাম্বার পাওয়া যায়নি*\n{DIV}\n"
                f"`{md(service)}` চ্যানেলে এখন ফ্রি নাম্বার নেই।\n"
                f"কিছুক্ষণ পর চেষ্টা করুন বা অন্য সার্ভিস বেছে নিন।"
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    phone = str(num_res["number"]).strip()
    
    history_id = await db_write(
        """INSERT INTO otp_history (user_id, phone_number, service, range_code, status) 
           VALUES ($1, $2, $3, $4, 'Pending')""",
        (user_id, phone, service, range_code)
    )
    
    await db_write(
        """INSERT INTO active_sessions (user_id, phone_number, service, range_code, history_id) 
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (user_id) DO UPDATE 
           SET phone_number = EXCLUDED.phone_number, service = EXCLUDED.service, 
               range_code = EXCLUDED.range_code, history_id = EXCLUDED.history_id, started_at = NOW()""",
        (user_id, phone, service, range_code, str(history_id))
    )
    
    await bot.send_message(
        chat_id=user_id,
        text=(
            f"✅ *নাম্বার বরাদ্দ সফল\\!*\n{DIV}\n"
            f"📞 *নাম্বার:* `{phone}`  _\\(ট্যাপ করে কপি করুন\\)_\n"
            f"📢 *সার্ভিস:* {md(service)}\n"
            f"📡 *চ্যানেল:* `{range_code}`\n"
            f"⏱️ *সক্রিয় সময়:* {OTP_TIMEOUT_SEC // 60} মিনিট\n"
            f"{DIV}\n"
            f"🔔 OTP আসলে স্বয়ংক্রিয়ভাবে জানানো হবে।"
        ),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 নতুন নাম্বার",  callback_data=f"change:{range_code}:{service}"),
                InlineKeyboardButton("📨 OTP চেক করুন", callback_data=f"manual_otp:{phone}:{service}"),
            ],
            [InlineKeyboardButton("↩️ সার্ভিস লিস্ট", callback_data="return_services")],
            [InlineKeyboardButton("🏠 মেইন মেনু",      callback_data="go_home")],
        ]),
    )
    task = asyncio.create_task(
        otp_worker(bot, client, user_id, phone, service, str(history_id)),
        name=f"otp_{user_id}",
    )
    register_task(user_id, task)

# ══════════════════════════════════════════════════════════════════
#  WITHDRAW LOGIC (INTEGRATED STATISTICS TRACKING)
# ══════════════════════════════════════════════════════════════════
async def process_withdraw(send_fn, client: httpx.AsyncClient, user_id: int, username: str, bot=None) -> None:
    async with get_lock(user_id):
        user_data = await db_read_one("SELECT balance FROM users WHERE user_id = $1", (user_id,))
        if not user_data:
            await send_fn("❌ ইউজার ডেটা পাওয়া যায়নি।")
            return
        balance = user_data["balance"]
        if balance < MIN_WITHDRAW:
            await send_fn(
                f"❌ *উইথড্র ব্যর্থ\\!*\n{DIV}\n"
                f"সর্বনিম্ন উইথড্র: `৳{MIN_WITHDRAW:.2f}`\n"
                f"আপনার ব্যালেন্স: `৳{balance:.2f}`\n\n"
                f"আরও `৳{MIN_WITHDRAW - balance:.2f}` আয় করুন।",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
        
        wd_id = f"WD{int(time.time())}{user_id}"
        
        # ব্যালেন্স কেটে পেন্ডিং এবং টোটাল উইথড্র ক্যাটাগরিতে যোগ করা
        await db_write(
            """UPDATE users 
               SET balance = 0.0, 
                   pending_withdraw = pending_withdraw + $1, 
                   total_withdraw_amount = total_withdraw_amount + $1, 
                   updated_at = NOW() 
               WHERE user_id = $2""", 
            (balance, user_id)
        )
        
        await db_write(
            "INSERT INTO withdraw_ledger (id, user_id, username, amount, status) VALUES ($1, $2, $3, $4, 'Pending')",
            (wd_id, user_id, username or "Unknown", balance)
        )

    if bot:
        admin_text = (
            f"💸 *নতুন উইথড্র রিকোয়েস্ট\\!*\n{DIV2}\n"
            f"👤 *ইউজার:* @{md(username or 'Unknown')}\n"
            f"🆔 *User ID:* `{user_id}`\n"
            f"💰 *পরিমাণ:* `৳{balance:.2f} BDT`\n"
            f"📋 *Ledger ID:* `{wd_id}`\n"
            f"👑 *ওল্ড ফান্ড:* `৳{balance:.2f} BDT`\n"
            f"🕐 *সময়:* `Just Now`\n"
            f"{DIV2}"
        )
        admin_kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ অ্যাপ্রুভ",  callback_data=f"wd_approve:{user_id}:{wd_id}"),
                InlineKeyboardButton("❌ রিজেক্ট",    callback_data=f"wd_reject:{user_id}:{wd_id}"),
            ],
            [InlineKeyboardButton("👤 ইউজার দেখুন", callback_data=f"admin_view:{user_id}")],
        ])
        await notify_admins(bot, client, admin_text, admin_kb)

    msg = (
        f"🔔 Withdraw Request — Taskio BD\n\n"
        f"User ID   : {user_id}\n"
        f"Username  : @{username or 'None'}\n"
        f"Amount    : ৳{balance:.2f} BDT\n"
        f"Ledger ID : {wd_id}"
    )
    admin_url = f"https://t.me/{SUPPORT_USER}?text={urllib.parse.quote(msg)}"
    await send_fn(
        f"✅ *উইথড্র রিকোয়েস্ট তৈরি\\!*\n{DIV}\n"
        f"💵 *পরিমাণ:* `৳{balance:.2f}`\n"
        f"🆔 *লেজার ID:* `{wd_id}`\n"
        f"📋 *স্ট্যাটাস:* Pending ⏳\n"
        f"{DIV}\n"
        f"অ্যাডমিন বিকাশ/নগদ নম্বর চাইবে। নিচে ক্লিক করুন।",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📩 অ্যাডমিনকে মেসেজ করুন", url=admin_url)
        ]]),
    )

# ══════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user   = update.effective_user
    client = _get_client(ctx)
    await ensure_user(client, user.id, user.username, user.first_name or "")
    await update.message.reply_text(
        f"🤖 *Taskio BD Pro Supabase — স্বাগতম\\!* ⚡\n"
        f"{DIV}\n"
        f"ভার্চুয়াল নাম্বারে OTP যাচাই করুন এবং প্রতিটি সফল OTP\\-এ আয় করুন\\!\n\n"
        f"💰 *প্রতি OTP আয়:* `৳{EARN_PER_OTP:.2f}`\n"
        f"⏱ *OTP রিফ্রেশ:* প্রতি `{OTP_REFRESH_SEC}` সেকেন্ডে\n"
        f"💳 *সর্বনিম্ন উইথড্র:* `৳{MIN_WITHDRAW:.2f}`\n"
        f"🔥 *ডেটাবেজ:* Supabase Enterprise PostgreSQL Cluster ☁\n"
        f"{DIV}\n"
        f"নিচের মেনু থেকে শুরু করুন 👇",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_keyboard(),
    )

# ══════════════════════════════════════════════════════════════════
#  ADMIN PANEL UI
# ══════════════════════════════════════════════════════════════════
def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 স্ট্যাটিস্টিক্স", callback_data="ap:stats"), InlineKeyboardButton("👥 সব ইউজার", callback_data="ap:users")],
        [InlineKeyboardButton("🔍 ইউজার খুঁজুন", callback_data="ap:find_user"), InlineKeyboardButton("💸 পেন্ডিং উইথড্র", callback_data="ap:pending_wd")],
        [InlineKeyboardButton("📢 ব্রডকাস্ট", callback_data="ap:broadcast"), InlineKeyboardButton("🚫 ব্যান্ড ইউজার", callback_data="ap:banned")],
        [InlineKeyboardButton("📋 অডিট লগ", callback_data="ap:audit"), InlineKeyboardButton("💬 সাপোর্ট মেসেজ", callback_data="ap:support_msgs")],
        [InlineKeyboardButton("⚙ সেটিংস", callback_data="ap:settings")]
    ])

def user_manage_keyboard(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 ব্যালেন্স যোগ", callback_data=f"adm_addbal:{target_id}"), InlineKeyboardButton("💸 ব্যালেন্স কাটুন", callback_data=f"adm_subbal:{target_id}")],
        [InlineKeyboardButton("🚫 ব্যান করুন", callback_data=f"adm_ban:{target_id}"), InlineKeyboardButton("✅ আনব্যান করুন", callback_data=f"adm_unban:{target_id}")],
        [InlineKeyboardButton("📝 নোট যোগ করুন", callback_data=f"adm_note:{target_id}"), InlineKeyboardButton("📜 OTP হিস্টোরি", callback_data=f"adm_otp_hist:{target_id}")],
        [InlineKeyboardButton("💬 মেসেজ পাঠান", callback_data=f"adm_msg:{target_id}"), InlineKeyboardButton("🔄 রিফ্রেশ", callback_data=f"admin_view:{target_id}")],
        [InlineKeyboardButton("◀ Admin Panel", callback_data="ap:main")],
    ])

async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        return
    await update.message.reply_text(
        f"🔧 *Admin Panel — Taskio BD*\n{DIV2}\n"
        f"স্বাগতম, {md(user.first_name)}\\! নিচের মেনু থেকে অ্যাকশন বেছে নিন 👇",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=admin_panel_keyboard(),
    )

async def handle_admin_callback(query, data: str, user, client, ctx) -> None:
    bot = ctx.application.bot

    if data == "ap:main":
        await query.edit_message_text(
            f"🔧 *Admin Panel — Taskio BD*\n{DIV2}\nনিচের মেনু থেকে অ্যাকশন বেছে নিন 👇",
            parse_mode=ParseMode.MARKDOWN, reply_markup=admin_panel_keyboard(),
        )
        return

    if data == "ap:stats":
        total_users  = (await db_read_one("SELECT COUNT(*) as cnt FROM users"))["cnt"]
        banned       = (await db_read_one("SELECT COUNT(*) as cnt FROM users WHERE account_status = 'banned'"))["cnt"]
        total_otps   = (await db_read_one("SELECT COUNT(*) as cnt FROM otp_history WHERE status = 'Completed'"))["cnt"]
        total_paid   = (await db_read_one("SELECT SUM(total_earned) as total FROM users"))["total"] or 0.0
        pending_wd   = (await db_read_one("SELECT SUM(amount) as total FROM withdraw_ledger WHERE status = 'Pending'"))["total"] or 0.0
        paid_wd      = (await db_read_one("SELECT SUM(amount) as total FROM withdraw_ledger WHERE status = 'Completed'"))["total"] or 0.0
        pending_otps = (await db_read_one("SELECT COUNT(*) as cnt FROM otp_history WHERE status = 'Pending'"))["cnt"]
        
        await query.edit_message_text(
            f"📊 *Bot Statistics — Taskio BD*\n{DIV2}\n"
            f"👥 মোট ইউজার: `{total_users}`\n"
            f"🚫 ব্যান্ড: `{banned}`\n"
            f"{DIV}\n"
            f"🔐 মোট OTP সম্পন্ন: `{total_otps}`\n"
            f"⏳ OTP পেন্ডিং: `{pending_otps}`\n"
            f"💰 মোট আয় বিতরণ: `৳{total_paid:.2f}`\n"
            f"{DIV}\n"
            f"⏳ পেন্ডিং উইথড্র: `৳{pending_wd:.2f}`\n"
            f"✅ পরিশোধিত উইথড্র: `৳{paid_wd:.2f}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")]]),
        )
        return

    if data == "ap:pending_wd":
        wds = await db_read_all("SELECT * FROM withdraw_ledger WHERE status = 'Pending' ORDER BY created_at DESC LIMIT 10")
        if not wds:
            await query.edit_message_text(f"✅ কোনো পেন্ডিং উইথড্র নেই\\!", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")]]))
            return
        rows = [[InlineKeyboardButton(f"@{w['username']} ৳{w['amount']:.0f}", callback_data=f"wd_detail:{w['user_id']}:{w['id']}")] for w in wds]
        rows.append([InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")])
        await query.edit_message_text(f"💸 *পেন্ডিং উইথড্র রিকোয়েস্ট* \\({len(wds)}টি\\)\n{DIV}", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(rows))
        return

    if data.startswith("wd_detail:"):
        parts = data.split(":")
        if len(parts) == 3:
            target_id, wd_id = int(parts[1]), parts[2]
            wd = await db_read_one("SELECT * FROM withdraw_ledger WHERE id = $1", (wd_id,))
            if wd:
                await query.edit_message_text(
                    f"💸 *উইথড্র বিবরণ*\n{DIV2}\n👤 ইউজার: @{md(wd['username'])}\n🆔 User ID: `{target_id}`\n💰 পরিমাণ: `৳{wd['amount']:.2f}`\n🆔 Ledger: `{wd_id}`\n{DIV2}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ অ্যাপ্রুভ", callback_data=f"wd_approve:{target_id}:{wd_id}"), InlineKeyboardButton("❌ রিজেক্ট", callback_data=f"wd_reject:{target_id}:{wd_id}")],
                        [InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:pending_wd")],
                    ]),
                )
        return

    if data.startswith("wd_approve:"):
        parts = data.split(":")
        if len(parts) == 3:
            target_uid, ledger_id = int(parts[1]), parts[2]
            
            wd_data = await db_read_one("SELECT amount FROM withdraw_ledger WHERE id = $1", (ledger_id,))
            if wd_data:
                amount = wd_data["amount"]
                # ইউজারের কলাম ডেটা রিয়েলটাইম ব্যালেন্স সিঙ্ক করা
                await db_write(
                    """UPDATE users 
                       SET pending_withdraw = GREATEST(0.0, pending_withdraw - $1),
                           withdrawn_amount = withdrawn_amount + $1,
                           updated_at = NOW()
                       WHERE user_id = $2""", (amount, target_uid)
                )

            await db_write("UPDATE withdraw_ledger SET status = 'Completed', resolved_at = NOW(), admin_note = $1 WHERE id = $2", (f"Approved by {user.id}", ledger_id))
            await log_admin_action(client, user.id, "wd_approve", target_uid, f"ledger={ledger_id}")
            await query.edit_message_text(f"✅ উইথড্র `{ledger_id}` অ্যাপ্রুভ করা হয়েছে\\!", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Admin Panel", callback_data="ap:main")]]))
            try:
                await bot.send_message(chat_id=target_uid, text=f"🎉 *উইথড্র অ্যাপ্রুভড\\!*\n{DIV}\nরিকোয়েস্ট `{ledger_id}` অ্যাপ্রুভ হয়েছে।\nশীঘ্রই বিকাশ/নগদে টাকা পাঠানো হবে। ✅", parse_mode=ParseMode.MARKDOWN, reply_markup=main_keyboard())
            except TelegramError: pass
        return

    if data.startswith("wd_reject:"):
        parts = data.split(":")
        if len(parts) == 3:
            target_uid, ledger_id = int(parts[1]), parts[2]
            wd_data = await db_read_one("SELECT amount FROM withdraw_ledger WHERE id = $1", (ledger_id,))
            if wd_data:
                amount = wd_data["amount"]
                # রিজেক্ট হলে পেন্ডিং ফোল্ডার খালি করে মেইন ব্যালেন্সে টাকা রিফান্ড ব্যাক করা
                await db_write(
                    """UPDATE users 
                       SET balance = balance + $1,
                           pending_withdraw = GREATEST(0.0, pending_withdraw - $1),
                           total_withdraw_amount = GREATEST(0.0, total_withdraw_amount - $1),
                           updated_at = NOW()
                       WHERE user_id = $2""", (amount, target_uid)
                )
            await db_write("UPDATE withdraw_ledger SET status = 'Rejected', resolved_at = NOW(), admin_note = $1 WHERE id = $2", (f"Rejected by {user.id}", ledger_id))
            await log_admin_action(client, user.id, "wd_reject", target_uid, f"ledger={ledger_id}")
            await query.edit_message_text(f"❌ উইথড্র `{ledger_id}` রিজেক্ট\\। ব্যালেন্স ফেরত দেওয়া হয়েছে।", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Admin Panel", callback_data="ap:main")]]))
            try:
                await bot.send_message(chat_id=target_uid, text=f"❌ *উইথড্র রিজেক্টেড*\n{DIV}\nরিকোয়েস্ট `{ledger_id}` রিজেক্ট হয়েছে।\nব্যালেন্স ফেরত দেওয়া হয়েছে। সাপোর্টে যোগাযোগ করুন।", parse_mode=ParseMode.MARKDOWN, reply_markup=main_keyboard())
            except TelegramError: pass
        return

    if data.startswith("admin_view:"):
        target_id = int(data.split(":")[1])
        u = await get_user(client, target_id)
        if not u:
            await query.answer("ইউজার পাওয়া যায়নি", show_alert=True)
            return
        status  = "🚫 BANNED" if u["account_status"] == "banned" else "✅ Active"
        await query.edit_message_text(
            f"👤 *ইউজার বিবরণ*\n{DIV2}\n🆔 ID: `{u['user_id']}`\n🏷 Username: @{md(u.get('username','?'))}\n📛 নাম: {md(u.get('first_name','?'))}\n📅 যোগদান: `{u.get('created_at','')[:10]}`\n🔖 স্ট্যাটাস: {status}\n{DIV}\n💳 ব্যালেন্স: `৳{u.get('balance',0):.2f}`\n📈 মোট আয়: `৳{u.get('total_earned',0):.2f}`\n🔢 সফল OTP: `{u.get('successful_otps',0)}` টি\n💸 উইথড্র: `৳{u.get('withdrawn_amount',0):.2f}`\n⏳ পেন্ডিং WD: `৳{u.get('pending_withdraw',0):.2f}`\n📊 মোট রিকোয়েস্ট: `{u.get('total_withdraw_amount',0):.2f}`\n{DIV}\n📝 নোট: {md(u.get('notes','—') or '—')}\n🚫 ব্যান কারণ: {md(u.get('ban_reason','—') or '—')}",
            parse_mode=ParseMode.MARKDOWN, reply_markup=user_manage_keyboard(target_id),
        )
        return

    if data.startswith("adm_addbal:"):
        target_id = int(data.split(":")[1])
        ctx.user_data["adm_action"] = "addbal"
        ctx.user_data["adm_target"] = target_id
        await query.edit_message_text(f"💰 *ব্যালেন্স যোগ করুন*\n{DIV}\nUser ID: `{target_id}`\n\nপরিমাণ লিখুন \\(শুধু সংখ্যা, যেমন: 50\\):", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ বাতিল", callback_data=f"admin_view:{target_id}")]]))
        ctx.user_data["awaiting_admin_input"] = "addbal"
        return

    if data.startswith("adm_subbal:"):
        target_id = int(data.split(":")[1])
        ctx.user_data["adm_action"] = "subbal"
        ctx.user_data["adm_target"] = target_id
        await query.edit_message_text(f"💸 *ব্যালেন্স কাটুন*\n{DIV}\nUser ID: `{target_id}`\n\nকতটুকু কাটবেন লিখুন \\(যেমন: 20\\):", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ বাতিল", callback_data=f"admin_view:{target_id}")]]))
        ctx.user_data["awaiting_admin_input"] = "subbal"
        return

    if data.startswith("adm_ban:"):
        target_id = int(data.split(":")[1])
        ctx.user_data["adm_action"] = "ban"
        ctx.user_data["adm_target"] = target_id
        await query.edit_message_text(f"🚫 *ব্যান কারণ লিখুন*\n{DIV}\nUser ID: `{target_id}`\n\nব্যান করার কারণ লিখুন:", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ বাতিল", callback_data=f"admin_view:{target_id}")]]))
        ctx.user_data["awaiting_admin_input"] = "ban_reason"
        return

    if data.startswith("adm_unban:"):
        target_id = int(data.split(":")[1])
        await db_write("UPDATE users SET account_status = 'active', ban_reason = '', updated_at = NOW() WHERE user_id = $1", (target_id,))
        await log_admin_action(client, user.id, "unban", target_id, "")
        await query.answer("✅ ইউজার আনব্যান করা হয়েছে", show_alert=True)
        try:
            await bot.send_message(chat_id=target_id, text="✅ *আপনার অ্যাকাউন্ট আনব্যান করা হয়েছে।* স্বাগতম ফিরে\\!", parse_mode=ParseMode.MARKDOWN, reply_markup=main_keyboard())
        except TelegramError: pass
        ctx.user_data["adm_action"] = None
        await handle_admin_callback(query, f"admin_view:{target_id}", user, client, ctx)
        return

    if data.startswith("adm_note:"):
        target_id = int(data.split(":")[1])
        ctx.user_data["adm_action"] = "note"
        ctx.user_data["adm_target"] = target_id
        await query.edit_message_text(f"📝 *ইউজার নোট লিখুন*\n{DIV}\nUser ID: `{target_id}`\n\nনোট লিখুন:", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ বাতিল", callback_data=f"admin_view:{target_id}")]]))
        ctx.user_data["awaiting_admin_input"] = "note"
        return

    if data.startswith("adm_msg:"):
        target_id = int(data.split(":")[1])
        ctx.user_data["adm_action"] = "msg"
        ctx.user_data["adm_target"] = target_id
        await query.edit_message_text(f"💬 *ইউজারকে মেসেজ পাঠান*\n{DIV}\nUser ID: `{target_id}`\n\nমেসেজ লিখুন:", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ বাতিল", callback_data=f"admin_view:{target_id}")]]))
        ctx.user_data["awaiting_admin_input"] = "admin_msg"
        return

    if data.startswith("adm_otp_hist:"):
        target_id = int(data.split(":")[1])
        docs = await db_read_all("SELECT * FROM otp_history WHERE user_id = $1 AND status = 'Completed' ORDER BY created_at DESC LIMIT 5", (target_id,))
        if not docs:
            await query.answer("কোনো OTP হিস্টোরি নেই", show_alert=True)
            return
        lines = [f"📜 *User {target_id} OTP History*\n{DIV}"]
        for r in docs:
            lines.append(f"✅ `{r['phone_number']}` | {md(r['service'])}\n   🔐 `{r['otp_code']}` | ৳{r['earned']:.2f}")
        await query.edit_message_text("\n\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ ফিরে যান", callback_data=f"admin_view:{target_id}")]]))
        return

    if data == "ap:find_user":
        ctx.user_data["awaiting_admin_input"] = "find_user"
        await query.edit_message_text(f"🔍 *ইউজার খুঁজুন*\n{DIV}\nTelegram User ID লিখুন:", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ বাতিল", callback_data="ap:main")]]))
        return

    if data == "ap:broadcast":
        ctx.user_data["awaiting_admin_input"] = "broadcast"
        await query.edit_message_text(f"📢 *ব্রডকাস্ট মেসেজ*\n{DIV}\nসব active ইউজারকে পাঠাতে চান এমন মেসেজ লিখুন:", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ বাতিল", callback_data="ap:main")]]))
        return

    if data == "ap:banned":
        banned_users = await db_read_all("SELECT * FROM users WHERE account_status = 'banned' LIMIT 10")
        if not banned_users:
            await query.edit_message_text("✅ কোনো ব্যান্ড ইউজার নেই\\!", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")]]))
            return
        rows = [[InlineKeyboardButton(f"@{u['username']} [{u['user_id']}]", callback_data=f"admin_view:{u['user_id']}")] for u in banned_users]
        rows.append([InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")])
        await query.edit_message_text(f"🚫 *ব্যান্ড ইউজার* \\({len(banned_users)}জন\\)\n{DIV}", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(rows))
        return

    if data == "ap:audit":
        logs = await db_read_all("SELECT * FROM admin_audit_log ORDER BY timestamp DESC LIMIT 10")
        if not logs:
            await query.edit_message_text("📋 কোনো অডিট লগ নেই।", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")]]))
            return
        lines = [f"📋 *Admin Audit Log* \\(শেষ {len(logs)}টি\\)\n{DIV}"]
        for log in logs:
            lines.append(f"👮 `{log['admin_id']}` → `{log['action']}` on `{log['target_id']}`\n   📝 {md(log['details'])}")
        await query.edit_message_text("\n\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")]]))
        return

    if data == "ap:support_msgs":
        msgs = await db_read_all("SELECT * FROM support_messages WHERE status = 'open' ORDER BY created_at DESC LIMIT 10")
        if not msgs:
            await query.edit_message_text("✅ কোনো খোলা সাপোর্ট টিকেট নেই\\!", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")]]))
            return
        rows = [[InlineKeyboardButton(f"@{m['username']}: {str(m['message'])[:30]}", callback_data=f"supp_detail:{m['user_id']}:{m['id']}")] for m in msgs]
        rows.append([InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")])
        await query.edit_message_text(f"💬 *খোলা সাপোর্ট টিকেট* \\({len(msgs)}টি\\)\n{DIV}", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(rows))
        return

    if data.startswith("supp_detail:"):
        parts = data.split(":")
        if len(parts) == 3:
            target_id, msg_id = int(parts[1]), int(parts[2])
            msg_doc = await db_read_one("SELECT * FROM support_messages WHERE id = $1", (msg_id,))
            if msg_doc:
                ctx.user_data["adm_action"] = "reply_support"
                ctx.user_data["adm_target"] = target_id
                ctx.user_data["supp_msg_id"] = msg_id
                await query.edit_message_text(
                    f"💬 *সাপোর্ট মেসেজ*\n{DIV2}\n👤 @{md(msg_doc['username'])} \\(`{target_id}`\\)\n📝 {md(msg_doc['message'])}\n{DIV2}\n\nরিপ্লাই মেসেজ লিখুন:",
                    parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ বাতিল", callback_data="ap:support_msgs")]]),
                )
                ctx.user_data["awaiting_admin_input"] = "reply_support"
        return

    if data == "ap:settings":
        await query.edit_message_text(
            f"⚙ *Bot Settings*\n{DIV2}\n💰 প্রতি OTP আয়: `৳{EARN_PER_OTP:.2f}`\n⏱ OTP টাইমআউট: `{OTP_TIMEOUT_SEC // 60} মিনিট`\n💳 সর্বনিম্ন উইথড্র: `৳{MIN_WITHDRAW:.2f}`\n🔄 OTP রিফ্রেশ: `{OTP_REFRESH_SEC} সেকেন্ড`\n{DIV}\n_সেটিংস পরিবর্তনে configuration কোড সম্পাদনা করুন।_",
            parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")]]),
        )
        return

    if data == "ap:users":
        users = await db_read_all("SELECT * FROM users ORDER BY joined_at DESC LIMIT 10")
        if not users:
            await query.answer("কোনো ইউজার নেই", show_alert=True)
            return
        rows = []
        for u in users:
            icon = "🚫" if u["account_status"] == "banned" else "👤"
            rows.append([InlineKeyboardButton(f"{icon} @{u['username']} [৳{u['balance']:.0f}]", callback_data=f"admin_view:{u['user_id']}")])
        rows.append([InlineKeyboardButton("◀ ফিরে যান", callback_data="ap:main")])
        await query.edit_message_text(f"👥 *ইউজার লিস্ট* \\(সাম্প্রতিক {len(users)}জন\\)\n{DIV}", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(rows))
        return

# ══════════════════════════════════════════════════════════════════
#  ADMIN INPUT HANDLER
# ══════════════════════════════════════════════════════════════════
async def handle_admin_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE, client: httpx.AsyncClient, input_type: str) -> None:
    text      = update.message.text.strip()
    target_id = ctx.user_data.get("adm_target")
    bot       = ctx.application.bot
    ctx.user_data.pop("awaiting_admin_input", None)

    if input_type == "find_user":
        try: uid = int(text)
        except ValueError:
            await update.message.reply_text("❌ বৈধ User ID লিখুন।", reply_markup=admin_panel_keyboard())
            return
        u = await get_user(client, uid)
        if not u:
            await update.message.reply_text("❌ ইউজার পাওয়া যায়নি।", reply_markup=admin_panel_keyboard())
            return
        status  = "🚫 BANNED" if u["account_status"] == "banned" else "✅ Active"
        await update.message.reply_text(
            f"👤 *ইউজার বিবরণ*\n{DIV2}\n🆔 ID: `{u['user_id']}`\n🏷 Username: @{md(u.get('username','?'))}\n📛 নাম: {md(u.get('first_name','?'))}\n🔖 স্ট্যাটাস: {status}\n💳 ব্যালেন্স: `৳{u.get('balance',0):.2f}`\n📈 মোট আয়: `৳{u.get('total_earned',0):.2f}`\n🔢 OTP: `{u.get('successful_otps',0)}` টি",
            parse_mode=ParseMode.MARKDOWN, reply_markup=user_manage_keyboard(uid),
        )
        return

    if input_type == "addbal":
        try:
            amount = float(text)
            assert amount > 0
        except (ValueError, AssertionError):
            await update.message.reply_text("❌ বৈধ পরিমাণ লিখুন।")
            return
        cur = await db_read_one("SELECT balance, total_earned FROM users WHERE user_id = $1", (target_id,))
        if not cur:
            await update.message.reply_text("❌ ইউজার পাওয়া যায়নি।")
            return
        new_bal = round(cur["balance"] + amount, 4)
        new_earn = round(cur["total_earned"] + amount, 4)
        await db_write("UPDATE users SET balance = $1, total_earned = $2, updated_at = NOW() WHERE user_id = $3", (new_bal, new_earn, target_id))
        await log_admin_action(client, update.effective_user.id, "addbal", target_id, f"\\+৳{amount:.2f}")
        await update.message.reply_text(f"✅ `৳{amount:.2f}` যোগ হয়েছে ইউজার `{target_id}`\\-এ।\nনতুন ব্যালেন্স: `৳{new_bal:.2f}`", parse_mode=ParseMode.MARKDOWN, reply_markup=user_manage_keyboard(target_id))
        try: await bot.send_message(chat_id=target_id, text=f"💰 *ব্যালেন্স আপডেট\\!*\n{DIV}\nআপনার অ্যাকাউন্টে `৳{amount:.2f}` যোগ হয়েছে।\nনতুন ব্যালেন্স: `৳{new_bal:.2f}`", parse_mode=ParseMode.MARKDOWN)
        except TelegramError: pass
        return

    if input_type == "subbal":
        try:
            amount = float(text)
            assert amount > 0
        except (ValueError, AssertionError):
            await update.message.reply_text("❌ বৈধ পরিমাণ লিখুন।")
            return
        cur = await db_read_one("SELECT balance FROM users WHERE user_id = $1", (target_id,))
        if not cur:
            await update.message.reply_text("❌ ইউজার পাওয়া যায়নি।")
            return
        new_bal = max(0.0, round(cur["balance"] - amount, 4))
        await db_write("UPDATE users SET balance = $1, updated_at = NOW() WHERE user_id = $2", (new_bal, target_id))
        await log_admin_action(client, update.effective_user.id, "subbal", target_id, f"\\-৳{amount:.2f}")
        await update.message.reply_text(f"✅ `৳{amount:.2f}` কাটা হয়েছে ইউজার `{target_id}`\\-এ।\nনতুন ব্যালেন্স: `৳{new_bal:.2f}`", parse_mode=ParseMode.MARKDOWN, reply_markup=user_manage_keyboard(target_id))
        return

    if input_type == "ban_reason":
        await db_write("UPDATE users SET account_status = 'banned', ban_reason = $1, updated_at = NOW() WHERE user_id = $2", (text, target_id))
        await log_admin_action(client, update.effective_user.id, "ban", target_id, f"reason={text}")
        await cancel_user_session(client, target_id)
        await update.message.reply_text(f"✅ ইউজার `{target_id}` ব্যান করা হয়েছে।", parse_mode=ParseMode.MARKDOWN, reply_markup=user_manage_keyboard(target_id))
        try: await bot.send_message(chat_id=target_id, text=f"🚫 *আপনার অ্যাকাউন্ট সাসপেন্ড\\!*\n{DIV}\nকারণ: {md(text)}\nআপিলের জন্য সাপোর্টে যোগাযোগ করুন।", parse_mode=ParseMode.MARKDOWN)
        except TelegramError: pass
        return

    if input_type == "note":
        await db_write("UPDATE users SET notes = ?, updated_at = NOW() WHERE user_id = $1", (text, target_id))
        await update.message.reply_text(f"✅ নোট সেভ করা হয়েছে।", reply_markup=user_manage_keyboard(target_id))
        return

    if input_type == "admin_msg":
        try:
            await bot.send_message(chat_id=target_id, text=f"📩 *অ্যাডমিন মেসেজ*\n{DIV}\n{md(text)}", parse_mode=ParseMode.MARKDOWN, reply_markup=main_keyboard())
            await update.message.reply_text(f"✅ মেসেজ পাঠানো হয়েছে ইউজার `{target_id}`\\-কে।", parse_mode=ParseMode.MARKDOWN, reply_markup=user_manage_keyboard(target_id))
        except TelegramError as e:
            await update.message.reply_text(f"❌ পাঠানো যায়নি: {e}")
        return

    if input_type == "broadcast":
        users = await db_read_all("SELECT user_id FROM users WHERE account_status != 'banned'")
        sent = failed = 0
        for u in users:
            try:
                await bot.send_message(chat_id=u["user_id"], text=f"📢 *Admin Broadcast*\n{DIV}\n{text}", parse_mode=ParseMode.MARKDOWN)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception: failed += 1
        await update.message.reply_text(f"✅ ব্রডকাস্ট সম্পন্ন\\!\n`{sent}` টি পাঠানো, `{failed}` টি ব্যর্থ।", parse_mode=ParseMode.MARKDOWN, reply_markup=admin_panel_keyboard())
        return

    if input_type == "reply_support":
        msg_id = ctx.user_data.pop("supp_msg_id", None)
        try:
            await bot.send_message(chat_id=target_id, text=f"📩 *সাপোর্ট রিপ্লাই*\n{DIV}\n{md(text)}", parse_mode=ParseMode.MARKDOWN, reply_markup=main_keyboard())
            if msg_id:
                await db_write("UPDATE support_messages SET status = 'resolved', reply = $1, resolved_at = NOW() WHERE id = $2", (text, msg_id))
            await update.message.reply_text("✅ রিপ্লাই পাঠানো হয়েছে।", reply_markup=admin_panel_keyboard())
        except TelegramError as e:
            await update.message.reply_text(f"❌ পাঠানো যায়নি: {e}")
        return

# ══════════════════════════════════════════════════════════════════
#  TEXT MENU HANDLER WITH INTEGRATED SUPPORT TEXT FLOW
# ══════════════════════════════════════════════════════════════════
async def handle_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user   = update.effective_user
    text   = update.message.text.strip()
    client = _get_client(ctx)

    await ensure_user(client, user.id, user.username, user.first_name or "")

    # ── Hook Awaiting Support Flow ───────────────────────────────
    if ctx.user_data.get("awaiting_support"):
        ctx.user_data.pop("awaiting_support", None)
        await db_write(
            "INSERT INTO support_messages (user_id, username, message, status) VALUES ($1, $2, $3, 'open')",
            (user.id, user.username or "Unknown", text)
        )
        await update.message.reply_text(f"✅ *টিকেট পাঠানো হয়েছে\\!*\n{DIV}\nঅ্যাডমিন শীঘ্রই আপনাকে রিপ্লাই দেবে।", parse_mode=ParseMode.MARKDOWN, reply_markup=main_keyboard())
        admin_text = f"📨 *নতুন সাপোর্ট টিকেট\\!*\n{DIV2}\n👤 @{md(user.username or 'Unknown')} \\(`{user.id}`\\)\n📝 {md(text)}\n{DIV2}"
        await notify_admins(ctx.application.bot, client, admin_text, InlineKeyboardMarkup([[InlineKeyboardButton("💬 সাপোর্ট মেসেজ", callback_data="ap:support_msgs")]]))
        return

    # ── Hook Pending Admin Input Flow ────────────────────────────
    pending_input = ctx.user_data.get("awaiting_admin_input")
    if pending_input and user.id in ADMIN_IDS:
        await handle_admin_input(update, ctx, client, pending_input)
        return

    if text == "🏠 Main Menu":
        await cancel_user_session(client, user.id)
        await update.message.reply_text("🏠 *মেইন মেনু*", parse_mode=ParseMode.MARKDOWN, reply_markup=main_keyboard())
        return

    banned, ban_reason = await is_banned(client, user.id)
    if banned:
        await update.message.reply_text(f"🚫 *অ্যাকাউন্ট সাসপেন্ড*\n{DIV}\nকারণ: {md(ban_reason) if ban_reason else 'নির্দিষ্ট করা হয়নি'}\n\nআপিলের জন্য সাপোর্টে যোগাযোগ করুন।", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💬 সাপোর্ট", url=SUPPORT_URL)]]))
        return

    if not check_rate_limit(user.id):
        await update.message.reply_text("⚠️ অনেক দ্রুত রিকোয়েস্ট। একটু থামুন।")
        return

    if text == "📱 Get Number":
        loading = await update.message.reply_text("🔄 _সার্ভিস লিস্ট লোড হচ্ছে..._", parse_mode=ParseMode.MARKDOWN)
        kb = await build_services_kb(client)
        await loading.delete()
        if kb: await update.message.reply_text(f"🌐 *সার্ভিস লিস্ট*\n{DIV}\nনিচে থেকে সার্ভিস বেছে নিন:", parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        else: await update.message.reply_text("❌ সার্ভার থেকে ডেটা আসেনি। পরে চেষ্টা করুন।")

    elif text == "💰 Balance":
        u   = await get_user(client, user.id)
        bal = u["balance"] if u else 0.0
        await update.message.reply_text(f"💰 *আপনার ব্যালেন্স*\n{DIV}\n👤 *ইউজার:* @{md(user.username or 'Unknown')}\n💵 *ব্যালেন্স:* `৳{bal:.2f}`\n📈 *প্রতি OTP:* `৳{EARN_PER_OTP:.2f}`\n{DIV}\n💡 ৳{MIN_WITHDRAW:.0f} হলে উইথড্র করতে পারবেন।", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💸 উইথড্র করুন", callback_data="do_withdraw")]]))

    elif text == "💸 Withdraw":
        await process_withdraw(update.message.reply_text, client, user.id, user.username, ctx.application.bot)

    elif text == "📜 History":
        docs = await db_read_all("SELECT * FROM otp_history WHERE user_id = $1 AND status = 'Completed' ORDER BY created_at DESC LIMIT 10", (user.id,))
        if not docs:
            await update.message.reply_text(f"📜 *OTP ইতিহাস*\n{DIV}\nএখনো কোনো সফল OTP নেই।", parse_mode=ParseMode.MARKDOWN)
            return
        lines = [f"📜 *সাম্প্রতিক OTP ইতিহাস* \\(শেষ {len(docs)}টি\\)\n{DIV}"]
        total = sum(r["earned"] for r in docs)
        for r in docs:
            lines.append(f"✅ `{r['phone_number']}` | {md(r['service'])}\n   🔐 `{r['otp_code'] or '—'}` | 💰 `৳{r['earned']:.2f}`")
        lines.append(f"\n{DIV}\n💰 *এই পাতায় মোট আয়:* `৳{total:.2f}`")
        await update.message.reply_text("\n\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    elif text == "👤 Profile":
        u = await get_user(client, user.id)
        if not u:
            await update.message.reply_text("❌ প্রোফাইল লোড করা সম্ভব হয়নি।")
            return
        status  = "🚫 BANNED" if u["account_status"] == "banned" else "✅ Active"
        await update.message.reply_text(
            f"👤 *আপনার প্রোফাইল*\n{DIV2}\n🆔 *Telegram ID:* `{u['user_id']}`\n🏷 *Username:* @{md(u['username'])}\n📅 *যোগদান:* `{u['created_at'][:10]}`\n🔖 *স্ট্যাটাস:* {status}\n{DIV}\n💳 *ব্যালেন্স:* `৳{u['balance']:.2f}`\n📈 *মোট আয়:* `৳{u['total_earned']:.2f}`\n🔢 *সফল OTP:* `{u['successful_otps']}` টি\n{DIV}\n💸 *উইথড্র হয়েছে:* `৳{u['withdrawn_amount']:.2f}`\n⏳ *পেন্ডিং উইথড্র:* `৳{u['pending_withdraw']:.2f}`\n📊 *মোট উইথড্র রিকোয়েস্ট:* `৳{u['total_withdraw_amount']:.2f}`",
            parse_mode=ParseMode.MARKDOWN
        )

    elif text == "🛒 Shop":
        await update.message.reply_text(f"🛍 *Taskio BD মার্কেটপ্লেস*\n{DIV}\nপ্রি-অ্যাক্টিভেটেড অ্যাকাউন্ট ও প্রিমিয়াম সার্ভিস কিনুন।", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🌐 শপ খুলুন", web_app=WebAppInfo(url=WEB_STORE_URL))]]))

    elif text == "❔ Support":
        await update.message.reply_text(f"🆘 *Taskio BD সাপোর্ট*\n{DIV}\n• 💳 পেমেন্ট / ডিপোজিট সমস্যা\n• ⚙ বাল্ক / পাইকারী অর্ডার\n• 🔧 যেকোনো টেকনিক্যাল সমস্যা\n\nসরাসরি মেসেজ করুন বা নিচের বাটনে চাপুন:", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👨‍💻 অ্যাডমিনের সাথে কথা বলুন", url=SUPPORT_URL)], [InlineKeyboardButton("📝 টিকেট পাঠান", callback_data="send_support_ticket")]]))

# ══════════════════════════════════════════════════════════════════
#  CALLBACK QUERY HANDLER
# ══════════════════════════════════════════════════════════════════
async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query  = update.callback_query
    data   = query.data or ""
    user   = query.from_user
    client = _get_client(ctx)

    if user.id in ADMIN_IDS and (
        data.startswith("ap:") or data.startswith("adm_")
        or data.startswith("wd_approve:") or data.startswith("wd_reject:")
        or data.startswith("wd_detail:") or data.startswith("admin_view:")
        or data.startswith("supp_detail:")
    ):
        await query.answer()
        await handle_admin_callback(query, data, user, client, ctx)
        return

    if data.startswith("copy:"):
        otp_code = data.split(":", 1)[1]
        await query.answer(f"✅ OTP: {otp_code}", show_alert=True)
        return

    await query.answer()
    await ensure_user(client, user.id, user.username, user.first_name or "")

    banned, ban_reason = await is_banned(client, user.id)
    if banned:
        await query.message.reply_text("🚫 আপনার অ্যাকাউন্ট সাসপেন্ড।")
        return

    if data == "go_home":
        await cancel_user_session(client, user.id)
        await query.message.reply_text("🏠 মেইন মেনুতে ফিরে এলেন।", reply_markup=main_keyboard())

    elif data == "do_withdraw":
        await process_withdraw(query.message.reply_text, client, user.id, user.username, ctx.application.bot)

    elif data == "send_support_ticket":
        ctx.user_data["awaiting_support"] = True
        await query.message.reply_text(f"📝 *সাপোর্ট টিকেট*\n{DIV}\nআপনার সমস্যা বা প্রশ্ন লিখুন। অ্যাডমিন শীঘ্রই রিপ্লাই দেবে।", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ বাতিল", callback_data="cancel_ticket")]]))

    elif data == "cancel_ticket":
        ctx.user_data.pop("awaiting_support", None)
        await query.edit_message_text("✅ বাতিল করা হয়েছে।")

    elif data in ("return_services", "refresh_services"):
        kb = await build_services_kb(client)
        if kb:
            try: await query.edit_message_text(f"🌐 *সার্ভিস লিস্ট*\n{DIV}\nনিচে থেকে সার্ভিস বেছে নিন:", parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
            except TelegramError: await query.message.reply_text(f"🌐 *সার্ভিস লিস্ট*\n{DIV}\n", parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        else: await query.message.reply_text("❌ সার্ভার থেকে ডেটা আসেনি।")

    elif data.startswith("srv:") or data.startswith("change:"):
        parts = data.split(":", 2)
        if len(parts) == 3:
            _, range_code, service = parts
            await allocate_number(ctx.application.bot, client, user.id, range_code, service)

    elif data.startswith("manual_otp:"):
        parts = data.split(":", 2)
        if len(parts) != 3: return
        _, phone, service = parts
        await query.message.reply_text("🔍 _OTP চেক করা হচ্ছে..._", parse_mode=ParseMode.MARKDOWN)
        sms = await api_fetch_sms(client, phone)
        if not (sms and sms.get("otp")):
            await query.message.reply_text("⏳ এখনো কোনো OTP আসেনি।")
            return
        full_msg = sms["full_message"]
        otp_code = extract_otp(full_msg)
        if not otp_code:
            await query.message.reply_text("❌ OTP পার্স করা যায়নি।")
            return
        h = make_otp_hash(phone, otp_code)
        async with get_lock(user.id):
            credited = await credit_otp(client, user.id, phone, service, otp_code, full_msg, h)
        u       = await get_user(client, user.id)
        new_bal = u["balance"] if u else 0.0
        note    = f"💰 *\\+৳{EARN_PER_OTP:.2f}  ক্রেডিট*" if credited else "ℹ _এই OTP আগেই ক্রেডিট হয়েছে_"
        await query.message.reply_text(f"🎉 *OTP পাওয়া গেছে\\!*\n{DIV}\n📞 *নাম্বার:* `{phone}`\n🔐 *OTP:* `{otp_code}`\n{DIV}\n{note}\n💳 *ব্যালেন্স:* `৳{new_bal:.2f}`", parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"📋 OTP কপি ({otp_code})", callback_data=f"copy:{otp_code}")]]))

# ══════════════════════════════════════════════════════════════════
#  ERROR HANDLER
# ══════════════════════════════════════════════════════════════════
async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", ctx.error, exc_info=ctx.error)

# ══════════════════════════════════════════════════════════════════
#  APPLICATION LIFECYCLE (POOL MANAGEMENT)
# ══════════════════════════════════════════════════════════════════
async def post_init(app: Application) -> None:
    global _db_pool
    app.bot_data["http_client"] = httpx.AsyncClient(headers=API_HEADERS, timeout=15.0)
    
    # Supabase কানেকশন পুল ইনিশিয়েট করা
    _db_pool = await asyncpg.create_pool(
        dsn=SUPABASE_DATABASE_URL,
        min_size=2,
        max_size=10
    )
    await init_db(_db_pool)
    logger.info("🚀 Taskio BD Pro Cloud Engine Online — Connected to Supabase ✅")

async def post_shutdown(app: Application) -> None:
    global _db_pool
    logger.info("🛑 Shutdown — cancelling tasks...")
    for tasks in _active_tasks.values():
        for t in tasks:
            if not t.done(): t.cancel()
    if "http_client" in app.bot_data:
        await app.bot_data["http_client"].aclose()
    if _db_pool:
        await _db_pool.close()
    logger.info("✅ Shutdown complete.")

# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════
def main() -> None:
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu",  cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_error_handler(error_handler)

    logger.info("🤖 Bot polling started…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
