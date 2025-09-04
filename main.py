#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Quiz Bot — Quiz Polls + Auto-close + Auto-numbering + 'Done' button

Features:
- Owner panel (ReplyKeyboard) ثابتة للمالك.
- CRUD أسئلة/خيارات + مرفقات خاصة بالسؤال + حزم مرفقات مشتركة.
- نشر كـ Telegram Quiz Polls (type="quiz") غير مجهولة (is_anonymous=False).
- ترقيم تلقائي للأسئلة عند النشر: [1], [2], ...
- حتى 10 خيارات لكل سؤال.
- إرسال المرفقات قبل الاستفتاء (Polls لا تدعم وسائط مدمجة).
- زر Inline "✔️ تم" لإنهاء مرحلة رفع المرفقات (سؤال/حزمة/استبدال).
- إغلاق تلقائي لكل الاستفتاءات عند انتهاء الوقت المحدد.
- دمج اختبارين في اختبار جديد.
- تصدير اختبار إلى JSON.
- لوحة نتائج.

بيئة التشغيل: aiogram v3
"""

import asyncio
import os
import sqlite3
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, List

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    FSInputFile, PollAnswer
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter

# ---------------------- ENV ----------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DB_PATH = os.getenv("DB_PATH", "quiz_bot.db")

CORRECT_STICKER_ID = os.getenv("CORRECT_STICKER_ID", "").strip()
WRONG_STICKER_ID   = os.getenv("WRONG_STICKER_ID", "").strip()
CORRECT_ANIM_ID    = os.getenv("CORRECT_ANIM_ID", "").strip()
WRONG_ANIM_ID      = os.getenv("WRONG_ANIM_ID", "").strip()

if not BOT_TOKEN:
    raise SystemExit("❌ BOT_TOKEN is missing in .env")
if not OWNER_ID:
    raise SystemExit("❌ OWNER_ID is missing in .env")

# ---------------------- BOT ----------------------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

async def _celebrate(chat_id:int, is_correct:bool):
    try:
        if is_correct and CORRECT_STICKER_ID:
            await bot.send_sticker(chat_id, CORRECT_STICKER_ID, disable_notification=True); return
        if (not is_correct) and WRONG_STICKER_ID:
            await bot.send_sticker(chat_id, WRONG_STICKER_ID, disable_notification=True); return
        if is_correct and CORRECT_ANIM_ID:
            await bot.send_animation(chat_id, CORRECT_ANIM_ID, disable_notification=True); return
        if (not is_correct) and WRONG_ANIM_ID:
            await bot.send_animation(chat_id, WRONG_ANIM_ID, disable_notification=True); return
    except Exception:
        pass

# ---------------------- DB Helpers ----------------------
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def col_exists(conn, table, col):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)

def _ensure_schema():
    with db() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS quizzes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                is_archived INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quiz_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                media_bundle_id INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question_id INTEGER NOT NULL,
                option_index INTEGER NOT NULL,
                text TEXT NOT NULL,
                is_correct INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                question_id INTEGER NOT NULL,
                option_index INTEGER NOT NULL,
                is_correct INTEGER NOT NULL,
                answered_at TEXT NOT NULL,
                UNIQUE(chat_id, user_id, question_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS user_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                origin_chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                quiz_id INTEGER NOT NULL,
                q_pos INTEGER NOT NULL DEFAULT 0,
                started_at TEXT NOT NULL,
                finished_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS sent_msgs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                quiz_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                expires_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS participant_names (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                origin_chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                quiz_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                UNIQUE(origin_chat_id, user_id, quiz_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS question_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question_id INTEGER NOT NULL,
                kind TEXT NOT NULL,        -- 'photo' | 'voice' | 'audio'
                file_id TEXT NOT NULL,
                position INTEGER NOT NULL  -- 0..4
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS media_bundles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quiz_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS media_bundle_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bundle_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                file_id TEXT NOT NULL,
                position INTEGER NOT NULL
            )
        """)
        # خريطة الاستفتاءات المنشورة
        c.execute("""
            CREATE TABLE IF NOT EXISTS sent_polls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                quiz_id INTEGER NOT NULL,
                question_id INTEGER NOT NULL,
                poll_id TEXT NOT NULL,
                message_id INTEGER,
                expires_at TEXT,
                is_closed INTEGER NOT NULL DEFAULT 0
            )
        """)
        # legacy columns
        if not col_exists(conn, "questions", "photo"):
            try: c.execute("ALTER TABLE questions ADD COLUMN photo TEXT")
            except: pass
        if not col_exists(conn, "questions", "audio"):
            try: c.execute("ALTER TABLE questions ADD COLUMN audio TEXT")
            except: pass
        if not col_exists(conn, "questions", "audio_is_voice"):
            try: c.execute("ALTER TABLE questions ADD COLUMN audio_is_voice INTEGER DEFAULT 0")
            except: pass
        if not col_exists(conn, "sent_msgs", "expires_at"):
            try: c.execute("ALTER TABLE sent_msgs ADD COLUMN expires_at TEXT")
            except: pass
        if not col_exists(conn, "sent_polls", "message_id"):
            try: c.execute("ALTER TABLE sent_polls ADD COLUMN message_id INTEGER")
            except: pass
        if not col_exists(conn, "sent_polls", "is_closed"):
            try: c.execute("ALTER TABLE sent_polls ADD COLUMN is_closed INTEGER NOT NULL DEFAULT 0")
            except: pass
        conn.commit()

def migrate_legacy_media():
    with db() as conn:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(questions)").fetchall()}
        if not {'photo','audio','audio_is_voice'}.issubset(cols):
            return
        rows = conn.execute("""
            SELECT id, photo, audio, COALESCE(audio_is_voice,0) AS audio_is_voice
            FROM questions
            WHERE (photo IS NOT NULL OR audio IS NOT NULL)
        """).fetchall()
        for r in rows:
            qid = r["id"]
            exists = conn.execute("SELECT 1 FROM question_attachments WHERE question_id=? LIMIT 1",(qid,)).fetchone()
            if exists: continue
            pos = 0
            if r["photo"]:
                conn.execute("INSERT INTO question_attachments(question_id, kind, file_id, position) VALUES (?,?,?,?)",
                             (qid, "photo", r["photo"], pos)); pos += 1
            if r["audio"]:
                kind = "voice" if int(r["audio_is_voice"])==1 else "audio"
                conn.execute("INSERT INTO question_attachments(question_id, kind, file_id, position) VALUES (?,?,?,?)",
                             (qid, kind, r["audio"], pos))
        conn.commit()

_ensure_schema()
migrate_legacy_media()

# ---------------------- Owner check ----------------------
def is_owner(user_id: int) -> bool: return user_id == OWNER_ID
async def ensure_owner(msg: Message) -> bool:
    if not is_owner(msg.from_user.id):
        await msg.reply("🚫 هذا الزر/الأمر خاص بالمالك.", reply_markup=owner_panel_reply_kb()); return False
    return True

# ---------------------- UI Text ----------------------
BTN_NEWQUIZ = "🆕 إنشاء اختبار"
BTN_ADDQ    = "➕ إضافة سؤال"
BTN_LISTQUIZ= "📚 عرض الاختبارات"
BTN_LISTQ   = "📖 عرض الأسئلة"
BTN_EDITQUIZ= "🛠️ تعديل اختبار"
BTN_DELQUIZ = "🗑️ حذف اختبار"
BTN_BUNDLES = "📎 مرفقات مشتركة"
BTN_MERGE   = "🔗 دمج الاختبارات"
BTN_EXPORT  = "📤 تصدير اختبار"
BTN_PUBLISH = "🚀 نشر اختبار"
BTN_WIPE_ALL= "🧹 حذف كل الاختبارات"
BTN_SCORE   = "🏆 لوحة النتائج"
BTN_BACK_HOME = "↩️ العودة للبداية"
BTN_BACK_STEP = "⬅️ رجوع للخلف"

ACT_EDIT_TEXT  = "✏️ تعديل النص"
ACT_EDIT_OPTS  = "🧩 تعديل الخيارات"
ACT_EDIT_MEDIA = "🖼️ تبديل المرفقات"
ACT_DELETE_Q   = "🗑️ حذف السؤال"
ACT_BACK       = "⬅️ رجوع"

BTN_USE_BUNDLE = "📎 استخدام مرفق مشترك"
BTN_USE_OWN    = "🖼️ مرفقات خاصة بالسؤال"
BTN_USE_NONE   = "❌ بدون مرفقات"

BTN_DUR_12H    = "⏱️ 12 ساعة"
BTN_DUR_24H    = "⏱️ 24 ساعة"
BTN_DUR_CUSTOM = "⏱️ إدخال يدوي"
BTN_DUR_NONE   = "♾️ بلا وقت"

# ---------------------- States ----------------------
@dataclass
class BuildSession:
    quiz_id: Optional[int] = None
    tmp_question_id: Optional[int] = None
    options_needed: int = 0
    options_collected: int = 0
    att_count: int = 0

build_session = BuildSession()

class BuildStates(StatesGroup):
    waiting_title = State()
    waiting_pick_quiz_for_addq = State()
    waiting_q_text = State()
    waiting_attach_mode = State()
    waiting_q_attachments = State()
    waiting_pick_bundle_for_q = State()
    waiting_options_count = State()
    waiting_option_text = State()
    waiting_correct_index = State()
    waiting_pick_quiz_generic = State()
    waiting_manage_question_pick = State()
    waiting_edit_quiz_title = State()
    waiting_replace_attachments = State()

class BundleStates(StatesGroup):
    waiting_pick_quiz_for_bundle = State()
    waiting_bundle_files = State()

class EditOptionStates(StatesGroup):
    waiting_count = State()
    waiting_text = State()
    waiting_correct = State()

class PublishStates(StatesGroup):
    waiting_pick_quiz = State()
    waiting_duration_choice = State()
    waiting_custom_hours = State()

class MergeStates(StatesGroup):
    waiting_pick_src = State()
    waiting_pick_dst = State()

class ExportStates(StatesGroup):
    waiting_pick_quiz = State()

# ---------------------- Keyboards ----------------------
def owner_panel_reply_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=BTN_BACK_HOME), KeyboardButton(text=BTN_BACK_STEP)],
        [KeyboardButton(text=BTN_NEWQUIZ)],
        [KeyboardButton(text=BTN_ADDQ)],
        [KeyboardButton(text=BTN_LISTQUIZ)],
        [KeyboardButton(text=BTN_LISTQ)],
        [KeyboardButton(text=BTN_EDITQUIZ)],
        [KeyboardButton(text=BTN_DELQUIZ)],
        [KeyboardButton(text=BTN_BUNDLES)],
        [KeyboardButton(text=BTN_MERGE), KeyboardButton(text=BTN_EXPORT)],
        [KeyboardButton(text=BTN_PUBLISH)],
        [KeyboardButton(text=BTN_WIPE_ALL)],
        [KeyboardButton(text=BTN_SCORE)],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def attach_mode_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=BTN_USE_BUNDLE, callback_data="attach_mode:bundle")
    kb.button(text=BTN_USE_OWN, callback_data="attach_mode:own")
    kb.button(text=BTN_USE_NONE, callback_data="attach_mode:none")
    kb.adjust(1)
    return kb.as_markup()

def done_button_kb(tag: str) -> InlineKeyboardMarkup:
    """Inline 'Done' button to finish attachments steps."""
    kb = InlineKeyboardBuilder()
    kb.button(text="✔️ تم", callback_data=f"done:{tag}")
    return kb.as_markup()

def paged_quizzes_kb(page: int = 0, tag: str = "pickq", per:int=8) -> InlineKeyboardMarkup:
    with db() as conn:
        rows = conn.execute("SELECT id,title FROM quizzes WHERE is_archived=0 ORDER BY id DESC").fetchall()
    start = page * per; chunk = rows[start:start+per]
    kb = InlineKeyboardBuilder()
    for r in chunk:
        kb.button(text=f"✅ ID {r['id']} — {r['title']}", callback_data=f"{tag}:{r['id']}")
    kb.adjust(1); kb.row()
    if start > 0: kb.button(text="⬅️", callback_data=f"{tag}_page:{page-1}")
    kb.button(text=f"صفحة {page+1}", callback_data="noop")
    if start + per < len(rows): kb.button(text="➡️", callback_data=f"{tag}_page:{page+1}")
    return kb.as_markup()

def paged_questions_kb(quiz_id:int, page:int=0, tag:str="manageq", per:int=10) -> InlineKeyboardMarkup:
    with db() as conn:
        rows = conn.execute("SELECT id, text FROM questions WHERE quiz_id=? ORDER BY id", (quiz_id,)).fetchall()
    start = page * per; chunk = rows[start:start+per]
    kb = InlineKeyboardBuilder()
    for r in chunk:
        label = r['text']
        if len(label) > 40: label = label[:40] + "…"
        kb.button(text=f"🔹 Q{r['id']} — {label}", callback_data=f"{tag}:{quiz_id}:{r['id']}:{page}")
    kb.adjust(1); kb.row()
    if start > 0: kb.button(text="⬅️", callback_data=f"{tag}_page:{quiz_id}:{page-1}")
    kb.button(text=f"صفحة {page+1}", callback_data="noop")
    if start + per < len(rows): kb.button(text="➡️", callback_data=f"{tag}_page:{quiz_id}:{page+1}")
    return kb.as_markup()

def paged_bundles_kb(quiz_id:int, page:int=0, tag:str="pickbundle", per:int=8) -> InlineKeyboardMarkup:
    with db() as conn:
        rows = conn.execute("SELECT id FROM media_bundles WHERE quiz_id=? ORDER BY id DESC", (quiz_id,)).fetchall()
    start = page * per; chunk = rows[start:start+per]
    kb = InlineKeyboardBuilder()
    for r in chunk:
        with db() as c2:
            att_cnt = c2.execute("SELECT COUNT(*) FROM media_bundle_attachments WHERE bundle_id=?", (r["id"],)).fetchone()[0]
            q_cnt = c2.execute("SELECT COUNT(*) FROM questions WHERE media_bundle_id=?", (r["id"],)).fetchone()[0]
        kb.button(text=f"📎 حزمة {r['id']} — ملفات:{att_cnt} / أسئلة:{q_cnt}", callback_data=f"{tag}:{quiz_id}:{r['id']}")
    kb.adjust(1); kb.row()
    if start > 0: kb.button(text="⬅️", callback_data=f"{tag}_page:{quiz_id}:{page-1}")
    kb.button(text=f"صفحة {page+1}", callback_data="noop")
    if start + per < len(rows): kb.button(text="➡️", callback_data=f"{tag}_page:{quiz_id}:{page+1}")
    return kb.as_markup()

def publish_duration_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=BTN_DUR_12H, callback_data="dur:12")
    kb.button(text=BTN_DUR_24H, callback_data="dur:24")
    kb.button(text=BTN_DUR_CUSTOM, callback_data="dur:custom")
    kb.button(text=BTN_DUR_NONE, callback_data="dur:none")
    kb.adjust(2)
    return kb.as_markup()

# ---------------------- Helpers ----------------------
def get_quiz_question_ids(quiz_id: int) -> List[int]:
    with db() as conn:
        rows = conn.execute("SELECT id FROM questions WHERE quiz_id=? ORDER BY id", (quiz_id,)).fetchall()
    return [r["id"] for r in rows]

def options_for_question(question_id:int) -> List[sqlite3.Row]:
    with db() as conn:
        rows = conn.execute("SELECT option_index, text, is_correct FROM options WHERE question_id=? ORDER BY option_index", (question_id,)).fetchall()
    return rows

def get_question_atts(question_id:int) -> List[sqlite3.Row]:
    with db() as conn:
        rows = conn.execute("SELECT kind, file_id, position FROM question_attachments WHERE question_id=? ORDER BY position",(question_id,)).fetchall()
    return rows

def get_bundle_atts(bundle_id:int) -> List[sqlite3.Row]:
    with db() as conn:
        rows = conn.execute("SELECT kind, file_id, position FROM media_bundle_attachments WHERE bundle_id=? ORDER BY position",(bundle_id,)).fetchall()
    return rows

def hlink_user(name:str, user_id:int) -> str:
    safe = name.replace("<","&lt;").replace(">","&gt;")
    return f'<a href="tg://user?id={user_id}">{safe}</a>'

def _now_utc() -> datetime: return datetime.now(timezone.utc)

def _display_name(u) -> str:
    parts = []
    if getattr(u, "first_name", None): parts.append(u.first_name)
    if getattr(u, "last_name", None): parts.append(u.last_name)
    nm = " ".join([p for p in parts if p]).strip()
    if not nm and getattr(u, "username", None):
        nm = f"@{u.username}"
    if not nm:
        nm = f"UID {u.id}"
    return nm

# ---------------------- Start & ReplyKeyboard ----------------------
@dp.message(Command("start"))
async def cmd_start(msg: Message):
    if is_owner(msg.from_user.id):
        await msg.answer("لوحة التحكم جاهزة — اختر من الأزرار:", reply_markup=owner_panel_reply_kb())
    else:
        await msg.answer("أهلاً! هذا بوت اختبارات بإدارة المعلم.\nالإجابات تتم عبر استفتاءات Quiz داخل المجموعة.")

@dp.message(F.text == BTN_BACK_HOME)
async def btn_back_home(msg:Message, state:FSMContext):
    await state.clear()
    await msg.answer("تم الرجوع للبداية.", reply_markup=owner_panel_reply_kb())

@dp.message(F.text == BTN_BACK_STEP)
async def btn_back_step(msg:Message, state:FSMContext):
    await state.clear()
    await msg.answer("رجعناك للبداية.", reply_markup=owner_panel_reply_kb())

# ---------------------- Buttons ----------------------
@dp.message(F.text == BTN_NEWQUIZ)
async def btn_newquiz(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): return
    await state.set_state(BuildStates.waiting_title)
    await msg.answer("🆕 أرسل عنوان/اسم الاختبار:", reply_markup=owner_panel_reply_kb())

@dp.message(F.text == BTN_ADDQ)
async def btn_addq(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): return
    await state.set_state(BuildStates.waiting_pick_quiz_for_addq)
    await msg.answer("اختر الاختبار لإضافة سؤال:", reply_markup=paged_quizzes_kb(0,"pick_for_addq"))

@dp.message(F.text == BTN_LISTQUIZ)
async def btn_list_quizzes(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): return
    await msg.answer("📚 اختر اختبار للاطلاع على تفاصيله:", reply_markup=paged_quizzes_kb(0,"overview_q"))

@dp.message(F.text == BTN_LISTQ)
async def btn_list_questions(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): return
    await state.set_state(BuildStates.waiting_pick_quiz_generic)
    await msg.answer("اختر الاختبار لعرض أسئلته:", reply_markup=paged_quizzes_kb(0,"listq_pickq"))

@dp.message(F.text == BTN_EDITQUIZ)
async def btn_edit_quiz(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): return
    await state.set_state(BuildStates.waiting_edit_quiz_title)
    await msg.answer("اختر اختبار لتعديل عنوانه:", reply_markup=paged_quizzes_kb(0,"renameq"))

@dp.message(F.text == BTN_DELQUIZ)
async def btn_del_quiz(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): return
    await state.set_state(BuildStates.waiting_pick_quiz_generic)
    await msg.answer("اختر اختبارًا لحذفه:", reply_markup=paged_quizzes_kb(0,"delqz"))

@dp.message(F.text == BTN_BUNDLES)
async def btn_bundles(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): return
    await state.set_state(BundleStates.waiting_pick_quiz_for_bundle)
    await msg.answer("اختر الاختبار لإنشاء/عرض المرفقات المشتركة:", reply_markup=paged_quizzes_kb(0,"bund_pickq"))

@dp.message(F.text == BTN_MERGE)
async def btn_merge(msg: Message, state:FSMContext):
    if not await ensure_owner(msg): return
    await state.set_state(MergeStates.waiting_pick_src)
    await msg.answer("🔗 اختاري الاختبار الأول (المصدر 1):", reply_markup=paged_quizzes_kb(0, "merge_src"))

@dp.message(F.text == BTN_EXPORT)
async def btn_export(msg: Message, state:FSMContext):
    if not await ensure_owner(msg): return
    await state.set_state(ExportStates.waiting_pick_quiz)
    await msg.answer("📤 اختاري الاختبار لتصديره:", reply_markup=paged_quizzes_kb(0, "export_pick"))

@dp.message(F.text == BTN_PUBLISH)
async def btn_publish(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): return
    if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await msg.reply("افتح هذا الخيار داخل المجموعة لنشر الاختبار.", reply_markup=owner_panel_reply_kb())
    await state.set_state(PublishStates.waiting_pick_quiz)
    await msg.answer("اختر الاختبار لنشره:", reply_markup=paged_quizzes_kb(0,"pub_pickq"))

@dp.message(F.text == BTN_WIPE_ALL)
async def btn_wipe_all(msg:Message):
    if not await ensure_owner(msg): return
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ نعم", callback_data="yes:wipe")
    kb.button(text="❌ لا", callback_data="no:wipe")
    await msg.answer("هل تريد حذف كل البيانات؟", reply_markup=kb.as_markup())

@dp.message(F.text == BTN_SCORE)
async def btn_score(msg:Message):
    if not await ensure_owner(msg): return
    await msg.answer("اختر اختبار لعرض النتائج:", reply_markup=paged_quizzes_kb(0,"score_pickq"))

# ---------------------- Create quiz ----------------------
@dp.message(BuildStates.waiting_title, F.text)
async def receive_title(msg: Message, state: FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    title = msg.text.strip()
    with db() as conn:
        cur = conn.execute("INSERT INTO quizzes(title, created_by, created_at) VALUES (?,?,?)",
                           (title, OWNER_ID, datetime.now(timezone.utc).isoformat()))
        build_session.quiz_id = cur.lastrowid; conn.commit()
    await state.clear()
    await msg.answer(f"✅ تم إنشاء الاختبار (<code>{build_session.quiz_id}</code>): <b>{title}</b>", reply_markup=owner_panel_reply_kb())

# ---------------------- Bundles (shared attachments) ----------------------
@dp.callback_query(F.data.startswith("bund_pickq_page:"))
async def bundles_page(cb:CallbackQuery, state:FSMContext):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(int(page),"bund_pickq"))

@dp.callback_query(F.data.startswith("bund_pickq:"), BundleStates.waiting_pick_quiz_for_bundle)
async def bundles_for_quiz(cb:CallbackQuery, state:FSMContext):
    _, quiz_id = cb.data.split(":",1)
    await state.update_data(quiz_id=int(quiz_id), bundle_pos=0, bundle_id=None)
    with db() as conn:
        cur = conn.execute("INSERT INTO media_bundles(quiz_id, created_at) VALUES (?,?)",
                           (int(quiz_id), datetime.now(timezone.utc).isoformat()))
        bundle_id = cur.lastrowid; conn.commit()
    await state.update_data(bundle_id=bundle_id)
    await state.set_state(BundleStates.waiting_bundle_files)
    await cb.message.edit_text(
        f"أرسلي حتى 5 مرفقات للحزمة رقم {bundle_id}.",
        reply_markup=done_button_kb("bundle_att")
    )

@dp.callback_query(F.data.startswith("done:"))
async def cb_done(cb: CallbackQuery, state: FSMContext):
    tag = cb.data.split(":",1)[1]
    if tag == "q_att":
        await state.set_state(BuildStates.waiting_options_count)
        await cb.message.edit_text("كم عدد الخيارات؟ (2-10)")
    elif tag == "bundle_att":
        await state.clear()
        await cb.message.edit_text("تم حفظ الحزمة. الآن اربطي الأسئلة بها من 'إضافة سؤال' → 'استخدام مرفق مشترك'.",
                                   reply_markup=owner_panel_reply_kb())
    elif tag == "replace_att":
        await state.clear()
        await cb.message.edit_text("تم تحديث المرفقات.", reply_markup=owner_panel_reply_kb())
    else:
        await cb.answer()

@dp.message(BundleStates.waiting_bundle_files, F.photo | F.voice | F.audio)
async def bundle_add_file(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    data = await state.get_data(); pos = int(data.get("bundle_pos",0))
    if pos >= 5: return await msg.reply("بلغتِ الحد الأقصى (5). اضغطي ✔️ تم أعلى الرسالة.")
    if msg.photo: kind, file_id = "photo", msg.photo[-1].file_id
    elif msg.voice: kind, file_id = "voice", msg.voice.file_id
    elif msg.audio: kind, file_id = "audio", msg.audio.file_id
    else: return await msg.reply("نوع غير مدعوم.")
    with db() as conn:
        conn.execute("""INSERT INTO media_bundle_attachments(bundle_id, kind, file_id, position)
                        VALUES (?,?,?,?)""", (int(data["bundle_id"]), kind, file_id, pos))
        conn.commit()
    await state.update_data(bundle_pos=pos+1)
    await msg.reply(f"تم إضافة المرفق ({pos+1}/5).")

# ---------------------- Add Question ----------------------
@dp.callback_query(F.data.startswith("pick_for_addq_page:"))
async def page_pick_for_addq(cb:CallbackQuery, state: FSMContext):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(page=int(page), tag="pick_for_addq"))

@dp.callback_query(F.data.startswith("pick_for_addq:"))
async def picked_quiz_for_addq(cb: CallbackQuery, state: FSMContext):
    _, qid = cb.data.split(":",1)
    build_session.quiz_id = int(qid)
    build_session.att_count = 0
    await state.set_state(BuildStates.waiting_q_text)
    await cb.message.edit_text("أرسل نص السؤال:")

@dp.message(BuildStates.waiting_q_text, F.text)
async def receive_q_text(msg: Message, state: FSMContext):
    data = await state.get_data()
    qid_for_edit = data.get("question_id")
    if qid_for_edit:
        if not await ensure_owner(msg): await state.clear(); return
        with db() as conn:
            conn.execute("UPDATE questions SET text=? WHERE id=?", (msg.text.strip(), int(qid_for_edit)))
            conn.commit()
        await state.clear()
        return await msg.answer("تم تحديث نص السؤال.", reply_markup=owner_panel_reply_kb())
    if not await ensure_owner(msg): await state.clear(); return
    with db() as conn:
        cur = conn.execute("INSERT INTO questions(quiz_id, text, created_at) VALUES (?,?,?)",
                           (build_session.quiz_id, msg.text.strip(), datetime.now(timezone.utc).isoformat()))
        build_session.tmp_question_id = cur.lastrowid; conn.commit()
    build_session.att_count = 0
    await state.set_state(BuildStates.waiting_attach_mode)
    await msg.answer("اختر طريقة المرفقات لهذا السؤال:", reply_markup=attach_mode_kb())

@dp.callback_query(F.data.startswith("attach_mode:"), BuildStates.waiting_attach_mode)
async def choose_attach_mode(cb:CallbackQuery, state:FSMContext):
    mode = cb.data.split(":",1)[1]
    if mode == "bundle":
        await state.set_state(BuildStates.waiting_pick_bundle_for_q)
        await cb.message.edit_text("اختر الحزمة:", reply_markup=paged_bundles_kb(build_session.quiz_id,0,"pickbundle_for_q"))
    elif mode == "own":
        await state.set_state(BuildStates.waiting_q_attachments)
        await cb.message.edit_text("أرسلي حتى 5 مرفقات لهذا السؤال.", reply_markup=done_button_kb("q_att"))
    else:
        await state.set_state(BuildStates.waiting_options_count)
        await cb.message.edit_text("كم عدد الخيارات؟ (2-10)")

@dp.callback_query(F.data.startswith("pickbundle_for_q_page:"), BuildStates.waiting_pick_bundle_for_q)
async def page_pickbundle_q(cb:CallbackQuery, state:FSMContext):
    _, quiz_id, page = cb.data.split(":",2)
    await cb.message.edit_reply_markup(reply_markup=paged_bundles_kb(int(quiz_id), int(page), "pickbundle_for_q"))

@dp.callback_query(F.data.startswith("pickbundle_for_q:"), BuildStates.waiting_pick_bundle_for_q)
async def picked_bundle_for_q(cb:CallbackQuery, state:FSMContext):
    _, quiz_id, bundle_id = cb.data.split(":",2)
    bundle_id = int(bundle_id)
    with db() as conn:
        conn.execute("UPDATE questions SET media_bundle_id=? WHERE id=?", (bundle_id, build_session.tmp_question_id))
        conn.commit()
    await state.set_state(BuildStates.waiting_options_count)
    await cb.message.edit_text("تم الربط بالحزمة.\nكم عدد الخيارات؟ (2-10)")

@dp.message(BuildStates.waiting_q_attachments, F.photo | F.voice | F.audio)
async def receive_attachment(msg: Message, state: FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    if build_session.att_count >= 5:
        return await msg.reply("وصلتِ للحد الأقصى (5). اضغطي ✔️ تم أعلى الرسالة.")
    if msg.photo: kind, file_id = "photo", msg.photo[-1].file_id
    elif msg.voice: kind, file_id = "voice", msg.voice.file_id
    elif msg.audio: kind, file_id = "audio", msg.audio.file_id
    else: return await msg.reply("نوع مرفق غير مدعوم.")
    with db() as conn:
        conn.execute("""
            INSERT INTO question_attachments(question_id, kind, file_id, position)
            VALUES (?,?,?,?)
        """, (build_session.tmp_question_id, kind, file_id, build_session.att_count))
        conn.commit()
    build_session.att_count += 1
    await msg.reply(f"تم حفظ المرفق ({build_session.att_count}/5).")

@dp.message(BuildStates.waiting_options_count, F.text)
async def receive_options_count(msg: Message, state: FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    try:
        n = int(msg.text.strip())
        if n < 2 or n > 10: raise ValueError
    except ValueError:
        return await msg.reply("أدخل رقمًا بين 2 و 10.")
    build_session.options_needed = n
    build_session.options_collected = 0
    await state.set_state(BuildStates.waiting_option_text)
    await msg.answer(f"أرسل نص الخيار 1 من {n}:", reply_markup=owner_panel_reply_kb())

@dp.message(BuildStates.waiting_option_text, F.text)
async def receive_option_text(msg: Message, state: FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    idx = build_session.options_collected
    with db() as conn:
        conn.execute("INSERT INTO options(question_id, option_index, text) VALUES (?,?,?)",
                     (build_session.tmp_question_id, idx, msg.text.strip()))
        conn.commit()
    build_session.options_collected += 1
    if build_session.options_collected < build_session.options_needed:
        await msg.answer(f"أرسل نص الخيار {build_session.options_collected+1} من {build_session.options_needed}:", reply_markup=owner_panel_reply_kb())
    else:
        await state.set_state(BuildStates.waiting_correct_index)
        await msg.answer(f"أرسل رقم الخيار الصحيح (1-{build_session.options_needed}):", reply_markup=owner_panel_reply_kb())

@dp.message(BuildStates.waiting_correct_index, F.text)
async def receive_correct_index(msg: Message, state: FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    try:
        i = int(msg.text.strip())
        if i < 1 or i > build_session.options_needed: raise ValueError
    except ValueError:
        return await msg.reply("أدخل رقمًا صحيحًا ضمن النطاق.")
    correct_idx0 = i - 1
    with db() as conn:
        conn.execute("UPDATE options SET is_correct=1 WHERE question_id=? AND option_index=?",
                     (build_session.tmp_question_id, correct_idx0))
        conn.commit()
    await state.clear()
    await msg.answer("✅ تم حفظ السؤال والخيارات.", reply_markup=owner_panel_reply_kb())

# ---------------------- Manage Questions ----------------------
@dp.callback_query(F.data.startswith("listq_pickq_page:"), BuildStates.waiting_pick_quiz_generic)
async def cb_list_questions_page(cb: CallbackQuery, state:FSMContext):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(page=int(page), tag="listq_pickq"))

@dp.callback_query(F.data.startswith("listq_pickq:"), BuildStates.waiting_pick_quiz_generic)
async def cb_list_questions_show(cb: CallbackQuery, state:FSMContext):
    _, quiz_id = cb.data.split(":",1)
    await state.update_data(quiz_id=int(quiz_id))
    await state.set_state(BuildStates.waiting_manage_question_pick)
    await cb.message.edit_text("اختر سؤالًا لإدارته:", reply_markup=paged_questions_kb(int(quiz_id), page=0, tag="manageq"))

@dp.callback_query(F.data.startswith("manageq_page:"), BuildStates.waiting_manage_question_pick)
async def cb_manageq_page(cb:CallbackQuery, state:FSMContext):
    _, quiz_id, page = cb.data.split(":",2)
    await cb.message.edit_reply_markup(reply_markup=paged_questions_kb(int(quiz_id), int(page), tag="manageq"))

@dp.callback_query(F.data.startswith("manageq:"), BuildStates.waiting_manage_question_pick)
async def cb_manageq_open(cb:CallbackQuery, state:FSMContext):
    _, quiz_id, qid, page = cb.data.split(":",3)
    quiz_id = int(quiz_id); qid = int(qid); page = int(page)
    with db() as conn:
        qrow = conn.execute("SELECT * FROM questions WHERE id=?", (qid,)).fetchone()
        opts = conn.execute("SELECT option_index, text, is_correct FROM options WHERE question_id=? ORDER BY option_index", (qid,)).fetchall()
    lines = [f"<b>{qrow['text']}</b>",""]
    for o in opts:
        circ = chr(0x2776 + int(o["option_index"])) if o["option_index"]<10 else f"{o['option_index']+1})"
        mark = " ✅" if int(o["is_correct"])==1 else ""
        lines.append(f"{circ} {o['text']}{mark}")
    txt = "\n".join(lines)
    kb = InlineKeyboardBuilder()
    kb.button(text=ACT_EDIT_TEXT,  callback_data=f"m_edit_text:{quiz_id}:{qid}:{page}")
    kb.button(text=ACT_EDIT_OPTS,  callback_data=f"m_edit_opts:{quiz_id}:{qid}:{page}")
    kb.button(text=ACT_EDIT_MEDIA, callback_data=f"m_edit_media:{quiz_id}:{qid}:{page}")
    kb.button(text=ACT_DELETE_Q,   callback_data=f"m_delete_q:{quiz_id}:{qid}:{page}")
    kb.button(text=ACT_BACK,       callback_data=f"m_back:{quiz_id}:{page}")
    kb.adjust(1)
    await cb.message.edit_text(txt, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("m_back:"))
async def cb_manage_back(cb:CallbackQuery):
    _, quiz_id, page = cb.data.split(":",2)
    await cb.message.edit_text("اختر سؤالًا لإدارته:", reply_markup=paged_questions_kb(int(quiz_id), int(page), tag="manageq"))

@dp.callback_query(F.data.startswith("m_edit_text:"))
async def cb_m_edit_text(cb:CallbackQuery, state:FSMContext):
    _, quiz_id, qid, page = cb.data.split(":",3)
    await state.update_data(quiz_id=int(quiz_id), question_id=int(qid), page=int(page))
    await state.set_state(BuildStates.waiting_q_text)
    await cb.message.edit_text("أرسل النص الجديد للسؤال:")

@dp.callback_query(F.data.startswith("m_edit_opts:"))
async def cb_m_edit_opts(cb:CallbackQuery, state:FSMContext):
    _, quiz_id, qid, page = cb.data.split(":",3)
    await state.update_data(quiz_id=int(quiz_id), question_id=int(qid), page=int(page))
    await state.set_state(EditOptionStates.waiting_count)
    await cb.message.edit_text("أدخل عدد الخيارات الجديد (2-10):")

@dp.message(EditOptionStates.waiting_count, F.text)
async def m_opts_count(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    try:
        n = int(msg.text.strip()); 
        if n<2 or n>10: raise ValueError
    except ValueError:
        return await msg.reply("أدخل رقمًا بين 2 و 10.")
    await state.update_data(n=n, i=0)
    await state.set_state(EditOptionStates.waiting_text)
    await msg.answer("أرسل نص الخيار 1:", reply_markup=owner_panel_reply_kb())

@dp.message(EditOptionStates.waiting_text, F.text)
async def m_opts_text(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    data = await state.get_data()
    n = int(data["n"]); i = int(data["i"]); qid = int(data["question_id"])
    if i == 0:
        with db() as conn:
            conn.execute("DELETE FROM options WHERE question_id=?", (qid,))
            conn.commit()
    with db() as conn:
        conn.execute("INSERT INTO options(question_id, option_index, text) VALUES (?,?,?)", (qid, i, msg.text.strip()))
        conn.commit()
    i += 1; await state.update_data(i=i)
    if i < n:
        await msg.answer(f"أرسل نص الخيار {i+1}:", reply_markup=owner_panel_reply_kb())
    else:
        await state.set_state(EditOptionStates.waiting_correct)
        await msg.answer(f"أرسل رقم الخيار الصحيح (1-{n}):", reply_markup=owner_panel_reply_kb())

@dp.message(EditOptionStates.waiting_correct, F.text)
async def m_opts_correct(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    data = await state.get_data(); n = int(data["n"]); qid = int(data["question_id"])
    try:
        k = int(msg.text.strip()); 
        if k<1 or k>n: raise ValueError
    except ValueError:
        return await msg.reply("رقم غير صحيح.")
    with db() as conn:
        conn.execute("UPDATE options SET is_correct=0 WHERE question_id=?", (qid,))
        conn.execute("UPDATE options SET is_correct=1 WHERE question_id=? AND option_index=?", (qid, k-1))
        conn.commit()
    await state.clear()
    await msg.answer("تم تحديث الخيارات.", reply_markup=owner_panel_reply_kb())

@dp.callback_query(F.data.startswith("m_edit_media:"))
async def cb_m_edit_media(cb:CallbackQuery, state:FSMContext):
    _, quiz_id, qid, page = cb.data.split(":",3)
    await state.update_data(question_id=int(qid), pos=0)
    await state.set_state(BuildStates.waiting_replace_attachments)
    await cb.message.edit_text(
        "أرسل حتى 5 مرفقات جديدة (سيتم استبدال القديمة).",
        reply_markup=done_button_kb("replace_att")
    )

@dp.message(BuildStates.waiting_replace_attachments, F.photo | F.voice | F.audio)
async def ch_media_collect(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    data = await state.get_data(); qid = int(data["question_id"]); pos = int(data.get("pos",0))
    if pos == 0:
        with db() as conn:
            conn.execute("DELETE FROM question_attachments WHERE question_id=?", (qid,))
            conn.commit()
    if pos >= 5: return await msg.reply("الحد الأقصى 5. اضغطي ✔️ تم أعلى الرسالة.")
    if msg.photo: kind, file_id = "photo", msg.photo[-1].file_id
    elif msg.voice: kind, file_id = "voice", msg.voice.file_id
    elif msg.audio: kind, file_id = "audio", msg.audio.file_id
    else: return await msg.reply("نوع غير مدعوم.")
    with db() as conn:
        conn.execute("""INSERT INTO question_attachments(question_id, kind, file_id, position) VALUES (?,?,?,?)""",
                     (qid, kind, file_id, pos))
        conn.commit()
    await state.update_data(pos=pos+1)
    await msg.reply(f"تم حفظ المرفق ({pos+1}/5).")

@dp.callback_query(F.data.startswith("m_delete_q:"))
async def cb_m_delete(cb:CallbackQuery):
    _, quiz_id, qid, page = cb.data.split(":",3)
    with db() as conn:
        conn.execute("DELETE FROM questions WHERE id=?", (int(qid),))
        conn.commit()
    await cb.message.edit_text("🗑️ تم حذف السؤال.", reply_markup=paged_questions_kb(int(quiz_id), int(page), tag="manageq"))

# ---------------------- Quizzes list / Rename / Delete ----------------------
@dp.callback_query(F.data.startswith("overview_q_page:"))
async def cb_list_quizzes_page(cb: CallbackQuery):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(page=int(page), tag="overview_q"))

@dp.callback_query(F.data.startswith("overview_q:"))
async def cb_overview_quiz(cb: CallbackQuery):
    _, qid = cb.data.split(":",1)
    with db() as conn:
        q = conn.execute("SELECT * FROM quizzes WHERE id=?", (int(qid),)).fetchone()
        cnt = conn.execute("SELECT COUNT(*) FROM questions WHERE quiz_id=?", (int(qid),)).fetchone()[0]
    await cb.message.edit_text(f"اختبار: <b>{q['title']}</b>\nعدد الأسئلة: <b>{cnt}</b>\n(id: <code>{q['id']}</code>)")

@dp.callback_query(F.data.startswith("renameq_page:"), BuildStates.waiting_edit_quiz_title)
async def cb_renameq_page(cb:CallbackQuery, state:FSMContext):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(int(page), "renameq"))

@dp.callback_query(F.data.startswith("renameq:"), BuildStates.waiting_edit_quiz_title)
async def cb_renameq_pick(cb:CallbackQuery, state:FSMContext):
    _, quiz_id = cb.data.split(":",1)
    await state.update_data(quiz_id=int(quiz_id))
    await cb.message.edit_text("أرسل العنوان الجديد:")

@dp.message(BuildStates.waiting_edit_quiz_title, F.text)
async def cb_renameq_do(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    data = await state.get_data(); quiz_id = data["quiz_id"]
    with db() as conn:
        conn.execute("UPDATE quizzes SET title=? WHERE id=?", (msg.text.strip(), quiz_id))
        conn.commit()
    await state.clear()
    await msg.answer("تم تحديث العنوان.", reply_markup=owner_panel_reply_kb())

@dp.callback_query(F.data.startswith("delqz_page:"), BuildStates.waiting_pick_quiz_generic)
async def cb_del_quiz_page(cb:CallbackQuery, state:FSMContext):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(int(page), "delqz"))

@dp.callback_query(F.data.startswith("delqz:"), BuildStates.waiting_pick_quiz_generic)
async def cb_del_quiz_do(cb:CallbackQuery, state:FSMContext):
    _, quiz_id = cb.data.split(":",1)
    with db() as conn:
        conn.execute("DELETE FROM quizzes WHERE id=?", (int(quiz_id),))
        conn.commit()
    await state.clear()
    await cb.message.edit_text("🗑️ تم حذف الاختبار وما يتبعه.")

# ---------------------- Merge Quizzes ----------------------
def _copy_bundle(quiz_dst:int, bundle_id:int, bundle_map:Dict[int,int]) -> int:
    if bundle_id in bundle_map: return bundle_map[bundle_id]
    with db() as conn:
        cur = conn.execute("INSERT INTO media_bundles(quiz_id, created_at) VALUES (?,?)",
                           (quiz_dst, datetime.now(timezone.utc).isoformat()))
        new_b = cur.lastrowid
        atts = conn.execute("SELECT kind, file_id, position FROM media_bundle_attachments WHERE bundle_id=? ORDER BY position",
                            (bundle_id,)).fetchall()
        for a in atts:
            conn.execute("INSERT INTO media_bundle_attachments(bundle_id, kind, file_id, position) VALUES (?,?,?,?)",
                         (new_b, a["kind"], a["file_id"], a["position"]))
        conn.commit()
    bundle_map[bundle_id] = new_b
    return new_b

def _copy_question_to_quiz(qrow:sqlite3.Row, quiz_dst:int, bundle_map:Dict[int,int]) -> int:
    with db() as conn:
        new_bundle_id = None
        if qrow["media_bundle_id"]:
            new_bundle_id = _copy_bundle(quiz_dst, int(qrow["media_bundle_id"]), bundle_map)
        cur = conn.execute("INSERT INTO questions(quiz_id, text, created_at, media_bundle_id) VALUES (?,?,?,?)",
                           (quiz_dst, qrow["text"], datetime.now(timezone.utc).isoformat(), new_bundle_id))
        new_qid = cur.lastrowid
        opts = conn.execute("SELECT option_index, text, is_correct FROM options WHERE question_id=? ORDER BY option_index",
                            (qrow["id"],)).fetchall()
        for o in opts:
            conn.execute("INSERT INTO options(question_id, option_index, text, is_correct) VALUES (?,?,?,?)",
                         (new_qid, o["option_index"], o["text"], o["is_correct"]))
        atts = conn.execute("SELECT kind, file_id, position FROM question_attachments WHERE question_id=? ORDER BY position",
                            (qrow["id"],)).fetchall()
        for a in atts:
            conn.execute("INSERT INTO question_attachments(question_id, kind, file_id, position) VALUES (?,?,?,?)",
                         (new_qid, a["kind"], a["file_id"], a["position"]))
        conn.commit()
        return new_qid

def merge_quizzes_create_new(src_id:int, dst_id:int) -> int:
    with db() as conn:
        src = conn.execute("SELECT * FROM quizzes WHERE id=?", (src_id,)).fetchone()
        dst = conn.execute("SELECT * FROM quizzes WHERE id=?", (dst_id,)).fetchone()
        title = f"دمج: {src['title']} + {dst['title']}"
        cur = conn.execute("INSERT INTO quizzes(title, created_by, created_at) VALUES (?,?,?)",
                           (title, OWNER_ID, datetime.now(timezone.utc).isoformat()))
        new_quiz_id = cur.lastrowid
        conn.commit()
    bundle_map: Dict[int,int] = {}
    for qz in (src_id, dst_id):
        with db() as conn:
            questions = conn.execute("SELECT * FROM questions WHERE quiz_id=? ORDER BY id", (qz,)).fetchall()
        for q in questions:
            _copy_question_to_quiz(q, new_quiz_id, bundle_map)
    return new_quiz_id

@dp.callback_query(F.data.startswith("merge_src_page:"), MergeStates.waiting_pick_src)
async def cb_merge_src_page(cb:CallbackQuery, state:FSMContext):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(int(page), "merge_src"))

@dp.callback_query(F.data.startswith("merge_src:"), MergeStates.waiting_pick_src)
async def cb_merge_pick_src(cb:CallbackQuery, state:FSMContext):
    _, src_id = cb.data.split(":",1)
    await state.update_data(src=int(src_id))
    await state.set_state(MergeStates.waiting_pick_dst)
    await cb.message.edit_text("اختاري الاختبار الثاني (المصدر 2):", reply_markup=paged_quizzes_kb(0, "merge_dst"))

@dp.callback_query(F.data.startswith("merge_dst_page:"), MergeStates.waiting_pick_dst)
async def cb_merge_dst_page(cb:CallbackQuery, state:FSMContext):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(int(page), "merge_dst"))

@dp.callback_query(F.data.startswith("merge_dst:"), MergeStates.waiting_pick_dst)
async def cb_merge_do(cb:CallbackQuery, state:FSMContext):
    _, dst_id = cb.data.split(":",1)
    data = await state.get_data()
    new_id = merge_quizzes_create_new(int(data["src"]), int(dst_id))
    await state.clear()
    await cb.message.edit_text(f"✅ تم إنشاء اختبار دمج جديد (ID <code>{new_id}</code>).", reply_markup=owner_panel_reply_kb())

# ---------------------- Export Quiz ----------------------
def export_quiz_json(quiz_id:int) -> dict:
    with db() as conn:
        quiz = conn.execute("SELECT * FROM quizzes WHERE id=?", (quiz_id,)).fetchone()
        questions = conn.execute("SELECT * FROM questions WHERE quiz_id=? ORDER BY id", (quiz_id,)).fetchall()
        bundle_ids = sorted({int(q["media_bundle_id"]) for q in questions if q["media_bundle_id"] is not None})
        bundles = []
        for bid in bundle_ids:
            atts = conn.execute("SELECT kind, file_id, position FROM media_bundle_attachments WHERE bundle_id=? ORDER BY position",
                                (bid,)).fetchall()
            bundles.append({
                "id": bid,
                "attachments": [{"kind": a["kind"], "file_id": a["file_id"], "position": a["position"]} for a in atts]
            })
        qs_out = []
        for q in questions:
            opts = conn.execute("SELECT option_index, text, is_correct FROM options WHERE question_id=? ORDER BY option_index",
                                (q["id"],)).fetchall()
            atts = conn.execute("SELECT kind, file_id, position FROM question_attachments WHERE question_id=? ORDER BY position",
                                (q["id"],)).fetchall()
            qs_out.append({
                "id": q["id"],
                "text": q["text"],
                "created_at": q["created_at"],
                "media_bundle_id": q["media_bundle_id"],
                "options": [{"option_index": o["option_index"], "text": o["text"], "is_correct": int(o["is_correct"])} for o in opts],
                "attachments": [{"kind": a["kind"], "file_id": a["file_id"], "position": a["position"]} for a in atts]
            })
        return {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "quiz": {"id": quiz["id"], "title": quiz["title"], "created_by": quiz["created_by"], "created_at": quiz["created_at"]},
            "media_bundles": bundles,
            "questions": qs_out
        }

@dp.callback_query(F.data.startswith("export_pick_page:"), ExportStates.waiting_pick_quiz)
async def cb_export_page(cb:CallbackQuery, state:FSMContext):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(int(page), "export_pick"))

@dp.callback_query(F.data.startswith("export_pick:"), ExportStates.waiting_pick_quiz)
async def cb_export_pick(cb:CallbackQuery, state:FSMContext):
    _, quiz_id = cb.data.split(":",1)
    data = export_quiz_json(int(quiz_id))
    safe_title = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in data["quiz"]["title"]) or f"quiz_{quiz_id}"
    fname = f"{safe_title}_ID{quiz_id}_{int(time.time())}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    await state.clear()
    await cb.message.answer_document(document=FSInputFile(fname), caption="📤 تصدير JSON للاختبار")
    try:
        os.remove(fname)
    except Exception:
        pass

# ---------------------- Publish as QUIZ POLLS (with numbering) ----------------------
RATE_LIMIT_SECONDS = 0.05

async def _safe_send(op, *args, **kwargs):
    try:
        msg = await op(*args, **kwargs)
        await asyncio.sleep(RATE_LIMIT_SECONDS)
        return msg
    except TelegramRetryAfter as e:
        wait = getattr(e, "retry_after", 1) or 1
        await asyncio.sleep(wait)
        try:
            msg = await op(*args, **kwargs)
            await asyncio.sleep(RATE_LIMIT_SECONDS)
            return msg
        except Exception:
            return None
    except Exception:
        return None

@dp.callback_query(F.data.startswith("pub_pickq_page:"), PublishStates.waiting_pick_quiz)
async def cb_pub_page(cb:CallbackQuery, state:FSMContext):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(int(page), "pub_pickq"))

@dp.callback_query(F.data.startswith("pub_pickq:"), PublishStates.waiting_pick_quiz)
async def cb_pub_choose_duration(cb:CallbackQuery, state:FSMContext):
    _, quiz_id = cb.data.split(":",1)
    await state.update_data(quiz_id=int(quiz_id))
    await state.set_state(PublishStates.waiting_duration_choice)
    await cb.message.edit_text("حددي مدة الاختبار:", reply_markup=publish_duration_kb())

@dp.callback_query(F.data.startswith("dur:"), PublishStates.waiting_duration_choice)
async def cb_pub_duration_selected(cb:CallbackQuery, state:FSMContext):
    _, sel = cb.data.split(":",1)
    data = await state.get_data(); quiz_id = int(data["quiz_id"])
    if sel == "custom":
        await state.set_state(PublishStates.waiting_custom_hours)
        return await cb.message.edit_text("أدخلي عدد الساعات (مثال: 3 أو 6 أو 48):")
    if sel == "none":
        expires_at = None
    else:
        hours = 12 if sel == "12" else 24
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    await _do_publish_polls(cb, quiz_id, expires_at); await state.clear()

@dp.message(PublishStates.waiting_custom_hours, F.text)
async def cb_pub_custom_hours(msg:Message, state:FSMContext):
    if not await ensure_owner(msg): await state.clear(); return
    data = await state.get_data(); quiz_id = int(data["quiz_id"])
    try:
        hours = int(msg.text.strip()); 
        if hours <= 0 or hours > 240: raise ValueError
    except ValueError:
        return await msg.reply("أدخل رقم ساعات صحيح (1 إلى 240).", reply_markup=owner_panel_reply_kb())
    expires_at = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    class Dummy: pass
    dummy = Dummy(); dummy.message = msg; dummy.from_user = msg.from_user
    await _do_publish_polls(dummy, quiz_id, expires_at); await state.clear()

async def _do_publish_polls(cb_or_msg, quiz_id:int, expires_at: Optional[str]):
    chat_id = cb_or_msg.message.chat.id
    with db() as conn:
        quiz = conn.execute("SELECT * FROM quizzes WHERE id=? AND is_archived=0",(quiz_id,)).fetchone()
        qs = conn.execute("SELECT id, text, media_bundle_id FROM questions WHERE quiz_id=? ORDER BY id",(quiz_id,)).fetchall()
    if not quiz or not qs:
        return await bot.send_message(chat_id, "اختبار غير صالح أو بلا أسئلة.")
    exp_line = "بدون حدّ زمني" if not expires_at else f"حتى: <code>{expires_at}</code> (UTC)"
    await _safe_send(bot.send_message, chat_id, f"📣 اختبار: <b>{quiz['title']}</b>\nالوقت: {exp_line}\nأجيبي على الاستفتاءات (Quiz) بالأسفل.")

    sent_bundles = set()
    for idx, q in enumerate(qs, start=1):
        qid = q["id"]; qtext = q["text"]; bundle_id = q["media_bundle_id"]
        # Bundle media (once per bundle)
        if bundle_id and bundle_id not in sent_bundles:
            for att in get_bundle_atts(int(bundle_id)):
                if att["kind"] == "photo":
                    await _safe_send(bot.send_photo, chat_id, att["file_id"])
                elif att["kind"] == "voice":
                    await _safe_send(bot.send_voice, chat_id, att["file_id"])
                else:
                    await _safe_send(bot.send_audio, chat_id, att["file_id"])
            sent_bundles.add(bundle_id)
        # Question attachments
        for att in get_question_atts(qid):
            if att["kind"] == "photo":
                await _safe_send(bot.send_photo, chat_id, att["file_id"])
            elif att["kind"] == "voice":
                await _safe_send(bot.send_voice, chat_id, att["file_id"])
            else:
                await _safe_send(bot.send_audio, chat_id, att["file_id"])
        # Build poll
        opts = options_for_question(qid)
        if not (2 <= len(opts) <= 10):
            await _safe_send(bot.send_message, chat_id, f"⚠️ السؤال Q{qid} لديه {len(opts)} خيار. تلغرام يسمح 2..10.")
            continue
        options_texts = [o["text"] for o in opts]
        correct_row = next((o for o in opts if int(o["is_correct"])==1), None)
        correct_option_id = int(correct_row["option_index"]) if correct_row is not None else 0
        # Auto-numbering
        qtext_numbered = f"[{idx}] {qtext}"
        # Send poll (quiz)
        try:
            poll_msg = await bot.send_poll(
                chat_id=chat_id,
                question=qtext_numbered[:255],  # Telegram limit
                options=options_texts,
                type="quiz",
                correct_option_id=correct_option_id,
                is_anonymous=False,
                allows_multiple_answers=False
            )
            with db() as conn:
                conn.execute("INSERT INTO sent_polls(chat_id, quiz_id, question_id, poll_id, message_id, expires_at, is_closed) VALUES (?,?,?,?,?,?,0)",
                             (chat_id, quiz_id, qid, poll_msg.poll.id, poll_msg.message_id, expires_at))
                conn.execute("INSERT INTO sent_msgs(chat_id, quiz_id, message_id, expires_at) VALUES (?,?,?,?)",
                             (chat_id, quiz_id, poll_msg.message_id, expires_at))
                conn.commit()
            await asyncio.sleep(RATE_LIMIT_SECONDS)
        except Exception as e:
            await _safe_send(bot.send_message, chat_id, f"⚠️ تعذّر إرسال استفتاء للسؤال Q{qid}: {e}")

# ---------------------- Auto-close expired polls ----------------------
async def _close_expired_polls_once():
    now = _now_utc()
    with db() as conn:
        rows = conn.execute("""
            SELECT id, chat_id, message_id, expires_at
            FROM sent_polls
            WHERE expires_at IS NOT NULL AND is_closed=0
        """).fetchall()
    for r in rows:
        exp_s = r["expires_at"]
        try:
            exp_dt = datetime.fromisoformat(exp_s)
        except Exception:
            continue
        if now <= exp_dt:
            continue
        chat_id = int(r["chat_id"])
        message_id = int(r["message_id"] or 0)
        if not message_id:
            with db() as conn:
                conn.execute("UPDATE sent_polls SET is_closed=1 WHERE id=?", (r["id"],))
                conn.commit()
            continue
        try:
            await bot.stop_poll(chat_id=chat_id, message_id=message_id)
            with db() as conn:
                conn.execute("UPDATE sent_polls SET is_closed=1 WHERE id=?", (r["id"],))
                conn.commit()
            await asyncio.sleep(RATE_LIMIT_SECONDS)
        except TelegramRetryAfter as e:
            wait = getattr(e, "retry_after", 1) or 1
            await asyncio.sleep(wait)
        except Exception:
            pass

async def expiry_watcher():
    while True:
        try:
            await _close_expired_polls_once()
        except Exception:
            pass
        await asyncio.sleep(20)  # افحص كل 20 ثانية

# ---------------------- Poll answers handler ----------------------
@dp.poll_answer()
async def handle_poll_answer(pa: PollAnswer):
    user_id = pa.user.id
    name = _display_name(pa.user)
    poll_id = pa.poll_id
    chosen = (pa.option_ids or [])[0] if pa.option_ids else None
    if chosen is None:
        return
    with db() as conn:
        row = conn.execute("SELECT chat_id, quiz_id, question_id, expires_at, is_closed FROM sent_polls WHERE poll_id=?", (poll_id,)).fetchone()
    if not row:
        return
    chat_id = row["chat_id"]; quiz_id = row["quiz_id"]; question_id = row["question_id"]
    expired = False
    if row["expires_at"]:
        try:
            expired = datetime.fromisoformat(row["expires_at"]) < _now_utc()
        except:
            expired = False
    if expired or int(row["is_closed"] or 0) == 1:
        return
    # Save name for scoreboard
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO participant_names(origin_chat_id,user_id,quiz_id,name) VALUES (?,?,?,?)",
            (chat_id, user_id, quiz_id, name),
        )
        conn.commit()
    # avoid duplicates
    with db() as conn:
        prev = conn.execute("SELECT 1 FROM responses WHERE chat_id=? AND user_id=? AND question_id=?",
                            (chat_id, user_id, question_id)).fetchone()
    if prev:
        return
    with db() as conn:
        opt = conn.execute("SELECT text, is_correct FROM options WHERE question_id=? AND option_index=?",
                           (question_id, chosen)).fetchone()
    is_correct = 1 if opt and int(opt["is_correct"])==1 else 0
    with db() as conn:
        conn.execute("""INSERT INTO responses(chat_id,user_id,question_id,option_index,is_correct,answered_at)
                        VALUES (?,?,?,?,?,?)""",
                     (chat_id, user_id, question_id, int(chosen), is_correct, datetime.now(timezone.utc).isoformat()))
        conn.commit()
    try:
        await _celebrate(chat_id, bool(is_correct))
    except:
        pass
    # final score if finished
    q_ids = get_quiz_question_ids(quiz_id)
    with db() as conn:
        marks = ",".join(["?"] * len(q_ids))
        sql_count = f"SELECT COUNT(DISTINCT question_id) FROM responses WHERE chat_id=? AND user_id=? AND question_id IN ({marks})"
        answered_cnt = conn.execute(sql_count, (chat_id, user_id, *q_ids)).fetchone()[0] or 0
    if answered_cnt == len(q_ids):
        with db() as conn:
            sql_score = f"SELECT SUM(is_correct) FROM responses WHERE chat_id=? AND user_id=? AND question_id IN ({marks})"
            total = conn.execute(sql_score, (chat_id, user_id, *q_ids)).fetchone()[0] or 0
        try:
            await bot.send_message(chat_id, f"🎉 النتيجة النهائية — {hlink_user(name, user_id)}: <b>{total}</b> / {len(q_ids)}")
        except TelegramBadRequest:
            pass

# ---------------------- Scoreboard ----------------------
@dp.callback_query(F.data.startswith("score_pickq_page:"))
async def cb_scoreboard_page(cb:CallbackQuery):
    _, page = cb.data.split(":",1)
    await cb.message.edit_reply_markup(reply_markup=paged_quizzes_kb(int(page), "score_pickq"))

@dp.callback_query(F.data.startswith("score_pickq:"))
async def cb_scoreboard_show(cb:CallbackQuery):
    _, quiz_id = cb.data.split(":",1); quiz_id = int(quiz_id)
    chat_id = cb.message.chat.id; q_ids = get_quiz_question_ids(quiz_id)
    if not q_ids: return await cb.answer("لا توجد أسئلة.")
    with db() as conn:
        q_marks = ",".join(["?"] * len(q_ids))
        sql = f"""
            SELECT user_id, SUM(is_correct) AS score, COUNT(*) AS answered
            FROM responses
            WHERE chat_id=? AND question_id IN ({q_marks})
            GROUP BY user_id
            ORDER BY score DESC, answered DESC
            LIMIT 20
        """
        rows = conn.execute(sql, (chat_id, *q_ids)).fetchall()
    if not rows:
        return await cb.message.edit_text("لا توجد إجابات بعد.")
    lines = ["🏆 <b>لوحة النتائج</b>"]
    for i, r in enumerate(rows, start=1):
        lines.append(f"{i}. UID <code>{r['user_id']}</code> — نقاط: <b>{r['score']}</b> (من {r['answered']})")
    await cb.message.edit_text("\n".join(lines))

# ---------------------- Danger Zone ----------------------
@dp.callback_query(F.data == "yes:wipe")
async def cb_wipe_yes(cb:CallbackQuery):
    with db() as conn:
        conn.executescript("""
            DELETE FROM responses;
            DELETE FROM options;
            DELETE FROM questions;
            DELETE FROM quizzes;
            DELETE FROM user_progress;
            DELETE FROM sent_msgs;
            DELETE FROM participant_names;
            DELETE FROM question_attachments;
            DELETE FROM media_bundle_attachments;
            DELETE FROM media_bundles;
            DELETE FROM sent_polls;
        """); conn.commit()
    await cb.message.edit_text("تم الحذف الشامل ✅")

@dp.callback_query(F.data == "no:wipe")
async def cb_wipe_no(cb:CallbackQuery):
    await cb.message.edit_text("تم الإلغاء.")

# ---------------------- File ID helper (Owner only) ----------------------
@dp.message(F.sticker | F.animation | F.photo | F.video | F.voice | F.audio)
async def show_file_id(msg: Message):
    try:
        if msg.from_user.id != OWNER_ID: return
        if msg.sticker:
            fid = msg.sticker.file_id; kind = "Sticker"
        elif msg.animation:
            fid = msg.animation.file_id; kind = "Animation"
        elif msg.photo:
            fid = msg.photo[-1].file_id; kind = "Photo"
        elif msg.video:
            fid = msg.video.file_id; kind = "Video"
        elif msg.voice:
            fid = msg.voice.file_id; kind = "Voice"
        elif msg.audio:
            fid = msg.audio.file_id; kind = "Audio"
        else:
            return
        print(f"[file_id] {kind}: {fid}")
        await msg.reply(f"{kind} file_id:\n<code>{fid}</code>", reply_markup=owner_panel_reply_kb())
    except Exception:
        pass

# ---------------------- Run ----------------------
async def main():
    print("✅ Bot is running…")
    # تشغيل ووتشر إغلاق الاستفتاءات
    asyncio.create_task(expiry_watcher())
    await dp.start_polling(
        bot,
        allowed_updates=[
            "message",
            "callback_query",
            "poll",
            "poll_answer",
        ]
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped.")
