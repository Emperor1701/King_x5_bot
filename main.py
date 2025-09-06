#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Migrate Telegram Quiz Bot data from SQLite -> PostgreSQL (Railway).

Usage:
  export SQLITE_PATH=/absolute/path/quiz_bot.db
  export DATABASE_URL=postgres://USER:PASS@HOST:PORT/DB
  python migrate_sqlite_to_postgres.py

- أوقف البوت أثناء الترحيل.
- السكربت ينشئ الجداول إذا ناقصة، ويحافظ على IDs، وآمن لإعادة التشغيل.
"""
import os, sys, sqlite3, psycopg
from psycopg.rows import dict_row

SQLITE_PATH = os.getenv("SQLITE_PATH") or os.getenv("DB_PATH")
PG_DSN = os.getenv("DATABASE_URL")

if not SQLITE_PATH or not os.path.exists(SQLITE_PATH):
    print("❌ Set SQLITE_PATH to your existing quiz_bot.db file.")
    sys.exit(1)
if not PG_DSN:
    print("❌ Set DATABASE_URL to your Postgres connection string.")
    sys.exit(1)

print(f"[i] Reading from SQLite: {SQLITE_PATH}")
print(f"[i] Writing to Postgres: {PG_DSN.split('@')[-1]}")

TABLES = [
    "quizzes","questions","options","responses","user_progress","sent_msgs",
    "participant_names","question_attachments","media_bundles","media_bundle_attachments",
    "sent_polls","writing_submissions","hl_results","brief_windows",
]

PG_SCHEMA = {
    "quizzes": """
        CREATE TABLE IF NOT EXISTS quizzes(
          id INTEGER PRIMARY KEY,
          title TEXT NOT NULL,
          created_by BIGINT NOT NULL,
          created_at TEXT NOT NULL,
          is_archived INTEGER NOT NULL DEFAULT 0,
          grading_profile TEXT NOT NULL DEFAULT 'NONE'
        );""",
    "questions": """
        CREATE TABLE IF NOT EXISTS questions(
          id INTEGER PRIMARY KEY,
          quiz_id INTEGER NOT NULL,
          text TEXT NOT NULL,
          created_at TEXT NOT NULL,
          media_bundle_id INTEGER,
          photo TEXT, audio TEXT, audio_is_voice INTEGER DEFAULT 0
        );""",
    "options": """
        CREATE TABLE IF NOT EXISTS options(
          id INTEGER PRIMARY KEY,
          question_id INTEGER NOT NULL,
          option_index INTEGER NOT NULL,
          text TEXT NOT NULL,
          is_correct INTEGER NOT NULL DEFAULT 0
        );""",
    "responses": """
        CREATE TABLE IF NOT EXISTS responses(
          id INTEGER PRIMARY KEY,
          chat_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          question_id INTEGER NOT NULL,
          option_index INTEGER NOT NULL,
          is_correct INTEGER NOT NULL,
          answered_at TEXT NOT NULL,
          CONSTRAINT uniq_resp UNIQUE(chat_id,user_id,question_id)
        );""",
    "user_progress": """
        CREATE TABLE IF NOT EXISTS user_progress(
          id INTEGER PRIMARY KEY,
          origin_chat_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          quiz_id INTEGER NOT NULL,
          q_pos INTEGER NOT NULL DEFAULT 0,
          started_at TEXT NOT NULL,
          finished_at TEXT
        );""",
    "sent_msgs": """
        CREATE TABLE IF NOT EXISTS sent_msgs(
          id INTEGER PRIMARY KEY,
          chat_id BIGINT NOT NULL,
          quiz_id INTEGER NOT NULL,
          message_id BIGINT NOT NULL,
          expires_at TEXT
        );""",
    "participant_names": """
        CREATE TABLE IF NOT EXISTS participant_names(
          id INTEGER PRIMARY KEY,
          origin_chat_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          quiz_id INTEGER NOT NULL,
          name TEXT NOT NULL,
          CONSTRAINT uniq_pn UNIQUE(origin_chat_id,user_id,quiz_id)
        );""",
    "question_attachments": """
        CREATE TABLE IF NOT EXISTS question_attachments(
          id INTEGER PRIMARY KEY,
          question_id INTEGER NOT NULL,
          kind TEXT NOT NULL,
          file_id TEXT NOT NULL,
          position INTEGER NOT NULL
        );""",
    "media_bundles": """
        CREATE TABLE IF NOT EXISTS media_bundles(
          id INTEGER PRIMARY KEY,
          quiz_id INTEGER NOT NULL,
          created_at TEXT NOT NULL
        );""",
    "media_bundle_attachments": """
        CREATE TABLE IF NOT EXISTS media_bundle_attachments(
          id INTEGER PRIMARY KEY,
          bundle_id INTEGER NOT NULL,
          kind TEXT NOT NULL,
          file_id TEXT NOT NULL,
          position INTEGER NOT NULL
        );""",
    "sent_polls": """
        CREATE TABLE IF NOT EXISTS sent_polls(
          id INTEGER PRIMARY KEY,
          chat_id BIGINT NOT NULL,
          quiz_id INTEGER NOT NULL,
          question_id INTEGER NOT NULL,
          poll_id TEXT NOT NULL,
          message_id BIGINT,
          expires_at TEXT,
          is_closed INTEGER NOT NULL DEFAULT 0
        );""",
    "writing_submissions": """
        CREATE TABLE IF NOT EXISTS writing_submissions(
          id INTEGER PRIMARY KEY,
          origin_chat_id BIGINT NOT NULL,
          quiz_id INTEGER,
          user_id BIGINT NOT NULL,
          text TEXT NOT NULL,
          score INTEGER NOT NULL,
          level TEXT NOT NULL,
          evaluated_at TEXT NOT NULL,
          details_json TEXT
        );""",
    "hl_results": """
        CREATE TABLE IF NOT EXISTS hl_results(
          id INTEGER PRIMARY KEY,
          origin_chat_id BIGINT NOT NULL,
          quiz_id INTEGER NOT NULL,
          user_id BIGINT NOT NULL,
          correct_count INTEGER NOT NULL,
          total_count INTEGER NOT NULL,
          level TEXT NOT NULL,
          finished_at TEXT NOT NULL,
          CONSTRAINT uniq_hl UNIQUE(origin_chat_id,quiz_id,user_id)
        );""",
    "brief_windows": """
        CREATE TABLE IF NOT EXISTS brief_windows(
          id INTEGER PRIMARY KEY,
          origin_chat_id BIGINT NOT NULL,
          opened_by BIGINT NOT NULL,
          opened_at TEXT NOT NULL,
          closes_at TEXT NOT NULL,
          is_open INTEGER NOT NULL DEFAULT 1
        );""",
}

INSERT_SQL = {
    "quizzes": ("id,title,created_by,created_at,is_archived,grading_profile", "%s,%s,%s,%s,%s,%s"),
    "questions": ("id,quiz_id,text,created_at,media_bundle_id,photo,audio,audio_is_voice", "%s,%s,%s,%s,%s,%s,%s,%s"),
    "options": ("id,question_id,option_index,text,is_correct", "%s,%s,%s,%s,%s"),
    "responses": ("id,chat_id,user_id,question_id,option_index,is_correct,answered_at", "%s,%s,%s,%s,%s,%s,%s"),
    "user_progress": ("id,origin_chat_id,user_id,quiz_id,q_pos,started_at,finished_at", "%s,%s,%s,%s,%s,%s,%s"),
    "sent_msgs": ("id,chat_id,quiz_id,message_id,expires_at", "%s,%s,%s,%s,%s"),
    "participant_names": ("id,origin_chat_id,user_id,quiz_id,name", "%s,%s,%s,%s,%s"),
    "question_attachments": ("id,question_id,kind,file_id,position", "%s,%s,%s,%s,%s"),
    "media_bundles": ("id,quiz_id,created_at", "%s,%s,%s"),
    "media_bundle_attachments": ("id,bundle_id,kind,file_id,position", "%s,%s,%s,%s,%s"),
    "sent_polls": ("id,chat_id,quiz_id,question_id,poll_id,message_id,expires_at,is_closed", "%s,%s,%s,%s,%s,%s,%s,%s"),
    "writing_submissions": ("id,origin_chat_id,quiz_id,user_id,text,score,level,evaluated_at,details_json", "%s,%s,%s,%s,%s,%s,%s,%s,%s"),
    "hl_results": ("id,origin_chat_id,quiz_id,user_id,correct_count,total_count,level,finished_at", "%s,%s,%s,%s,%s,%s,%s,%s"),
    "brief_windows": ("id,origin_chat_id,opened_by,opened_at,closes_at,is_open", "%s,%s,%s,%s,%s,%s"),
}
ON_CONFLICT = {t: "id" for t in TABLES}

def ensure_schema(pg):
    with pg.cursor() as cur:
        for ddl in PG_SCHEMA.values():
            cur.execute(ddl)
    pg.commit()

def copy_table(sconn, pconn, table):
    cols, placeholders = INSERT_SQL[table]
    sconn.row_factory = sqlite3.Row
    rows = sconn.execute(f"SELECT {cols.replace('%s,','')} FROM {table}").fetchall()
    if not rows:
        print(f"[=] {table}: no rows"); return
    print(f"[>] {table}: copying {len(rows)} rows …")
    with pconn.cursor() as cur:
        stmt = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({ON_CONFLICT[table]}) DO NOTHING"
        cur.executemany(stmt, [tuple(r[c.strip()] for c in cols.split(",")) for r in rows])
    pconn.commit()

def main():
    with sqlite3.connect(SQLITE_PATH) as sconn, psycopg.connect(PG_DSN, autocommit=False) as pg:
        ensure_schema(pg)
        for t in TABLES:
            try:
                copy_table(sconn, pg, t)
            except Exception as e:
                print(f"[!] {t}: {e}")
        print("\n[✓] Migration done.")
        with pg.cursor(row_factory=dict_row) as cur:
            for t in TABLES:
                try:
                    cur.execute(f"SELECT COUNT(*) AS n FROM {t}")
                    print(f"  - {t}: {cur.fetchone()['n']}")
                except Exception as e:
                    print(f"  - {t}: ERROR {e}")

if __name__ == "__main__":
    main()
