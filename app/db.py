import json
from datetime import datetime, timedelta, timezone

import aiosqlite

from . import config


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS stream_state (
    name TEXT PRIMARY KEY,
    messages INTEGER,
    bytes INTEGER,
    first_seq INTEGER,
    last_seq INTEGER,
    subjects TEXT,
    retention TEXT,
    created TEXT,
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS consumer_state (
    stream TEXT NOT NULL,
    name TEXT NOT NULL,
    durable TEXT,
    num_pending INTEGER,
    num_ack_pending INTEGER,
    delivered INTEGER,
    redelivered INTEGER,
    ack_floor INTEGER,
    updated_at TEXT,
    PRIMARY KEY (stream, name)
);
CREATE TABLE IF NOT EXISTS snapshots (
    ts TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    stream TEXT,
    messages INTEGER,
    bytes INTEGER,
    num_pending INTEGER,
    num_ack_pending INTEGER,
    delivered INTEGER,
    redelivered INTEGER
);
CREATE INDEX IF NOT EXISTS idx_snapshots ON snapshots (entity_type, entity_id, ts);
CREATE TABLE IF NOT EXISTS events (
    ts TEXT NOT NULL,
    type TEXT NOT NULL,
    stream TEXT,
    consumer TEXT,
    subject TEXT,
    details TEXT
);
CREATE INDEX IF NOT EXISTS idx_events ON events (ts);
"""


async def init_db():
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(_SCHEMA)
        await db.commit()


async def upsert_stream(name, messages, bytes_, first_seq, last_seq, subjects, retention, created):
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            """INSERT INTO stream_state
               (name, messages, bytes, first_seq, last_seq, subjects, retention, created, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(name) DO UPDATE SET
                 messages=excluded.messages, bytes=excluded.bytes, first_seq=excluded.first_seq,
                 last_seq=excluded.last_seq, subjects=excluded.subjects, retention=excluded.retention,
                 created=excluded.created, updated_at=excluded.updated_at""",
            (name, messages, bytes_, first_seq, last_seq, subjects, retention, created, now_iso()),
        )
        await db.commit()


async def upsert_consumer(stream, name, durable, num_pending, num_ack_pending, delivered, redelivered, ack_floor):
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            """INSERT INTO consumer_state
               (stream, name, durable, num_pending, num_ack_pending, delivered, redelivered, ack_floor, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(stream, name) DO UPDATE SET
                 durable=excluded.durable, num_pending=excluded.num_pending, num_ack_pending=excluded.num_ack_pending,
                 delivered=excluded.delivered, redelivered=excluded.redelivered, ack_floor=excluded.ack_floor,
                 updated_at=excluded.updated_at""",
            (stream, name, durable, num_pending, num_ack_pending, delivered, redelivered, ack_floor, now_iso()),
        )
        await db.commit()


async def insert_snapshot(entity_type, entity_id, stream=None, messages=None, bytes_=None,
                          num_pending=None, num_ack_pending=None, delivered=None, redelivered=None):
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            """INSERT INTO snapshots
               (ts, entity_type, entity_id, stream, messages, bytes, num_pending, num_ack_pending, delivered, redelivered)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (now_iso(), entity_type, entity_id, stream, messages, bytes_,
             num_pending, num_ack_pending, delivered, redelivered),
        )
        await db.commit()


async def insert_event(type_, stream, consumer, subject, details):
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            "INSERT INTO events (ts, type, stream, consumer, subject, details) VALUES (?,?,?,?,?,?)",
            (now_iso(), type_, stream, consumer, subject,
             json.dumps(details, ensure_ascii=False, default=str)),
        )
        await db.commit()


async def get_streams():
    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM stream_state ORDER BY name")
        return [dict(r) for r in await cur.fetchall()]


async def get_consumers():
    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM consumer_state ORDER BY stream, name")
        return [dict(r) for r in await cur.fetchall()]


async def get_snapshots(entity_type=None, entity_id=None, minutes=60):
    sql = "SELECT * FROM snapshots"
    conds, params = [], []
    if entity_type:
        conds.append("entity_type=?")
        params.append(entity_type)
    if entity_id:
        conds.append("entity_id=?")
        params.append(entity_id)
    if minutes:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat(timespec="seconds")
        conds.append("ts>=?")
        params.append(cutoff)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY ts"
    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, params)
        return [dict(r) for r in await cur.fetchall()]


async def get_events(limit=200):
    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM events ORDER BY ts DESC LIMIT ?", (limit,))
        out = []
        for r in await cur.fetchall():
            d = dict(r)
            if d.get("details"):
                try:
                    d["details"] = json.loads(d["details"])
                except Exception:
                    pass
            out.append(d)
        return out


async def reconcile(stream_names, consumer_map):
    """Entfernt State-Zeilen für Streams/Consumer, die nicht mehr existieren."""
    async with aiosqlite.connect(config.DB_PATH) as db:
        if stream_names:
            ph = ",".join("?" for _ in stream_names)
            await db.execute(f"DELETE FROM stream_state WHERE name NOT IN ({ph})", stream_names)
            await db.execute(f"DELETE FROM consumer_state WHERE stream NOT IN ({ph})", stream_names)
        for stream, names in consumer_map.items():
            if names:
                ph = ",".join("?" for _ in names)
                await db.execute(
                    f"DELETE FROM consumer_state WHERE stream=? AND name NOT IN ({ph})",
                    [stream] + names,
                )
            else:
                await db.execute("DELETE FROM consumer_state WHERE stream=?", (stream,))
        await db.commit()
