"""SQLite persistence with atomic, compare-and-set status transitions.

Exactly-once dispatch hinges on ``claim``: a worker may send a notification
only after it atomically flips the row PENDING -> SENDING. The UPDATE's
``WHERE status = 'PENDING'`` makes this a compare-and-set, so if two workers
(or two processes sharing the DB file) race for the same row, SQLite's write
lock serializes them and exactly one sees ``rowcount == 1``.

Every transition also appends to ``dispatch_log`` inside the same transaction,
so the audit trail can never disagree with the current status.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import time
from contextlib import contextmanager
from typing import Iterator

from .models import Channel, Notification, Priority, Status

SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    id                  TEXT PRIMARY KEY,
    idempotency_key     TEXT UNIQUE,
    channel             TEXT NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    recipient           TEXT NOT NULL,
    subject             TEXT,
    body                TEXT NOT NULL,
    priority            INTEGER NOT NULL CHECK (priority BETWEEN 1 AND 3),
    status              TEXT NOT NULL
                        CHECK (status IN ('PENDING', 'SENDING', 'DISPATCHED', 'FAILED')),
    attempts            INTEGER NOT NULL DEFAULT 0,
    created_at          REAL NOT NULL,
    updated_at          REAL NOT NULL,
    next_attempt_at     REAL NOT NULL DEFAULT 0,
    lease_expires_at    REAL,
    dispatched_at       REAL,
    provider_message_id TEXT,
    last_error          TEXT
);
CREATE INDEX IF NOT EXISTS idx_notifications_open
    ON notifications (status, priority, created_at);

CREATE TABLE IF NOT EXISTS dispatch_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    notification_id TEXT NOT NULL REFERENCES notifications (id),
    attempt         INTEGER NOT NULL,
    from_status     TEXT,
    to_status       TEXT NOT NULL,
    detail          TEXT,
    at              REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dispatch_log_notification
    ON dispatch_log (notification_id);
"""

_COLUMNS = (
    "id, idempotency_key, channel, recipient, subject, body, priority, status, attempts, "
    "created_at, updated_at, next_attempt_at, dispatched_at, provider_message_id, last_error"
)


def _row_to_notification(row: sqlite3.Row) -> Notification:
    return Notification(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        channel=Channel(row["channel"]),
        recipient=row["recipient"],
        subject=row["subject"],
        body=row["body"],
        priority=Priority(row["priority"]),
        status=Status(row["status"]),
        attempts=row["attempts"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        next_attempt_at=row["next_attempt_at"],
        dispatched_at=row["dispatched_at"],
        provider_message_id=row["provider_message_id"],
        last_error=row["last_error"],
    )


class NotificationStore:
    def __init__(self, path: str, clock=time.time) -> None:
        self.path = path
        self._clock = clock
        self._local = threading.local()
        self._tmpdir = None
        if path == ":memory:":
            # A true in-memory DB is private to one connection, and shared-cache
            # mode uses table locks that ignore busy_timeout. A throwaway file
            # keeps the exact WAL semantics production uses.
            self._tmpdir = tempfile.TemporaryDirectory(prefix="notifier-")
            self.path = os.path.join(self._tmpdir.name, "notifications.db")
        self._conn().executescript(SCHEMA)

    # -- connection / transaction plumbing ---------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path, timeout=10.0,
            isolation_level=None,  # autocommit; transactions are explicit below
            check_same_thread=True,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")  # readers don't block the writer
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    def _conn(self) -> sqlite3.Connection:
        """One connection per thread: sqlite3 connections aren't thread-safe."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = self._connect()
        return conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """BEGIN IMMEDIATE takes the write lock up front, so read-then-write
        sequences inside the block can't interleave with another writer."""
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # -- writes -------------------------------------------------------------

    def insert(self, n: Notification) -> tuple[Notification, bool]:
        """Persist a new notification as PENDING.

        Returns ``(notification, created)``. If ``idempotency_key`` was already
        used, nothing is inserted and the original row is returned with
        ``created=False`` -- client retries of the same request are harmless.
        """
        now = self._clock()
        with self._tx() as conn:
            cur = conn.execute(
                """INSERT INTO notifications
                       (id, idempotency_key, channel, recipient, subject, body, priority,
                        status, attempts, created_at, updated_at, next_attempt_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?, 0)
                   ON CONFLICT (idempotency_key) DO NOTHING""",
                (n.id, n.idempotency_key, n.channel.value, n.recipient, n.subject,
                 n.body, int(n.priority), now, now),
            )
            if cur.rowcount == 0:
                row = conn.execute(
                    f"SELECT {_COLUMNS} FROM notifications WHERE idempotency_key = ?",
                    (n.idempotency_key,),
                ).fetchone()
                return _row_to_notification(row), False
            self._log(conn, n.id, 0, None, Status.PENDING, "accepted", now)
        return self.get(n.id), True  # type: ignore[return-value]

    def claim(self, notification_id: str, lease_seconds: float) -> Notification | None:
        """Atomically move PENDING -> SENDING and bump ``attempts``.

        Returns the claimed row, or None if it isn't PENDING (another worker
        holds it, or it already reached a terminal state). Callers must not
        send unless this returns a notification.
        """
        now = self._clock()
        with self._tx() as conn:
            cur = conn.execute(
                """UPDATE notifications
                      SET status = 'SENDING', attempts = attempts + 1,
                          lease_expires_at = ?, updated_at = ?
                    WHERE id = ? AND status = 'PENDING'""",
                (now + lease_seconds, now, notification_id),
            )
            if cur.rowcount != 1:
                return None
            row = conn.execute(
                f"SELECT {_COLUMNS} FROM notifications WHERE id = ?", (notification_id,)
            ).fetchone()
            self._log(conn, notification_id, row["attempts"], Status.PENDING, Status.SENDING,
                      "claimed", now)
        return _row_to_notification(row)

    def mark_dispatched(self, notification_id: str, provider_message_id: str) -> bool:
        now = self._clock()
        return self._transition(
            notification_id, Status.DISPATCHED, f"provider_message_id={provider_message_id}",
            "dispatched_at = ?, provider_message_id = ?, last_error = NULL, "
            "lease_expires_at = NULL",
            (now, provider_message_id), now,
        )

    def mark_retry(self, notification_id: str, next_attempt_at: float, error: str) -> bool:
        now = self._clock()
        return self._transition(
            notification_id, Status.PENDING, f"retry at {next_attempt_at:.3f}: {error}",
            "next_attempt_at = ?, last_error = ?, lease_expires_at = NULL",
            (next_attempt_at, error), now,
        )

    def mark_failed(self, notification_id: str, error: str) -> bool:
        now = self._clock()
        return self._transition(
            notification_id, Status.FAILED, error,
            "last_error = ?, lease_expires_at = NULL", (error,), now,
        )

    def _transition(self, notification_id: str, to: Status, detail: str,
                    set_sql: str, params: tuple, now: float) -> bool:
        """SENDING -> ``to``. Only the worker holding the claim can do this."""
        with self._tx() as conn:
            cur = conn.execute(
                f"""UPDATE notifications SET status = ?, updated_at = ?, {set_sql}
                     WHERE id = ? AND status = 'SENDING'""",
                (to.value, now, *params, notification_id),
            )
            if cur.rowcount != 1:
                return False
            attempts = conn.execute(
                "SELECT attempts FROM notifications WHERE id = ?", (notification_id,)
            ).fetchone()[0]
            self._log(conn, notification_id, attempts, Status.SENDING, to, detail, now)
            return True

    def recover_expired_leases(self) -> list[Notification]:
        """Return SENDING rows whose worker died (lease expired) to PENDING.

        The original send may or may not have reached the provider, so the
        dispatcher passes the notification id as the provider idempotency
        token; a provider that honours it (the mocks do) drops the duplicate.
        """
        now = self._clock()
        with self._tx() as conn:
            rows = conn.execute(
                f"""SELECT {_COLUMNS} FROM notifications
                     WHERE status = 'SENDING' AND lease_expires_at < ?""",
                (now,),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """UPDATE notifications
                          SET status = 'PENDING', lease_expires_at = NULL, updated_at = ?
                        WHERE id = ? AND status = 'SENDING'""",
                    (now, row["id"]),
                )
                self._log(conn, row["id"], row["attempts"], Status.SENDING, Status.PENDING,
                          "lease expired; requeued", now)
        return [self.get(row["id"]) for row in rows]  # type: ignore[misc]

    @staticmethod
    def _log(conn, notification_id, attempt, from_status, to_status, detail, now) -> None:
        conn.execute(
            """INSERT INTO dispatch_log
                   (notification_id, attempt, from_status, to_status, detail, at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (notification_id, attempt, from_status.value if from_status else None,
             to_status.value, detail, now),
        )

    # -- reads --------------------------------------------------------------

    def get(self, notification_id: str) -> Notification | None:
        row = self._conn().execute(
            f"SELECT {_COLUMNS} FROM notifications WHERE id = ?", (notification_id,)
        ).fetchone()
        return _row_to_notification(row) if row else None

    def pending(self) -> list[Notification]:
        rows = self._conn().execute(
            f"""SELECT {_COLUMNS} FROM notifications WHERE status = 'PENDING'
                 ORDER BY priority, created_at"""
        ).fetchall()
        return [_row_to_notification(r) for r in rows]

    def history(self, notification_id: str) -> list[dict]:
        rows = self._conn().execute(
            """SELECT attempt, from_status, to_status, detail, at FROM dispatch_log
                WHERE notification_id = ? ORDER BY id""",
            (notification_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def open_count(self) -> int:
        return self._conn().execute(
            "SELECT COUNT(*) FROM notifications WHERE status IN ('PENDING', 'SENDING')"
        ).fetchone()[0]

    def stats(self) -> dict:
        rows = self._conn().execute(
            """SELECT status, priority, COUNT(*) AS n FROM notifications
                GROUP BY status, priority"""
        ).fetchall()
        by_status = {s.value: 0 for s in Status}
        by_priority: dict[str, dict[str, int]] = {}
        for r in rows:
            by_status[r["status"]] += r["n"]
            by_priority.setdefault(Priority(r["priority"]).name, {})[r["status"]] = r["n"]
        return {"by_status": by_status, "by_priority": by_priority}
