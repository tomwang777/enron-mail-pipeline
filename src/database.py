"""SQLite storage layer for the Enron email pipeline."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from .parser import ParsedEmail

_ROOT = Path(__file__).resolve().parent.parent
DB_PATH: Path = _ROOT / "enron.db"
SCHEMA_SQL_PATH: Path = _ROOT / "schema.sql"

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

SCHEMA = """\
CREATE TABLE IF NOT EXISTS emails (
    id                INTEGER  PRIMARY KEY AUTOINCREMENT,
    message_id        TEXT     NOT NULL UNIQUE,
    date              DATETIME,
    from_address      TEXT,
    subject           TEXT,
    body              TEXT,
    source_file       TEXT,
    x_from            TEXT,
    x_to              TEXT,
    x_cc              TEXT,
    x_bcc             TEXT,
    x_folder          TEXT,
    x_origin          TEXT,
    content_type      TEXT,
    has_attachment    BOOLEAN  NOT NULL DEFAULT FALSE,
    forwarded_content TEXT,
    quoted_content    TEXT,
    is_duplicate      BOOLEAN  NOT NULL DEFAULT FALSE,
    duplicate_of      TEXT     REFERENCES emails(message_id),
    notification_sent BOOLEAN  NOT NULL DEFAULT FALSE,
    notification_date DATETIME,
    created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS email_recipients (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id       INTEGER NOT NULL REFERENCES emails(id) ON DELETE CASCADE,
    address        TEXT    NOT NULL,
    recipient_type TEXT    NOT NULL CHECK (recipient_type IN ('to', 'cc', 'bcc')),
    UNIQUE (email_id, address, recipient_type)
);

CREATE INDEX IF NOT EXISTS idx_emails_date         ON emails(date);
CREATE INDEX IF NOT EXISTS idx_emails_from_address ON emails(from_address);
CREATE INDEX IF NOT EXISTS idx_emails_subject      ON emails(subject);
CREATE INDEX IF NOT EXISTS idx_emails_is_duplicate ON emails(is_duplicate);
CREATE INDEX IF NOT EXISTS idx_recipients_email_id ON email_recipients(email_id);
CREATE INDEX IF NOT EXISTS idx_recipients_address  ON email_recipients(address);
"""

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


@contextmanager
def _connect(db_path: Path = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Initialisation & schema export
# ---------------------------------------------------------------------------


def init_db(db_path: Path = DB_PATH) -> None:
    """Create tables and indexes. Safe to call repeatedly (IF NOT EXISTS)."""
    with _connect(db_path) as conn:
        conn.executescript(SCHEMA)


def export_schema(dest: Path = SCHEMA_SQL_PATH) -> None:
    """Write the schema DDL to schema.sql."""
    dest.write_text(SCHEMA, encoding="utf-8")


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

_INSERT_EMAIL = """\
INSERT OR IGNORE INTO emails (
    message_id, date, from_address, subject, body, source_file,
    x_from, x_to, x_cc, x_bcc, x_folder, x_origin,
    content_type, has_attachment, forwarded_content, quoted_content
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_RECIPIENT = """\
INSERT OR IGNORE INTO email_recipients (email_id, address, recipient_type)
VALUES (?, ?, ?)
"""


def _email_row(e: ParsedEmail) -> tuple:  # type: ignore[type-arg]
    return (
        e.message_id,
        e.date.isoformat() if e.date else None,
        e.from_address or None,
        e.subject or None,
        e.body or None,
        e.source_file,
        e.x_from or None,
        e.x_to or None,
        e.x_cc or None,
        e.x_bcc or None,
        e.x_folder or None,
        e.x_origin or None,
        e.content_type or None,
        e.has_attachment,
        e.forwarded_content or None,
        e.quoted_content or None,
    )


def reset_duplicate_flags(db_path: Path = DB_PATH) -> None:
    """Clear all duplicate and notification flags so the next run starts fresh."""
    with _connect(db_path) as conn:
        conn.execute("""
            UPDATE emails
            SET is_duplicate      = FALSE,
                duplicate_of      = NULL,
                notification_sent = FALSE,
                notification_date = NULL
        """)


def insert_emails(
    emails: list[ParsedEmail],
    db_path: Path = DB_PATH,
) -> tuple[int, int]:
    """
    Bulk-insert a list of ParsedEmail into SQLite within a single transaction.
    Skips any row whose message_id already exists (INSERT OR IGNORE).
    Returns (inserted, skipped).
    """
    inserted = 0
    skipped = 0

    with _connect(db_path) as conn:
        for em in emails:
            cur = conn.execute(_INSERT_EMAIL, _email_row(em))
            if cur.rowcount == 0:
                skipped += 1
                continue

            email_id = cur.lastrowid
            inserted += 1

            recipients = (
                [(email_id, addr, "to")  for addr in em.to_addresses]
                + [(email_id, addr, "cc")  for addr in em.cc_addresses]
                + [(email_id, addr, "bcc") for addr in em.bcc_addresses]
            )
            if recipients:
                conn.executemany(_INSERT_RECIPIENT, recipients)

    return inserted, skipped


# ---------------------------------------------------------------------------
# CLI — parse → insert pipeline
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from .parser import run as parse_run

    print("Initialising database …")
    init_db()
    export_schema()
    print(f"Schema written → {SCHEMA_SQL_PATH}")

    print("Parsing emails …")
    emails, stats = parse_run()
    print(stats.report())

    print("\nInserting into database …")
    ins, skip = insert_emails(emails)
    print(f"Inserted : {ins:>7,}")
    print(f"Skipped  : {skip:>7,}  (duplicate message_id)")
    print(f"Database → {DB_PATH}")
