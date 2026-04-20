"""Duplicate-notification sender for the Enron email pipeline.

Dry run (default): writes one .eml file per group to output/replies/
Live mode (--send-live): sends via a Gmail MCP server (mcp_config.json)

A group = one original email + all its duplicates.  One notification is sent
to the from_address of the latest duplicate in each group.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import subprocess
from datetime import datetime, timezone
from email.utils import formatdate
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from typing import NamedTuple

_ROOT = Path(__file__).resolve().parent.parent

DB_PATH    = _ROOT / "enron.db"
REPORT_CSV = _ROOT / "duplicates_report.csv"
OUTPUT_DIR = _ROOT / "output" / "replies"
SEND_LOG   = _ROOT / "output" / "send_log.csv"
MCP_CONFIG = _ROOT / "mcp_config.json"

SEND_LOG_FIELDS = ["timestamp", "recipient", "subject", "status", "error"]

_UNSAFE_RE = re.compile(r'[<>:"/\\|?*\s@]')

# ---------------------------------------------------------------------------
# Data container
# ---------------------------------------------------------------------------


class NotificationGroup(NamedTuple):
    orig_msg_id:  str
    orig_date:    str
    dup_msg_id:   str   # latest duplicate in the group
    dup_date:     str
    dup_from:     str   # notification recipient
    subject:      str
    similarity:   float


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def _load_similarity_scores(report_csv: Path) -> dict[str, float]:
    """Return {duplicate_message_id: similarity_score} from the CSV report."""
    if not report_csv.exists():
        return {}
    scores: dict[str, float] = {}
    with report_csv.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                scores[row["duplicate_message_id"]] = float(row["similarity_score"])
            except (KeyError, ValueError):
                pass
    return scores


def _load_pending_groups(
    conn: sqlite3.Connection,
    scores: dict[str, float],
) -> list[NotificationGroup]:
    """
    Return one NotificationGroup per pending original, using the latest
    (most recent) duplicate as the representative.
    """
    sql = """
        SELECT
            o.message_id  AS orig_id,
            o.date        AS orig_date,
            d.message_id  AS dup_id,
            d.date        AS dup_date,
            d.from_address,
            d.subject
        FROM   emails d
        JOIN   emails o ON o.message_id = d.duplicate_of
        WHERE  d.is_duplicate      = TRUE
          AND  d.notification_sent = FALSE
          AND  d.from_address IS NOT NULL
          AND  d.from_address != ''
          AND  d.date = (
                SELECT MAX(d2.date)
                FROM   emails d2
                WHERE  d2.duplicate_of      = d.duplicate_of
                  AND  d2.is_duplicate      = TRUE
                  AND  d2.notification_sent = FALSE
               )
        GROUP  BY d.duplicate_of
        ORDER  BY d.date DESC
    """
    groups: list[NotificationGroup] = []
    for r in conn.execute(sql).fetchall():
        groups.append(NotificationGroup(
            orig_msg_id = r["orig_id"]      or "",
            orig_date   = r["orig_date"]    or "unknown",
            dup_msg_id  = r["dup_id"]       or "",
            dup_date    = r["dup_date"]      or "unknown",
            dup_from    = (r["from_address"] or "").strip(),
            subject     = (r["subject"]      or "(no subject)").strip(),
            similarity  = scores.get(r["dup_id"] or "", 90.0),
        ))
    return groups


# ---------------------------------------------------------------------------
# Notification rendering
# ---------------------------------------------------------------------------


def _render(g: NotificationGroup) -> tuple[str, str, str]:
    """
    Return (subject, body, eml_text) for the notification.
    eml_text is a fully-formed RFC 2822 message string.
    """
    subj   = f"[Duplicate Notice] Re: {g.subject}"
    now    = formatdate(localtime=False)
    ref_id = f"<{g.dup_msg_id}>"

    body = f"""\
This is an automated notification from the Email Deduplication System.

Your email has been identified as a potential duplicate:

  Your Email (Flagged):
    Message-ID:  {g.dup_msg_id}
    Date Sent:   {g.dup_date}
    Subject:     {g.subject}

  Original Email on Record:
    Message-ID:  {g.orig_msg_id}
    Date Sent:   {g.orig_date}

  Similarity Score: {g.similarity:.1f}%

If this was NOT a duplicate and you intended to send this email,
please reply with CONFIRM to restore it to active status.

No action is required if this is indeed a duplicate."""

    eml = (
        f"To: {g.dup_from}\r\n"
        f"Subject: {subj}\r\n"
        f"Date: {now}\r\n"
        f"References: {ref_id}\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\n"
        f"MIME-Version: 1.0\r\n"
        f"\r\n"
        f"{body}"
    )
    return subj, body, eml


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _safe_filename(msg_id: str) -> str:
    return _UNSAFE_RE.sub("_", msg_id)[:120] + ".eml"


def _write_eml(g: NotificationGroup, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    _, _, eml = _render(g)
    dest = output_dir / _safe_filename(g.dup_msg_id)
    dest.write_text(eml, encoding="utf-8")
    return dest


def _append_send_log(
    log_path: Path,
    *,
    recipient: str,
    subject: str,
    status: str,
    error: str = "",
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not log_path.exists()
    with log_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SEND_LOG_FIELDS)
        if write_header:
            w.writeheader()
        w.writerow({
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "recipient": recipient,
            "subject":   subject,
            "status":    status,
            "error":     error,
        })


# ---------------------------------------------------------------------------
# Gmail MCP client
# ---------------------------------------------------------------------------


class MCPGmailClient:
    """
    Minimal stdio JSON-RPC client for a Gmail MCP server.
    Starts the server process, initialises the session, then exposes send().
    """

    def __init__(self, config_path: Path = MCP_CONFIG) -> None:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
        srv = cfg.get("mcpServers", {}).get("gmail", {})
        if not srv:
            raise RuntimeError("No 'gmail' entry under 'mcpServers' in mcp_config.json")

        cmd  = srv.get("command", "npx")
        args = srv.get("args", [])
        env  = {**__import__("os").environ, **srv.get("env", {})}

        tool_cfg            = cfg.get("send_tool", {})
        self._tool_name     = tool_cfg.get("name",   "send_email")
        params_map          = tool_cfg.get("params",  {})
        self._to_field      = params_map.get("to_field",      "to")
        self._subject_field = params_map.get("subject_field", "subject")
        self._body_field    = params_map.get("body_field",    "body")

        self._proc = subprocess.Popen(
            [cmd, *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        self._seq = 0
        self._initialize()

    # ------------------------------------------------------------------
    # Protocol helpers
    # ------------------------------------------------------------------

    def _readline(self, timeout: float = 15.0) -> str:
        """Read a line from stdout with a timeout (portable via thread)."""
        q: Queue[str | Exception] = Queue()

        def _reader() -> None:
            try:
                q.put(self._proc.stdout.readline())  # type: ignore[arg-type]
            except Exception as exc:
                q.put(exc)

        Thread(target=_reader, daemon=True).start()
        try:
            result = q.get(timeout=timeout)
        except Empty:
            raise TimeoutError("MCP server did not respond within timeout")
        if isinstance(result, Exception):
            raise result
        if not result:
            raise RuntimeError("MCP server closed stdout unexpectedly")
        return result

    def _send(self, method: str, params: dict | None = None, *, req_id: int | None = None) -> dict | None:
        msg: dict = {"jsonrpc": "2.0", "method": method}
        if params:
            msg["params"] = params
        if req_id is not None:
            msg["id"] = req_id
        line = json.dumps(msg) + "\n"
        self._proc.stdin.write(line)  # type: ignore[union-attr]
        self._proc.stdin.flush()      # type: ignore[union-attr]
        if req_id is None:
            return None  # notification — no response expected
        raw = self._readline()
        return json.loads(raw)

    def _initialize(self) -> None:
        self._seq += 1
        resp = self._send(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "enron-notifier", "version": "1.0"},
            },
            req_id=self._seq,
        )
        if resp and resp.get("error"):
            raise RuntimeError(f"MCP initialize error: {resp['error']}")
        self._send("notifications/initialized")  # notify (no id)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send(self, to: str, subject: str, body: str) -> None:
        self._seq += 1
        resp = self._send(
            "tools/call",
            {
                "name": self._tool_name,
                "arguments": {
                    self._to_field:      to,
                    self._subject_field: subject,
                    self._body_field:    body,
                },
            },
            req_id=self._seq,
        )
        if resp and resp.get("error"):
            raise RuntimeError(f"MCP send_email error: {resp['error']}")

    def close(self) -> None:
        try:
            self._proc.stdin.close()  # type: ignore[union-attr]
            self._proc.wait(timeout=5)
        except Exception:
            self._proc.kill()


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run(
    live: bool = False,
    db_path: Path = DB_PATH,
    report_csv: Path = REPORT_CSV,
    output_dir: Path = OUTPUT_DIR,
    send_log: Path = SEND_LOG,
    mcp_config: Path = MCP_CONFIG,
) -> dict[str, int]:
    """
    Generate (dry run) or send (live) duplicate notifications.
    Returns stats: {attempted, sent, skipped, errors}.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    scores = _load_similarity_scores(report_csv)
    groups = _load_pending_groups(conn, scores)

    mcp: MCPGmailClient | None = None
    if live:
        mcp = MCPGmailClient(mcp_config)

    stats = {"attempted": 0, "sent": 0, "skipped": 0, "errors": 0}
    sent_orig_ids: list[str] = []  # original message_ids whose groups were notified

    try:
        for g in groups:
            stats["attempted"] += 1
            subj, body, _ = _render(g)
            status = error_msg = ""

            try:
                if live and mcp:
                    mcp.send(g.dup_from, subj, body)
                    status = "sent"
                else:
                    path = _write_eml(g, output_dir)
                    status = f"saved:{path.name}"

                stats["sent"] += 1
                sent_orig_ids.append(g.orig_msg_id)

            except Exception as exc:
                error_msg = str(exc)
                status = "error"
                stats["errors"] += 1

            _append_send_log(
                send_log,
                recipient=g.dup_from,
                subject=subj,
                status=status,
                error=error_msg,
            )

    finally:
        if mcp:
            mcp.close()

    # Mark ALL duplicates in each notified group as sent — not just the latest
    # representative — so re-running the notifier doesn't re-notify the same groups.
    if sent_orig_ids:
        now_iso = datetime.now(timezone.utc).isoformat()
        placeholders = ",".join("?" * len(sent_orig_ids))
        with conn:
            conn.execute(
                f"UPDATE emails SET notification_sent=TRUE, notification_date=?"
                f" WHERE is_duplicate=TRUE AND duplicate_of IN ({placeholders})",
                [now_iso, *sent_orig_ids],
            )

    conn.close()
    stats["skipped"] = stats["attempted"] - stats["sent"] - stats["errors"]
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli() -> None:
    ap = argparse.ArgumentParser(description="Send duplicate-detection notifications")
    ap.add_argument(
        "--send-live",
        action="store_true",
        help="Send via Gmail MCP (requires mcp_config.json). Default: dry-run.",
    )
    args = ap.parse_args()

    mode = "LIVE" if args.send_live else "DRY RUN"
    print(f"Notifier running in {mode} mode …")

    stats = run(live=args.send_live)
    print(f"Attempted : {stats['attempted']:>6,}")
    print(f"Sent/saved: {stats['sent']:>6,}")
    print(f"Errors    : {stats['errors']:>6,}")
    if not args.send_live:
        print(f".eml files → {OUTPUT_DIR}")
    print(f"Send log  → {SEND_LOG}")


if __name__ == "__main__":
    _cli()
