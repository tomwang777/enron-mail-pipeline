#!/usr/bin/env python3
"""Enron Email Deduplication Pipeline — entry point.

Usage
-----
    python main.py                                  # full pipeline, dry-run
    python main.py --send-live                      # live Gmail notifications
    python main.py --db-path custom.db              # custom database file
    python main.py --mailboxes buy-r grigsby-m      # subset of mailboxes
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Allow  `python main.py`  from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import src.database as db_mod
import src.dedup    as dedup_mod
import src.notifier as notifier_mod
import src.parser   as parser_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hr(char: str = "─", width: int = 54) -> str:
    return char * width


def _step(n: int, label: str) -> None:
    print(f"\n[{n}/4] {label}")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    mailboxes: list[str] | None,
    send_live: bool,
    db_path: Path,
) -> None:
    if mailboxes:
        parser_mod.MAILBOXES = mailboxes

    mode = "LIVE" if send_live else "DRY RUN"
    print(_hr("═"))
    print("  Enron Email Deduplication Pipeline")
    print(f"  Notifications : {mode}")
    print(f"  Database      : {db_path}")
    print(f"  Mailboxes     : {', '.join(parser_mod.MAILBOXES)}")
    print(_hr("═"))

    t_total = time.perf_counter()

    # ------------------------------------------------------------------
    # 1 · Parse
    # ------------------------------------------------------------------
    _step(1, "Parsing emails …")
    t = time.perf_counter()
    emails, parse_stats = parser_mod.run()
    elapsed = time.perf_counter() - t

    print(f"    Parsed     {parse_stats.successful:>7,} / {parse_stats.total_files:,}"
          f"   ({parse_stats.failures} failure{'s' if parse_stats.failures != 1 else ''})"
          f"   [{elapsed:.1f}s]")
    if parse_stats.failures:
        print(f"    Failures → {parser_mod.ERROR_LOG}")

    # ------------------------------------------------------------------
    # 2 · Store
    # ------------------------------------------------------------------
    _step(2, "Storing in database …")
    t = time.perf_counter()
    db_mod.init_db(db_path)
    db_mod.export_schema()
    inserted, skipped = db_mod.insert_emails(emails, db_path)
    elapsed = time.perf_counter() - t

    print(f"    Inserted   {inserted:>7,}   skipped {skipped:,} (duplicate message_id)"
          f"   [{elapsed:.1f}s]")

    # ------------------------------------------------------------------
    # 3 · Dedup
    # ------------------------------------------------------------------
    db_mod.reset_duplicate_flags(db_path)
    _step(3, "Detecting duplicates …")
    t = time.perf_counter()
    dedup_stats = dedup_mod.run(db_path=db_path)
    elapsed = time.perf_counter() - t

    print(f"    Clusters   {dedup_stats['total_groups']:>7,}"
          f"   flagged {dedup_stats['total_flagged']:,}"
          f"   avg size {dedup_stats['avg_group_size']:.2f}"
          f"   largest {dedup_stats['largest_group']}"
          f"   [{elapsed:.1f}s]")

    # ------------------------------------------------------------------
    # 4 · Notify
    # ------------------------------------------------------------------
    _step(4, f"Sending notifications ({mode}) …")
    t = time.perf_counter()
    notify_stats = notifier_mod.run(live=send_live, db_path=db_path)
    elapsed = time.perf_counter() - t

    print(f"    Sent/saved {notify_stats['sent']:>7,}"
          f"   errors {notify_stats['errors']}"
          f"   [{elapsed:.1f}s]")
    if not send_live:
        print(f"    .eml files → {notifier_mod.OUTPUT_DIR}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    total_elapsed = time.perf_counter() - t_total
    print(f"\n{_hr()}")
    print(f"  Pipeline complete in {total_elapsed:.1f}s")
    print(f"  Emails parsed    : {parse_stats.successful:,}")
    print(f"  Duplicates found : {dedup_stats['total_flagged']:,}"
          f"  ({dedup_stats['total_flagged'] / max(parse_stats.successful, 1) * 100:.1f}%"
          " of dataset)")
    print(f"  Notifications    : {notify_stats['sent']:,} {mode.lower()}")
    print()
    print(f"  Database  → {db_path}")
    print(f"  Dedup CSV → {dedup_mod.REPORT_PATH}")
    print(f"  Send log  → {notifier_mod.SEND_LOG}")
    print(_hr())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Enron Email Deduplication Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python main.py\n"
            "  python main.py --send-live\n"
            "  python main.py --db-path custom.db\n"
            "  python main.py --mailboxes buy-r grigsby-m\n"
        ),
    )
    ap.add_argument(
        "--mailboxes",
        nargs="+",
        metavar="MAILBOX",
        help="Mailboxes to process (default: all 5 configured in src/parser.py)",
    )
    ap.add_argument(
        "--send-live",
        action="store_true",
        help="Send via Gmail MCP (default: dry-run, saves .eml files)",
    )
    ap.add_argument(
        "--db-path",
        type=Path,
        default=db_mod.DB_PATH,
        metavar="FILE",
        help=f"SQLite database path (default: {db_mod.DB_PATH.name})",
    )
    args = ap.parse_args()
    run_pipeline(
        mailboxes=args.mailboxes,
        send_live=args.send_live,
        db_path=args.db_path,
    )


if __name__ == "__main__":
    main()
