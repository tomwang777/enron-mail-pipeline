# Enron Email Deduplication Pipeline

A Python pipeline that parses a curated subset of the Enron email dataset,
stores structured data in SQLite, detects duplicate emails using fuzzy matching,
and sends notification emails via Gmail MCP.

## Dataset

The full Enron maildir corpus contains ~150 employee mailboxes and over 500,000
emails. This project uses a focused subset of **5 mailboxes totalling 11,239
emails** — large enough to exercise the deduplication logic at scale, small enough
to run end-to-end in a reasonable time.

### Selected Mailboxes

| Mailbox | Name | Role | Emails |
|---|---|---|---|
| `buy-r` | Rick Buy | Chief Risk Officer | 2,429 |
| `grigsby-m` | Mike Grigsby | VP Gas Trading | 2,237 |
| `keavey-p` | Peter Keavey | Enron Capital & Trade (ECT) | 2,177 |
| `lewis-a` | Andrew Lewis | Enron Americas | 2,191 |
| `presto-k` | Kevin Presto | VP Wholesale Gas & Power, East | 2,204 |

**Total: 11,239 emails → 76,717 recipient rows (to/cc/bcc)**

### Selection Rationale

1. **Balanced volume** — all five mailboxes fall in the 2,177–2,429 range, so no
   single mailbox skews aggregate statistics or dominates runtime.

2. **Manageable total** — 11,239 emails is large enough to surface realistic
   duplicate patterns (forwarded chains, mailing-list copies, re-sent messages)
   while keeping full-pipeline runs fast during development.

3. **Functional diversity** — the five accounts span risk management, gas trading,
   capital markets, Americas operations, and wholesale power. Emails between
   these groups create natural cross-mailbox duplicates (the same forwarded thread
   appearing in multiple inboxes), which is the core case the deduplication logic
   must handle.

4. **Notable executive included** — Rick Buy (Chief Risk Officer) provides
   senior-level correspondence that tends to carry high-importance signals and
   broad CC lists, making it a useful stress case for the notifier component.

## Tech Stack

- Python 3.10+
- SQLite for structured storage
- thefuzz / rapidfuzz for fuzzy duplicate detection
- Gmail MCP server for email notifications

## Project Structure

```
enron-mail-pipeline/
├── main.py              # Entry point — runs full pipeline
├── src/
│   ├── __init__.py
│   ├── parser.py        # RFC 2822 parsing, field extraction, body separation
│   ├── database.py      # SQLite schema, bulk insert, connection helpers
│   ├── dedup.py         # Duplicate detection with fuzzy matching
│   └── notifier.py      # Email notification via MCP / draft generation
├── schema.sql           # Database schema DDL (auto-generated)
├── sample_queries.sql   # Demo queries with expected output comments
├── AI_USAGE.md          # AI-assisted implementation log
└── maildir/             # Curated dataset (5 mailboxes, 11,239 emails)
```

## Pipeline Components

### `src/parser.py`
Recursively traverses `maildir/`, parses each RFC 2822 file, and returns a
`ParsedEmail` dataclass. Key behaviours:

- Dates normalized to UTC; handles bare timezone abbreviations (`PST`, `CDT`)
  and non-standard formats (`Wednesday, January 23, 2002 5:08:32 GMT`)
- Addresses extracted via `email.utils.getaddresses`; X.500 Exchange paths
  (`/O=ENRON/...`) filtered out automatically
- Body separated from forwarded blocks (Enron `--- Forwarded by` and Outlook
  `--- Original Message ---` patterns) and `>` quoted lines
- Non-ASCII header values (e.g. raw Japanese bytes in `X-From`) coerced safely
  to strings before any processing
- All failures logged to `error_log.txt` with timestamp, path, and reason

**Parse results:** 11,239 / 11,239 files parsed successfully (0 failures).

| Field | Completeness |
|---|---|
| date, from\_address, x\_folder, x\_from, x\_origin | 100% |
| subject | 97.6% |
| to\_addresses, body | 95.4 – 95.6% |
| forwarded\_content | 23.0% |
| cc\_addresses, bcc\_addresses | 12.9% |
| quoted\_content | 1.4% |

### `src/database.py`
Stores parsed emails in SQLite (`enron.db`). Schema has two tables:

- **`emails`** — one row per message; unique on `message_id`; includes
  `is_duplicate`, `duplicate_of`, `notification_sent`, `notification_date`
  columns for later pipeline stages
- **`email_recipients`** — normalised `(email_id, address, recipient_type)`
  rows for `to`, `cc`, and `bcc` fields

Indexes on `date`, `from_address`, and `subject` for fast query access.
`insert_emails()` uses `INSERT OR IGNORE` inside a single transaction —
safe to re-run; duplicates are skipped, not overwritten.

**Insert results:** 11,239 inserted, 0 skipped.

### `sample_queries.sql`
Five ready-to-run queries against `enron.db`:

| # | Query | Notable finding |
|---|---|---|
| 1 | Top 10 senders | `mike.grigsby` leads at 11.7%; `m..presto` and `kevin.presto` are the same person — a dedup signal |
| 2 | Emails in a date range | Oct–Dec 2001 window captures the Enron collapse and bankruptcy filing |
| 3 | Emails with CC recipients | `joannie.williamson` sent meeting invites to 79 recipients; `kathie.grabstald` sent near-identical "News Deadline" blasts — prime dedup candidates |
| 4 | Duplicate preview | Shows emails flagged by `dedup.py` (empty until dedup runs) |
| 5 | Notification backlog | Shows unfired notifications for `notifier.py` to process |

## Usage

```bash
# Parse and load all emails into enron.db
python -m src.database

# Run full pipeline (dedup + notify)
python main.py            # dry run — drafts only, no live send
python main.py --send-live  # send live notification emails via Gmail MCP

# Run sample queries
sqlite3 enron.db < sample_queries.sql
```

## Conventions

- All dates normalized to UTC
- Error handling: catch, log, skip (never crash the pipeline)
- Re-running any stage is safe — inserts skip on conflict, schema uses `IF NOT EXISTS`
