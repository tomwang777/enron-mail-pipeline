# Enron Email Deduplication Pipeline

A Python pipeline that parses a curated subset of the Enron email dataset,
stores structured data in SQLite, detects duplicate emails using fuzzy matching,
and sends notification emails via Gmail MCP.

## Dataset

The full Enron maildir corpus contains ~150 employee mailboxes and over 500,000
emails. This project uses a focused subset of **5 mailboxes totalling ~11,238
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

**Total: ~11,238 emails**

### Selection Rationale

1. **Balanced volume** — all five mailboxes fall in the 2,177–2,429 range, so no
   single mailbox skews aggregate statistics or dominates runtime.

2. **Manageable total** — ~11,238 emails is large enough to surface realistic
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
│   ├── parser.py        # Email parsing and field extraction
│   ├── database.py      # SQLite schema, insert, and query logic
│   ├── dedup.py         # Duplicate detection with fuzzy matching
│   └── notifier.py      # Email notification via MCP / draft generation
├── schema.sql           # Database schema definition
├── sample_queries.sql   # Demo queries
└── maildir/             # Curated dataset (5 mailboxes, ~11k emails)
```

## Usage

```bash
python main.py            # Dry run — drafts only, no live send
python main.py --send-live  # Send live notification emails via Gmail MCP
```

## Conventions

- All dates normalized to UTC
- Error handling: catch, log, skip (never crash the pipeline)
