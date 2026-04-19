# Enron Email Deduplication Pipeline

## Project Overview
A Python pipeline that parses the Enron email dataset, stores structured data in SQLite,
detects duplicate emails using fuzzy matching, and sends notification emails via Gmail MCP.

## Tech Stack
- Python 3.10+
- SQLite for storage
- thefuzz (or rapidfuzz) for fuzzy matching
- Gmail MCP server for email notifications

## Key Files
- main.py — Entry point, runs full pipeline
- src/parser.py — Email parsing and field extraction
- src/database.py — SQLite schema, insert, and query logic
- src/dedup.py — Duplicate detection with fuzzy matching
- src/notifier.py — Email notification via MCP / draft generation
- schema.sql — Database schema definition
- sample_queries.sql — Demo queries

## Conventions
- All dates normalized to UTC
- Error handling: catch, log, skip (never crash)
- Run with: python main.py [--send-live]
