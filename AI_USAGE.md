# AI Usage Log

## src/parser.py — Email Parsing Pipeline

**Task:** Build a Python email parsing pipeline that recursively traverses `maildir/`, parses RFC 2822 emails, extracts mandatory and optional fields, separates body from forwarded/quoted content, handles edge cases, logs failures, and outputs statistics.

**Key design decisions:**

| Concern | Approach |
|---|---|
| **Date normalization** | `email.utils.parsedate_to_datetime` → UTC; fallback regex strips full weekday names (`Wednesday, Jan...`); bare TZ abbreviations (`PST`, `CDT`) mapped to numeric offsets |
| **Address parsing** | `email.utils.getaddresses` handles comma lists + line continuations; filters bare X.500 Exchange paths (`/O=ENRON/...`) that have no `@` |
| **Body separation** | Regex splits on Enron (`---------------------- Forwarded by`) and Outlook (`-----Original Message-----`) delimiters; `>` quoted lines extracted separately |
| **Header objects** | `_hstr()` coerces all `compat32` Header objects (triggered by raw non-ASCII bytes like Japanese characters in `X-From`) to plain strings before any `.strip()` call |
| **Attachments** | Walks MIME tree; flags `has_attachment=True` on any non-text/non-multipart part or explicit `Content-Disposition: attachment` |
| **Error logging** | `_log_error()` appends `timestamp\tpath\treason` to `error_log.txt`; never raises |

**Parse statistics (11,239 emails across 5 mailboxes):**

| Metric | Value |
|---|---|
| Total files | 11,239 |
| Successful parses | 11,239 |
| Failures | 0 |

| Field | Completeness |
|---|---|
| date | 100.0% |
| from_address | 100.0% |
| x_folder | 100.0% |
| x_from | 100.0% |
| x_origin | 100.0% |
| body | 95.4% |
| to_addresses | 95.6% |
| subject | 97.6% |
| forwarded_content | 23.0% |
| cc_addresses | 12.9% |
| bcc_addresses | 12.9% |
| quoted_content | 1.4% |

Notable: 4.6% of emails have no body (forwarded-only messages); 23% contain forwarded content blocks.

21.	Tool Used: Name and version of the AI coding tool.
22.	Prompting Strategy: How did you break down the problem for the AI? Did you prompt task-by-task or provide the full spec at once? Include 3–5 example prompts you used and explain why you structured them that way.
23.	Iterations & Debugging: Describe at least 2 cases where the AI-generated code did not work on the first attempt. What went wrong? How did you refine your prompts or manually fix the issue?
24.	What You Wrote vs. What AI Wrote: Provide a rough percentage breakdown and identify specific sections you wrote manually vs. AI-generated.
25.	Lessons Learned: What worked well with AI assistance? What was harder than expected?

