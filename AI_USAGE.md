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

**MCP Integration Documentation**

**1.	Which MCP server you chose and why.** Gmail MCP Server. It connects directly to the Google Cloud Services API—a solution that is both free and convenient. It allows for personalized configuration of target email addresses and custom test users, and supports the addition of various features—such as sending and receiving emails or modifying content—thereby greatly facilitating the testing of my pipeline's functionality.

**2.	Step-by-step setup instructions (how to configure credentials, register the MCP server with Claude Code or your AI tool).** 1. Go to Google CLoud Console and enable the Gmail API. Then create OAuth 2.0 credentials by setting a test user and set several scopes such as gmail.readonly and gmail.send. Finally download the JSON file that contains the API Key.
2.Install ahd configure the Gmail MCP Server by creating config directory in the local desktop, placing credentials and running authentication.
3.Register MCP Server by creating mcp.json.
4.Create a mcp_config.json file to activate the service.
5.Start the Claude Code and test the MCP service by sending a test email.

**3.	How you prompted the AI tool to use the MCP send_email tool — include example prompts.** By setting the mcp.json in the .claude directory and creating the mcp_config.json. 
"Send a test email to myself at wangchengabvd@gmail.com with subject 'MCP Test' and body 'This is a test from the Enron dedup pipeline.'"

**4.	Any issues encountered during MCP setup or sending, and how you resolved them.** 1. mcpServers doesn't belong in `settings.json` — the schema doesn't recognize it there; it's silently ignored. Replaced invalid `mcpServers` block with `enableAllProjectMcpServers: true`

2. Servers from `.claude/mcp.json` need explicit approval — added `"enableAllProjectMcpServers": true` to `.claude/settings.json` so Claude Code auto-approves the server defined in `mcp.json`

3. No global server registration — used `claude mcp add -s user` to register the server in `~/.claude.json`, and I added the `GMAIL_MCP_CONFIG_DIR` env var pointing to `~/.gmail-mcp`

**5.	A screenshot or log excerpt showing at least one successful email send in live mode.**
<img width="390" height="400" alt="截屏2026-04-21 20 03 06" src="https://github.com/user-attachments/assets/fa249f9f-1bd4-455c-b077-30794efad7cf" />



**AI Tool Usage Documentation**

**21.	Tool Used:** Claude Code Version 2.1.114 Sonnet 4.6 Used

**22.	Prompting Strategy:** I employed a step-by-step implementation strategy to systematically break down the major challenges outlined in the assignment requirements into manageable, actionable sub-problems. I further distilled the detailed specifications into clear, readable prompts fed to Claude Code, ensuring that each individual step was executed successfully. I meticulously reviewed every generated file and process step—proceeding to the next stage only after verifying that the code was entirely bug-free—while consistently updating, committing, and pushing changes to maintain robust version control.

1.“A Python pipeline that parses the Enron email dataset, stores structured data in SQLite,
detects duplicate emails using fuzzy matching, and sends notification emails via Gmail MCP.”
This is the initial project architecture I requested Claude Code to understand within `CLAUDE.md`, ensuring it clearly grasped the overall concept of the project.

2."Don't compare every email to every other email. Group by (from_address, normalized_subject) first, then fuzzy-match within groups"
To tell the Claude Code to matter the pipeline performance by mentioning the priority of the process.

3."Create main.py as the single entry point that:
1. Accepts command-line args: --mailboxes, --send-live, --db-path
2. Runs the full pipeline in order: parse → store → dedup → notify
3. Prints a summary at the end
4. A single command 'python main.py' runs everything end-to-end"
To make sure that the Claude Code know how to align with the whole pipeline by mentioning all the steps in a list.

**23.	Iterations & Debugging:** 1. When I dry ran the whole pipeline, I found out that the number of the duplicate mail was 0, which turns out to be the notification_sent is set to be TRUE that the Claude Code thinks the duplication notification has already been sent so there are no duplicate mail. So I rewrote the whole logic to set it ture only with '--send-live' with the help of Claude Code and now the duplicates_report.csv is filled with duplicates instead of an empty file.

2.The send log is full of records after dry run. However, nothing is sent during this phrase. It is still the problem of flagging, I just changed the logic and now it is only written with '--send-live'.

**24.	What You Wrote vs. What AI Wrote**: I have completed roughly one-third of the entire project—including conceptualizing the overall architecture, researching prompts, managing project progress and version control, fixing several critical bugs, handling MCP server calls and drafting configuration files, as well as expanding various `README.md` files and updating the `.gitignore`; AI handled the remainder.

**25.	Lessons Learned**: When drafting the general framework and debugging environmental issues, AI proves to be extremely convenient; it spares me the trouble of having to deal with particularly vexing version conflicts, and the overall pipeline proceeds very smoothly. However, when handling finer details, the AI's level of alignment is not yet sufficiently high; it occasionally produces inconsistencies or fails to meet specific requirements. Consequently, I must manually intervene to make the necessary adjustments, ensuring the project is completed to a high standard.

