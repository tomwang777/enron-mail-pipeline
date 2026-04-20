"""Email parsing pipeline for the Enron maildir dataset."""

from __future__ import annotations

import email
import email.header
import email.policy
import email.utils
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MAILBOXES: list[str] = ["buy-r", "grigsby-m", "keavey-p", "lewis-a", "presto-k"]

_ROOT = Path(__file__).resolve().parent.parent
MAILDIR: Path = _ROOT / "maildir"
ERROR_LOG: Path = _ROOT / "error_log.txt"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Non-standard timezone abbreviation → UTC offset in hours.
# Numeric offsets like -0800 are handled natively by email.utils.
_TZ_MAP: dict[str, int] = {
    "PST": -8, "PDT": -7,
    "MST": -7, "MDT": -6,
    "CST": -6, "CDT": -5,
    "EST": -5, "EDT": -4,
    "GMT": 0,  "UTC": 0,
    "CET": 1,  "CEST": 2,
}

# Forwarding delimiters: Enron (long-dash + "Forwarded by") and Outlook
# ("-----Original Message-----", optionally preceded by whitespace).
_FWD_RE = re.compile(
    r"^[ \t]*-{3,}[ \t]*(?:Original Message|Forwarded)[^\n]*(?:-{0,5})?[ \t]*$"
    r"|^-{10,}[ \t]*Forwarded",
    re.IGNORECASE | re.MULTILINE,
)

_QUOTED_RE = re.compile(r"(?m)^>.*$")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ParsedEmail:
    # Mandatory — always present (may be empty string / empty list if source
    # is missing the field, but the key is always set)
    message_id: str       # unique identifier; falls back to source_file path
    date: datetime | None # UTC-normalised; None when unparseable
    from_address: str     # bare email address, lowercased
    to_addresses: list[str]
    subject: str
    body: str             # clean text before forwarded/quoted blocks
    source_file: str

    # Optional
    cc_addresses: list[str] = field(default_factory=list)
    bcc_addresses: list[str] = field(default_factory=list)
    x_from: str = ""
    x_to: str = ""
    x_cc: str = ""
    x_bcc: str = ""
    x_folder: str = ""
    x_origin: str = ""
    content_type: str = ""
    has_attachment: bool = False
    forwarded_content: str = ""
    quoted_content: str = ""
    headers: dict[str, str] = field(default_factory=dict)  # all raw headers


class ParseStats:
    def __init__(self) -> None:
        self.total_files: int = 0
        self.successful: int = 0
        self.failures: int = 0
        self._hits: dict[str, int] = {}

    def record(self, parsed: ParsedEmail) -> None:
        self.successful += 1
        presence = {
            "date":              parsed.date is not None,
            "from_address":      bool(parsed.from_address),
            "to_addresses":      bool(parsed.to_addresses),
            "subject":           bool(parsed.subject),
            "body":              bool(parsed.body),
            "cc_addresses":      bool(parsed.cc_addresses),
            "bcc_addresses":     bool(parsed.bcc_addresses),
            "x_from":            bool(parsed.x_from),
            "x_folder":          bool(parsed.x_folder),
            "x_origin":          bool(parsed.x_origin),
            "has_attachment":    parsed.has_attachment,
            "forwarded_content": bool(parsed.forwarded_content),
            "quoted_content":    bool(parsed.quoted_content),
        }
        for key, present in presence.items():
            if present:
                self._hits[key] = self._hits.get(key, 0) + 1

    def report(self) -> str:
        lines = [
            f"{'Total files:':<26} {self.total_files:>7}",
            f"{'Successful parses:':<26} {self.successful:>7}",
            f"{'Failures:':<26} {self.failures:>7}",
            "",
            "Field completeness (% of successful parses):",
        ]
        if self.successful:
            for fname in sorted(self._hits):
                pct = self._hits[fname] / self.successful * 100
                lines.append(f"  {fname:<24} {pct:5.1f}%")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _hstr(v: object) -> str:
    """Coerce any header value (str, Header object, None) to a plain string."""
    if v is None:
        return ""
    return str(v).strip()


def _decode_header(raw: object) -> str:
    """Decode a possibly MIME-encoded header value to a plain Unicode string."""
    if not raw:
        return ""
    s = str(raw)
    try:
        chunks = email.header.decode_header(s)
        parts: list[str] = []
        for chunk, charset in chunks:
            if isinstance(chunk, bytes):
                parts.append(chunk.decode(charset or "utf-8", errors="replace"))
            else:
                parts.append(chunk)
        return "".join(parts).strip()
    except Exception:
        return s.strip()


def _parse_date(raw: str | None) -> datetime | None:
    """Convert an RFC 2822 (or Enron-variant) date string to a UTC datetime."""
    if not raw:
        return None
    s = raw.strip()

    # Some Enron emails have full weekday names that parsedate_to_datetime rejects.
    s = re.sub(
        r"^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*",
        "",
        s,
        flags=re.IGNORECASE,
    )

    # Replace bare tz abbreviations at the end of the string (e.g. bare "PST"
    # without a preceding numeric offset) with a numeric offset.
    def _sub_tz(m: re.Match) -> str:  # type: ignore[type-arg]
        name = m.group(1).upper()
        if name in _TZ_MAP:
            h = _TZ_MAP[name]
            return f"{h:+03d}00"
        return m.group(0)

    s = re.sub(r"\b([A-Za-z]{2,5})\s*$", _sub_tz, s)

    # Primary path: handles standard RFC 2822 and numeric offsets.
    try:
        dt = email.utils.parsedate_to_datetime(s)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Fallback for "January 23, 2002 5:08:32 +0000" style (seen in forwarded blocks).
    for fmt in (
        "%B %d, %Y %H:%M:%S %z",
        "%B %d, %Y %H:%M:%S",
        "%d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    return None


def _parse_addresses(raw: str | None) -> list[str]:
    """
    Extract all valid email addresses from a header value.
    Handles comma-separated lists, line continuations (already folded by
    email.utils), and mixed display-name formats. Skips X.500 Exchange paths.
    """
    if not raw:
        return []
    result: list[str] = []
    for _, addr in email.utils.getaddresses([raw]):
        addr = addr.strip()
        if "@" in addr:
            result.append(addr.lower())
    return result


def _parse_from(raw: str | None) -> str:
    """Return the bare email address from a From header, lowercased."""
    if not raw:
        return ""
    _, addr = email.utils.parseaddr(raw)
    addr = addr.strip()
    return addr.lower() if "@" in addr else ""


def _get_payload(msg: email.message.Message) -> tuple[str, bool]:
    """
    Extract plain-text payload and detect attachments from a MIME message.
    Returns (text, has_attachment).
    """
    if not msg.is_multipart():
        payload = msg.get_payload(decode=True)
        if not payload:
            return "", False
        charset = msg.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="replace"), False

    text_parts: list[str] = []
    has_attachment = False

    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        disposition = (part.get_content_disposition() or "").lower()
        ctype_main = part.get_content_maintype()
        if disposition == "attachment" or ctype_main not in ("text",):
            has_attachment = True
            continue
        if part.get_content_subtype() == "plain":
            raw = part.get_payload(decode=True)
            if raw:
                charset = part.get_content_charset() or "utf-8"
                text_parts.append(raw.decode(charset, errors="replace"))

    return "\n".join(text_parts), has_attachment


def _split_body(text: str) -> tuple[str, str, str]:
    """
    Partition email text into (body, forwarded_content, quoted_content).

    body              — text the author wrote, minus quoted lines
    forwarded_content — everything after the first forwarding delimiter
    quoted_content    — '>' quoted lines collected from the body section
    """
    m = _FWD_RE.search(text)
    if m:
        body_raw = text[: m.start()]
        forwarded = text[m.start():].strip()
    else:
        body_raw = text
        forwarded = ""

    quoted_lines = _QUOTED_RE.findall(body_raw)
    clean_body = _QUOTED_RE.sub("", body_raw).strip()

    return clean_body, forwarded, "\n".join(quoted_lines)


def _log_error(path: Path, reason: str) -> None:
    """Append a parse failure to error_log.txt. Never raises."""
    try:
        with ERROR_LOG.open("a", encoding="utf-8") as f:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            f.write(f"{ts}\t{path}\t{reason}\n")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Core parser
# ---------------------------------------------------------------------------


def parse_email(path: Path) -> ParsedEmail | None:
    """
    Parse a single Enron email file into a ParsedEmail.
    Returns None on failure; appends the error to error_log.txt.
    """
    try:
        raw_bytes = path.read_bytes()
        msg = email.message_from_bytes(raw_bytes, policy=email.policy.compat32)

        # --- Mandatory ---
        raw_mid = _decode_header(msg.get("Message-ID", "")).strip("<> ")
        message_id = raw_mid or str(path)

        date = _parse_date(msg.get("Date"))
        from_address = _parse_from(msg.get("From"))
        to_addresses = _parse_addresses(msg.get("To"))
        subject = _decode_header(msg.get("Subject", ""))
        source_file = str(path)

        full_text, has_attachment = _get_payload(msg)
        body, forwarded, quoted = _split_body(full_text)

        # --- Optional ---
        cc_addresses  = _parse_addresses(msg.get("Cc"))
        bcc_addresses = _parse_addresses(msg.get("Bcc"))
        x_from   = _hstr(msg.get("X-From"))
        x_to     = _hstr(msg.get("X-To"))
        x_cc     = _hstr(msg.get("X-cc"))
        x_bcc    = _hstr(msg.get("X-bcc"))
        x_folder = _hstr(msg.get("X-Folder"))
        x_origin = _hstr(msg.get("X-Origin"))
        content_type = msg.get_content_type()

        # Flat dict of all raw headers; last value wins for repeated names.
        headers = {k: _hstr(v) for k, v in msg.items()}

        return ParsedEmail(
            message_id=message_id,
            date=date,
            from_address=from_address,
            to_addresses=to_addresses,
            subject=subject,
            body=body,
            source_file=source_file,
            cc_addresses=cc_addresses,
            bcc_addresses=bcc_addresses,
            x_from=x_from,
            x_to=x_to,
            x_cc=x_cc,
            x_bcc=x_bcc,
            x_folder=x_folder,
            x_origin=x_origin,
            content_type=content_type,
            has_attachment=has_attachment,
            forwarded_content=forwarded,
            quoted_content=quoted,
            headers=headers,
        )

    except Exception as exc:
        _log_error(path, str(exc))
        return None


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def discover_files(maildir: Path = MAILDIR) -> Generator[Path, None, None]:
    """Yield all email file paths within the configured mailboxes."""
    for mailbox in MAILBOXES:
        mbox_path = maildir / mailbox
        if not mbox_path.is_dir():
            print(f"[warn] mailbox not found: {mbox_path}")
            continue
        yield from (p for p in mbox_path.rglob("*") if p.is_file())


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def run(maildir: Path = MAILDIR) -> tuple[list[ParsedEmail], ParseStats]:
    """
    Parse all emails across the configured mailboxes.
    Returns (emails, stats).  Never raises — bad files are logged and skipped.
    """
    stats = ParseStats()
    results: list[ParsedEmail] = []

    for path in discover_files(maildir):
        stats.total_files += 1
        parsed = parse_email(path)
        if parsed is None:
            stats.failures += 1
        else:
            stats.record(parsed)
            results.append(parsed)

    return results, stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    emails, stats = run()
    print(stats.report())
    if stats.failures:
        print(f"\nSee {ERROR_LOG} for {stats.failures} failure(s).")
