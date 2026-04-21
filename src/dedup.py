"""Duplicate detection for the Enron email pipeline.

Strategy:
  1. Group emails by (from_address, normalised_subject) — cheap O(n) pre-filter.
  2. Within each group do pairwise body-similarity comparisons (rapidfuzz).
  3. Build connected components with UnionFind (transitive duplicates are caught).
  4. Per component: earliest-dated email is the original; all others are flagged.
  5. Write duplicates_report.csv and update is_duplicate / duplicate_of in the DB.
"""

from __future__ import annotations

import csv
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

try:
    from rapidfuzz import fuzz
except ImportError:
    from thefuzz import fuzz  # type: ignore[no-redef]

_ROOT = Path(__file__).resolve().parent.parent
DB_PATH: Path = _ROOT / "enron.db"
REPORT_PATH: Path = _ROOT / "duplicates_report.csv"

SIMILARITY_THRESHOLD: float = 90.0
# Require at least this many characters of combined body content before
# applying fuzzy matching; otherwise both-empty bodies would trivially score 100.
_MIN_BODY_LEN: int = 20

# ---------------------------------------------------------------------------
# Subject normalisation
# ---------------------------------------------------------------------------

# Strips one or more Re: / Fwd: / FW: prefixes (any case, with optional spaces).
_PREFIX_RE = re.compile(r"^(?:(?:re|fwd?|fw)\s*:\s*)+", re.IGNORECASE)
# Collapses whitespace including tabs and newlines folded into headers.
_WS_RE = re.compile(r"\s+")


def normalize_subject(subject: str | None) -> str:
    """Strip reply/forward prefixes, collapse whitespace, lowercase."""
    s = _PREFIX_RE.sub("", subject or "")
    return _WS_RE.sub(" ", s).strip().lower()


# ---------------------------------------------------------------------------
# Union-Find for connected-component clustering
# ---------------------------------------------------------------------------


class UnionFind:
    def __init__(self, n: int) -> None:
        self._parent = list(range(n))
        self._rank = [0] * n

    def find(self, x: int) -> int:
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])  # path compression
        return self._parent[x]

    def union(self, x: int, y: int) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1

    def clusters(self) -> list[list[int]]:
        """Return lists of indices that form clusters of size ≥ 2."""
        groups: dict[int, list[int]] = defaultdict(list)
        for i in range(len(self._parent)):
            groups[self.find(i)].append(i)
        return [v for v in groups.values() if len(v) > 1]


# ---------------------------------------------------------------------------
# Similarity
# ---------------------------------------------------------------------------


def body_similarity(a: str, b: str) -> float:
    """
    Return 0–100 Levenshtein ratio between two body strings.
    Returns 0.0 when neither has enough content to compare meaningfully.
    """
    combined_len = len(a) + len(b)
    if combined_len < _MIN_BODY_LEN:
        # Too little content: only treat as similar if they are identical.
        return 100.0 if a == b else 0.0
    if not a or not b:
        return 0.0
    return fuzz.ratio(a, b)


# ---------------------------------------------------------------------------
# Row helper
# ---------------------------------------------------------------------------


class _EmailRow(NamedTuple):
    idx: int          # position in sorted group list (0 = earliest)
    db_id: int
    message_id: str
    date: str | None
    from_address: str
    subject: str | None
    body: str


def _load_rows(conn: sqlite3.Connection) -> list[_EmailRow]:
    """Load all non-duplicate emails sorted by date (nulls last)."""
    sql = """
        SELECT id, message_id, date, from_address, subject, body
        FROM   emails
        WHERE  is_duplicate = FALSE
        ORDER  BY CASE WHEN date IS NULL THEN 1 ELSE 0 END, date ASC
    """
    result = []
    for i, r in enumerate(conn.execute(sql).fetchall()):
        result.append(_EmailRow(
            idx=i,
            db_id=r["id"],
            message_id=r["message_id"],
            date=r["date"],
            from_address=(r["from_address"] or "").lower(),
            subject=r["subject"],
            body=(r["body"] or "").strip(),
        ))
    return result


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------


def run(
    db_path: Path = DB_PATH,
    report_path: Path = REPORT_PATH,
    threshold: float = SIMILARITY_THRESHOLD,
) -> dict[str, object]:
    """
    Detect duplicates across all emails in the database.
    Updates is_duplicate and duplicate_of in-place, writes a CSV report.
    Returns a stats dict.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    rows = _load_rows(conn)

    # --- Group by (from_address, normalised_subject) ---
    groups: dict[tuple[str, str], list[_EmailRow]] = defaultdict(list)
    for row in rows:
        key = (row.from_address, normalize_subject(row.subject))
        groups[key].append(row)

    candidate_groups = {k: v for k, v in groups.items() if len(v) > 1}

    # Accumulators
    db_updates: list[tuple[bool, str, str]] = []  # (True, duplicate_msg_id, original_msg_id)
    report_rows: list[dict[str, object]] = []
    dup_group_count = 0
    total_flagged = 0
    cluster_sizes: list[int] = []

    for (from_addr, norm_subj), members in candidate_groups.items():
        # members are already sorted by date ASC (nulls last) from the query
        n = len(members)
        uf = UnionFind(n)
        scores: dict[tuple[int, int], float] = {}  # (i,j) → similarity

        for i in range(n):
            for j in range(i + 1, n):
                score = body_similarity(members[i].body, members[j].body)
                if score >= threshold:
                    uf.union(i, j)
                    scores[(i, j)] = score

        clusters = uf.clusters()
        if not clusters:
            continue

        dup_group_count += len(clusters)

        for cluster in clusters:
            # Smallest index = earliest date (query ordered by date ASC)
            original_idx = min(cluster)
            original = members[original_idx]
            duplicate_idxs = [k for k in cluster if k != original_idx]

            cluster_sizes.append(len(cluster))
            total_flagged += len(duplicate_idxs)

            for di in duplicate_idxs:
                dup = members[di]
                # Similarity between this duplicate and the original
                pair = (min(original_idx, di), max(original_idx, di))
                score = scores.get(pair) or body_similarity(original.body, dup.body)

                db_updates.append((True, dup.message_id, original.message_id, round(score, 2)))
                report_rows.append({
                    "duplicate_message_id": dup.message_id,
                    "original_message_id":  original.message_id,
                    "subject":              original.subject or "",
                    "from_address":         from_addr,
                    "duplicate_date":       dup.date or "",
                    "original_date":        original.date or "",
                    "similarity_score":     round(score, 2),
                })

    # --- Persist to DB ---
    if db_updates:
        with conn:
            conn.executemany(
                "UPDATE emails SET is_duplicate=?, duplicate_of=?, similarity_score=? WHERE message_id=?",
                [(flag, orig, score, dup) for flag, dup, orig, score in db_updates],
            )

    # --- Write CSV report (all duplicates in DB, not just newly found) ---
    fieldnames = [
        "duplicate_message_id", "original_message_id", "subject",
        "from_address", "duplicate_date", "original_date", "similarity_score",
    ]
    all_rows_sql = """
        SELECT
            d.message_id      AS dup_id,
            o.message_id      AS orig_id,
            d.subject,
            d.from_address,
            d.date            AS dup_date,
            o.date            AS orig_date,
            d.similarity_score
        FROM   emails d
        JOIN   emails o ON o.message_id = d.duplicate_of
        WHERE  d.is_duplicate = TRUE
        ORDER  BY d.date ASC
    """
    all_db_rows = conn.execute(all_rows_sql).fetchall()
    conn.close()

    all_report_rows = [
        {
            "duplicate_message_id": r["dup_id"],
            "original_message_id":  r["orig_id"],
            "subject":              r["subject"] or "",
            "from_address":         r["from_address"] or "",
            "duplicate_date":       r["dup_date"] or "",
            "original_date":        r["orig_date"] or "",
            "similarity_score":     r["similarity_score"] if r["similarity_score"] is not None else "",
        }
        for r in all_db_rows
    ]

    with open(report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_report_rows)

    avg_size = round(sum(cluster_sizes) / len(cluster_sizes), 2) if cluster_sizes else 0.0
    return {
        "total_groups":     dup_group_count,
        "total_flagged":    total_flagged,
        "avg_group_size":   avg_size,
        "largest_group":    max(cluster_sizes, default=0),
        "report_rows":      len(report_rows),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Running duplicate detection …")
    stats = run()
    print(f"Duplicate clusters found : {stats['total_groups']:>6,}")
    print(f"Emails flagged           : {stats['total_flagged']:>6,}")
    print(f"Average cluster size     : {stats['avg_group_size']:>6.2f}")
    print(f"Largest cluster          : {stats['largest_group']:>6,}")
    print(f"Report rows              : {stats['report_rows']:>6,}")
    print(f"Report written →         : {REPORT_PATH}")
