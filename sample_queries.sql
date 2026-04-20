-- Enron Email Pipeline — Sample Queries
-- Run against enron.db:  sqlite3 enron.db < sample_queries.sql

-- ============================================================
-- Query 1: Top 10 senders by email count
-- ============================================================
-- Expected output: ~10 rows showing the most prolific senders
-- in the dataset. Addresses like rick.buy@enron.com,
-- mike.grigsby@enron.com, and mailing-list addresses tend to
-- appear at the top. Internal Enron addresses dominate.

SELECT
    from_address,
    COUNT(*)         AS email_count,
    COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER () AS pct_of_total
FROM   emails
WHERE  from_address IS NOT NULL
GROUP  BY from_address
ORDER  BY email_count DESC
LIMIT  10;


-- ============================================================
-- Query 2: All emails in a specific date range
-- ============================================================
-- Expected output: rows covering the Enron collapse period
-- (Oct–Dec 2001). Dates are stored as UTC ISO-8601 strings so
-- lexicographic BETWEEN works correctly.

SELECT
    message_id,
    date,
    from_address,
    subject
FROM   emails
WHERE  date BETWEEN '2001-10-01T00:00:00+00:00'
               AND  '2001-12-31T23:59:59+00:00'
ORDER  BY date;


-- ============================================================
-- Query 3: Emails that have CC recipients (with CC count)
-- ============================================================
-- Expected output: emails that were CC'd to at least one
-- address. Higher CC counts often indicate broadcast updates,
-- legal notices, or widely-circulated risk reports.

SELECT
    e.message_id,
    e.date,
    e.from_address,
    e.subject,
    COUNT(r.id) AS cc_count
FROM   emails e
JOIN   email_recipients r
       ON  r.email_id       = e.id
       AND r.recipient_type = 'cc'
GROUP  BY e.id
ORDER  BY cc_count DESC, e.date;


-- ============================================================
-- Query 4: Duplicate detection preview
-- ============================================================
-- Expected output: all emails already flagged as duplicates,
-- paired with the message_id of the original they duplicate.
-- Initially empty; populated after src/dedup.py runs.

SELECT
    e.message_id,
    e.date,
    e.from_address,
    e.subject,
    e.duplicate_of
FROM   emails e
WHERE  e.is_duplicate = TRUE
ORDER  BY e.duplicate_of, e.date;


-- ============================================================
-- Query 5: Notification backlog
-- ============================================================
-- Expected output: duplicate emails for which a notification
-- has not yet been sent. Used by src/notifier.py to find work.

SELECT
    e.message_id,
    e.date,
    e.from_address,
    e.subject,
    e.duplicate_of
FROM   emails e
WHERE  e.is_duplicate      = TRUE
  AND  e.notification_sent = FALSE
ORDER  BY e.date;
