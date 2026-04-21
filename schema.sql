CREATE TABLE IF NOT EXISTS emails (
    id                INTEGER  PRIMARY KEY AUTOINCREMENT,
    message_id        TEXT     NOT NULL UNIQUE,
    date              DATETIME,
    from_address      TEXT,
    subject           TEXT,
    body              TEXT,
    source_file       TEXT,
    x_from            TEXT,
    x_to              TEXT,
    x_cc              TEXT,
    x_bcc             TEXT,
    x_folder          TEXT,
    x_origin          TEXT,
    content_type      TEXT,
    has_attachment    BOOLEAN  NOT NULL DEFAULT FALSE,
    forwarded_content TEXT,
    quoted_content    TEXT,
    is_duplicate      BOOLEAN  NOT NULL DEFAULT FALSE,
    duplicate_of      TEXT     REFERENCES emails(message_id),
    similarity_score  REAL,
    notification_sent BOOLEAN  NOT NULL DEFAULT FALSE,
    notification_date DATETIME,
    created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS email_recipients (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id       INTEGER NOT NULL REFERENCES emails(id) ON DELETE CASCADE,
    address        TEXT    NOT NULL,
    recipient_type TEXT    NOT NULL CHECK (recipient_type IN ('to', 'cc', 'bcc')),
    UNIQUE (email_id, address, recipient_type)
);

CREATE INDEX IF NOT EXISTS idx_emails_date         ON emails(date);
CREATE INDEX IF NOT EXISTS idx_emails_from_address ON emails(from_address);
CREATE INDEX IF NOT EXISTS idx_emails_subject      ON emails(subject);
CREATE INDEX IF NOT EXISTS idx_emails_is_duplicate ON emails(is_duplicate);
CREATE INDEX IF NOT EXISTS idx_recipients_email_id ON email_recipients(email_id);
CREATE INDEX IF NOT EXISTS idx_recipients_address  ON email_recipients(address);
