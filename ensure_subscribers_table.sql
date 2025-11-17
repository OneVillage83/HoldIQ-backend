CREATE TABLE IF NOT EXISTS subscribers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    cik   TEXT NOT NULL,
    tier  TEXT NOT NULL CHECK (tier IN ('nano','mini','premium')),
    active INTEGER NOT NULL DEFAULT 1,
    billing_provider TEXT,      -- e.g. 'stripe', 'paypal' (optional for future)
    customer_id TEXT,           -- external billing customer id
    subscription_id TEXT,       -- external subscription id
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
