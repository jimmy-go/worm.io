CREATE TABLE worm (
    id text PRIMARY KEY ASC,
    worker_name TEXT,
    status TEXT,
    error TEXT DEFAULT '',
    created_at DATETIME,
    data TEXT,
    log_file TEXT DEFAULT ''
);
