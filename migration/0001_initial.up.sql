
DROP TABLE IF EXISTS worm;
CREATE TABLE worm (
    id text PRIMARY KEY ASC,
    worker_name text,
    status text,
    error text,
    created_at text,
    data blob
);
