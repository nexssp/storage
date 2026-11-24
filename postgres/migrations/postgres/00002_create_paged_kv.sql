-- +goose Up
CREATE TABLE nexss_storage_kv_pages (
    page_sequence BIGSERIAL PRIMARY KEY,
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    cursor TEXT NOT NULL,
    value BYTEA NOT NULL,
    UNIQUE (namespace, key, cursor)
);
CREATE INDEX nexss_storage_kv_pages_ordered
    ON nexss_storage_kv_pages (namespace, key, page_sequence);
