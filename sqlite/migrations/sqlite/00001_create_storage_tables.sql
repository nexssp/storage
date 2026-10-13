-- +goose Up
CREATE TABLE nexss_storage_kv (
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    value BLOB NOT NULL,
    expires_at BIGINT,
    PRIMARY KEY (namespace, key)
);

CREATE TABLE nexss_storage_blobs (
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    value BLOB NOT NULL,
    blob_size BIGINT NOT NULL,
    digest TEXT NOT NULL,
    owner_user TEXT NOT NULL,
    owner_tenant TEXT NOT NULL,
    expires_at BIGINT,
    PRIMARY KEY (namespace, owner_tenant, owner_user, key)
);
