CREATE SCHEMA IF NOT EXISTS etl_internal;

create table if not exists etl_internal.failed_uris
(
    uri            varchar not null,
    entity_type    varchar not null,
    error_reason   text      default 'API returned null'::text,
    failed_at      timestamp default CURRENT_TIMESTAMP,
    retry_attempts integer   default 0,
    primary key (uri)
);