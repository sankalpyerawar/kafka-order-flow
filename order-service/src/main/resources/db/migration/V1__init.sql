create table orders (
    id              uuid primary key,
    customer_id     varchar(64)    not null,
    amount          numeric(19, 2) not null check (amount > 0),
    currency        varchar(3)     not null,
    status          varchar(32)    not null,
    payment_id      uuid,
    failure_reason  varchar(255),
    created_at      timestamptz    not null,
    updated_at      timestamptz    not null,
    version         bigint         not null
);

-- Transactional outbox: written in the same transaction as the order change.
create table outbox_events (
    id           uuid primary key,          -- doubles as the eventId header
    topic        varchar(255) not null,
    message_key  varchar(255) not null,
    event_type   varchar(128) not null,
    payload      jsonb        not null,
    created_at   timestamptz  not null default now(),
    published_at timestamptz
);

-- The relay only ever scans unpublished rows.
create index ix_outbox_unpublished on outbox_events (created_at, id) where published_at is null;

-- Idempotent consumer bookkeeping.
create table processed_events (
    event_id     uuid         not null,
    consumer     varchar(128) not null,
    processed_at timestamptz  not null,
    primary key (event_id, consumer)
);
