create table payments (
    id              uuid primary key,
    order_id        uuid           not null unique, -- at most one payment per order
    amount          numeric(19, 2) not null,
    currency        varchar(3)     not null,
    status          varchar(32)    not null,
    failure_reason  varchar(255),
    created_at      timestamptz    not null
);

create table outbox_events (
    id           uuid primary key,
    topic        varchar(255) not null,
    message_key  varchar(255) not null,
    event_type   varchar(128) not null,
    payload      jsonb        not null,
    created_at   timestamptz  not null default now(),
    published_at timestamptz
);

create index ix_outbox_unpublished on outbox_events (created_at, id) where published_at is null;

create table processed_events (
    event_id     uuid         not null,
    consumer     varchar(128) not null,
    processed_at timestamptz  not null,
    primary key (event_id, consumer)
);
