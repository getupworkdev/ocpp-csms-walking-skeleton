-- Charge points, keyed by the identity in the WebSocket URL (/ocpp/{id}).
create table chargers (
    id                  text primary key,
    vendor              text,
    model               text,
    serial_number       text,
    firmware_version    text,
    first_seen_at       timestamptz not null default now(),
    last_seen_at        timestamptz not null default now(),
    last_boot_at        timestamptz,
    last_heartbeat_at   timestamptz
);

-- Local authorisation list. Anything not in here is Invalid.
create table id_tags (
    id_tag      text primary key,
    status      text not null check (status in ('Accepted', 'Blocked', 'Expired', 'Invalid')),
    expires_at  timestamptz
);

-- Prices are integers in minor units (cents, pence) so cost maths is exact.
create table tariffs (
    id                   serial primary key,
    name                 text not null,
    currency             char(3) not null,
    price_per_kwh_minor  bigint not null check (price_per_kwh_minor >= 0),
    session_fee_minor    bigint not null check (session_fee_minor >= 0),
    active               boolean not null default false
);
create unique index tariffs_one_active on tariffs (active) where active;

-- One row per charging session. The serial id *is* the OCPP transactionId the
-- CSMS hands back in StartTransaction.conf (OCPP 1.6 uses a 32-bit integer).
--
-- started_at / stopped_at are the charger's timestamps, not when we received
-- the message, so a stop that arrives hours late still records when it happened.
-- The tariff is snapshotted at start so a price change mid-session does not
-- reprice it.
create table transactions (
    id                   serial primary key,
    charger_id           text not null references chargers (id),
    connector_id         integer not null,
    id_tag               text not null,
    id_tag_status        text not null,
    meter_start_wh       bigint not null,
    started_at           timestamptz not null,

    tariff_id            integer not null references tariffs (id),
    currency             char(3) not null,
    price_per_kwh_minor  bigint not null,
    session_fee_minor    bigint not null,

    meter_stop_wh        bigint,
    stopped_at           timestamptz,
    stop_reason          text,
    energy_wh            bigint,
    cost_minor           bigint,

    start_received_at    timestamptz not null default now(),
    stop_received_at     timestamptz,

    check ((stopped_at is null) = (cost_minor is null))
);
create index transactions_charger on transactions (charger_id, started_at desc);

-- Every charger-initiated CALL (except Heartbeat) and the frame we answered
-- with. The primary key is what makes processing idempotent: a replayed
-- message with the same uniqueId hits the conflict and gets the stored
-- response back without being handled again.
create table ocpp_messages (
    charger_id      text not null references chargers (id),
    unique_id       text not null,
    action          text not null,
    payload         jsonb not null,
    response        jsonb,
    transaction_id  integer,
    received_at     timestamptz not null default now(),
    duplicates      integer not null default 0,
    primary key (charger_id, unique_id)
);
create index ocpp_messages_transaction on ocpp_messages (transaction_id) where transaction_id is not null;

-- Individual samples from MeterValues and StopTransaction.transactionData.
-- transaction_id is what the charger reported and is deliberately not a
-- foreign key: chargers do send ids we have never seen, and the sample is
-- still worth keeping.
create table meter_values (
    id                bigserial primary key,
    charger_id        text not null,
    source_unique_id  text not null,
    connector_id      integer not null,
    transaction_id    integer,
    sampled_at        timestamptz not null,
    measurand         text not null,
    value             text not null,
    unit              text,
    context           text,
    phase             text,
    location          text,
    energy_wh         bigint,
    foreign key (charger_id, source_unique_id) references ocpp_messages (charger_id, unique_id)
);
create index meter_values_transaction on meter_values (transaction_id, sampled_at);
