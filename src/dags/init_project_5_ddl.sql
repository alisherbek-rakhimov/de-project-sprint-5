create table if not exists cdm.dm_courier_ledger
(
    id                   serial primary key,
    courier_id           integer        not null,
    courier_name         text           not null,
    settlement_year      integer        not null check (settlement_year >= 2000),
    settlement_month     integer        not null check (settlement_month between 1 and 12),
    orders_count         integer        not null check (orders_count >= 0),
    orders_total_sum     numeric(10, 2) not null check (orders_total_sum >= 0),
    rate_avg             numeric(3, 2)  not null check (rate_avg between 0 and 5),
    order_processing_fee numeric(10, 2) not null check (order_processing_fee >= 0),
    courier_order_sum    numeric(10, 2) not null check (courier_order_sum >= 0),
    courier_tips_sum     numeric(10, 2) not null check (courier_tips_sum >= 0),
    courier_reward_sum   numeric(10, 2) not null check (courier_reward_sum >= 0)
);


create table if not exists stg.couriers
(
    id   serial primary key,
    _id  varchar not null unique,
    data json    not null
);

create table if not exists stg.deliveries
(
    id          serial primary key,
    delivery_id varchar not null unique,
    data        json    not null
);

create table if not exists dds.dm_couriers
(
    id   serial primary key,
    _id  varchar not null unique,
    name varchar not null
);

create table if not exists dds.dm_deliveries
(
    id          serial primary key,
    order_id    varchar        not null unique,
    order_ts    timestamp      not null,
    delivery_id varchar        not null unique,
    courier_id  integer        not null references dds.dm_couriers (id),
    address     text           not null,
    delivery_ts timestamp      not null,
    rate        integer        not null,
    sum         numeric(14, 2) not null check (sum >= 0),
    tip_sum     numeric(14, 2) not null check (tip_sum >= 0)
);


