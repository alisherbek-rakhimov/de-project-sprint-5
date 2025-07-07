-- drop table if exists stg.bonussystem_users;
create table if not exists stg.bonussystem_users
(
    id            integer primary key,
    order_user_id text not null
);