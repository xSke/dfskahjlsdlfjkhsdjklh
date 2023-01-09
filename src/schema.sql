create table if not exists fetches (
    url text,
    timestamp_before number,
    timestamp_after number,
    server_date text,
    etag text,
    data blob,
    status_code number,
    was_cached boolean
);

create table if not exists events (
    timestamp number,
    channel text,
    event text,
    payload text
);

create table if not exists games (
    game_id text,
    update_time text,
    fetch_timestamp number,
    season text,
    day number,
    data text,
    unique (game_id, update_time)
);

create table if not exists players (
    player_id text,
    timestamp_before number,
    timestamp_after number,
    etag text,
    data text,
    season text,
    day int,
    unique (player_id, etag, season, day)
);

create table if not exists game_updates (
    game_id text,
    display_order integer,
    timestamp text,
    data text,
    primary key (game_id, display_order)
);