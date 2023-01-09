use std::{str::FromStr, sync::Arc, time::Duration};

use dashmap::DashMap;
use reqwest::Url;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Pool, Sqlite,
};

use crate::{client::SavedRequest, pusher::PusherMessage, ScheduledGame};

#[derive(Clone)]
pub struct DataSaver {
    pool: Pool<Sqlite>,
    last_etags: Arc<DashMap<Url, String>>,
}

impl DataSaver {
    pub async fn new() -> anyhow::Result<DataSaver> {
        let options = SqliteConnectOptions::from_str("sqlite:data/file.db?mode=rwc")?
            .journal_mode(SqliteJournalMode::Wal);

        let pool = SqlitePoolOptions::new().connect_with(options).await?;
        sqlx::query(include_str!("schema.sql"))
            .execute(&pool)
            .await?;

        Ok(DataSaver {
            pool,
            last_etags: Arc::new(DashMap::new()),
        })
    }

    pub async fn shutdown(&self) {
        self.pool.close().await;
    }

    pub async fn save_fetch(&self, r: &SavedRequest) -> anyhow::Result<()> {
        if let Some(ref etag) = r.etag {
            if self
                .last_etags
                .insert(r.url.clone(), etag.to_string())
                .as_deref()
                == Some(etag)
            {
                return Ok(());
            }
        }

        sqlx::query("insert into fetches (url, timestamp_before, timestamp_after, server_date, etag, data, status_code, was_cached) values (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(r.url.to_string())
            .bind(r.timestamp_before.as_secs_f64())
            .bind(r.timestamp_after.as_secs_f64())
            .bind(&r.server_date)
            .bind(&r.etag)
            .bind(&r.data)
            .bind(r.status_code.as_u16())
            .bind(r.was_cached)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn save_pusher(&self, timestamp: Duration, evt: PusherMessage) -> anyhow::Result<()> {
        sqlx::query("insert into events (timestamp, channel, event, payload) values (?, ?, ?, ?)")
            .bind(timestamp.as_secs_f64())
            .bind(evt.channel)
            .bind(evt.event)
            .bind(evt.data)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn save_game(
        &self,
        game: &ScheduledGame,
        data: serde_json::Value,
        timestamp: Duration,
    ) -> anyhow::Result<()> {
        sqlx::query("insert or ignore into games (game_id, update_time, fetch_timestamp, season, day, data) values (?, ?, ?, ?, ?, ?)")
            .bind(&game.id)
            .bind(&game.updated)
            .bind(timestamp.as_secs_f64())
            .bind(&game.season_id)
            .bind(game.day)
            .bind(data.to_string())
            .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn save_player(
        &self,
        id: &str,
        season: &str,
        day: i32,
        r: &SavedRequest,
    ) -> anyhow::Result<()> {
        sqlx::query("insert or ignore into players (player_id, timestamp_before, timestamp_after, etag, data, season, day) values (?, ?, ?, ?, ?, ?, ?)")
            .bind(id)
            .bind(r.timestamp_before.as_secs_f64())
            .bind(r.timestamp_after.as_secs_f64())
            .bind(&r.etag)
            .bind(&r.data)
            .bind(season)
            .bind(day)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn get_player_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut res: Vec<String> = sqlx::query_scalar("select player_id from players;").fetch_all(&self.pool).await?;
        res.sort();
        res.dedup();
        Ok(res)
    }
}

