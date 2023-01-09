use std::{
    collections::{HashMap, HashSet},
    future,
    sync::Arc,
    time::{Duration, SystemTime},
};

use client::DataClient;

use futures::{stream, Future, Stream, StreamExt};

use pusher::{Pusher, PusherMessage};

use saver::DataSaver;
use serde::Deserialize;

use tokio::{
    sync::RwLock,
    time::{interval, MissedTickBehavior},
};

mod client;
mod pusher;
mod saver;

#[derive(Clone)]
struct Context {
    client: Arc<DataClient>,
    saver: Arc<DataSaver>,
    pusher: Pusher,
    state: Arc<RwLock<SimState>>,
}

impl Context {
    async fn get_season_day(&self) -> Option<(String, i32)> {
        let state = self.state.read().await;
        if let Some(season) = &state.season {
            Some((season.clone(), state.day))
        } else {
            None
        }
    }

    async fn update_state(&self, season: String, day: i32) {
        let mut state = self.state.write().await;
        state.season = Some(season);
        state.day = day;

        println!("updated state: {:?}", state);
    }
}

#[derive(Debug)]
struct SimState {
    season: Option<String>,
    day: i32,
}

fn now() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

#[derive(Deserialize, Debug)]
struct SimData {
    #[serde(rename = "simData")]
    sim_data: SimDataInner,
}

#[derive(Deserialize, Debug)]
struct SimDataInner {
    #[serde(rename = "currentSeasonId")]
    current_season_id: String,

    #[serde(rename = "currentDay")]
    current_day: i32,
}

#[derive(Deserialize, Debug)]
struct TeamData {
    // id: String,
    roster: Vec<TeamRosterSlot>,
}

#[derive(Deserialize, Debug)]
struct TeamRosterSlot {
    id: String,
}

async fn poll_sim(ctx: Context) -> anyhow::Result<()> {
    let resp = ctx.client.fetch("https://api2.blaseball.com/sim").await?;
    ctx.saver.save_fetch(&resp).await?;

    let json_data = serde_json::from_slice::<SimData>(&resp.data)?;
    ctx.update_state(
        json_data.sim_data.current_season_id,
        json_data.sim_data.current_day,
    )
    .await;
    Ok(())
}

async fn poll_flag(ctx: Context) -> anyhow::Result<()> {
    let resp = ctx
        .client
        .fetch("https://api2.blaseball.com/flagsmith")
        .await?;
    ctx.saver.save_fetch(&resp).await?;

    Ok(())
}

async fn poll_temporal(ctx: Context) -> anyhow::Result<()> {
    let resp = ctx
        .client
        .fetch("https://api2.blaseball.com/temporal")
        .await?;
    ctx.saver.save_fetch(&resp).await?;

    Ok(())
}

async fn poll_ticker(ctx: Context) -> anyhow::Result<()> {
    let resp = ctx
        .client
        // so, as far as i can tell, the user id here isn't actually checked or ever validated
        // but this is umpdog@sibr.dev
        .fetch("https://api2.blaseball.com/user-ticker/user/be2e2189-85e1-400b-ad24-e717fb6483a5")
        .await?;
    ctx.saver.save_fetch(&resp).await?;

    Ok(())
}

async fn fetch_player(ctx: Context, player_id: &str) -> anyhow::Result<()> {
    if let Some((season, day)) = ctx.get_season_day().await {
        let player = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/days/{}/players/{}",
                season, day, player_id
            ))
            .await?;
        ctx.saver.save_fetch(&player).await?;
        ctx.saver
            .save_player(player_id, &season, day, &player)
            .await?;
    }
    Ok(())
}

async fn poll_post_feed(ctx: Context) -> anyhow::Result<()> {
    let resp = ctx
        .client
        .fetch(&format!("https://api2.blaseball.com/feed?page=0",))
        .await?;
    ctx.saver.save_fetch(&resp).await?;
    Ok(())
}

async fn fetch_box_score(ctx: Context, game_id: &str) -> anyhow::Result<()> {
    if let Some((season, _)) = ctx.get_season_day().await {
        let resp = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/games/{}/boxScore",
                season, game_id
            ))
            .await?;
        ctx.saver.save_fetch(&resp).await?;
    }
    Ok(())
}

async fn poll_all_players(ctx: Context) -> anyhow::Result<()> {
    if let Some((season, day)) = ctx.get_season_day().await {
        let player_ids = ctx.saver.get_player_ids().await?;

        stream::iter(player_ids)
            .for_each_concurrent(2, |player_id| {
                let ctx = ctx.clone();
                async move {
                    if let Err(e) = fetch_player(ctx, &player_id).await {
                        dbg!(e);
                    }

                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            })
            .await;
    }

    Ok(())
}

async fn poll_players_teams(ctx: Context) -> anyhow::Result<()> {
    if let Some((season, day)) = ctx.get_season_day().await {
        let resp = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/days/{}/teams",
                season, day
            ))
            .await?;
        ctx.saver.save_fetch(&resp).await?;

        let team_data = serde_json::from_slice::<HashMap<String, Vec<TeamData>>>(&resp.data)?;

        let player_ids = team_data
            .into_values()
            .flatten()
            .flat_map(|x| x.roster)
            .map(|x| x.id)
            .collect::<HashSet<_>>();
        // player_ids.extend(include_str!("all_player_ids.txt").split("\n").map(|x| x.trim().to_string()));

        // stream::iter(team_data.into_values().flatten())
        // .flat_map(|team| stream::iter(team.roster))
        stream::iter(player_ids)
            .for_each_concurrent(4, |player_id| {
                let ctx = ctx.clone();
                async move {
                    if let Err(e) = fetch_player(ctx, &player_id).await {
                        dbg!(e);
                    }

                    // tokio::time::sleep(Duration::from_millis(500)).await;
                }
            })
            .await;
    }

    Ok(())
}

#[derive(Deserialize)]
struct LiveGames {
    #[serde(rename = "gameIds")]
    game_ids: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct ScheduledGame {
    id: String,
    #[serde(rename = "seasonId")]
    season_id: String,
    day: i32,
    complete: bool,
    updated: String,
}

async fn poll_games_live(mut ctx: Context) -> anyhow::Result<()> {
    if let Some((season, _)) = ctx.get_season_day().await {
        let resp = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/schedule/{}/live",
                season
            ))
            .await?;
        ctx.saver.save_fetch(&resp).await?;

        let live_games = serde_json::from_slice::<LiveGames>(&resp.data)?;
        for id in &live_games.game_ids {
            ctx.pusher.subscribe(&format!("game-feed-{}", id)).await?;
        }

        stream::iter(live_games.game_ids)
            .for_each_concurrent(2, |game_id| {
                let ctx = ctx.clone();
                async move {
                    if let Err(e) = fetch_box_score(ctx, &game_id).await {
                        dbg!(e);
                    }
                }
            })
            .await;
    }

    Ok(())
}

async fn poll_elections(ctx: Context) -> anyhow::Result<()> {
    if let Some((season, _)) = ctx.get_season_day().await {
        let resp = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/elections",
                season
            ))
            .await?;
        ctx.saver.save_fetch(&resp).await?;
    }
    Ok(())
}

async fn poll_game_schedule(mut ctx: Context) -> anyhow::Result<()> {
    if let Some((season, day)) = ctx.get_season_day().await {
        let resp = ctx
            .client
            .fetch(&format!(
                "https://api2.blaseball.com/seasons/{}/games",
                season
            ))
            .await?;
        ctx.saver.save_fetch(&resp).await?;

        let games = serde_json::from_slice::<Vec<serde_json::Value>>(&resp.data)?;
        for game in games {
            let game_struct = serde_json::from_value::<ScheduledGame>(game.clone())?;
            // dbg!(&game_struct);
            if !game_struct.complete && game_struct.day <= day + 2 {
                ctx.pusher
                    .subscribe(&format!("game-feed-{}", game_struct.id))
                    .await?;
            }

            ctx.saver
                .save_game(&game_struct, game, resp.timestamp_after)
                .await?;
        }
    }

    Ok(())
}

async fn run_timed<F, Fut>(ctx: Context, secs: u64, f: F)
where
    F: for<'a, 'b> Fn(Context) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let mut interval = interval(Duration::from_secs(secs));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        if let Err(e) = f(ctx.clone()).await {
            dbg!(e);
        }
    }
}

async fn read_pusher(
    ctx: Context,
    events: impl Stream<Item = (Duration, PusherMessage)>,
) -> anyhow::Result<()> {
    events
        .inspect(|x| {
            dbg!(x);
        })
        .filter(|(_, x)| future::ready(!x.event.starts_with("pusher_internal:")))
        .for_each(|(timestamp, x)| {
            dbg!((timestamp, &x));

            let ctx = ctx.clone();
            async move {
                if let Err(e) = ctx.saver.save_pusher(timestamp, x).await {
                    dbg!(e);
                }
            }
        })
        .await;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (mut pusher, events) = pusher::Pusher::connect("c481dafb635a60adffdd").await?;

    let ctx = Context {
        client: Arc::new(DataClient::new()?),
        pusher: pusher.clone(),
        saver: Arc::new(DataSaver::new().await?),
        state: Arc::new(RwLock::new(SimState {
            season: None,
            day: -1,
        })),
    };

    pusher.subscribe("ticker").await?;
    pusher.subscribe("sim-data").await?;
    pusher.subscribe("temporal").await?;
    pusher.subscribe("fall-ball-drop").await?;

    tokio::spawn(run_timed(ctx.clone(), 30, poll_sim));

    // let it poll sim data first
    tokio::time::sleep(Duration::from_secs(1)).await;

    tokio::spawn(run_timed(ctx.clone(), 5, poll_temporal));
    tokio::spawn(run_timed(ctx.clone(), 15, poll_post_feed));
    tokio::spawn(run_timed(ctx.clone(), 30, poll_flag));
    tokio::spawn(run_timed(ctx.clone(), 30, poll_ticker));
    tokio::spawn(run_timed(ctx.clone(), 30, poll_elections));
    tokio::spawn(run_timed(ctx.clone(), 3, poll_games_live));
    tokio::spawn(run_timed(ctx.clone(), 60, poll_players_teams));
    tokio::spawn(run_timed(ctx.clone(), 60, poll_game_schedule));
    tokio::spawn(run_timed(ctx.clone(), 60*10, poll_all_players));

    // i'm really too tired to figure out how to do retry on this so i'm just gonna make it end if it gets an error
    read_pusher(ctx.clone(), events).await?;

    Ok(())
}
