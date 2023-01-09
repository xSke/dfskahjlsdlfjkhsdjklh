use std::{
    sync::Arc,
    time::{Duration, SystemTime}, collections::HashSet, pin::Pin, ops::DerefMut,
};

use dashmap::DashSet;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    stream::SplitSink,
    SinkExt, Stream, StreamExt, Sink, TryStreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

#[derive(Clone)]
pub struct Pusher {
    subscribe_tx: Sender<String>,
    subscribed_channels: Arc<DashSet<String>>,
}

async fn send_worker(
    sub_rx: Receiver<String>,
    mut ws_tx: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
) {
    let mut messages = sub_rx.map(|channel| {
        let command = PusherCommand {
            event: "pusher:subscribe".to_string(),
            data: PusherSubscribeCommand {
                auth: "".to_string(),
                channel: channel.to_string(),
            },
        };

        let json = serde_json::to_string(&command).expect("couldn't serialize pusher command");
        Message::Text(json)
    });

    while let Some(x) = messages.next().await {
        if let Err(e) = ws_tx.send(x).await {
            dbg!(e);
        }
    }
}

impl Pusher {
    pub async fn connect(
        pusher_key: &str,
    ) -> anyhow::Result<(Pusher, impl Stream<Item = (Duration, PusherMessage)>)> {
        let url = format!("wss://ws-us3.pusher.com/app/{}?protocol=7", pusher_key);
        let (ws, _) = connect_async(&url).await?;
        let (ws_tx, ws_rx) = ws.split();

        let (msg_tx, msg_rx) = mpsc::channel::<String>(1000);

        let (sub_tx, sub_rx) = mpsc::channel::<String>(1000);
        tokio::spawn(send_worker(sub_rx, ws_tx));

        let p = Pusher {
            subscribe_tx: sub_tx,
            subscribed_channels: Arc::new(DashSet::new()),
        };

        let mut stream = Box::pin(ws_rx.filter_map(|x| {
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            async move {
                match x {
                    Ok(Message::Text(text)) => serde_json::from_str::<PusherMessage>(&text)
                        .ok()
                        .map(|x| (timestamp, x)),
                    _ => None,
                }
            }
        }));

        // wait until we're ready to subscribe
        while let Some((_, x)) = stream.next().await {
            if x.event == "pusher:connection_established" {
                break;
            }
        }

        Ok((p, stream))
    }

    pub async fn subscribe(&mut self, channel: &str) -> anyhow::Result<()> {
        if self.subscribed_channels.insert(channel.to_string()) {
            self.subscribe_tx.send(channel.to_string()).await?;
        }

        Ok(())
    }
}

#[derive(Deserialize, Debug)]
pub struct PusherMessage {
    pub channel: Option<String>,
    pub event: String,
    pub data: String,
}

#[derive(Serialize, Debug)]
struct PusherSubscribeCommand {
    auth: String,
    channel: String,
}

#[derive(Serialize, Debug)]
struct PusherCommand<T> {
    event: String,
    data: T,
}
