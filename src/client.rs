use std::time::Duration;

use dashmap::DashMap;
use reqwest::{
    header::{self, HeaderMap, HeaderValue},
    Client, ClientBuilder, StatusCode, Url,
};

use crate::now;

#[derive(Debug, Clone)]
pub struct SavedRequest {
    pub url: Url,
    pub timestamp_before: Duration,
    pub timestamp_after: Duration,
    pub server_date: Option<String>,
    pub etag: Option<String>,
    pub data: Vec<u8>,
    pub status_code: StatusCode,
    pub was_cached: bool,
}

#[derive(Clone)]
pub struct DataClient {
    client: Client,
    cached_responses: DashMap<String, SavedRequest>,
}

impl DataClient {
    pub fn new() -> anyhow::Result<DataClient> {
        let mut headers = HeaderMap::new();

        // i'm too tired for this
        headers.insert("Cookie", HeaderValue::from_str(include_str!("cookie.txt").trim())?);

        let client = ClientBuilder::new()
            .user_agent("archiver/0.1 (hello umps this is sibr, please do not ban us)")
            .deflate(true)
            .brotli(true)
            .gzip(true)
            .default_headers(headers)
            .build()?;

        Ok(DataClient {
            client,
            cached_responses: DashMap::new(),
        })
    }

    pub async fn fetch(&self, orig_url: &str) -> anyhow::Result<SavedRequest> {
        let timestamp_before = now();

        let mut request = self.client.get(orig_url);
        if let Some(cached_etag) = self
            .cached_responses
            .get(orig_url)
            .and_then(|x| x.etag.clone())
        {
            request = request.header(header::IF_NONE_MATCH, cached_etag);
        }

        let response = request.send().await?;
        let timestamp_after = now();

        let url = response.url().clone();
        let server_date = response
            .headers()
            .get(header::DATE)
            .and_then(|x| x.to_str().ok())
            .map(|x| x.to_owned());
        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|x| x.to_str().ok())
            .map(|x| x.to_owned());
        let status_code = response.status();

        println!(
            "{} {} ({}s)",
            response.status(),
            response.url().to_string(),
            (timestamp_after - timestamp_before).as_secs_f64()
        );

        let response = response.error_for_status()?;

        if response.status() == StatusCode::NOT_MODIFIED {
            if let Some(resp) = self.cached_responses.get(orig_url) {
                if resp.etag == etag {
                    let mut cached_resp = resp.clone();
                    cached_resp.status_code = response.status();
                    cached_resp.was_cached = true;
                    return Ok(cached_resp);
                }
            }
        }

        let data = response.bytes().await?.to_vec();

        let sr = SavedRequest {
            url,
            timestamp_before,
            timestamp_after,
            server_date,
            etag,
            data,
            status_code,
            was_cached: false,
        };

        if sr.etag.is_some() {
            self.cached_responses
                .insert(orig_url.to_string(), sr.clone());
        }

        Ok(sr)
    }
}
