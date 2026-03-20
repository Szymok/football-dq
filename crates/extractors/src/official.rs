use anyhow::{Result, Context};
use reqwest::Client;

pub struct OfficialApiExtractor {
    client: Client,
    base_url: String,
}

impl OfficialApiExtractor {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.to_string(),
        }
    }
}
