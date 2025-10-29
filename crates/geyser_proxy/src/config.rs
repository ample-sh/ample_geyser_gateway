use std::net::SocketAddr;
use serde::Deserialize;
use transport::server::{TransportServerConfig};
use transport::TransportOpts;

#[derive(Debug, Deserialize)]
pub(crate) struct AmpleGeyserProxyConfig {
    #[serde(rename = "libpath")]
    _libpath: String,
    _comment: Option<String>,
    
    pub transport_opts: TransportOpts,
    pub transport_cfg: TransportServerConfig,
    pub bind_addr: SocketAddr,
    pub log_level: String,
    pub use_account_coalescer: bool,
    pub account_coalescer_duration_us: u64
}

impl AmpleGeyserProxyConfig {
    pub fn load_from_file(path: &str) -> Result<Self, std::io::Error> {
        let contents = std::fs::read_to_string(path)?;
        let json = serde_json::from_str(&contents)?;
        Ok(json)
    }
}
