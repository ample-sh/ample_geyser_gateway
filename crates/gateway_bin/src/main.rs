mod replicator;

use std::mem;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use rustls::crypto::ring::default_provider;
use tracing_subscriber;
use transport::{metrics::TransportMetrics, ReplicaChannels, TransportOpts};
use opentelemetry::metrics::MeterProvider;
use rustls::Stream;
use solana_geyser_plugin_manager::geyser_plugin_manager::GeyserPluginManager;
use tracing::log;
use transport::metrics::{init_metrics, StreamMetricHelper};
use crate::replicator::Replicator;

#[derive(Parser)]
#[command(name = "ample-geyser-gateway")]
#[command(about = "Gateway for Ample Geyser proxy", long_about = None)]
struct Args {
    /// Ample proxy server address to connect to
    #[arg(long, value_name = "ADDR")]
    upstream_proxy_addr: SocketAddr,

    /// Fully qualified domain name
    #[arg(long, value_name = "FQDN")]
    fqdn: String,

    /// Path to certificate file
    #[arg(long, value_name = "PATH", default_value = "certs/cert.pem")]
    cert_path: PathBuf,

    /// OpenTelemetry metrics HTTP collector url
    #[arg(long)]
    metrics_otlp_url: Option<String>,


    #[arg(short, long)]
    geyser_plugin_config: Vec<String>
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let _ = default_provider().install_default();

    tracing::info!(
        upstream_proxy_addr = %args.upstream_proxy_addr,
        fqdn = %args.fqdn,
        cert_path = ?args.cert_path,
        "starting ample geyser gateway"
    );

    // Initialize OpenTelemetry metrics
    let metrics = if let Some(metrics_otlp_url) = &args.metrics_otlp_url {
        let meter_provider = init_metrics(metrics_otlp_url);
        let meter = meter_provider.meter("transport-gateway");
        mem::forget(meter_provider); // todo ugly hack, prevents meter provider from being dropped in the beginning
        Some(Arc::new(TransportMetrics::new(&meter)))
    } else {
        None
    };

    let replica_channels = ReplicaChannels::new(
        u16::MAX as usize * 10,
        u16::MAX as usize,
        4000,
        1024,
        1024,
    );

    let replica_receivers = transport::client::TransportClient::connect(
        args.upstream_proxy_addr,
        TransportOpts {
            cert_path: args.cert_path,
            key_path: None,
            fqdn: args.fqdn,
        },
        replica_channels.clone(),
        metrics.clone(),
    ).await?;

    let mut replicator = Replicator::new(replica_receivers);

    let mut manager = GeyserPluginManager::new();

    for path in args.geyser_plugin_config {
        log::info!("loading geyser plugin: {}", path);
        let name = manager.load_plugin(path.clone())?;
        StreamMetricHelper::record_geyser_plugin_loaded(
            metrics.as_ref(),
            &path,
            &name
        )
    }

    loop {
        if replicator.replicate(&manager).is_err() {
            break;
        }
    }

    Ok(())
}
