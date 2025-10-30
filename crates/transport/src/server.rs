use std::net::SocketAddr;
use std::sync::Arc;
use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::log;
use crate::{TransportOpts, TransportResult, error::TransportError, ReplicaChannels, StreamOp, metrics::TransportMetrics};

pub struct TransportServer {
    endpoint: quinn::Endpoint,
    replica_channels: ReplicaChannels,
    config: TransportServerConfig,
    metrics: Option<Arc<TransportMetrics>>,
}

#[derive(Clone, Copy, Deserialize, Debug)]
pub struct TransportServerConfig {
    pub use_lz4_compression: bool,
    pub use_zstd_compression: bool,
}

impl TransportServer {
    pub fn bind(addr: SocketAddr, opts: TransportOpts, config: TransportServerConfig, replica_channels: ReplicaChannels, metrics: Option<Arc<TransportMetrics>>) -> TransportResult<Self> {
        let Some(key_path) = &opts.key_path else {
            return Err(TransportError::MissingKeyPath);
        };

        let cert_pem_bytes = std::fs::read(&opts.cert_path)?;
        let key_pem_bytes = std::fs::read(&key_path)?;

        let cert_pem = pem::parse(cert_pem_bytes)?;
        let key_pem = pem::parse(key_pem_bytes)?;

        let cert_der = CertificateDer::from(cert_pem.contents().to_vec());
        let key_der = PrivatePkcs8KeyDer::from(key_pem.contents().to_vec());

        let mut server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], PrivateKeyDer::Pkcs8(key_der))?;

        server_crypto.alpn_protocols = crate::ALPN_QUIC_AMPLE.iter().map(|&x| x.into()).collect();

        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto).unwrap()));
        let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
        transport_config.max_concurrent_bidi_streams(0_u8.into());

        let endpoint = quinn::Endpoint::server(server_config, addr)?;

        Ok(Self {
            endpoint,
            replica_channels,
            config,
            metrics,
        })
    }

    #[tracing::instrument(skip_all, fields(peer_addr = %incoming.remote_address()))]
    async fn accept_connection(incoming: quinn::Incoming, replica_channels: ReplicaChannels, config: TransportServerConfig, metrics: Option<Arc<TransportMetrics>>) -> TransportResult<quinn::Connection> {
        let connection = incoming.await?;

        log::info!("accepted connection, opening replica channels");

        // account channel
        let send = connection.open_uni().await?;
        tokio::spawn(Self::handle_channel(
            send,
            replica_channels.account.clone(),
            StreamOp::Account,
            config,
            connection.remote_address(),
            metrics.clone(),
        ));

        // transaction channel
        let send = connection.open_uni().await?;
        let _ = send.set_priority(5);
        tokio::spawn(Self::handle_channel(
            send,
            replica_channels.transaction.clone(),
            StreamOp::Transaction,
            config,
            connection.remote_address(),
            metrics.clone(),
        ));

        // entry channel
        let send = connection.open_uni().await?;
        tokio::spawn(Self::handle_channel(
            send,
            replica_channels.entry.clone(),
            StreamOp::Entry,
            config,
            connection.remote_address(),
            metrics.clone(),
        ));

        // block channel
        let send = connection.open_uni().await?;
        tokio::spawn(Self::handle_channel(
            send,
            replica_channels.block.clone(),
            StreamOp::Block,
            config,
            connection.remote_address(),
            metrics.clone(),
        ));

        // slot status channel
        let send = connection.open_uni().await?;
        let _ = send.set_priority(4);
        tokio::spawn(Self::handle_channel(
            send,
            replica_channels.slot.clone(),
            StreamOp::SlotStatus,
            config,
            connection.remote_address(),
            metrics.clone(),
        ));

        Ok(connection)
    }

    #[tracing::instrument(skip_all, fields(remote_peer_addr = %_remote_peer_addr))]
    async fn handle_channel<T>(
        mut send: quinn::SendStream,
        channel: tokio::sync::broadcast::Sender<T>,
        op: StreamOp,
        config: TransportServerConfig,
        _remote_peer_addr: SocketAddr,
        metrics: Option<Arc<TransportMetrics>>,
    ) -> TransportResult<()>
    where
        T: Serialize + Clone,
    {
        let mut tx: Box<dyn AsyncWrite + Send + Unpin>;
        if config.use_lz4_compression {
            send.write_u8(StreamOp::UseLz4Compression as u8).await?;
            send.write_u8(op as u8).await?;
            tx = Box::new(async_compression::tokio::write::Lz4Encoder::new(send));
        } else if config.use_zstd_compression {
            send.write_u8(StreamOp::UseZstdCompression as u8).await?;
            send.write_u8(op as u8).await?;
            tx = Box::new(async_compression::tokio::write::ZstdEncoder::new(send));
        } else {
            send.write_u8(StreamOp::UseNoCompression as u8).await?;
            send.write_u8(op as u8).await?;
            tx = Box::new(send);
        };

        let mut notif_rx = channel.subscribe();
        loop {
            match notif_rx.recv().await {
                Ok(data) => {
                    let serialized = bincode::serialize(&data)?;
                    let data_len = serialized.len() as u32;
                    tx.write_u32_le(data_len).await?;
                    tx.write_all(&serialized).await?;
                    tx.flush().await?;
                    log::trace!("sent {} bytes on channel {:?}", data_len, op);

                    // Record metrics
                    crate::metrics::StreamMetricHelper::record_message(
                        metrics.as_ref(),
                        op,
                        data_len as u64,
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    log::warn!("lagged on channel {:?}, skipped {} messages", op, skipped);
                    crate::metrics::StreamMetricHelper::record_packets_dropped(
                        metrics.as_ref(),
                        op,
                        skipped,
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    log::debug!("channel {:?} closed, terminating stream", op);
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn serve(self) {
        while let Some(incoming) = self.endpoint.accept().await {
            let replica_channels = self.replica_channels.clone();
            let config = self.config;
            let metrics = self.metrics.clone();
            tokio::spawn(async move {
                match Self::accept_connection(incoming, replica_channels, config, metrics).await {
                    Ok(_) => log::info!("connection closed"),
                    Err(e) => log::error!("connection error: {}", e),
                }
            });
        }
    }
}