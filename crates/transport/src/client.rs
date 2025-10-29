use crate::{NUM_EXPECTED_REPLICA_CHANNELS, ReplicaChannels, StreamOp, TransportOpts, TransportResult, error, metrics::TransportMetrics, ReplicaReceivers};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{Endpoint, RecvStream};
use rustls::pki_types::CertificateDer;
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, BufReader, ReadBuf};
use tracing::log;
use crate::metrics::StreamMetricHelper;

/// Wrapper that tracks compressed bytes read from the network
struct CompressedBytesWrapper<R> {
    inner: R,
    metrics: Option<Arc<TransportMetrics>>,
    stream_type: StreamOp,
}

impl<R: AsyncBufRead + Unpin> AsyncRead for CompressedBytesWrapper<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let bytes_read = buf.filled().len() - filled_before;
                if bytes_read > 0 {
                    StreamMetricHelper::record_compressed_bytes(
                        self.metrics.as_ref(),
                        self.stream_type,
                        bytes_read as u64,
                    );
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<R: AsyncBufRead + Unpin> AsyncBufRead for CompressedBytesWrapper<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        StreamMetricHelper::record_compressed_bytes(
            this.metrics.as_ref(),
            this.stream_type,
            amt as u64,
        );
        Pin::new(&mut this.inner).consume(amt);
    }
}

pub struct TransportClient {
    replica_channels: ReplicaChannels,
    metrics: Option<Arc<TransportMetrics>>,
}

impl TransportClient {
    pub async fn connect(
        addr: SocketAddr,
        opts: TransportOpts,
        replica_channels: ReplicaChannels,
        metrics: Option<Arc<TransportMetrics>>,
    ) -> TransportResult<ReplicaReceivers> {
        let mut endpoint = Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))?;

        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from(
            pem::parse(fs::read(opts.cert_path)?)?.contents(),
        ))?;

        let mut client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        client_crypto.alpn_protocols = crate::ALPN_QUIC_AMPLE.iter().map(|&x| x.into()).collect();

        let client_config =
            quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto).unwrap()));
        endpoint.set_default_client_config(client_config);

        let connection = endpoint.connect(addr, &opts.fqdn)?.await?;
        let client = Self { replica_channels: replica_channels.clone(), metrics };
        client.spawn_replica_channel_tasks(connection).await?;

        Ok(ReplicaReceivers {
            account: replica_channels.account.subscribe(),
            transaction: replica_channels.transaction.subscribe(),
            entry: replica_channels.entry.subscribe(),
            block: replica_channels.block.subscribe(),
            slot: replica_channels.slot.subscribe(),
        })
    }

    async fn handle_explicit_stream_type<T>(
        sender: tokio::sync::broadcast::Sender<T>,
        mut stream: Box<dyn AsyncRead + Send + Unpin>,
        stream_op: StreamOp,
        metrics: Option<Arc<TransportMetrics>>,
        exit: Arc<AtomicBool>
    ) where
        T: for<'de> Deserialize<'de> + Send,
    {
        // receive data from stream and forward to sender
        while let Ok(len) = stream.read_u32_le().await {
            if exit.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            log::trace!("reading {} bytes from stream", len);

            let mut buf = vec![0u8; len as usize];
            if let Err(e) = stream.read_exact(&mut buf).await {
                log::error!("failed to read from stream: {}", e);
                exit.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
            /*
            StreamMetricHelper::record_network_bytes_transferred(
                metrics.as_ref(),
                len as u64 + 4,
            );
             */
            match bincode::deserialize::<T>(buf.as_slice()) {
                Ok(data) => {
                    let _ = sender.send(data);
                    // Record metrics for successfully received message
                    StreamMetricHelper::record_message(
                        metrics.as_ref(),
                        stream_op,
                        len as u64,
                    );
                }
                Err(e) => {
                    log::error!("failed to deserialize data: {}", e);
                    continue;
                }
            };
        }
        exit.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    async fn handle_replica_channel_and_stream(
        replica_channels: ReplicaChannels,
        stream: RecvStream,
        lz4_enabled: bool,
        zstd_enabled: bool,
        stream_type: StreamOp,
        metrics: Option<Arc<TransportMetrics>>,
        exit: Arc<AtomicBool>
    ) {
        let buf_stream = BufReader::new(stream);
        let metrics_wrapper = CompressedBytesWrapper {
            inner: buf_stream,
            metrics: metrics.clone(),
            stream_type,
        };

        let rx: Box<dyn AsyncRead + Send + Unpin>;
        if lz4_enabled {
            rx = Box::new(async_compression::tokio::bufread::Lz4Decoder::new(
                metrics_wrapper,
            ));
        } else if zstd_enabled {
            rx = Box::new(async_compression::tokio::bufread::ZstdDecoder::new(
                metrics_wrapper,
            ));
        } else {
            rx = Box::new(metrics_wrapper);
        }
        match stream_type {
            StreamOp::Account => {
                Self::handle_explicit_stream_type(replica_channels.account, rx, stream_type, metrics, exit.clone()).await
            }
            StreamOp::Transaction => {
                Self::handle_explicit_stream_type(replica_channels.transaction, rx, stream_type, metrics, exit.clone()).await
            }
            StreamOp::Entry => {
                Self::handle_explicit_stream_type(replica_channels.entry, rx, stream_type, metrics, exit.clone()).await
            },
            StreamOp::Block => {
                Self::handle_explicit_stream_type(replica_channels.block, rx, stream_type, metrics, exit.clone()).await
            },
            StreamOp::SlotStatus => {
                Self::handle_explicit_stream_type(replica_channels.slot, rx, stream_type, metrics, exit.clone()).await
            }
            _ => {}
        }
        log::debug!("replica channel {:?} opened", stream_type);
    }

    async fn spawn_replica_channel_tasks(
        &self,
        connection: quinn::Connection,
    ) -> TransportResult<()> {
        let replica_channels = self.replica_channels.clone();
        let metrics = self.metrics.clone();
        let exit = Arc::new(AtomicBool::new(false));

        for i in 0..NUM_EXPECTED_REPLICA_CHANNELS {
            let mut recv = connection.accept_uni().await?;

            let first_op =
                StreamOp::try_from(recv.read_u8().await?).unwrap_or(StreamOp::UseNoCompression);
            let use_lz4_compression = matches!(first_op, StreamOp::UseLz4Compression);
            let use_zstd_compression = matches!(first_op, StreamOp::UseZstdCompression);

            let second_op_u8 = recv.read_u8().await?;
            let second_op = StreamOp::try_from(second_op_u8)
                .map_err(|_| error::TransportError::InvalidStreamOp(second_op_u8))?;

            log::debug!(
                "channel {}: use_lz4_compression={}, use_zstd_compression={}, second_op={:?}",
                i,
                use_lz4_compression,
                use_zstd_compression,
                second_op
            );

            tokio::spawn(Self::handle_replica_channel_and_stream(
                replica_channels.clone(),
                recv,
                use_lz4_compression,
                use_zstd_compression,
                second_op,
                metrics.clone(),
                exit.clone()
            ));
        }

        Ok(())
    }
}
