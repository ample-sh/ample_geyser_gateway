use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Meter};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use std::sync::Arc;

pub struct TransportMetrics {
    // General
    pub network_bytes_transferred: Counter<u64>,

    // Account channel
    pub account_buffered_messages: Gauge<u64>,
    pub account_total_messages: Counter<u64>,
    pub account_total_bytes: Counter<u64>,
    pub account_compressed_bytes: Counter<u64>,
    pub account_packets_dropped: Counter<u64>,

    // Transaction channel
    pub transaction_buffered_messages: Gauge<u64>,
    pub transaction_total_messages: Counter<u64>,
    pub transaction_total_bytes: Counter<u64>,
    pub transaction_compressed_bytes: Counter<u64>,
    pub transaction_packets_dropped: Counter<u64>,

    // Entry channel
    pub entry_buffered_messages: Gauge<u64>,
    pub entry_total_messages: Counter<u64>,
    pub entry_total_bytes: Counter<u64>,
    pub entry_compressed_bytes: Counter<u64>,
    pub entry_packets_dropped: Counter<u64>,

    // Block channel
    pub block_buffered_messages: Gauge<u64>,
    pub block_total_messages: Counter<u64>,
    pub block_total_bytes: Counter<u64>,
    pub block_compressed_bytes: Counter<u64>,
    pub block_packets_dropped: Counter<u64>,

    // Slot channel
    pub slot_buffered_messages: Gauge<u64>,
    pub slot_total_messages: Counter<u64>,
    pub slot_total_bytes: Counter<u64>,
    pub slot_compressed_bytes: Counter<u64>,
    pub slot_packets_dropped: Counter<u64>,

    // Loaded plugins
    pub loaded_plugins: Counter<u64>,
}

impl TransportMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            network_bytes_transferred: meter
                .u64_counter("transport.network.bytes_transferred")
                .with_description("Total network bytes transferred")
                .build(),

            // Account metrics
            account_buffered_messages: meter
                .u64_gauge("transport.channel.account.buffered_messages")
                .with_description("Number of buffered account update messages")
                .build(),
            account_total_messages: meter
                .u64_counter("transport.channel.account.messages_total")
                .with_description("Total account update messages sent")
                .build(),
            account_total_bytes: meter
                .u64_counter("transport.channel.account.bytes_total")
                .with_description("Total bytes of account updates sent")
                .build(),
            account_compressed_bytes: meter
                .u64_counter("transport.channel.account.compressed_bytes_total")
                .with_description("Total compressed bytes of account updates received")
                .build(),
            account_packets_dropped: meter
                .u64_counter("transport.channel.account.packets_dropped_total")
                .with_description("Account update packets dropped due to buffer overflow")
                .build(),

            // Transaction metrics
            transaction_buffered_messages: meter
                .u64_gauge("transport.channel.transaction.buffered_messages")
                .with_description("Number of buffered transaction messages")
                .build(),
            transaction_total_messages: meter
                .u64_counter("transport.channel.transaction.messages_total")
                .with_description("Total transaction messages sent")
                .build(),
            transaction_total_bytes: meter
                .u64_counter("transport.channel.transaction.bytes_total")
                .with_description("Total bytes of transactions sent")
                .build(),
            transaction_compressed_bytes: meter
                .u64_counter("transport.channel.transaction.compressed_bytes_total")
                .with_description("Total compressed bytes of transactions received")
                .build(),
            transaction_packets_dropped: meter
                .u64_counter("transport.channel.transaction.packets_dropped_total")
                .with_description("Transaction packets dropped due to buffer overflow")
                .build(),

            // Entry metrics
            entry_buffered_messages: meter
                .u64_gauge("transport.channel.entry.buffered_messages")
                .with_description("Number of buffered entry messages")
                .build(),
            entry_total_messages: meter
                .u64_counter("transport.channel.entry.messages_total")
                .with_description("Total entry messages sent")
                .build(),
            entry_total_bytes: meter
                .u64_counter("transport.channel.entry.bytes_total")
                .with_description("Total bytes of entries sent")
                .build(),
            entry_compressed_bytes: meter
                .u64_counter("transport.channel.entry.compressed_bytes_total")
                .with_description("Total compressed bytes of entries received")
                .build(),
            entry_packets_dropped: meter
                .u64_counter("transport.channel.entry.packets_dropped_total")
                .with_description("Entry packets dropped due to buffer overflow")
                .build(),

            // Block metrics
            block_buffered_messages: meter
                .u64_gauge("transport.channel.block.buffered_messages")
                .with_description("Number of buffered block messages")
                .build(),
            block_total_messages: meter
                .u64_counter("transport.channel.block.messages_total")
                .with_description("Total block messages sent")
                .build(),
            block_total_bytes: meter
                .u64_counter("transport.channel.block.bytes_total")
                .with_description("Total bytes of blocks sent")
                .build(),
            block_compressed_bytes: meter
                .u64_counter("transport.channel.block.compressed_bytes_total")
                .with_description("Total compressed bytes of blocks received")
                .build(),
            block_packets_dropped: meter
                .u64_counter("transport.channel.block.packets_dropped_total")
                .with_description("Block packets dropped due to buffer overflow")
                .build(),

            // Slot metrics
            slot_buffered_messages: meter
                .u64_gauge("transport.channel.slot.buffered_messages")
                .with_description("Number of buffered slot messages")
                .build(),
            slot_total_messages: meter
                .u64_counter("transport.channel.slot.messages_total")
                .with_description("Total slot messages sent")
                .build(),
            slot_total_bytes: meter
                .u64_counter("transport.channel.slot.bytes_total")
                .with_description("Total bytes of slots sent")
                .build(),
            slot_compressed_bytes: meter
                .u64_counter("transport.channel.slot.compressed_bytes_total")
                .with_description("Total compressed bytes of slots received")
                .build(),
            slot_packets_dropped: meter
                .u64_counter("transport.channel.slot.packets_dropped_total")
                .with_description("Slot packets dropped due to buffer overflow")
                .build(),
            loaded_plugins: meter
                .u64_counter("gateway.loaded_plugins")
                .with_description("Plugins loaded by the geyser gateway")
                .build(),
        }
    }
}

pub struct StreamMetricHelper;

impl StreamMetricHelper {
    pub fn record_message(
        metrics: Option<&Arc<TransportMetrics>>,
        stream_op: crate::StreamOp,
        bytes_count: u64,
    ) {
        if let Some(metrics) = metrics {
            match stream_op {
                crate::StreamOp::Account => {
                    metrics.account_total_messages.add(1, &[]);
                    metrics.account_total_bytes.add(bytes_count, &[]);
                }
                crate::StreamOp::Transaction => {
                    metrics.transaction_total_messages.add(1, &[]);
                    metrics.transaction_total_bytes.add(bytes_count, &[]);
                }
                crate::StreamOp::Entry => {
                    metrics.entry_total_messages.add(1, &[]);
                    metrics.entry_total_bytes.add(bytes_count, &[]);
                }
                crate::StreamOp::Block => {
                    metrics.block_total_messages.add(1, &[]);
                    metrics.block_total_bytes.add(bytes_count, &[]);
                }
                crate::StreamOp::SlotStatus => {
                    metrics.slot_total_messages.add(1, &[]);
                    metrics.slot_total_bytes.add(bytes_count, &[]);
                }
                _ => {}
            }
        }
    }

    pub fn record_packets_dropped(
        metrics: Option<&Arc<TransportMetrics>>,
        stream_op: crate::StreamOp,
        count: u64,
    ) {
        if let Some(metrics) = metrics {
            match stream_op {
                crate::StreamOp::Account => {
                    metrics.account_packets_dropped.add(count, &[]);
                }
                crate::StreamOp::Transaction => {
                    metrics.transaction_packets_dropped.add(count, &[]);
                }
                crate::StreamOp::Entry => {
                    metrics.entry_packets_dropped.add(count, &[]);
                }
                crate::StreamOp::Block => {
                    metrics.block_packets_dropped.add(count, &[]);
                }
                crate::StreamOp::SlotStatus => {
                    metrics.slot_packets_dropped.add(count, &[]);
                }
                _ => {}
            }
        }
    }

    pub fn record_buffer_size(
        metrics: Option<&Arc<TransportMetrics>>,
        stream_op: crate::StreamOp,
        size: u64,
    ) {
        if let Some(metrics) = metrics {
            match stream_op {
                crate::StreamOp::Account => {
                    metrics.account_buffered_messages.record(size, &[]);
                }
                crate::StreamOp::Transaction => {
                    metrics.transaction_buffered_messages.record(size, &[]);
                }
                crate::StreamOp::Entry => {
                    metrics.entry_buffered_messages.record(size, &[]);
                }
                crate::StreamOp::Block => {
                    metrics.block_buffered_messages.record(size, &[]);
                }
                crate::StreamOp::SlotStatus => {
                    metrics.slot_buffered_messages.record(size, &[]);
                }
                _ => {}
            }
        }
    }

    pub fn record_compressed_bytes(
        metrics: Option<&Arc<TransportMetrics>>,
        stream_op: crate::StreamOp,
        bytes_count: u64,
    ) {
        if let Some(metrics) = metrics {
            match stream_op {
                crate::StreamOp::Account => {
                    metrics.account_compressed_bytes.add(bytes_count, &[]);
                }
                crate::StreamOp::Transaction => {
                    metrics.transaction_compressed_bytes.add(bytes_count, &[]);
                }
                crate::StreamOp::Entry => {
                    metrics.entry_compressed_bytes.add(bytes_count, &[]);
                }
                crate::StreamOp::Block => {
                    metrics.block_compressed_bytes.add(bytes_count, &[]);
                }
                crate::StreamOp::SlotStatus => {
                    metrics.slot_compressed_bytes.add(bytes_count, &[]);
                }
                _ => {}
            }
        }
    }

    pub fn record_network_bytes_transferred(
        metrics: Option<&Arc<TransportMetrics>>,
        bytes_count: u64,
    ) {
        if let Some(metrics) = metrics {
            metrics.network_bytes_transferred.add(bytes_count, &[]);
        }
    }

    pub fn record_geyser_plugin_loaded(
        metrics: Option<&Arc<TransportMetrics>>,
        plugin_path: &str,
        plugin_name: &str,
    ) {
        if let Some(metrics) = metrics {
            metrics.loaded_plugins.add(
                1,
                &[
                    KeyValue::new("plugin_path", plugin_path.to_string()),
                    KeyValue::new("plugin_name", plugin_name.to_string()),
                ],
            );
        }
    }
}

/// Initialize OpenTelemetry metrics with stdout exporter
pub fn init_metrics(otlp_endpoint: &str) -> opentelemetry_sdk::metrics::SdkMeterProvider {
    use opentelemetry_otlp::MetricExporter;

    let exporter = MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(otlp_endpoint)
        .build()
        .unwrap();

    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .build();

    tracing::info!("OpenTelemetry metrics initialized @ {}", otlp_endpoint);
    provider
}
