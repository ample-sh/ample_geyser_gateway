//! Nothing complex here, just an implementation for the Geyser plugin interface to forward data further down the pipeline.

use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use agave_geyser_plugin_interface::geyser_plugin_interface::{GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions, ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, SlotStatus};
use tracing::log;
use log::info;
use tokio::runtime::Runtime;
use transport::{ReplicaChannels, UniformAccountInfo, UniformBlockInfo, UniformEntryInfo, UniformSlotInfo, UniformTransactionInfo};
use transport::server::{TransportServer};
use crate::config::AmpleGeyserProxyConfig;
use rustls::crypto::ring::default_provider;
use crate::account_coalescer::AccountCoalescer;

#[derive(Default)]
pub struct AmpleGeyserPluginOuter {
    inner: Option<AmpleGeyserPluginInner>,
    runtime: Option<Runtime>,
    use_account_coalescer: bool
}

impl Debug for AmpleGeyserPluginOuter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.inner.is_some() {
            write!(f, "AmpleGeyserPluginOuter {{ inner: Some(...) }}")
        } else {
            write!(f, "AmpleGeyserPluginOuter {{ inner: None }}")
        }
    }
}

impl AmpleGeyserPluginOuter {
    fn channels(&self) -> &ReplicaChannels {
        &self.inner.as_ref().unwrap().channels
    }
}

struct AmpleGeyserPluginInner {
    channels: ReplicaChannels,
    account_coalescer: AccountCoalescer
}

static THREAD_ID: AtomicU64 = AtomicU64::new(0);
impl agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin for AmpleGeyserPluginOuter {
    fn name(&self) -> &'static str {
        "ample_geyser_proxy"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> agave_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        let config = AmpleGeyserProxyConfig::load_from_file(config_file)?;
        
        self.use_account_coalescer = config.use_account_coalescer;

        solana_logger::setup_with_default(&config.log_level);

        let _ = default_provider().install_default();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name_fn(|| {
                let id = THREAD_ID.fetch_add(1, Ordering::Relaxed);
                format!("ample-geyser-proxy-tokio-rt-{id}")
            })
            .build()?;
        
        let account_coalescer_duration_us = config.account_coalescer_duration_us;

        let channels = runtime.block_on(async move {
            let channels = ReplicaChannels::new(u16::MAX as usize * 10, u16::MAX as usize, 4000, 1024, 1024);

            let transport_server = TransportServer::bind(
                config.bind_addr,
                config.transport_opts,
                config.transport_cfg,
                channels.clone(),
                None
            ).map_err(|e| GeyserPluginError::Custom(e.into()))?;

            tokio::task::spawn(transport_server.serve());

            Ok::<_, GeyserPluginError>(channels)
        })?;
        
        let account_coalescer = AccountCoalescer::new(Duration::from_micros(account_coalescer_duration_us));

        self.inner = Some(AmpleGeyserPluginInner {
            channels,
            account_coalescer
        });
        self.runtime = Some(runtime);

        info!("ample_geyser_proxy plugin loaded");

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
        self.inner = None;
        info!("ample_geyser_proxy plugin unloaded");
    }

    fn update_account(&self, account: ReplicaAccountInfoVersions, slot: solana_clock::Slot, is_startup: bool) -> agave_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        if !is_startup {
            match account {
                ReplicaAccountInfoVersions::V0_0_3(info) => {
                    let notif = UniformAccountInfo::from_replica(info, slot);
                    let inner = self.inner.as_ref().unwrap();
                    
                    if self.use_account_coalescer {
                        if let Some(notifs) = self.inner.as_ref().unwrap().account_coalescer.coalesce(notif) {
                            for notif in notifs {
                                let _ = inner.channels.account.send(notif);
                            }
                        }
                    } else {
                        let _ = inner.channels.account.send(notif);
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn notify_end_of_startup(&self) -> agave_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        Ok(())
    }

    fn update_slot_status(&self, slot: solana_clock::Slot, parent: Option<u64>, status: &SlotStatus) -> agave_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        let _ = self.channels().slot.send(UniformSlotInfo::from_replica(slot, parent, status.to_owned()));
        
        Ok(())
    }

    fn notify_transaction(&self, transaction: ReplicaTransactionInfoVersions, slot: solana_clock::Slot) -> agave_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        match transaction {
            ReplicaTransactionInfoVersions::V0_0_3(info) => {
                let _ = self.channels().transaction.send(UniformTransactionInfo::from_replica(info, slot));
            },
            _ => {}
        }

        Ok(())
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions) -> agave_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        match entry {
            ReplicaEntryInfoVersions::V0_0_2(info) => {
                let _ = self.channels().entry.send(UniformEntryInfo::from_replica(info));
            },
            _ => {}
        }

        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> agave_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_4(info) => {
                let _ = self.channels().block.send(UniformBlockInfo::from_replica(info));
            },
            _ => {}
        }

        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    // Startup is currently unsupported due to large data volumes
    // TBD: in the future the ample gateway should download snapshots on it's own and sync to the geyser proxy
    fn account_data_snapshot_notifications_enabled(&self) -> bool {
        false
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}