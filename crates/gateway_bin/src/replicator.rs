use agave_geyser_plugin_interface::geyser_plugin_interface::{ReplicaAccountInfoV3, ReplicaAccountInfoVersions, ReplicaBlockInfoV4, ReplicaBlockInfoVersions, ReplicaEntryInfoV2, ReplicaEntryInfoVersions, ReplicaTransactionInfoV3, ReplicaTransactionInfoVersions};
use solana_geyser_plugin_manager::geyser_plugin_manager::GeyserPluginManager;
use tokio::sync::broadcast::error::TryRecvError;
use transport::{ReplicaChannels, ReplicaReceivers, UniformAccountInfo, UniformBlockInfo, UniformEntryInfo, UniformSlotInfo, UniformTransactionInfo};

macro_rules! try_recv_and_handle {
    ($receiver:expr, $replica:ident => $ok_handler:expr) => {
        match $receiver.try_recv() {
            Ok($replica) => $ok_handler,
            Err(TryRecvError::Closed) => {}
            Err(_) => {}
        }
    };
}

pub struct Replicator {
    replica_receivers: ReplicaReceivers
}

impl Replicator {
    pub fn new(replica_receivers: ReplicaReceivers) -> Self {
        Self {
            replica_receivers
        }
    }
    
    fn notify_account_replica(&self, manager: &GeyserPluginManager, replica: UniformAccountInfo) {
        for plugin in manager.plugins.iter() {
            match plugin.update_account(ReplicaAccountInfoVersions::V0_0_3(&ReplicaAccountInfoV3 {
                pubkey: replica.pubkey.as_ref(),
                lamports: replica.lamports,
                owner: replica.owner.as_ref(),
                executable: replica.executable,
                rent_epoch: replica.rent_epoch,
                data: replica.data.as_ref(),
                write_version: replica.write_version,
                txn: None, // TODO fetch transaction reference from the transaction stream cache
            }), replica.slot, false) {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("Error processing account replica in plugin {}: {:?}", plugin.name(), err);
                }
            }
        }
    }
    
    fn notify_transaction_replica(&self, manager: &GeyserPluginManager, replica: UniformTransactionInfo) {
        for plugin in manager.plugins.iter() {
            match plugin.notify_transaction(ReplicaTransactionInfoVersions::V0_0_3(&ReplicaTransactionInfoV3 {
                signature: &replica.signature,
                message_hash: &replica.message_hash,
                is_vote: replica.is_vote,
                transaction: &replica.transaction,
                transaction_status_meta: &replica.transaction_status_meta,
                index: replica.index,
            }), replica.slot) {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("Error processing transaction replica in plugin {}: {:?}", plugin.name(), err);
                }
            }
        }
    }
    
    fn notify_block_replica(&self, manager: &GeyserPluginManager, replica: UniformBlockInfo) {
        for plugin in manager.plugins.iter() {
            match plugin.notify_block_metadata(ReplicaBlockInfoVersions::V0_0_4(&ReplicaBlockInfoV4 {
                parent_slot: replica.parent_slot,
                parent_blockhash: &replica.parent_blockhash,
                slot: replica.slot,
                blockhash: &replica.blockhash,
                rewards: &replica.rewards,
                block_time: replica.block_time,
                block_height: replica.block_height,
                executed_transaction_count: replica.executed_transaction_count,
                entry_count: replica.entry_count,
            })) {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("Error processing block replica in plugin {}: {:?}", plugin.name(), err);
                }
            }
        }
    }
    
    fn notify_entry_replica(&self, manager: &GeyserPluginManager, replica: UniformEntryInfo) {
        for plugin in manager.plugins.iter() {
            match plugin.notify_entry(ReplicaEntryInfoVersions::V0_0_2(&ReplicaEntryInfoV2 {
                slot: replica.slot,
                index: replica.index,
                num_hashes: replica.num_hashes,
                hash: &replica.hash.to_bytes(),
                executed_transaction_count: replica.executed_transaction_count,
                starting_transaction_index: replica.starting_transaction_index,
            })) {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("Error processing entry replica in plugin {}: {:?}", plugin.name(), err);
                }
            }
        }
    }
    
    fn notify_slot_replica(&self, manager: &GeyserPluginManager, replica: UniformSlotInfo) {
        for plugin in manager.plugins.iter() {
            match plugin.update_slot_status(replica.slot, replica.parent, &replica.status) {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("Error processing slot replica in plugin {}: {:?}", plugin.name(), err);
                }
            }
        }
    }
    
    pub fn replicate(&mut self, manager: &GeyserPluginManager) -> Result<(), ()> {
        try_recv_and_handle!(self.replica_receivers.account, account_replica => self.notify_account_replica(manager, account_replica));
        try_recv_and_handle!(self.replica_receivers.transaction, transaction_replica => self.notify_transaction_replica(manager, transaction_replica));
        try_recv_and_handle!(self.replica_receivers.block, block_replica => self.notify_block_replica(manager, block_replica));
        try_recv_and_handle!(self.replica_receivers.entry, entry_replica => self.notify_entry_replica(manager, entry_replica));
        try_recv_and_handle!(self.replica_receivers.slot, slot_replica => self.notify_slot_replica(manager, slot_replica));
        
        Ok(())
    }
}