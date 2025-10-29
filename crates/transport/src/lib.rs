pub mod server;
pub mod client;
pub mod error;
pub mod metrics;

use std::path::PathBuf;
use agave_geyser_plugin_interface::geyser_plugin_interface::{ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV3, SlotStatus};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};
use solana_clock::Slot;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::RewardsAndNumPartitions;
use solana_transaction_status_client_types::TransactionStatusMeta;

pub type TransportResult<T> = Result<T, error::TransportError>;

pub const ALPN_QUIC_AMPLE: &[&[u8]] = &[b"ample/0.1"];

#[derive(Debug, IntoPrimitive, TryFromPrimitive, Copy, Clone)]
#[repr(u8)]
pub enum StreamOp {
    Account = 0,
    Transaction = 1,
    Entry = 2,
    Block = 3,
    SlotStatus = 4,
    UseLz4Compression = 5,
    UseZstdCompression = 6,
    UseNoCompression = 7,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransportOpts {
    pub cert_path: PathBuf,
    pub key_path: Option<PathBuf>,
    pub fqdn: String,
}

pub const NUM_EXPECTED_REPLICA_CHANNELS: usize = 5;

// Cloning is cheap and clones the pointers to the inner broadcast channels
#[derive(Clone)]
pub struct ReplicaChannels {
    pub account: tokio::sync::broadcast::Sender<UniformAccountInfo>,
    pub transaction: tokio::sync::broadcast::Sender<UniformTransactionInfo>,
    pub entry: tokio::sync::broadcast::Sender<UniformEntryInfo>,
    pub block: tokio::sync::broadcast::Sender<UniformBlockInfo>,
    pub slot: tokio::sync::broadcast::Sender<UniformSlotInfo>,
}

pub struct ReplicaReceivers {
    pub account: tokio::sync::broadcast::Receiver<UniformAccountInfo>,
    pub transaction: tokio::sync::broadcast::Receiver<UniformTransactionInfo>,
    pub entry: tokio::sync::broadcast::Receiver<UniformEntryInfo>,
    pub block: tokio::sync::broadcast::Receiver<UniformBlockInfo>,
    pub slot: tokio::sync::broadcast::Receiver<UniformSlotInfo>,
}

impl ReplicaChannels {
    pub fn new(
        account_buffer_size: usize,
        transaction_buffer_size: usize,
        entry_buffer_size: usize,
        block_buffer_size: usize,
        slot_buffer_size: usize,
    ) -> Self {
        create_replica_channels(
            account_buffer_size,
            transaction_buffer_size,
            entry_buffer_size,
            block_buffer_size,
            slot_buffer_size,
        )
    }
}


fn create_replica_channels(
    account_buffer_size: usize,
    transaction_buffer_size: usize,
    entry_buffer_size: usize,
    block_buffer_size: usize,
    slot_buffer_size: usize,
) -> ReplicaChannels {
    let (account_sender, _) = tokio::sync::broadcast::channel(account_buffer_size);
    let (transaction_sender, _) = tokio::sync::broadcast::channel(transaction_buffer_size);
    let (entry_sender, _) = tokio::sync::broadcast::channel(entry_buffer_size);
    let (block_sender, _) = tokio::sync::broadcast::channel(block_buffer_size);
    let (slot_sender, _) = tokio::sync::broadcast::channel(slot_buffer_size);
    
    ReplicaChannels {
        account: account_sender,
        transaction: transaction_sender,
        entry: entry_sender,
        block: block_sender,
        slot: slot_sender,
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UniformAccountInfo {
    pub slot: Slot,
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: u64,
    pub data: Vec<u8>,
    pub transaction_ref: Option<Signature>,
    pub executable: bool,
    pub rent_epoch: u64,
    /// A global monotonically increasing atomic number, which can be used
    /// to tell the order of the account update. For example, when an
    /// account is updated in the same slot multiple times, the update
    /// with higher write_version should supersede the one with lower
    /// write_version.
    pub write_version: u64
}

impl UniformAccountInfo {
    pub fn from_replica(v: &ReplicaAccountInfoV3, slot: Slot) -> Self {
        Self {
            slot,
            pubkey: Pubkey::try_from(v.pubkey).unwrap(),
            owner: Pubkey::try_from(v.owner).unwrap(),
            lamports: v.lamports,
            data: v.data.to_vec(),
            transaction_ref: v.txn.map(|txn| *txn.signature()),
            executable: v.executable,
            rent_epoch: v.rent_epoch,
            write_version: v.write_version
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UniformTransactionInfo {
    pub slot: Slot,
    pub signature: Signature,
    pub message_hash: solana_hash::Hash,
    pub is_vote: bool,
    pub transaction: VersionedTransaction,
    pub transaction_status_meta: TransactionStatusMeta,
    pub index: usize
}

impl UniformTransactionInfo {
    pub fn from_replica(v: &ReplicaTransactionInfoV3<'_>, slot: Slot) -> Self {
        Self {
            slot,
            signature: *v.signature,
            message_hash: solana_hash::Hash::new_from_array(v.message_hash.to_bytes()),
            is_vote: v.is_vote,
            transaction: v.transaction.clone(),
            transaction_status_meta: v.transaction_status_meta.clone(),
            index: v.index,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UniformEntryInfo {
    /// The slot number of the block containing this Entry
    pub slot: u64,
    /// The Entry's index in the block
    pub index: usize,
    /// The number of hashes since the previous Entry
    pub num_hashes: u64,
    /// The Entry's SHA-256 hash, generated from the previous Entry's hash with
    /// `solana_entry::entry::next_hash()`
    pub hash: solana_hash::Hash,
    /// The number of executed transactions in the Entry
    pub executed_transaction_count: u64,
    /// The index-in-block of the first executed transaction in this Entry
    pub starting_transaction_index: usize,
}

impl UniformEntryInfo {
    pub fn from_replica(v: &ReplicaEntryInfoV2<'_>) -> Self {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(v.hash);
        Self {
            slot: v.slot,
            index: v.index,
            num_hashes: v.num_hashes,
            hash: solana_hash::Hash::new_from_array(hash),
            executed_transaction_count: v.executed_transaction_count,
            starting_transaction_index: v.starting_transaction_index,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UniformBlockInfo {
    pub parent_slot: u64,
    pub parent_blockhash: String,
    pub slot: u64,
    pub blockhash: String,
    pub rewards: RewardsAndNumPartitions,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
    pub entry_count: u64
}

impl UniformBlockInfo {
    pub fn from_replica(v: &ReplicaBlockInfoV4<'_>) -> Self {
        Self {
            parent_slot: v.parent_slot,
            parent_blockhash: v.parent_blockhash.to_string(),
            slot: v.slot,
            blockhash: v.blockhash.to_string(),
            rewards: v.rewards.clone(),
            block_time: v.block_time,
            block_height: v.block_height,
            executed_transaction_count: v.executed_transaction_count,
            entry_count: v.entry_count
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UniformSlotInfo {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub status: SlotStatus
}

impl UniformSlotInfo {
    pub fn from_replica(slot: Slot, parent: Option<Slot>, status: SlotStatus) -> Self {
        Self {
            slot,
            parent,
            status
        }
    }
}

impl TransportOpts {
    // Possible structures:
    // cert_path:key_path@fqdn
    // cert_path@fqdn
    pub fn parse_from_str(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('@').collect();
        if parts.len() != 2 {
            return None;
        }

        let cert_and_key = parts[0];
        let fqdn = parts[1].to_string();

        let cert_key_parts: Vec<&str> = cert_and_key.split(':').collect();
        let cert_path = PathBuf::from(cert_key_parts[0]);
        let key_path = if cert_key_parts.len() == 2 {
            Some(PathBuf::from(cert_key_parts[1]))
        } else {
            None
        };

        Some(TransportOpts {
            cert_path,
            key_path,
            fqdn,
        })
    }
}
