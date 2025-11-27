use std::num::{NonZeroUsize};
use std::time::Duration;
use lru::LruCache;
use solana_signature::Signature;
use transport::{UniformTransactionInfo};

pub struct TransactionCache {
    lru: LruCache<Signature, UniformTransactionInfo>,
    signature_added_tx: tokio::sync::broadcast::Sender<Signature>,
}

impl TransactionCache {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(1024);
        Self {
            lru: LruCache::new(NonZeroUsize::try_from(capacity).unwrap()),
            signature_added_tx: sender,
        }
    }

    /// Returns a cloned Arc to the UniformAccountInfo if it exists in the cache.
    pub fn take(&mut self, pubkey: &Signature) -> Option<UniformTransactionInfo> {
        self.lru.pop(pubkey)
    }

    /// Asynchronously waits for the transaction info to be available in the cache, up to the specified timeout.
    /// TODO: eventually use this instead of take() in the replicator, since some transactions may be delayed - they are on a separate QUIC channel.
    pub async fn _get_await(&mut self, signature: &Signature, timeout: Duration) -> Option<UniformTransactionInfo> {
        if self.lru.contains(&signature) {
            self.take(signature)
        } else {
            let mut recv = self.signature_added_tx.subscribe();
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                let recv_fut = recv.recv();
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => {
                        return None;
                    }
                    result = recv_fut => {
                        match result {
                            Ok(added_signature) => {
                                if &added_signature == signature {
                                    return self.take(signature);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                // Ignore lagged errors
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                return None;
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn insert(&mut self, transaction_info: UniformTransactionInfo) {
        let _ = self.signature_added_tx.send(transaction_info.signature);
        self.lru.put(transaction_info.signature, transaction_info);
    }
}