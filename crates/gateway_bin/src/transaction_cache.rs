use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;
use lru::LruCache;
use solana_signature::Signature;
use transport::{UniformTransactionInfo};

pub struct TransactionCache {
    lru: LruCache<Signature, UniformTransactionInfo>,
    added_pubkey_bd: tokio::sync::broadcast::Sender<Signature>,
}

impl TransactionCache {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(1024);
        Self {
            lru: LruCache::new(NonZeroUsize::try_from(capacity).unwrap()),
            added_pubkey_bd: sender,
        }
    }

    /// Returns a cloned Arc to the UniformAccountInfo if it exists in the cache.
    pub fn take(&mut self, pubkey: &Signature) -> Option<UniformTransactionInfo> {
        self.lru.pop(pubkey)
    }

    /// Asynchronously waits for the account info to be available in the cache, up to the specified timeout.
    pub async fn get_await(&mut self, pubkey: &Signature, timeout: Duration) -> Option<UniformTransactionInfo> {
        if self.lru.contains(&pubkey) {
            self.take(pubkey)
        } else {
            let mut recv = self.added_pubkey_bd.subscribe();
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
                            Ok(added_pubkey) => {
                                if &added_pubkey == pubkey {
                                    return self.take(pubkey);
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
        let _ = self.added_pubkey_bd.send(transaction_info.signature);
        self.lru.put(transaction_info.signature, transaction_info);
    }
}