
use std::sync::atomic::{AtomicU64};
use std::time::Duration;
use solana_pubkey::Pubkey;
use transport::UniformAccountInfo;

pub struct AccountCoalescer {
    inner: parking_lot::Mutex<AccountCoalescerInner>,
    coalesce_duration: Duration,
    coalesced_count: AtomicU64
}

struct AccountCoalescerInner {
    buffer: std::collections::HashMap<Pubkey, UniformAccountInfo>,
    last_coalesce_time: std::time::Instant
}

impl AccountCoalescer {
    pub fn new(coalesce_duration: Duration) -> Self {
        Self {
            inner: parking_lot::Mutex::new(AccountCoalescerInner {
                buffer: std::collections::HashMap::new(),
                last_coalesce_time: std::time::Instant::now(),
            }),
            coalesce_duration,
            coalesced_count: Default::default(),
        }
    }
    pub fn coalesce(&self, replica: UniformAccountInfo) -> Option<Vec<UniformAccountInfo>> {
        let now = std::time::Instant::now();
        let mut inner = self.inner.lock();

        if inner.buffer.insert(replica.pubkey, replica).is_some() {
            if self.coalesced_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 1000 == 0 {
                tracing::debug!("coalesced {} account updates", self.coalesced_count.load(std::sync::atomic::Ordering::Relaxed));
            }
        }

        if now.duration_since(inner.last_coalesce_time) >= self.coalesce_duration {
            let coalesced_accounts: Vec<UniformAccountInfo> = inner.buffer.drain().map(|(_k, v)| v).collect();
            inner.last_coalesce_time = now;
            Some(coalesced_accounts)
        } else {
            None
        }
    }
}
