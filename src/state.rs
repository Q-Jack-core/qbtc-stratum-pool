use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use quantum_btc::block::Block;
use quantum_btc::transaction::Transaction;
use serde::{Deserialize, Serialize};

/// Trailing window the published hashrate is measured over.
pub const HASHRATE_WINDOW: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RawBlockTemplate {
    pub previous_hash: String,
    pub current_height: u64,
    pub target: u64,
    pub transactions: Vec<Transaction>,
}

pub struct PoolState {
    pub current_block: RwLock<Option<Block>>,
    pub current_height: RwLock<u64>,
    pub current_target: RwLock<u64>,
    pub current_prev_hash: RwLock<String>,
    pub active_miners: Arc<AtomicUsize>,
    pub pool_shares_count: Arc<AtomicU64>,
    /// Accepted shares in the trailing window, as (arrival, expected hashes).
    /// Expected hashes is what the share proves was done, so summing it is the
    /// only way to state a hashrate that reflects the work actually submitted.
    recent_share_work: RwLock<VecDeque<(Instant, u64)>>,
}

impl PoolState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            current_block: RwLock::new(None),
            current_height: RwLock::new(0),
            current_target: RwLock::new(0),
            current_prev_hash: RwLock::new(String::new()),
            active_miners: Arc::new(AtomicUsize::new(0)),
            pool_shares_count: Arc::new(AtomicU64::new(0)),
            recent_share_work: RwLock::new(VecDeque::new()),
        })
    }

    /// Records an accepted share. `share_target` is the target the share was
    /// judged against, so u64::MAX / share_target is the expected number of
    /// hashes behind it.
    pub async fn record_share_work(&self, share_target: u64) {
        if share_target == 0 {
            return;
        }
        let work = u64::MAX / share_target;
        let now = Instant::now();
        let mut queue = self.recent_share_work.write().await;
        queue.push_back((now, work));
        while let Some(&(stamp, _)) = queue.front() {
            if now.duration_since(stamp) > HASHRATE_WINDOW {
                queue.pop_front();
            } else {
                break;
            }
        }
    }

    /// Hashes per second implied by the shares accepted in the trailing
    /// window. Divided by the full window, so it ramps up over the first
    /// HASHRATE_WINDOW of uptime rather than reading high on one early share.
    pub async fn measured_hashrate(&self) -> u64 {
        let now = Instant::now();
        let queue = self.recent_share_work.read().await;
        let total: u128 = queue
            .iter()
            .filter(|(stamp, _)| now.duration_since(*stamp) <= HASHRATE_WINDOW)
            .map(|(_, work)| u128::from(*work))
            .sum();
        (total / u128::from(HASHRATE_WINDOW.as_secs())) as u64
    }
}
