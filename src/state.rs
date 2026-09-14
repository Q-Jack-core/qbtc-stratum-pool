use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64};
use tokio::sync::RwLock;
use quantum_btc::block::Block;
use quantum_btc::transaction::Transaction;
use serde::{Deserialize, Serialize};

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
        })
    }
}