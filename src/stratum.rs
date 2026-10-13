use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LinesCodec};
use futures::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use crate::state::PoolState;
use quantum_btc::wallet::QuantumWallet;
use redis::Client;

/// A Stratum line is a short JSON object. LinesCodec::new() applies no limit,
/// so a client that sends bytes without a newline would make the codec buffer
/// until the process runs out of memory.
const MAX_LINE_BYTES: usize = 8 * 1024;

/// Vardiff retarget window, and the accepted-share count band that is left
/// alone. Outside the band the per-session target multiplier halves or
/// doubles.
const VARDIFF_WINDOW: Duration = Duration::from_secs(10);
const VARDIFF_MAX_SHARES: u32 = 3;
const VARDIFF_MIN_SHARES: u32 = 1;

/// How long a job's seen-nonce set is kept. Only needs to outlive the job.
const SEEN_NONCE_TTL_SECS: u64 = 3600;

#[derive(Deserialize, Debug)]
pub struct StratumRequest {
    pub id: Option<u64>,
    pub method: String,
    pub params: Vec<serde_json::Value>,
}

#[derive(Serialize, Debug)]
pub struct StratumResponse {
    pub id: Option<u64>,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

#[derive(Serialize, Debug)]
pub struct StratumNotify {
    pub id: Option<u64>,
    pub method: String,
    pub params: Vec<serde_json::Value>,
}

/// Holds the active-miner gauge down for the life of the connection task.
/// The count is published at /api/stats, so a task that unwinds past a bare
/// fetch_sub would inflate it for as long as the process runs.
struct ActiveMinerGuard(Arc<AtomicUsize>);

impl ActiveMinerGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl Drop for ActiveMinerGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub async fn start_stratum_server(state: Arc<PoolState>, port: u16, submit_url: String, redis_client: Client) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await.unwrap();
    tracing::info!("Stratum TCP Server active on {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                let state_clone = Arc::clone(&state);
                let submit_url_clone = submit_url.clone();
                let redis_clone = redis_client.clone();
                
                tokio::spawn(async move {
                    handle_miner_connection(socket, peer_addr, state_clone, submit_url_clone, redis_clone).await;
                });
            }
            Err(e) => tracing::error!("TCP accept fault: {}", e),
        }
    }
}

fn hash_to_u64(hash: &[u8; 32]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash[0..8]);
    u64::from_be_bytes(bytes)
}

async fn handle_miner_connection(socket: TcpStream, peer_addr: std::net::SocketAddr, state: Arc<PoolState>, submit_url: String, redis_client: Client) {
    let mut framed = Framed::new(socket, LinesCodec::new_with_max_length(MAX_LINE_BYTES));
    let mut authorized_miner: Option<String> = None;
    let mut active_guard: Option<ActiveMinerGuard> = None;
    
    let mut redis_conn = match redis_client.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!("Redis connection failed: {}", e);
            return;
        }
    };
    
    let mut pool_target_multiplier: u64 = 1000;
    let mut share_count: u32 = 0;
    let mut window_start = Instant::now();
    let session_id = format!("{:x}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());

    let mut last_notified_height = 0u64;
    // The share target this session was last told to work against. Submits are
    // judged against this rather than the live multiplier, so a retarget in
    // flight cannot reject a share that was valid for the announced job.
    let mut announced_pool_target = 0u64;
    let mut ticker = tokio::time::interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if authorized_miner.is_some() {
                    // Vardiff runs on the timer, not inside mining.submit. A
                    // session that is too slow for its current target submits
                    // nothing, so a submit-driven check never observes the
                    // zero-share case and the target can only ratchet harder.
                    if window_start.elapsed() >= VARDIFF_WINDOW {
                        let previous = pool_target_multiplier;
                        if share_count > VARDIFF_MAX_SHARES {
                            pool_target_multiplier = (pool_target_multiplier / 2).max(1);
                        } else if share_count < VARDIFF_MIN_SHARES {
                            pool_target_multiplier = pool_target_multiplier.saturating_mul(2);
                        }
                        share_count = 0;
                        window_start = Instant::now();
                        if pool_target_multiplier != previous {
                            tracing::info!(
                                "Vardiff retarget for {}: multiplier {} -> {}",
                                authorized_miner.as_deref().unwrap_or("?"),
                                previous,
                                pool_target_multiplier
                            );
                            // Force a re-notify so the miner learns the new target.
                            last_notified_height = 0;
                        }
                    }

                    let current_h = *state.current_height.read().await;
                    if current_h > last_notified_height {
                        let mainnet_t = *state.current_target.read().await;
                        
                        let (prev_hex, merkle_hex, commit_hex, ts) = {
                            let block_lock = state.current_block.read().await;
                            if let Some(ref block) = *block_lock {
                                (
                                    hex::encode(block.header.previous_hash),
                                    hex::encode(block.header.merkle_root),
                                    hex::encode(block.header.commit_merkle_root),
                                    block.header.timestamp
                                )
                            } else {
                                (String::new(), String::new(), String::new(), 0u64)
                            }
                        };
                        
                        if !prev_hex.is_empty() {
                            let pool_target = mainnet_t.saturating_mul(pool_target_multiplier);
                            let notify = StratumNotify {
                                id: None,
                                method: "mining.notify".to_string(),
                                params: vec![
                                    serde_json::json!(current_h),
                                    serde_json::json!(prev_hex),
                                    serde_json::json!(merkle_hex),
                                    serde_json::json!(commit_hex),
                                    serde_json::json!(ts),
                                    serde_json::json!(mainnet_t),
                                    serde_json::json!(pool_target),
                                ],
                            };
                            if framed.send(serde_json::to_string(&notify).unwrap()).await.is_err() {
                                break;
                            }
                            // Only commit to the announced job once it is on the wire.
                            last_notified_height = current_h;
                            announced_pool_target = pool_target;
                        }
                    }
                }
            }
            
            result = framed.next() => {
                let line = match result {
                    Some(Ok(line)) => line,
                    Some(Err(e)) => {
                        tracing::warn!("Protocol fault from {}: {}", peer_addr, e);
                        break;
                    }
                    None => break,
                };
                
                if let Ok(req) = serde_json::from_str::<StratumRequest>(&line) {
                    if req.method == "mining.subscribe" {
                        let res = StratumResponse { id: req.id, result: Some(serde_json::json!(["qbtc-stratum-v1", session_id])), error: None };
                        let _ = framed.send(serde_json::to_string(&res).unwrap()).await;
                        
                    } else if req.method == "mining.authorize" {
                        let miner_addr = req.params.first().and_then(|v| v.as_str()).unwrap_or("");

                        // An address that does not decode can never be paid, and
                        // it still lands in Redis as a sorted-set member of
                        // whatever length the client chose. Reject it here rather
                        // than storing it.
                        if QuantumWallet::decode_qbtc_address(miner_addr).is_none() {
                            tracing::warn!("Rejected mining.authorize from {}: payout address does not decode", peer_addr);
                            let res = StratumResponse {
                                id: req.id,
                                result: Some(serde_json::json!(false)),
                                error: Some(serde_json::json!("Invalid Q-BTC payout address")),
                            };
                            let _ = framed.send(serde_json::to_string(&res).unwrap()).await;
                            continue;
                        }

                        if active_guard.is_none() {
                            active_guard = Some(ActiveMinerGuard::new(Arc::clone(&state.active_miners)));
                        }
                        authorized_miner = Some(miner_addr.to_string());
                        tracing::info!("Miner active: {} (IP: {})", miner_addr, peer_addr);

                        let res = StratumResponse { id: req.id, result: Some(serde_json::json!(true)), error: None };
                        let _ = framed.send(serde_json::to_string(&res).unwrap()).await;
                        
                        last_notified_height = 0;
                        
                    } else if req.method == "mining.submit" {
                        let current_miner = match &authorized_miner {
                            Some(addr) => addr.clone(),
                            None => continue,
                        };

                        if req.params.len() < 3 { continue; }

                        let submitted_job_id = req.params[1].as_u64().unwrap_or(0);
                        let submitted_nonce = req.params[2].as_u64().unwrap_or(0);

                        let mut candidate_block = {
                            let block_lock = state.current_block.read().await;
                            if let Some(ref block) = *block_lock {
                                let current_h = *state.current_height.read().await;
                                if submitted_job_id != current_h {
                                    continue;
                                }
                                block.clone()
                            } else {
                                continue;
                            }
                        };

                        // A share is only work if its nonce is new for this job.
                        // Without this an accepted share can be replayed to
                        // inflate pool:shares, which is the record any
                        // proportional payout is computed from. If Redis is
                        // unreachable the submission is treated as new, because
                        // the crediting ZINCRBY below will fail too.
                        let seen_key = format!("pool:seen:{}", submitted_job_id);
                        let newly_seen: i64 = match redis::cmd("SADD")
                            .arg(&seen_key)
                            .arg(submitted_nonce)
                            .query_async(&mut redis_conn)
                            .await
                        {
                            Ok(n) => n,
                            Err(e) => {
                                tracing::warn!("Share dedup unavailable: {}", e);
                                1
                            }
                        };
                        if newly_seen == 0 {
                            tracing::debug!("Duplicate share from {} for job {}", current_miner, submitted_job_id);
                            let res = StratumResponse { id: req.id, result: None, error: Some(serde_json::json!("Duplicate share")) };
                            let _ = framed.send(serde_json::to_string(&res).unwrap()).await;
                            continue;
                        }
                        let _: redis::RedisResult<()> = redis::cmd("EXPIRE")
                            .arg(&seen_key)
                            .arg(SEEN_NONCE_TTL_SECS)
                            .query_async(&mut redis_conn)
                            .await;

                        candidate_block.header.nonce = submitted_nonce;
                        
                        // Hand the block back out of the hashing task and submit
                        // that exact block. Re-reading the shared template after
                        // hashing would submit a different block than the one the
                        // nonce solves, if the upstream poller swapped it in the
                        // meantime.
                        let (candidate_block, hash_result) = tokio::task::spawn_blocking(move || {
                            let hash = candidate_block.calculate_hash();
                            (candidate_block, hash)
                        }).await.unwrap();

                        let hash_num = hash_to_u64(&hash_result);
                        let mainnet_target = *state.current_target.read().await;
                        let current_pool_target = if announced_pool_target > 0 {
                            announced_pool_target
                        } else {
                            mainnet_target.saturating_mul(pool_target_multiplier)
                        };

                        if hash_num <= mainnet_target {
                            tracing::warn!("L1 BLOCK FOUND! Broadcasting to mainnet...");
                            share_count += 1;
                            let res = StratumResponse { id: req.id, result: Some(serde_json::json!(true)), error: None };
                            let _ = framed.send(serde_json::to_string(&res).unwrap()).await;

                            let url = submit_url.clone();
                            let submit_block = candidate_block;
                            let current_job_id = submitted_job_id;

                            tokio::spawn(async move {
                                let client = reqwest::Client::new();
                                let payload = serde_json::json!({ "block": submit_block });
                                let _ = client.post(&url).json(&payload).send().await;
                            });

                            let block_key = format!("pool:blocks:{}", current_job_id);
                            let _: redis::RedisResult<()> = redis::cmd("SET").arg(&block_key).arg(&current_miner).query_async(&mut redis_conn).await;

                        } else if hash_num <= current_pool_target {
                            share_count += 1;
                            state.pool_shares_count.fetch_add(1, Ordering::Relaxed);
                            state.record_share_work(current_pool_target).await;
                            tracing::debug!("Valid Pool Share recorded for: {}", current_miner);
                            let redis_key = format!("pool:shares:{}", submitted_job_id);
                            let _: redis::RedisResult<()> = redis::cmd("ZINCRBY").arg(&redis_key).arg(1).arg(&current_miner).query_async(&mut redis_conn).await;

                            let res = StratumResponse { id: req.id, result: Some(serde_json::json!(true)), error: None };
                            let _ = framed.send(serde_json::to_string(&res).unwrap()).await;
                        } else {
                            let res = StratumResponse { id: req.id, result: None, error: Some(serde_json::json!("Low difficulty share")) };
                            let _ = framed.send(serde_json::to_string(&res).unwrap()).await;
                        }
                    }
                }
            }
        }
    }

    // active_guard drops here, and on an unwind, so the gauge stays balanced.
    if let Some(miner_addr) = authorized_miner {
        tracing::info!("Miner disconnected: {}", miner_addr);
    }
}
