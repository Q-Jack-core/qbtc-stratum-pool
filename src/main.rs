mod state;
mod upstream;
mod stratum;

use axum::{routing::get, Json, Router};
use serde::Serialize;
use state::PoolState;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Serialize)]
pub struct PoolStatsResponse {
    pub pool_name: String,
    pub fee_percent: f64,
    pub active_miners: usize,
    pub pool_hashrate_est: u64,
    pub current_height: u64,
    pub current_target: u64,
}

async fn get_pool_stats(
    axum::extract::State(state): axum::extract::State<Arc<PoolState>>,
) -> Json<PoolStatsResponse> {
    let miners = state.active_miners.load(Ordering::Relaxed);
    let height = *state.current_height.read().await;
    let target = *state.current_target.read().await;
    
    let estimated_hashrate = (miners as u64) * 50_000; 

    Json(PoolStatsResponse {
        pool_name: "Q-BTC Core Official Pool".to_string(),
        fee_percent: 0.0,
        active_miners: miners,
        pool_hashrate_est: estimated_hashrate,
        current_height: height,
        current_target: target,
    })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();
    tracing::info!("Starting Q-BTC Stratum Pool Reference Gateway...");

    let pool_state = PoolState::new();

    let node_rpc_url = "http://127.0.0.1:12001/api/get_block_template".to_string();
    let node_submit_url = "http://127.0.0.1:12001/api/submit_block".to_string();
    let pool_reward_address = "qbtc13h0s8jwk6nt6l6ucvgfwp2ysz6sn59jpj06xj4llrzfjrpm3tj2qvn6pys".to_string(); 

    let redis_client = redis::Client::open("redis://127.0.0.1/").expect("Redis connection failed.");

    let state_clone_upstream = Arc::clone(&pool_state);
    let reward_clone_upstream = pool_reward_address.clone();
    tokio::spawn(async move {
        upstream::poll_node_for_jobs(state_clone_upstream, node_rpc_url, reward_clone_upstream).await;
    });

    let state_clone_stratum = Arc::clone(&pool_state);
    tokio::spawn(async move {
        stratum::start_stratum_server(state_clone_stratum, 3334, node_submit_url, redis_client).await;
    });

    let state_clone_api = Arc::clone(&pool_state);
    tokio::spawn(async move {
        let app = Router::new()
            .route("/api/stats", get(get_pool_stats))
            .with_state(state_clone_api);
        
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3335").await.unwrap();
        tracing::info!("Pool HTTP API listening on http://0.0.0.0:3335/api/stats");
        axum::serve(listener, app).await.unwrap();
    });

    tokio::signal::ctrl_c().await.unwrap();
    tracing::info!("Shutting down Q-BTC Stratum Pool.");
}