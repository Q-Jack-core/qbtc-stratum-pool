mod state;
mod upstream;
mod stratum;

use state::PoolState;
use std::sync::Arc;

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

    tokio::signal::ctrl_c().await.unwrap();
    tracing::info!("Shutting down Q-BTC Stratum Pool.");
}