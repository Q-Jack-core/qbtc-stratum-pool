use std::time::Duration;
use reqwest::Client;
use std::sync::Arc;
use quantum_btc::block::Block;
use crate::state::{PoolState, RawBlockTemplate};

pub async fn poll_node_for_jobs(state: Arc<PoolState>, node_url: String, miner_address: String) {
    let client = Client::builder().timeout(Duration::from_secs(3)).build().unwrap();
    
    loop {
        let req_body = serde_json::json!({
            "miner_address": miner_address
        });

        match client.post(&node_url).json(&req_body).send().await {
            Ok(resp) => {
                if let Ok(Some(template)) = resp.json::<Option<RawBlockTemplate>>().await {
                    let mut is_new_job = false;
                    {
                        let current_h = *state.current_height.read().await;
                        let current_p = state.current_prev_hash.read().await;
                        if current_h != template.current_height || *current_p != template.previous_hash {
                            is_new_job = true;
                        }
                    }

                    if is_new_job {
                        let mut prev_bytes = [0u8; 32];
                        if template.previous_hash.len() == 64 {
                            for i in 0..32 {
                                prev_bytes[i] = u8::from_str_radix(&template.previous_hash[i*2..i*2+2], 16).unwrap_or(0);
                            }
                        }

                        let assembled_block = Block::new(
                            prev_bytes,
                            template.transactions,
                            template.target,
                            0,
                        );

                        *state.current_block.write().await = Some(assembled_block);
                        *state.current_height.write().await = template.current_height;
                        *state.current_target.write().await = template.target;
                        *state.current_prev_hash.write().await = template.previous_hash.clone();

                        tracing::info!("New L1 block template cached. Height: {}", template.current_height);
                    }
                }
            }
            Err(e) => tracing::warn!("L1 poll failed: {}", e),
        }
        
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
}