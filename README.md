# Q-BTC Stratum Pool Gateway

[![Rust](https://img.shields.io/badge/rust-1.75%2B-blue.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance, async Stratum mining pool gateway engineered in Rust for the Quantum Bitcoin (Q-BTC) network. Natively handles variable difficulty (Vardiff), live share aggregation, and HAProxy load balancing for post-quantum PoW hashing.

---

## ⚡ Features

- **Native Rust Async Runtime**: Powered by Tokio for high-concurrency TCP socket connections.
- **Dynamic Vardiff Engine**: Automatically adjusts share difficulty per worker based on incoming hashrate.
- **HAProxy Integration**: Pre-configured `haproxy.cfg` for horizontal scaling and DDoS resilience.
- **Zero-Fee Infrastructure**: Designed for fair-launch community pools.

---

## 🛠️ Build & Run

### Prerequisites
- Rust 1.75+
- HAProxy (optional, for production front-end load balancing)

### Build from Source
```bash
git clone https://github.com/Q-Jack-core/qbtc-stratum-pool.git
cd qbtc-stratum-pool
cargo build --release
```

### Run
```bash
./target/release/qbtc-stratum-pool
```

---

## 🔗 Official Reference Pool
- **Stratum URL:** `stratum+tcp://144.172.110.193:3334`
- **Live Stats API:** `http://144.172.110.193:3335/api/stats`
