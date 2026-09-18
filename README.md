<p align="center">
  <img src="<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="512" height="512">
  <!-- Monolith Dark Base Plate -->
  <rect width="512" height="512" rx="112" fill="#09090B" />
  <rect width="510" height="510" x="1" y="1" rx="111" fill="none" stroke="#27272A" stroke-width="2" />

  <!-- THE DORSAL-M (Unmistakable M + Twin Mako Fins + Sonic Cone) -->
  <g transform="translate(0, 0)">
    <!-- Primary Titanium White 'M' Body -->
    <path d="
      M 115,380 
      L 155,175 
      C 165,135 185,120 205,125 
      C 200,165 215,225 256,285 
      C 297,225 312,165 307,125 
      C 327,120 347,135 357,175 
      L 397,380 
      L 335,380 
      L 305,245 
      L 256,310 
      L 207,245 
      L 177,380 
      Z
    " fill="#FFFFFF" />

    <!-- Center Supersonic Shockwave Core (Electric Cyan) -->
    <polygon points="256,155 272,215 256,250 240,215" fill="#00F5D4" />

    <!-- Trailing Edge Hydrodynamic Shading (Subtle Graphite Contrast) -->
    <polygon points="256,285 207,245 177,380 200,380 218,295" fill="#71717A" opacity="0.3" />
    <polygon points="256,285 305,245 335,380 312,380 294,295" fill="#71717A" opacity="0.3" />
  </g>
</svg>
" alt="MAKO Logo" width="60%" />
</p> MAKO ⚡ — Decentralized Serverless Edge Runtime

> A masterless, zero-cloud-bill AWS Lambda alternative powered by Rust, WebAssembly, and LibP2P.

---

## 🚀 Key Features

| Feature | Description |
|---------|-------------|
| **Sub-millisecond cold start & execution** | Wasmtime sandbox with fuel metering — no V8/SpiderMonkey overhead |
| **Pure P2P decentralized mesh** | LibP2P + Kademlia DHT — no master node, no central coordinator |
| **Automatic quorum failover** | Sub-millisecond failover across worker nodes on crash/timeout |
| **Fuel-metered sandboxing** | 16MB memory ceiling, 500ms execution timeout per invocation |
| **Bounded concurrency** | `Arc<Semaphore>` 16-permit isolation — heavy compute never blocks P2P heartbeats |
| **Round-robin load balancing** | Gateway distributes requests evenly across all discovered workers |
| **Content-addressed modules** | SHA-256 hash = Function ID — immutable, cacheable, verifiable |
| **Bearer token auth** | Optional `MAKO_API_KEY` for `/deploy` and `/invoke` protection |

---

## 📊 Verified Benchmarks

| Metric | Single Worker | Dual-Worker Cluster | AWS Lambda Equivalent |
| :--- | :---: | :---: | :---: |
| **Throughput (RPS)** | ~45 RPS | **70.3 RPS (1.56×)** | Cold-start dependent |
| **Avg Latency** | 24 ms | **13.1 ms** | ~200–800 ms |
| **P99 Latency** | 525 ms | **91.5 ms** | ~1,200 ms |
| **Failover Time** | N/A | **<2 ms** | 30s+ (DNS lag) |
| **Infrastructure Cost** | **$0** | **$0** | Per-millisecond bill |

> Tested on Windows 11, Intel i7, 200 concurrent requests, `json_transform.wasm` (116 KB), fuel=50,000.

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              MAKO CLUSTER                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────┐      LibP2P Mesh (Kademlia DHT + mDNS)      ┌────────┐   │
│   │   CLIENTS    │ ◄──────────────────────────────────────────► │ WORKER │   │
│   │  (HTTP/JSON) │                                              │  NODE  │   │
│   └──────┬───────┘                                              │ (4001) │   │
│          │                                                      └────┬───┘   │
│          │                                                           │       │
│    ┌─────▼──────┐                                          ┌──────────▼────┐   │
│    │  GATEWAY   │ ◄── Round-Robin + Failover ─────────────► │ WORKER NODE   │   │
│    │  (Axum)    │ ◄── Broadcast Deploy ────────────────────► │ (4003)        │   │
│    │ :8080      │                                            └───────────────┘   │
│    └────────────┘                                                                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

Worker Node Internals:
┌────────────────────────────────────────────────────────────┐
│  libp2p Swarm Event Loop (never blocked)                   │
│       │                                                    │
│       ▼                                                    │
│  ┌────────────────────────────────────────────────────┐   │
│  │ tokio::select! {                                   │   │
│  │   swarm.select_next_some() => handle P2P events    │   │
│  │   resp_rx.recv() => send deferred responses        │   │
│  │ }                                                   │   │
│  └────────────────────────────────────────────────────┘   │
│       │                                                    │
│       ▼ (spawn)                                           │
│  ┌────────────────────────────────────────────────────┐   │
│  │ Semaphore (16 permits) → acquire → spawn_blocking  │   │
│  │   Wasmtime.execute_module() with 500ms timeout     │   │
│  │   → release permit → send response via mpsc        │   │
│  └────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────┘
```

---

## ⚡ Quickstart (10 seconds)

```bash
# 1. Start Worker Daemon (P2P port 4001)
cargo run --release -- daemon --port 4001

# 2. Start HTTP Ingress Gateway (HTTP 8080, P2P 4002, bootstrap to daemon)
cargo run --release -- gateway --port 8080 --p2p-port 4002 --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/<DAEMON_PEER_ID>

# 3. Deploy a Wasm module (returns Function ID)
cargo run --release -- deploy my_logic.wasm
# Function ID: a1b2c3d4e5f6

# 4. Invoke via HTTP (Bearer token optional, default: mako-secret-dev-key)
curl -X POST http://localhost:8080/invoke/a1b2c3d4e5f6?func=handle \
  -H "Authorization: Bearer mako-secret-dev-key" \
  -H "Content-Type: application/json" \
  -d '{"data": 1}'
```

### Multi-Worker Cluster (Horizontal Scaling)

```bash
# Terminal 1: Seed Worker
cargo run --release -- daemon --port 4001

# Terminal 2: Additional Worker (bootstraps to seed)
cargo run --release -- daemon --port 4003 --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/<SEED_PEER_ID>

# Terminal 3: Gateway (discovers both workers automatically)
cargo run --release -- gateway --port 8080 --p2p-port 4002 --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/<SEED_PEER_ID>
```

---

## 🔐 Security Guardrails (Production Ready)

| Guardrail | Limit | Behavior |
|-----------|-------|----------|
| **Payload Size** | 2 MB | HTTP 413 if exceeded |
| **Fuel Ceiling** | 100,000 units | Auto-clamped from query param |
| **Execution Timeout** | 500 ms | Worker aborts, returns error, releases semaphore |
| **Bearer Auth** | `MAKO_API_KEY` env | 401 Unauthorized on `/deploy` & `/invoke` |

---

## 📦 Writing Wasm Modules (Rust)

```rust
// Cargo.toml
[package]
name = "my_logic"
crate-type = ["cdylib"]

[dependencies]
serde = { version = "1.0", features = ["derive", "alloc"] }
serde_json = { version = "1.0", features = ["alloc"] }

// lib.rs
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Input { value: i32 }

#[derive(Serialize)]
struct Output { result: i32, processed_by: &'static str }

#[no_mangle]
pub extern "C" fn alloc(size: usize) -> *mut u8 { ... }

#[no_mangle]
pub extern "C" fn dealloc(ptr: *mut u8, size: usize) { ... }

#[no_mangle]
pub extern "C" fn handle(ptr: *mut u8, len: usize) -> u64 {
    // 1. Read input from linear memory
    // 2. Deserialize JSON
    // 3. Compute
    // 4. Serialize result
    // 5. Alloc + write to guest memory
    // 6. Return packed (ptr << 32) | len
}
```

Compile:
```bash
cargo build --target wasm32-unknown-unknown --release
# → target/wasm32-unknown-unknown/release/my_logic.wasm
```

---

## 🛠 CLI Reference

| Command | Description |
|---------|-------------|
| `mako daemon --port 4001` | Start P2P worker node |
| `mako gateway --port 8080 --p2p-port 4002 --bootstrap <ADDR>` | Start HTTP gateway |
| `mako deploy file.wasm [--gateway-url URL] [--peer <ADDR>]` | Deploy module to cluster |
| `mako dispatch <PEER_ADDR> file.wasm --func handle --args 1 2` | Direct P2P invocation |
| `mako run file.wasm --func handle --args 1 2` | Local execution (no P2P) |

---

## 🌐 HTTP API

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/deploy` | POST | Bearer | Deploy Wasm module (binary or multipart) |
| `/invoke/:hash` | POST | Bearer | Invoke deployed function |
| `/execute` | POST | Bearer | One-shot deploy + invoke |
| `/peers` | GET | — | List connected worker peers |

**Invoke Query Params:**
- `func` — exported function name (default: `multiply`)
- `fuel` — fuel units (default: 50,000, max: 100,000)
- `max_memory_mb` — memory limit (default: 16)

---

## 📂 Project Structure

```
mako/
├── src/
│   └── main.rs          # Single-file: daemon, gateway, CLI, Wasmtime runner
├── fixtures/
│   ├── multiply.wat     # Simple multiply demo (WAT)
│   └── json_transform/  # JSON transform demo (Rust → Wasm)
│       ├── Cargo.toml
│       └── src/lib.rs
├── test_json_payload.ps1      # E2E JSON payload test
├── test_failover.ps1          # Zero-downtime failover test
├── test_cluster_scaling.ps1   # Horizontal scaling benchmark
├── test_guardrails.ps1        # Security guardrail validation
├── stress_test.ps1            # 200-request concurrency test
├── Cargo.toml
└── README.md
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## ⚖️ System Architecture Constraints & Trade-offs

- **Stateless Execution**: MAKO worker nodes are strictly ephemeral. Persistent data must be pushed to external stores (e.g., S3/Blob storage).
- **Strict Bounded Ceiling**: Guest modules are clamped to 16MB linear memory and 100,000 fuel units to guarantee deterministic QoS and prevent worker saturation.
- **Discovery Latency**: Intra-node clustering relies on local mDNS for zero-config subnets; WAN traversal requires configured bootstrap relay nodes over LibP2P.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- [Wasmtime](https://wasmtime.dev/) — Fast, secure WebAssembly runtime
- [LibP2P](https://libp2p.io/) — Modular P2P networking stack
- [Axum](https://github.com/tokio-rs/axum) — Ergonomic HTTP server
- [Tokio](https://tokio.rs/) — Async runtime for Rust

---

**MAKO** — *Sovereign compute at the edge. No cloud bills. No masters. Just code.*
