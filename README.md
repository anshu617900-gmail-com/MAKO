<p align="center">
  <img src="assets/banner.svg" alt="MAKO Architecture" width="100%" />
</p><div align="center">

  <img src="https://private-user-images.githubusercontent.com/256764231/654859123-32272332-2a35-4120-a00d-5bc295332c64.svg?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3ODk3NTg3NDIsIm5iZiI6MTc4OTc1ODQ0MiwicGF0aCI6Ii8yNTY3NjQyMzEvNjU0ODU5MTIzLTMyMjcyMzMyLTJhMzUtNDEyMC1hMDBkLTViYzI5NTMzMmM2NC5zdmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjYwOTE4JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI2MDkxOFQxOTA3MjJaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT01NDA1OWJkMjUxZGQ0YjkwMmZiZjE4MWZmMTRlMGVhOGQ4MDY5ODBmNjBjOTBkZmEzOTFmZmI3Y2U0NThiZmFlJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCZyZXNwb25zZS1jb250ZW50LXR5cGU9aW1hZ2UlMkZzdmclMkJ4bWwifQ.yNOQt0BfLgTpmYfbm07ApTB74OQiq3U2pc3-QSDu0Qc" alt="MAKO" width="220" />

  <br />
  <br />

  <p align="center">
    <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-zinc.svg?style=flat" alt="License: MIT"></a>
    <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/Language-Rust%202021-zinc.svg?style=flat" alt="Rust 2021"></a>
    <a href="https://github.com/bytecodealliance/wasmtime"><img src="https://img.shields.io/badge/Core-Wasmtime%20Engine-zinc.svg?style=flat" alt="Wasmtime"></a>
    <a href="https://libp2p.io/"><img src="https://img.shields.io/badge/Mesh-LibP2P%20DHT-zinc.svg?style=flat" alt="LibP2P"></a>
    <a href="https://github.com/anshu617900-gmail-com/MAKO/releases"><img src="https://img.shields.io/badge/Release-v0.1.0--alpha-zinc.svg?style=flat" alt="v0.1.0-alpha"></a>
  </p>

  <p align="center">
    <b>A masterless, decentralized WebAssembly edge runtime. Deterministic sandboxing, sub-millisecond cold starts, and zero-coordination failover over a peer-to-peer Kademlia DHT.</b>
  </p>

  <p align="center">
    <a href="#overview">Overview</a> •
    <a href="#architectural-primitives">Primitives</a> •
    <a href="#empirical-benchmarks">Benchmarks</a> •
    <a href="#topology--system-design">Topology</a> •
    <a href="#quickstart">Quickstart</a> •
    <a href="#invariants--constraints">Invariants</a>
  </p>

---

</div>

## Overview

MAKO is a distributed serverless runtime designed from first principles to eliminate centralized orchestration overhead. Traditional serverless offerings (e.g., AWS Lambda, Google Cloud Functions) rely on heavy container virtualizations (microVMs), complex centralized control planes (Kubernetes/Borg), and opaque per-millisecond billing models.

MAKO replaces centralized orchestrators with an autonomous, masterless peer-to-peer mesh. By pairing the **Bytecode Alliance Wasmtime** runtime with **LibP2P Kademlia DHT**, compute nodes act as sovereign, self-routing peers capable of accepting HTTP ingress, executing sandboxed guest bytecode, and routing requests without a single point of failure.

### Core Primitives

| Primitive | Implementation | Operational Guarantee |
| :--- | :--- | :--- |
| **Execution Sandboxing** | Wasmtime Engine (JIT) | Bounded linear memory, zero V8/SpiderMonkey overhead. |
| **Topology** | LibP2P Kademlia DHT + mDNS | Decentralized peer discovery; no master nodes or coordinators. |
| **Fault Tolerance** | Autonomous Quorum Routing | <2ms dynamic failover rerouting upon peer crash or timeout. |
| **Resource Metering** | Explicit Fuel Counter | Hard-bounded CPU allocation (100,000 fuel ceiling) per invocation. |
| **Concurrency Shield** | `Arc<Semaphore>` Isolation | Bounded execution permits (16) protect P2P network heartbeats from compute saturation. |
| **Module Addressing** | SHA-256 Content-Addressing | Immutable, deterministic, and cryptographic guest module verification. |

---

## Empirical Benchmarks

Tested on bare-metal hardware (Windows 11, Intel Core i7, 200 concurrent HTTP requests executing a 116 KB `json_transform.wasm` module at a 50,000 fuel baseline).

| Evaluation Metric | Single Node Worker | Dual-Worker Cluster | Centralized Cloud Baseline (AWS Lambda) |
| :--- | :--- | :--- | :--- |
| **Throughput (RPS)** | ~45.0 RPS | **70.3 RPS** (1.56× scale) | Cold-start dependent |
| **Average Latency** | 24.0 ms | **13.1 ms** | ~200 – 800 ms (VPC cold start) |
| **Tail Latency (P99)** | 525.0 ms | **91.5 ms** | ~1,200 ms |
| **Failover Convergence** | N/A | **< 2.0 ms** | 30s+ (DNS/ALB health check lag) |
| **Compute Overhead** | $0.00 (Self-hosted) | **$0.00** (Decentralized Mesh) | Continuous per-millisecond pricing |

---## Topology & System Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 MAKO MESH                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────┐      LibP2P Mesh (Kademlia DHT + mDNS)      ┌────────┐   │
│   │   CLIENTS    │ ◄──────────────────────────────────────────► │ WORKER │   │
│   │  (HTTP/JSON) │                                              │  NODE  │   │
│   └──────┬───────┘                                              │ (4001) │   │
│          │                                                      └────┬───┘   │
│          │                                                           │       │
│    ┌─────▼──────┐                                          ┌─────────▼─────┐ │
│    │  GATEWAY   │ ◄── Round-Robin + Quorum Failover ──────► │ WORKER NODE   │ │
│    │  (Axum)    │ ◄── Broadcast Deployment ────────────────► │ (4003)        │ │
│    │  :8080     │                                          └───────────────┘ │
│    └────────────┘                                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Worker Runtime Loop

To prevent compute workloads from blocking the P2P networking thread, MAKO decouples network polling from compute threads via an asynchronous actor loop:

```
libp2p Swarm Event Loop (Non-blocking Tokio loop)
     │
     ├──> [Event] Peer Discovery / DHT Route Update
     └──> [Event] Incoming Ingress Execution Request
               │
               ▼
     Acquire Bounded Permit (Arc<Semaphore>, limit: 16)
               │
               ▼
     tokio::task::spawn_blocking
          ├── Instantiate Sandboxed Wasmtime Store
          ├── Fuel Injection (clamped: 100,000 units)
          ├── Execute Module with 500ms Wall-Clock Timeout
          └── Yield Result -> Release Permit -> Return P2P Response
```

---

## Quickstart

### Prerequisites
* Rust 2021 Edition (`cargo >= 1.75`) or pre-compiled `mako.exe` binary.

### 1. Initialize Cluster Daemon
Spin up the initial seed peer on network port `4001`:

```bash
cargo run --release -- daemon --port 4001
```

### 2. Launch HTTP Ingress Gateway
Boot an Axum HTTP gateway on port `8080`, binding to the seed worker:

```bash
cargo run --release -- gateway \
  --port 8080 \
  --p2p-port 4002 \
  --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/<DAEMON_PEER_ID>
```

### 3. Deploy Guest Module
Deploy a compiled WebAssembly bytecode module to the cluster:

```bash
cargo run --release -- deploy my_logic.wasm
# Returns SHA-256 Function ID (e.g. 8f3c9e...)
```

### 4. Invoke Over HTTP
Invoke the function via standard HTTP POST:

```bash
curl -X POST "http://localhost:8080/invoke/<FUNCTION_ID>?func=handle" \
  -H "Authorization: Bearer mako-secret-dev-key" \
  -H "Content-Type: application/json" \
  -d '{"value": 42}'
```

---

## Multi-Worker Horizontal Scaling

```bash
# Terminal 1: Seed Worker
cargo run --release -- daemon --port 4001

# Terminal 2: Secondary Worker (discovers seed via multiaddr)
cargo run --release -- daemon --port 4003 --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/<SEED_PEER_ID>

# Terminal 3: Gateway (automatically balances load between 4001 & 4003)
cargo run --release -- gateway --port 8080 --p2p-port 4002 --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/<SEED_PEER_ID>
```

---

## Writing Guest Modules (Rust)

MAKO guest modules compile directly to the standard `wasm32-unknown-unknown` target:

```rust
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Input {
    value: i32,
}

#[derive(Serialize)]
struct Output {
    result: i32,
    processed_by: &'static str,
}

#[no_mangle]
pub extern "C" fn alloc(size: usize) -> *mut u8 {
    let mut buf = Vec::with_capacity(size);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

#[no_mangle]
pub extern "C" fn handle(ptr: *mut u8, len: usize) -> u64 {
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    let input: Input = serde_json::from_slice(slice).unwrap();

    let output = Output {
        result: input.value * 2,
        processed_by: "mako-worker",
    };

    let out_bytes = serde_json::to_vec(&output).unwrap();
    let out_ptr = out_bytes.as_ptr() as u64;
    let out_len = out_bytes.len() as u64;
    std::mem::forget(out_bytes);

    (out_ptr << 32) | out_len
}
```

Compile command:
```bash
cargo build --target wasm32-unknown-unknown --release
```

---

## CLI & HTTP Interface

### CLI Reference

| Command | Arguments | Description |
| :--- | :--- | :--- |
| `mako daemon` | `--port <PORT> [--bootstrap <ADDR>]` | Starts P2P execution worker. |
| `mako gateway` | `--port <PORT> --p2p-port <PORT> --bootstrap <ADDR>` | Starts HTTP gateway interface. |
| `mako deploy` | `<FILE.WASM> [--gateway-url <URL>]` | Deploys module to cluster. |
| `mako dispatch` | `<PEER_ADDR> <FILE.WASM> --func <NAME>` | Direct P2P module invocation. |
| `mako run` | `<FILE.WASM> --func <NAME> --args <ARGS>` | Local execution without P2P mesh. |

### HTTP Gateway Endpoints

| Endpoint | Method | Authorization | Description |
| :--- | :--- | :--- | :--- |
| `/deploy` | `POST` | Bearer Token | Deploys raw or multipart `.wasm` module. |
| `/invoke/:hash` | `POST` | Bearer Token | Dispatches execution request across worker cluster. |
| `/execute` | `POST` | Bearer Token | One-shot atomic deploy and execution. |
| `/peers` | `GET` | None | Returns active peer topology and mesh health. |

---

## Architectural Invariants & Constraints

- **Stateless Guest Boundary**: MAKO workers are strictly ephemeral. Persistent data must be offloaded to external distributed stores (e.g. S3/MinIO or decentralized block storage).
- **Strict Bounded Ceiling**: Guest modules are clamped to 16MB linear memory and 100,000 fuel units to guarantee deterministic QoS and prevent noisy-neighbor saturation.
- **P2P Convergence**: Intra-subnet clustering utilizes local mDNS for zero-config discovery. WAN topologies require configured LibP2P bootstrap relays.

---

## Security Disclosure

If you discover a vulnerability or sandbox escape vector within MAKO, please report it via confidential security disclosure. Refer to `SECURITY.md` for guidelines.

---

## License

MAKO is licensed under the [MIT License](LICENSE).

## Topology & System Design
