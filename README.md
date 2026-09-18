<div align="center">

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

---

## Topology & System Design
