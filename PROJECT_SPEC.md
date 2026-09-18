# MAKO: The Sovereign Edge Compute Engine
**Vision:** An unstoppable, zero-token, open-source peer-to-peer serverless compute network that replaces centralized edge providers (Cloudflare Workers, AWS Lambda) by pooling idle machine compute globally.

---

## 1. Core Principles (Non-Negotiable)
1. **Zero Crypto / Zero Tokens:** No blockchains, no wallets, no gas fees, no smart contracts. Pure developer barter economy (give idle compute -> earn priority execution).
2. **Stateless Edge First:** Do not attempt full database replication at Genesis. Focus 100% on stateless execution: APIs, webhooks, microservices, proxies, and auth logic.
3. **Sub-Millisecond Isolation:** No Docker containers. Everything runs as lightweight WebAssembly (.wasm) compiled binaries inside isolated memory sandboxes using Wasmtime.
4. **Resilient Failover:** Every deployment is replicated to 3 nodes. Dropping a node mid-execution must route to a replica within 150ms without user-facing connection loss.

---

## 2. Technical Stack
- **Core Systems Language:** Rust (latest stable)
- **Wasm Runtime:** `wasmtime` (pure memory sandboxing, WASI standard)
- **P2P Networking:** `libp2p` (gossipsub, kademlia DHT, ping, identify)
- **NAT Traversal:** WebRTC Data Channels + Free Public STUN (Google/Cloudflare)
- **CLI Framework:** `clap` (Rust)
- **Serialization:** `bincode` / `serde` (ultra-fast binary transfers)

---

## 3. The 3 Mastermind Free-Tier Hacks (Jugaad Layer)
1. **Ghost Seed Cluster:** Initial 20-30 permanent bootstrap nodes run on free-tier VPS (Oracle Free Tier, GitHub Actions scheduled runners, HuggingFace Docker spaces) to guarantee 100% genesis uptime.
2. **Zero-Dollar SSL & DNS:** A single root domain (`mako.run`) with Cloudflare wildcard DNS (`*.mako.run`) routing requests to active ingress nodes.
3. **NAT Punching:** Using open public STUN servers for direct peer-to-peer tunnels between domestic Wi-Fi routers without rented TURN servers.