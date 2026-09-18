# MAKO Execution Roadmap

### Phase 1: The Local Sandboxed Worker (Day 1 - 7)
- [ ] Setup Rust workspace (`mako-runtime`, `mako-cli`).
- [ ] Embed `wasmtime` runtime to execute an arbitrary `.wasm` file passed via CLI.
- [ ] Enforce CPU execution timeout (kill after 50ms) and RAM limit (max 64MB).
- [ ] Measure cold start execution time (target: < 5ms).

### Phase 2: The Two-Node P2P Bridge (Day 8 - 18)
- [ ] Create `mako-network` crate using `libp2p`.
- [ ] Establish direct P2P connection between two machines across different Wi-Fi routers using WebRTC hole-punching.
- [ ] Node A sends a `.wasm` file to Node B via libp2p stream; Node B executes it and streams stdout/response back.

### Phase 3: The 3x Redundancy Swarm (Day 19 - 30)
- [ ] Implement Kademlia DHT for node discovery.
- [ ] Setup Gossipsub heartbeat: ping nodes every 100ms.
- [ ] Implement instant failover: if primary node fails to heartbeat within 300ms, secondary node takes incoming traffic.

### Phase 4: Public Seed & Live Ingress Demo (Day 31 - 45)
- [ ] Deploy 10 permanent bootstrap nodes on free cloud tiers.
- [ ] Setup CLI command `mako deploy <binary.wasm>` that outputs a working `https://<hash>.mako.run` endpoint.
- [ ] Record the kill-switch live demo video (unplugging node with 0% downtime) for Hacker News / Twitter launch.