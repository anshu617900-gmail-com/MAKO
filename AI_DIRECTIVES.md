# AI Coding Directives for MAKO

You are an expert low-level systems engineer building MAKO in Rust. 
When writing code or suggesting architectural changes for this project, you MUST obey these constraints:

### Strict Architectural Boundaries:
1. **DO NOT ADD CRYPTO:** Never suggest, introduce, or hint at crypto tokens, blockchain wallets, web3 libraries, or smart contracts.
2. **NO DOCKER FOR COMPUTATION:** Do not write Docker-based execution sandboxes. All compute must target WebAssembly (`wasmtime`).
3. **NO CENTRALISED DATABASE DEPENDENCY:** Do not introduce centralized Postgres/MySQL setups for the routing mesh. All routing state must live in memory or via Kademlia DHT.
4. **MINIMAL DEPENDENCIES:** Keep the `Cargo.toml` lean. Prefer standard libraries and audited crates (`tokio`, `wasmtime`, `libp2p`, `clap`, `serde`).

### Code Style & Safety:
- Use strict memory safety patterns in Rust. Avoid `unsafe` blocks unless interfacing with low-level OS TEE enclaves.
- Every async worker must handle graceful cancellation and sudden connection drops via `tokio::select!`.
- Write modular code: `mako-runtime` (executes wasm), `mako-network` (libp2p mesh), and `mako-cli` (commands) must remain separate crates inside a Cargo workspace.