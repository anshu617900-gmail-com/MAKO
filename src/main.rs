#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use axum::{
    body::Bytes,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use libp2p::{
    identity::Keypair,
    kad::{self, store::MemoryStore, Behaviour as KadBehaviour},
    mdns, noise,
    request_response::{self, cbor, OutboundRequestId, ProtocolSupport},
    swarm::SwarmEvent,
    tcp, yamux, Multiaddr, PeerId, StreamProtocol, SwarmBuilder,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, RwLock};
use std::env;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use wasmtime::{Config, Engine, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Val};
use wasmtime_wasi::p1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

/// Protocol identifier for the MAKO compute exchange
const COMPUTE_PROTOCOL: StreamProtocol = StreamProtocol::new("/mako/compute/1.0.0");

/// Security guardrail constants
const MAX_PAYLOAD_SIZE: usize = 2 * 1024 * 1024; // 2MB
const MAX_FUEL: u64 = 100_000;
const DEFAULT_FUEL: u64 = 50_000;
const EXECUTION_TIMEOUT_MS: u64 = 500;
const DEFAULT_API_KEY: &str = "mako-secret-dev-key";

// ============================================================================
// 1. CONTENT-ADDRESSING & PROTOCOL PAYLOADS
// ============================================================================

/// Computes SHA-256 hash truncated to 12 hex characters for content-addressing
pub fn compute_function_id(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let full = format!("{:x}", hasher.finalize());
    full[..12].to_string()
}

/// Request payloads exchanged between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComputeRequest {
    /// Deploy and JIT-compile a module into worker memory cache
    DeployModule {
        hash: String,
        file_bytes: Vec<u8>,
    },
    /// Invoke a previously deployed module directly from worker memory cache (numeric args)
    InvokeModule {
        hash: String,
        func_name: String,
        args: Vec<i32>,
        fuel_limit: u64,
        max_memory_mb: usize,
    },
    /// Invoke a previously deployed module using linear memory buffer ABI (JSON/string payload)
    InvokeModuleMemory {
        hash: String,
        func_name: String,
        input_bytes: Vec<u8>,
        fuel_limit: u64,
        max_memory_mb: usize,
    },
    /// Direct one-shot execution of raw bytes
    ExecuteRaw {
        file_bytes: Vec<u8>,
        func_name: String,
        args: Vec<i32>,
        fuel_limit: u64,
        max_memory_mb: usize,
    },
}

/// Response payload sent back across the P2P wire
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeResponse {
    pub status: String,
    pub hash: Option<String>,
    pub results: Vec<String>,
    pub execution_micros: u128,
    pub fuel_consumed: u64,
    pub error: Option<String>,
}

/// Combined NetworkBehaviour supporting mDNS LAN discovery, Kademlia WAN DHT, and CBOR request-response
#[derive(libp2p::swarm::NetworkBehaviour)]
pub struct MakoBehaviour {
    pub request_response: cbor::Behaviour<ComputeRequest, ComputeResponse>,
    pub mdns: mdns::tokio::Behaviour,
    pub kad: KadBehaviour<MemoryStore>,
}

// ============================================================================
// 2. CLI ARGUMENT DEFINITIONS
// ============================================================================

#[derive(Parser, Debug)]
#[command(
    name = "mako",
    author = "MAKO Core Team",
    version = "0.3.0",
    about = "MAKO: Sovereign Edge Compute Engine (Content-Addressed Serverless Wasm)"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the P2P worker daemon to discover peers via mDNS/Kademlia and execute jobs
    Daemon {
        /// TCP port to bind the daemon listener (default: 4001)
        #[arg(short, long, default_value_t = 4001)]
        port: u16,

        /// IP address to bind (default: 0.0.0.0)
        #[arg(long, default_value = "0.0.0.0")]
        host: String,

        /// Optional bootstrap peer multiaddress to connect to WAN/Global Mesh
        #[arg(short, long)]
        bootstrap: Option<Multiaddr>,
    },

    /// Start the HTTP Ingress Gateway that routes requests to active mDNS/Kademlia worker peers
    Gateway {
        /// HTTP server listening port (default: 8080)
        #[arg(short, long, default_value_t = 8080)]
        port: u16,

        /// HTTP server listening host (default: 127.0.0.1)
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// P2P mesh listening port for mDNS/Kademlia communication (default: 4002)
        #[arg(long, default_value_t = 4002)]
        p2p_port: u16,

        /// Optional bootstrap peer multiaddress to connect to WAN/Global Mesh
        #[arg(short, long)]
        bootstrap: Option<Multiaddr>,
    },

    /// Content-address, JIT-compile, and deploy a Wasm module to the peer network
    Deploy {
        /// Path to .wasm or .wat file to deploy
        wasm_file: PathBuf,

        /// Optional Gateway HTTP endpoint (default: http://127.0.0.1:8080)
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        gateway_url: String,

        /// Optional P2P direct peer multiaddress to deploy to
        #[arg(long)]
        peer: Option<Multiaddr>,
    },

    /// Dispatch a WebAssembly computation directly to a remote peer over libp2p
    Dispatch {
        /// Target peer multiaddress (e.g. /ip4/127.0.0.1/tcp/4001/p2p/<PEER_ID>)
        peer_addr: Multiaddr,

        /// Path to .wasm or .wat file to dispatch
        wasm_file: PathBuf,

        /// Exported function name to execute
        #[arg(long, default_value = "multiply")]
        func: String,

        /// Integer arguments passed to the function (e.g. --args 6 7)
        #[arg(long, num_args = 0..)]
        args: Vec<i32>,

        /// Gas/fuel units allocated for the execution (default: 10,000)
        #[arg(long, default_value_t = 10_000)]
        fuel: u64,

        /// Maximum linear memory limit in Megabytes (default: 16 MB)
        #[arg(long, default_value_t = 16)]
        max_memory_mb: usize,
    },

    /// Run a WebAssembly module locally (with fuel & 16MB memory guardrails)
    Run {
        /// Path to the .wasm binary or .wat text file
        wasm_file: PathBuf,

        /// Name of the exported function to execute
        #[arg(long, default_value = "add")]
        func: String,

        /// Integer arguments passed to the function (e.g. --args 5 7)
        #[arg(long, num_args = 0..)]
        args: Vec<i32>,

        /// Initial gas/fuel units allocated for the execution
        #[arg(long, default_value_t = 10_000)]
        fuel: u64,

        /// Maximum linear memory limit in Megabytes (16 MB = 256 Wasm pages)
        #[arg(long, default_value_t = 16)]
        max_memory_mb: usize,
    },
}

// ============================================================================
// 3. SANDBOXED WASMTIME EXECUTION ENGINE & MODULE CACHE
// ============================================================================

pub struct HostState {
    pub wasi: WasiP1Ctx,
    pub limits: StoreLimits,
}

#[derive(Debug, Clone)]
pub struct ExecutionMetrics {
    pub results: Vec<String>,
    pub cold_start_micros: u128,
    pub fuel_consumed: u64,
    pub fuel_remaining: u64,
}

/// Sandboxed WebAssembly runner configured with fuel metering, memory limits, and in-memory module cache
#[derive(Clone)]
pub struct IsolatedWasmRunner {
    engine: Engine,
    linker: Linker<HostState>,
    module_cache: Arc<RwLock<HashMap<String, Module>>>,
}

impl IsolatedWasmRunner {
    pub fn new() -> Result<Self> {
        let mut wasm_config = Config::new();
        wasm_config.consume_fuel(true);

        let engine = Engine::new(&wasm_config)
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .context("Failed to initialize Wasmtime engine with fuel metering")?;

        let mut linker: Linker<HostState> = Linker::new(&engine);
        p1::add_to_linker_sync(&mut linker, |state: &mut HostState| &mut state.wasi)
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .context("Failed to register WASI Preview 1 interfaces into Linker")?;

        let module_cache = Arc::new(RwLock::new(HashMap::new()));

        Ok(Self {
            engine,
            linker,
            module_cache,
        })
    }

    fn create_sandboxed_wasi_context() -> WasiP1Ctx {
        WasiCtxBuilder::new()
            .inherit_stdout()
            .inherit_stderr()
            .build_p1()
    }

    fn create_sandboxed_store(&self, fuel: u64, max_memory_bytes: usize) -> Result<Store<HostState>> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(max_memory_bytes)
            .trap_on_grow_failure(true)
            .build();

        let host_state = HostState {
            wasi: Self::create_sandboxed_wasi_context(),
            limits,
        };

        let mut store = Store::new(&self.engine, host_state);
        store.limiter(|state| &mut state.limits);
        store
            .set_fuel(fuel)
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .context("Failed to set initial fuel on Wasm store")?;

        Ok(store)
    }

    /// Compiles and caches a module in RAM under a given content hash
    pub async fn deploy_module(&self, hash: &str, bytes: &[u8]) -> Result<Module> {
        // Check if already present in RAM cache
        {
            let cache = self.module_cache.read().await;
            if let Some(m) = cache.get(hash) {
                return Ok(m.clone());
            }
        }

        // Parse and compile module
        let module = match Module::new(&self.engine, bytes) {
            Ok(m) => m,
            Err(_) => {
                let binary = wat::parse_bytes(bytes)
                    .map_err(|e| anyhow::anyhow!("{:#}", e))
                    .context("Failed to parse module bytes as Wasm binary or WAT text")?;
                Module::new(&self.engine, &binary)
                    .map_err(|e| anyhow::anyhow!("{:#}", e))
                    .context("Failed to compile WebAssembly module from WAT")?
            }
        };

        // Insert into cache
        let mut cache = self.module_cache.write().await;
        cache.insert(hash.to_string(), module.clone());
        Ok(module)
    }

    /// Retrieves a pre-compiled module directly from RAM cache
    pub async fn get_cached_module(&self, hash: &str) -> Option<Module> {
        let cache = self.module_cache.read().await;
        cache.get(hash).cloned()
    }

    /// Executes an instance of a compiled module
    pub fn execute_module(
        &self,
        module: &Module,
        func_name: &str,
        args: &[i32],
        fuel_limit: u64,
        max_memory_mb: usize,
    ) -> Result<ExecutionMetrics> {
        let timer = Instant::now();

        let max_memory_bytes = max_memory_mb * 1024 * 1024;
        let mut store = self.create_sandboxed_store(fuel_limit, max_memory_bytes)?;

        let instance = self
            .linker
            .instantiate(&mut store, module)
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .context("Failed to instantiate WebAssembly module inside sandbox")?;

        let func = instance
            .get_func(&mut store, func_name)
            .with_context(|| format!("Exported function '{}' not found in module", func_name))?;

        let params: Vec<Val> = args.iter().map(|&a| Val::I32(a)).collect();
        let num_results = func.ty(&store).results().len();
        let mut results = vec![Val::I32(0); num_results];

        func.call(&mut store, &params, &mut results)
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .with_context(|| format!("Execution of function '{}' trapped or failed inside sandbox", func_name))?;

        let elapsed_micros = timer.elapsed().as_micros();
        let fuel_remaining = store
            .get_fuel()
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .context("Failed to query remaining fuel")?;
        let fuel_consumed = fuel_limit.saturating_sub(fuel_remaining);

        let formatted_results: Vec<String> = results
            .iter()
            .map(|val| match val {
                Val::I32(v) => v.to_string(),
                Val::I64(v) => v.to_string(),
                Val::F32(v) => format!("{:.4}", f32::from_bits(*v)),
                Val::F64(v) => format!("{:.4}", f64::from_bits(*v)),
                other => format!("{:?}", other),
            })
            .collect();

        Ok(ExecutionMetrics {
            results: formatted_results,
            cold_start_micros: elapsed_micros,
            fuel_consumed,
            fuel_remaining,
        })
    }

    /// One-shot execution from raw bytes (compiles + executes)
    pub async fn execute_bytes(
        &self,
        bytes: &[u8],
        func_name: &str,
        args: &[i32],
        fuel_limit: u64,
        max_memory_mb: usize,
    ) -> Result<ExecutionMetrics> {
        let hash = compute_function_id(bytes);
        let module = self.deploy_module(&hash, bytes).await?;
        self.execute_module(&module, func_name, args, fuel_limit, max_memory_mb)
    }

    /// Executes a function using linear memory buffer ABI (alloc/dealloc + packed u64 return)
    pub fn execute_module_memory(
        &self,
        module: &Module,
        func_name: &str,
        input_bytes: &[u8],
        fuel_limit: u64,
        max_memory_mb: usize,
    ) -> Result<ExecutionMetrics> {
        let timer = Instant::now();

        let max_memory_bytes = max_memory_mb * 1024 * 1024;
        let mut store = self.create_sandboxed_store(fuel_limit, max_memory_bytes)?;

        let instance = self
            .linker
            .instantiate(&mut store, module)
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .context("Failed to instantiate WebAssembly module inside sandbox")?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .context("Module does not export linear memory")?;

        let alloc_func = instance
            .get_func(&mut store, "alloc")
            .context("Module does not export 'alloc' function")?;

        let dealloc_func = instance
            .get_func(&mut store, "dealloc")
            .context("Module does not export 'dealloc' function")?;

        let transform_func = instance
            .get_func(&mut store, func_name)
            .with_context(|| format!("Exported function '{}' not found in module", func_name))?;

        let input_len = input_bytes.len();
        let alloc_params = [Val::I32(input_len as i32)];
        let mut alloc_results = [Val::I32(0)];
        alloc_func.call(&mut store, &alloc_params, &mut alloc_results)
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .context("Failed to call alloc")?;

        let guest_ptr = match alloc_results[0] {
            Val::I32(v) => v as u32,
            _ => return Err(anyhow::anyhow!("alloc returned unexpected type")),
        };

        if guest_ptr == 0 {
            return Err(anyhow::anyhow!("alloc returned null pointer"));
        }

        {
            let mem_data = memory.data_mut(&mut store);
            let guest_slice = &mut mem_data[guest_ptr as usize..(guest_ptr as usize + input_len)];
            guest_slice.copy_from_slice(input_bytes);
        }

        let transform_params = [Val::I32(guest_ptr as i32), Val::I32(input_len as i32)];
        let mut transform_results = [Val::I64(0)];
        transform_func.call(&mut store, &transform_params, &mut transform_results)
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .context("Failed to call transform function")?;

        let packed_result = match transform_results[0] {
            Val::I64(v) => v as u64,
            Val::I32(v) => v as u64,
            _ => return Err(anyhow::anyhow!("transform function returned unexpected type")),
        };

        let out_ptr = (packed_result >> 32) as u32;
        let out_len = (packed_result & 0xFFFFFFFF) as u32;

        let output_bytes = if out_ptr != 0 && out_len > 0 {
            let mem_data = memory.data(&store);
            if (out_ptr as usize + out_len as usize) <= mem_data.len() {
                mem_data[out_ptr as usize..(out_ptr as usize + out_len as usize)].to_vec()
            } else {
                return Err(anyhow::anyhow!("Output pointer/length out of bounds"));
            }
        } else {
            Vec::new()
        };

        if out_ptr != 0 && out_len > 0 {
            let dealloc_params = [Val::I32(out_ptr as i32), Val::I32(out_len as i32)];
            let mut dealloc_results = [Val::I32(0)];
            let _ = dealloc_func.call(&mut store, &dealloc_params, &mut dealloc_results);
        }

        let elapsed_micros = timer.elapsed().as_micros();
        let fuel_remaining = store
            .get_fuel()
            .map_err(|e| anyhow::anyhow!("{:#}", e))
            .context("Failed to query remaining fuel")?;
        let fuel_consumed = fuel_limit.saturating_sub(fuel_remaining);

        let output_str = String::from_utf8(output_bytes).unwrap_or_else(|_| String::new());
        let formatted_results = vec![output_str];

        Ok(ExecutionMetrics {
            results: formatted_results,
            cold_start_micros: elapsed_micros,
            fuel_consumed,
            fuel_remaining,
        })
    }

    /// One-shot memory-buffer execution from raw bytes (compiles + executes)
    pub async fn execute_bytes_memory(
        &self,
        bytes: &[u8],
        func_name: &str,
        input_bytes: &[u8],
        fuel_limit: u64,
        max_memory_mb: usize,
    ) -> Result<ExecutionMetrics> {
        let hash = compute_function_id(bytes);
        let module = self.deploy_module(&hash, bytes).await?;
        self.execute_module_memory(&module, func_name, input_bytes, fuel_limit, max_memory_mb)
    }

    pub async fn execute_file(
        &self,
        file_path: &Path,
        func_name: &str,
        args: &[i32],
        fuel_limit: u64,
        max_memory_mb: usize,
    ) -> Result<ExecutionMetrics> {
        let bytes = std::fs::read(file_path)
            .with_context(|| format!("Failed to read file: '{}'", file_path.display()))?;
        self.execute_bytes(&bytes, func_name, args, fuel_limit, max_memory_mb).await
    }
}

// ============================================================================
// 4. IDENTITY & HELPERS
// ============================================================================

fn get_or_create_identity(key_path: &Path) -> Result<Keypair> {
    if key_path.exists() {
        let bytes = std::fs::read(key_path)
            .with_context(|| format!("Failed to read node key from '{}'", key_path.display()))?;
        if let Ok(keypair) = Keypair::from_protobuf_encoding(&bytes) {
            return Ok(keypair);
        }
    }

    let keypair = Keypair::generate_ed25519();
    if let Ok(bytes) = keypair.to_protobuf_encoding() {
        let _ = std::fs::write(key_path, bytes);
    }
    Ok(keypair)
}

fn chrono_timestamp() -> String {
    let total_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let s = total_secs % 60;
    let m = (total_secs / 60) % 60;
    let h = (total_secs / 3600) % 24;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

/// Lightweight async HTTP client that posts raw bytes to the gateway without C/OpenSSL dependencies
async fn post_bytes_to_gateway(gateway_url: &str, file_bytes: &[u8]) -> Result<String> {
    let stripped = gateway_url.trim_start_matches("http://").trim_start_matches("https://");
    let (host, path) = match stripped.split_once('/') {
        Some((h, p)) => (h, format!("/{}", p)),
        None => (stripped, "/deploy".to_string()),
    };
    let path = if path.is_empty() || path == "/" { "/deploy".to_string() } else { path };

    let mut stream = tokio::net::TcpStream::connect(host)
        .await
        .with_context(|| format!("Failed to connect to Gateway at '{}'", host))?;

    let header = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
        path, host, file_bytes.len()
    );

    stream.write_all(header.as_bytes()).await?;
    stream.write_all(file_bytes).await?;
    stream.flush().await?;

    let mut response_buf = Vec::new();
    stream.read_to_end(&mut response_buf).await?;
    let resp_str = String::from_utf8_lossy(&response_buf);

    if let Some((_, body)) = resp_str.split_once("\r\n\r\n") {
        Ok(body.to_string())
    } else {
        Ok(resp_str.to_string())
    }
}

fn strip_peer_id(mut addr: Multiaddr) -> Multiaddr {
    while let Some(proto) = addr.iter().last() {
        if let libp2p::multiaddr::Protocol::P2p(_) = proto {
            addr.pop();
        } else {
            break;
        }
    }
    addr
}

/// Extracts the PeerId and base dialable Multiaddr from a full multiaddress
fn parse_peer_multiaddr(addr: &Multiaddr) -> Result<(PeerId, Multiaddr)> {
    let mut clean_addr = addr.clone();
    let mut target_peer_id = None;
    for proto in addr.iter() {
        if let libp2p::multiaddr::Protocol::P2p(id) = proto {
            target_peer_id = Some(id);
        }
    }

    if let Some(peer_id) = target_peer_id {
        clean_addr = strip_peer_id(clean_addr);
        Ok((peer_id, clean_addr))
    } else {
        bail!("Multiaddress '{}' must contain a peer ID (/p2p/<PEER_ID>)", addr);
    }
}

// ============================================================================
// 5. DAEMON NODE (Worker with In-Memory Cache & Kademlia Server)
// ============================================================================

const MAX_CONCURRENT_WASM: usize = 16;

async fn run_daemon(host: &str, port: u16, bootstrap: Option<Multiaddr>) -> Result<()> {
    println!("============================================================");
    println!("     MAKO Sovereign Edge Compute Daemon (Kademlia Worker)   ");
    println!("============================================================");

    let key_file_name = format!(".mako_node_{}.key", port);
    let key_path = Path::new(&key_file_name);
    let id_keys = get_or_create_identity(key_path)?;
    let local_peer_id = PeerId::from(id_keys.public());

    println!("[Node Identity]");
    println!("  * PeerId: {}", local_peer_id);
    println!("  * Identity File: {}", key_path.display());

    let runner = Arc::new(IsolatedWasmRunner::new()?);
    let active_peers: Arc<RwLock<HashSet<PeerId>>> = Arc::new(RwLock::new(HashSet::new()));

    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_WASM));
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<(
        request_response::ResponseChannel<ComputeResponse>,
        ComputeResponse,
    )>(64);

    let mut swarm = SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|keypair| {
            let peer_id = keypair.public().to_peer_id();
            let mdns = mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                peer_id,
            )?;
            let request_response = cbor::Behaviour::<ComputeRequest, ComputeResponse>::new(
                [(COMPUTE_PROTOCOL, ProtocolSupport::Full)],
                request_response::Config::default(),
            );
            let store = MemoryStore::new(peer_id);
            let kad = KadBehaviour::new(peer_id, store);

            Ok(MakoBehaviour {
                request_response,
                mdns,
                kad,
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();

    let listen_addr: Multiaddr = format!("/ip4/{}/tcp/{}", host, port)
        .parse()
        .context("Invalid listen multiaddress")?;
    swarm.listen_on(listen_addr.clone())?;

    println!("\n[P2P Mesh Network]");
    println!("  * Protocol: {}", COMPUTE_PROTOCOL);
    println!("  * Target Bind: {}", listen_addr);
    println!("  * Discovery: mDNS (LAN) + Kademlia DHT (Global WAN Server)");
    println!("  * Memory Cache: Online (JIT Compiled Wasm Modules)");
    println!("  * Concurrency Limit: {} Wasm instances", MAX_CONCURRENT_WASM);
    println!("  * Thread Isolation: Enabled (spawn_blocking + semaphore)");
    println!("  * Status: Awaiting deployments and invocations...\n");

    // Handle initial bootstrap seed peering if supplied
    if let Some(ref b_addr) = bootstrap {
        println!("[Bootstrap] Processing bootstrap seed address: {}", b_addr);
        match parse_peer_multiaddr(b_addr) {
            Ok((seed_peer_id, transport_addr)) => {
                let clean_addr = strip_peer_id(transport_addr);
                println!("  * Seed PeerId: {}", seed_peer_id);
                println!("  * Adding seed to Kademlia routing table: {}", clean_addr);
                swarm.behaviour_mut().kad.add_address(&seed_peer_id, clean_addr.clone());
                swarm.add_peer_address(seed_peer_id, clean_addr);
                active_peers.write().await.insert(seed_peer_id);

                if let Err(e) = swarm.dial(b_addr.clone()) {
                    eprintln!("  * Dial to bootstrap seed peer failed: {:#}", e);
                } else {
                    println!("  * Dial initiated to seed peer. Triggering Kademlia bootstrap...");
                    match swarm.behaviour_mut().kad.bootstrap() {
                        Ok(_) => println!("  * Kademlia DHT bootstrap discovery running..."),
                        Err(e) => eprintln!("  * Kademlia bootstrap query note: {:?}", e),
                    }
                }
            }
            Err(e) => {
                eprintln!("[Bootstrap] Multiaddress note: {}. Attempting direct dial...", e);
                let _ = swarm.dial(b_addr.clone());
            }
        }
    }

    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        let full_addr = format!("{}/p2p/{}", address, local_peer_id);
                        println!(">>> Daemon is ACTIVE and listening on:");
                        println!("    {}\n", full_addr);
                        let addr_str = address.to_string();
                        if addr_str.contains("0.0.0.0") {
                            let loopback_addr = format!("/ip4/127.0.0.1/tcp/{}/p2p/{}", port, local_peer_id);
                            println!("    {}\n", loopback_addr);
                        }
                    }
                    SwarmEvent::Behaviour(MakoBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, multiaddr) in list {
                            if peer_id == local_peer_id {
                                continue;
                            }
                            let clean_addr = strip_peer_id(multiaddr);
                            println!("[mDNS] Discovered peer: {} at {}", peer_id, clean_addr);
                            swarm.add_peer_address(peer_id, clean_addr.clone());
                            swarm.behaviour_mut().kad.add_address(&peer_id, clean_addr);
                            active_peers.write().await.insert(peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(MakoBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, multiaddr) in list {
                            println!("[mDNS] Peer expired: {} at {}", peer_id, multiaddr);
                            active_peers.write().await.remove(&peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(MakoBehaviourEvent::Kad(kad_event)) => {
                        match kad_event {
                            kad::Event::RoutingUpdated { peer, addresses, .. } => {
                                println!("[Kademlia DHT] Routing updated for peer: {}", peer);
                                for addr in addresses.iter() {
                                    let clean_addr = strip_peer_id(addr.clone());
                                    swarm.add_peer_address(peer, clean_addr);
                                }
                                active_peers.write().await.insert(peer);
                            }
                            kad::Event::OutboundQueryProgressed { result, .. } => {
                                if let kad::QueryResult::Bootstrap(Ok(kad::BootstrapOk { peer, .. })) = result {
                                    println!("[Kademlia DHT] Bootstrap query returned peer: {}", peer);
                                    active_peers.write().await.insert(peer);
                                }
                            }
                            _ => {}
                        }
                    }
                    SwarmEvent::Behaviour(MakoBehaviourEvent::RequestResponse(
                        request_response::Event::Message {
                            peer: _,
                            message:
                                request_response::Message::Request {
                                    request_id: _,
                                    request,
                                    channel,
                                },
                        },
                    )) => {
                        match request {
                            ComputeRequest::DeployModule { hash, file_bytes } => {
                                let runner_clone = Arc::clone(&runner);
                                let timer = Instant::now();
                                println!(
                                    "[{}] Deploy request for hash '{}' ({} bytes)",
                                    chrono_timestamp(),
                                    hash,
                                    file_bytes.len()
                                );
                                let resp_tx_clone = resp_tx.clone();
                                let sem = semaphore.clone();
                                tokio::spawn(async move {
                                    let permit = match tokio::time::timeout(Duration::from_secs(5), sem.acquire_owned()).await {
                                        Ok(Ok(p)) => p,
                                        Ok(Err(_)) => {
                                            let resp = ComputeResponse {
                                                status: "error".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some("Semaphore closed".to_string()),
                                            };
                                            let _ = resp_tx_clone.send((channel, resp)).await;
                                            return;
                                        }
                                        Err(_) => {
                                            let resp = ComputeResponse {
                                                status: "error".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some("Node execution capacity saturated".to_string()),
                                            };
                                            let _ = resp_tx_clone.send((channel, resp)).await;
                                            return;
                                        }
                                    };

                                    let result = runner_clone.deploy_module(&hash, &file_bytes).await;
                                    let elapsed = timer.elapsed().as_micros();
                                    let response = match result {
                                        Ok(_) => {
                                            println!("  --> Module '{}' compiled and cached in RAM ({} \u{00B5}s)", hash, elapsed);
                                            ComputeResponse {
                                                status: "deployed".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: elapsed,
                                                fuel_consumed: 0,
                                                error: None,
                                            }
                                        }
                                        Err(e) => {
                                            let err_str = format!("{:#}", e);
                                            eprintln!("  --> Deploy failed: {}", err_str);
                                            ComputeResponse {
                                                status: "error".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some(err_str),
                                            }
                                        }
                                    };
                                    drop(permit);
                                    let _ = resp_tx_clone.send((channel, response)).await;
                                });
                            }
                            ComputeRequest::InvokeModule {
                                hash,
                                func_name,
                                args,
                                fuel_limit,
                                max_memory_mb,
                            } => {
                                let runner_clone = Arc::clone(&runner);
                                let resp_tx_clone = resp_tx.clone();
                                let sem = semaphore.clone();
                                println!(
                                    "[{}] Invoke cached module '{}': func='{}', args={:?}",
                                    chrono_timestamp(),
                                    hash,
                                    func_name,
                                    args
                                );
                                tokio::spawn(async move {
                                    let permit = match tokio::time::timeout(Duration::from_secs(5), sem.acquire_owned()).await {
                                        Ok(Ok(p)) => p,
                                        Ok(Err(_)) => {
                                            let resp = ComputeResponse {
                                                status: "error".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some("Semaphore closed".to_string()),
                                            };
                                            let _ = resp_tx_clone.send((channel, resp)).await;
                                            return;
                                        }
                                        Err(_) => {
                                            let resp = ComputeResponse {
                                                status: "error".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some("Node execution capacity saturated".to_string()),
                                            };
                                            let _ = resp_tx_clone.send((channel, resp)).await;
                                            return;
                                        }
                                    };

                                    let module_opt = runner_clone.get_cached_module(&hash).await;
                                    let response = match module_opt {
                                        Some(module) => {
                                            let res = tokio::time::timeout(
                                                Duration::from_millis(EXECUTION_TIMEOUT_MS),
                                                tokio::task::spawn_blocking(move || {
                                                    runner_clone.execute_module(
                                                        &module,
                                                        &func_name,
                                                        &args,
                                                        fuel_limit,
                                                        max_memory_mb,
                                                    )
                                                })
                                            ).await;
                                            match res {
                                                Ok(Ok(Ok(metrics))) => {
                                                    println!(
                                                        "  --> Fast cached run in {} \u{00B5}s (fuel: {}). Output: {:?}",
                                                        metrics.cold_start_micros, metrics.fuel_consumed, metrics.results
                                                    );
                                                    ComputeResponse {
                                                        status: "success".to_string(),
                                                        hash: Some(hash),
                                                        results: metrics.results,
                                                        execution_micros: metrics.cold_start_micros,
                                                        fuel_consumed: metrics.fuel_consumed,
                                                        error: None,
                                                    }
                                                }
                                                Ok(Ok(Err(e))) => {
                                                    let err_str = format!("{:#}", e);
                                                    eprintln!("  --> Invocation error: {}", err_str);
                                                    ComputeResponse {
                                                        status: "error".to_string(),
                                                        hash: Some(hash),
                                                        results: Vec::new(),
                                                        execution_micros: 0,
                                                        fuel_consumed: 0,
                                                        error: Some(err_str),
                                                    }
                                                }
                                                Ok(Err(e)) => {
                                                    let err_str = format!("Task join error: {:#}", e);
                                                    eprintln!("  --> Task panic: {}", err_str);
                                                    ComputeResponse {
                                                        status: "error".to_string(),
                                                        hash: Some(hash),
                                                        results: Vec::new(),
                                                        execution_micros: 0,
                                                        fuel_consumed: 0,
                                                        error: Some(err_str),
                                                    }
                                                }
                                                Err(_) => {
                                                    let err_str = format!("Execution timed out ({}ms limit exceeded)", EXECUTION_TIMEOUT_MS);
                                                    eprintln!("  --> {}", err_str);
                                                    ComputeResponse {
                                                        status: "error".to_string(),
                                                        hash: Some(hash),
                                                        results: Vec::new(),
                                                        execution_micros: 0,
                                                        fuel_consumed: 0,
                                                        error: Some(err_str),
                                                    }
                                                }
                                            }
                                        }
                                        None => {
                                            let err_msg = format!("Module '{}' not found in memory cache. Deploy it first.", hash);
                                            eprintln!("  --> {}", err_msg);
                                            ComputeResponse {
                                                status: "error".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some(err_msg),
                                            }
                                        }
                                    };
                                    drop(permit);
                                    let _ = resp_tx_clone.send((channel, response)).await;
                                });
                            }
                            ComputeRequest::InvokeModuleMemory {
                                hash,
                                func_name,
                                input_bytes,
                                fuel_limit,
                                max_memory_mb,
                            } => {
                                let runner_clone = Arc::clone(&runner);
                                let resp_tx_clone = resp_tx.clone();
                                let sem = semaphore.clone();
                                println!(
                                    "[{}] Invoke cached module (memory) '{}': func='{}', input_len={}",
                                    chrono_timestamp(),
                                    hash,
                                    func_name,
                                    input_bytes.len()
                                );
                                tokio::spawn(async move {
                                    let permit = match tokio::time::timeout(Duration::from_secs(5), sem.acquire_owned()).await {
                                        Ok(Ok(p)) => p,
                                        Ok(Err(_)) => {
                                            let resp = ComputeResponse {
                                                status: "error".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some("Semaphore closed".to_string()),
                                            };
                                            let _ = resp_tx_clone.send((channel, resp)).await;
                                            return;
                                        }
                                        Err(_) => {
                                            let resp = ComputeResponse {
                                                status: "error".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some("Node execution capacity saturated".to_string()),
                                            };
                                            let _ = resp_tx_clone.send((channel, resp)).await;
                                            return;
                                        }
                                    };

                                    let module_opt = runner_clone.get_cached_module(&hash).await;
                                    let response = match module_opt {
                                        Some(module) => {
                                            let res = tokio::time::timeout(
                                                Duration::from_millis(EXECUTION_TIMEOUT_MS),
                                                tokio::task::spawn_blocking(move || {
                                                    runner_clone.execute_module_memory(
                                                        &module,
                                                        &func_name,
                                                        &input_bytes,
                                                        fuel_limit,
                                                        max_memory_mb,
                                                    )
                                                })
                                            ).await;
                                            match res {
                                                Ok(Ok(Ok(metrics))) => {
                                                    println!(
                                                        "  --> Fast cached run in {} \u{00B5}s (fuel: {}). Output: {:?}",
                                                        metrics.cold_start_micros, metrics.fuel_consumed, metrics.results
                                                    );
                                                    ComputeResponse {
                                                        status: "success".to_string(),
                                                        hash: Some(hash),
                                                        results: metrics.results,
                                                        execution_micros: metrics.cold_start_micros,
                                                        fuel_consumed: metrics.fuel_consumed,
                                                        error: None,
                                                    }
                                                }
                                                Ok(Ok(Err(e))) => {
                                                    let err_str = format!("{:#}", e);
                                                    eprintln!("  --> Invocation error: {}", err_str);
                                                    ComputeResponse {
                                                        status: "error".to_string(),
                                                        hash: Some(hash),
                                                        results: Vec::new(),
                                                        execution_micros: 0,
                                                        fuel_consumed: 0,
                                                        error: Some(err_str),
                                                    }
                                                }
                                                Ok(Err(e)) => {
                                                    let err_str = format!("Task join error: {:#}", e);
                                                    eprintln!("  --> Task panic: {}", err_str);
                                                    ComputeResponse {
                                                        status: "error".to_string(),
                                                        hash: Some(hash),
                                                        results: Vec::new(),
                                                        execution_micros: 0,
                                                        fuel_consumed: 0,
                                                        error: Some(err_str),
                                                    }
                                                }
                                                Err(_) => {
                                                    let err_str = format!("Execution timed out ({}ms limit exceeded)", EXECUTION_TIMEOUT_MS);
                                                    eprintln!("  --> {}", err_str);
                                                    ComputeResponse {
                                                        status: "error".to_string(),
                                                        hash: Some(hash),
                                                        results: Vec::new(),
                                                        execution_micros: 0,
                                                        fuel_consumed: 0,
                                                        error: Some(err_str),
                                                    }
                                                }
                                            }
                                        }
                                        None => {
                                            let err_msg = format!("Module '{}' not found in memory cache. Deploy it first.", hash);
                                            eprintln!("  --> {}", err_msg);
                                            ComputeResponse {
                                                status: "error".to_string(),
                                                hash: Some(hash),
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some(err_msg),
                                            }
                                        }
                                    };
                                    drop(permit);
                                    let _ = resp_tx_clone.send((channel, response)).await;
                                });
                            }
                            ComputeRequest::ExecuteRaw {
                                file_bytes,
                                func_name,
                                args,
                                fuel_limit,
                                max_memory_mb,
                            } => {
                                let runner_clone = Arc::clone(&runner);
                                let resp_tx_clone = resp_tx.clone();
                                let sem = semaphore.clone();
                                tokio::spawn(async move {
                                    let permit = match tokio::time::timeout(Duration::from_secs(5), sem.acquire_owned()).await {
                                        Ok(Ok(p)) => p,
                                        Ok(Err(_)) => {
                                            let resp = ComputeResponse {
                                                status: "error".to_string(),
                                                hash: None,
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some("Semaphore closed".to_string()),
                                            };
                                            let _ = resp_tx_clone.send((channel, resp)).await;
                                            return;
                                        }
                                        Err(_) => {
                                            let resp = ComputeResponse {
                                                status: "error".to_string(),
                                                hash: None,
                                                results: Vec::new(),
                                                execution_micros: 0,
                                                fuel_consumed: 0,
                                                error: Some("Node execution capacity saturated".to_string()),
                                            };
                                            let _ = resp_tx_clone.send((channel, resp)).await;
                                            return;
                                        }
                                    };

                                    let res = tokio::time::timeout(
                                        Duration::from_millis(EXECUTION_TIMEOUT_MS),
                                        tokio::task::spawn_blocking(move || {
                                            tokio::runtime::Handle::current().block_on(async {
                                                runner_clone.execute_bytes(&file_bytes, &func_name, &args, fuel_limit, max_memory_mb).await
                                            })
                                        })
                                    ).await;

                                    let response = match res {
                                        Ok(Ok(Ok(metrics))) => ComputeResponse {
                                            status: "success".to_string(),
                                            hash: None,
                                            results: metrics.results,
                                            execution_micros: metrics.cold_start_micros,
                                            fuel_consumed: metrics.fuel_consumed,
                                            error: None,
                                        },
                                        Ok(Ok(Err(e))) => ComputeResponse {
                                            status: "error".to_string(),
                                            hash: None,
                                            results: Vec::new(),
                                            execution_micros: 0,
                                            fuel_consumed: 0,
                                            error: Some(format!("{:#}", e)),
                                        },
                                        Ok(Err(e)) => ComputeResponse {
                                            status: "error".to_string(),
                                            hash: None,
                                            results: Vec::new(),
                                            execution_micros: 0,
                                            fuel_consumed: 0,
                                            error: Some(format!("Task join error: {:#}", e)),
                                        },
                                        Err(_) => ComputeResponse {
                                            status: "error".to_string(),
                                            hash: None,
                                            results: Vec::new(),
                                            execution_micros: 0,
                                            fuel_consumed: 0,
                                            error: Some(format!("Execution timed out ({}ms limit exceeded)", EXECUTION_TIMEOUT_MS)),
                                        },
                                    };
                                    drop(permit);
                                    let _ = resp_tx_clone.send((channel, response)).await;
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
            resp = resp_rx.recv() => {
                if let Some((resp_channel, response)) = resp {
                    let _ = swarm.behaviour_mut().request_response.send_response(resp_channel, response);
                }
            }
        }
    }
}

// ============================================================================
// 6. HTTP EDGE INGRESS GATEWAY (Deploy & Invoke)
// ============================================================================

enum SwarmCommand {
    SendToPeer {
        target_peer: PeerId,
        request: ComputeRequest,
        reply: oneshot::Sender<Result<ComputeResponse>>,
    },
    BroadcastDeploy {
        request: ComputeRequest,
        reply: oneshot::Sender<usize>,
    },
}

#[derive(Clone)]
struct GatewayState {
    cmd_tx: mpsc::Sender<SwarmCommand>,
    active_peers: Arc<RwLock<Vec<PeerId>>>,
    rr_index: Arc<std::sync::atomic::AtomicUsize>,
    local_runner: Arc<IsolatedWasmRunner>,
    local_peer_id: PeerId,
    raw_byte_cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

#[derive(Deserialize)]
struct InvokeQuery {
    func: Option<String>,
    #[serde(default)]
    fuel: Option<u64>,
    #[serde(default)]
    max_memory_mb: Option<usize>,
}

/// Extracts the file payload from either raw binary bytes or multipart/form-data
fn extract_module_bytes(headers: &axum::http::HeaderMap, raw_bytes: &[u8]) -> Vec<u8> {
    let is_multipart = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.starts_with("multipart/form-data"))
        .unwrap_or(false)
        || raw_bytes.starts_with(b"--");

    if !is_multipart {
        return raw_bytes.to_vec();
    }

    // Multipart parser: find header separator \r\n\r\n or \n\n
    let header_sep = if let Some(pos) = raw_bytes.windows(4).position(|w| w == b"\r\n\r\n") {
        Some(pos + 4)
    } else {
        raw_bytes.windows(2).position(|w| w == b"\n\n").map(|p| p + 2)
    };

    if let Some(content_start) = header_sep {
        let remainder = &raw_bytes[content_start..];
        let content_end = if let Some(pos) = remainder.windows(4).position(|w| w == b"\r\n--") {
            pos
        } else if let Some(pos) = remainder.windows(3).position(|w| w == b"\n--") {
            pos
        } else {
            remainder.len()
        };
        return remainder[..content_end].to_vec();
    }

    raw_bytes.to_vec()
}

/// Extract API key from Authorization header
fn extract_api_key(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

/// Verify API key against environment variable or default
fn verify_api_key(provided: Option<&str>) -> bool {
    let expected = env::var("MAKO_API_KEY").unwrap_or_else(|_| DEFAULT_API_KEY.to_string());
    provided.map(|p| p == expected).unwrap_or(false)
}

/// Clamp fuel to maximum allowed value
fn clamp_fuel(fuel: u64) -> u64 {
    std::cmp::min(fuel, MAX_FUEL)
}

/// POST /invoke/:hash?func=<FUNC>
async fn invoke_handler(
    State(state): State<GatewayState>,
    AxumPath(hash): AxumPath<String>,
    Query(query): Query<InvokeQuery>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    // Auth check
    let api_key = extract_api_key(&headers);
    if !verify_api_key(api_key.as_deref()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "status": "error",
                "error": "Invalid or missing API key"
            })),
        );
    }

    let func_name = query.func.unwrap_or_else(|| "multiply".to_string());
    
    // Clamp fuel to maximum
    let fuel = clamp_fuel(query.fuel.unwrap_or(DEFAULT_FUEL));
    let max_memory_mb = query.max_memory_mb.unwrap_or(16);

    // Parse payload: detect if it's numeric args (array of ints) or JSON payload (object/string)
    let payload: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    
    let is_numeric_args = matches!(&payload, Value::Array(arr) if arr.iter().all(|v| v.is_i64()));
    let is_object_with_args = matches!(&payload, Value::Object(obj) if obj.get("args").map(|v| v.is_array()).unwrap_or(false));
    
    let (args, input_bytes) = if is_numeric_args || is_object_with_args {
        let args: Vec<i32> = match &payload {
            Value::Array(arr) => arr.iter().filter_map(|v| v.as_i64().map(|n| n as i32)).collect(),
            Value::Object(obj) => {
                if let Some(Value::Array(arr)) = obj.get("args") {
                    arr.iter().filter_map(|v| v.as_i64().map(|n| n as i32)).collect()
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        };
        (args, Vec::new())
    } else {
        // Arbitrary JSON payload or string - use memory buffer path
        let input_bytes = if payload.is_null() {
            Vec::new()
        } else {
            serde_json::to_vec(&payload).unwrap_or_else(|_| body.to_vec())
        };
        (Vec::new(), input_bytes)
    };

    let use_memory_path = !input_bytes.is_empty();

    let candidate_peers: Vec<PeerId> = {
        let peers = state.active_peers.read().await;
        peers.clone()
    };

    if candidate_peers.is_empty() {
        // No workers available, try local fallback
        return try_local_fallback(state, hash, func_name, use_memory_path, input_bytes, args, fuel, max_memory_mb).await;
    }

    // Round-robin load balancing: pick next worker atomically
    let start_idx = state.rr_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % candidate_peers.len();
    
    // Try each worker starting from round-robin position, with failover
    let mut peer_errors: Vec<String> = Vec::new();
    let max_attempts = candidate_peers.len();
    
    for attempt in 0..max_attempts {
        let peer_idx = (start_idx + attempt) % candidate_peers.len();
        let peer_id = candidate_peers[peer_idx];

        println!(
            "[{}] Gateway dispatching invoke for hash '{}' to worker {} (RR index {}/{}) path={}",
            chrono_timestamp(),
            hash,
            peer_id,
            attempt + 1,
            candidate_peers.len(),
            if use_memory_path { "memory" } else { "numeric" }
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        let compute_request = if use_memory_path {
            ComputeRequest::InvokeModuleMemory {
                hash: hash.clone(),
                func_name: func_name.clone(),
                input_bytes: input_bytes.clone(),
                fuel_limit: fuel,
                max_memory_mb,
            }
        } else {
            ComputeRequest::InvokeModule {
                hash: hash.clone(),
                func_name: func_name.clone(),
                args: args.clone(),
                fuel_limit: fuel,
                max_memory_mb,
            }
        };

        if let Err(e) = state.cmd_tx.send(SwarmCommand::SendToPeer {
            target_peer: peer_id,
            request: compute_request,
            reply: reply_tx,
        }).await {
            let msg = format!("Worker {}: internal dispatch send failed: {}", peer_id, e);
            eprintln!("[Failover] {}", msg);
            peer_errors.push(msg);
            continue;
        }

        // 5-second execution timeout per candidate worker for fast failover
        match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
            Ok(Ok(Ok(response))) => {
                if let Some(err) = response.error {
                    // Check for saturation error - failover to next worker
                    if err.contains("saturated") || err.contains("capacity") {
                        let msg = format!("Worker {} saturated: {}", peer_id, err);
                        eprintln!("[Failover] {}. Retrying next peer...", msg);
                        peer_errors.push(msg);
                        continue;
                    }
                    
                    if err.contains("not found in memory cache") {
                        if let Some(bytes) = state.raw_byte_cache.read().await.get(&hash).cloned() {
                            println!("[Failover] Worker {} missed cache for '{}'. On-demand JIT deploying...", peer_id, hash);
                            let deploy_req = ComputeRequest::DeployModule {
                                hash: hash.clone(),
                                file_bytes: bytes,
                            };
                            let (dep_tx, dep_rx) = oneshot::channel();
                            let _ = state.cmd_tx.send(SwarmCommand::SendToPeer {
                                target_peer: peer_id,
                                request: deploy_req,
                                reply: dep_tx,
                            }).await;
                            let _ = dep_rx.await;

                            let (re_tx, re_rx) = oneshot::channel();
                            let re_invoke = if use_memory_path {
                                ComputeRequest::InvokeModuleMemory {
                                    hash: hash.clone(),
                                    func_name: func_name.clone(),
                                    input_bytes: input_bytes.clone(),
                                    fuel_limit: fuel,
                                    max_memory_mb,
                                }
                            } else {
                                ComputeRequest::InvokeModule {
                                    hash: hash.clone(),
                                    func_name: func_name.clone(),
                                    args: args.clone(),
                                    fuel_limit: fuel,
                                    max_memory_mb,
                                }
                            };
                            let _ = state.cmd_tx.send(SwarmCommand::SendToPeer {
                                target_peer: peer_id,
                                request: re_invoke,
                                reply: re_tx,
                            }).await;

                            if let Ok(Ok(Ok(re_resp))) = tokio::time::timeout(Duration::from_secs(5), re_rx).await {
                                if re_resp.error.is_none() {
                                    return (
                                        StatusCode::OK,
                                        Json(json!({
                                            "status": "success",
                                            "function_id": hash,
                                            "result": re_resp.results,
                                            "execution_micros": re_resp.execution_micros,
                                            "fuel_consumed": re_resp.fuel_consumed,
                                            "worker_peer_id": peer_id.to_string(),
                                            "failover_attempt": attempt + 1
                                        })),
                                    );
                                }
                            }
                        }
                    }

                    let msg = format!("Worker {} returned execution error: {}", peer_id, err);
                    eprintln!("[Failover] {}. Retrying next peer in pool...", msg);
                    peer_errors.push(msg);
                    continue;
                } else {
                    return (
                        StatusCode::OK,
                        Json(json!({
                            "status": "success",
                            "function_id": hash,
                            "result": response.results,
                            "execution_micros": response.execution_micros,
                            "fuel_consumed": response.fuel_consumed,
                            "worker_peer_id": peer_id.to_string(),
                            "failover_attempt": attempt + 1
                        })),
                    );
                }
            }
            Ok(Ok(Err(e))) => {
                let msg = format!("Worker {} P2P wire error: {:#}", peer_id, e);
                eprintln!("[Failover] {}. Retrying next peer in pool...", msg);
                peer_errors.push(msg);
                continue;
            }
            Ok(Err(_)) => {
                let msg = format!("Worker {} internal reply channel closed", peer_id);
                eprintln!("[Failover] {}. Retrying next peer in pool...", msg);
                peer_errors.push(msg);
                continue;
            }
            Err(_) => {
                let msg = format!("Worker {} timed out after 5 seconds", peer_id);
                eprintln!("[Failover] {}. Retrying next peer in pool...", msg);
                peer_errors.push(msg);
                continue;
            }
        }
    }

    // Fallback: If all remote worker peers failed or none were discovered
    println!(
        "[{}] All {} remote peers failed. Checking local gateway cache for '{}'",
        chrono_timestamp(),
        candidate_peers.len(),
        hash
    );

    try_local_fallback(state, hash, func_name, use_memory_path, input_bytes, args, fuel, max_memory_mb).await
}

async fn try_local_fallback(
    state: GatewayState,
    hash: String,
    func_name: String,
    use_memory_path: bool,
    input_bytes: Vec<u8>,
    args: Vec<i32>,
    fuel: u64,
    max_memory_mb: usize,
) -> (StatusCode, Json<Value>) {
    if let Some(module) = state.local_runner.get_cached_module(&hash).await {
        let metrics_result = if use_memory_path {
            state.local_runner.execute_module_memory(&module, &func_name, &input_bytes, fuel, max_memory_mb)
        } else {
            state.local_runner.execute_module(&module, &func_name, &args, fuel, max_memory_mb)
        };
        
        match metrics_result {
            Ok(metrics) => (
                StatusCode::OK,
                Json(json!({
                    "status": "success",
                    "function_id": hash,
                    "result": metrics.results,
                    "execution_micros": metrics.cold_start_micros,
                    "fuel_consumed": metrics.fuel_consumed,
                    "worker_peer_id": format!("{}-local-fallback", state.local_peer_id),
                    "failover_note": "Remote peers failed or timed out; executed on local gateway fallback"
                })),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "function_id": hash,
                    "error": format!("Local fallback execution failed: {:#}", e),
                    "worker_peer_id": format!("{}-local", state.local_peer_id)
                })),
            ),
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "status": "error",
                "function_id": hash,
                "error": "No workers available and module not in local cache",
                "worker_peer_id": "none"
            })),
        )
    }
}

/// POST /deploy (accepts raw binary, wat text, or multipart/form-data)
async fn deploy_handler(
    State(state): State<GatewayState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    // Auth check
    let api_key = extract_api_key(&headers);
    if !verify_api_key(api_key.as_deref()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "status": "error",
                "error": "Invalid or missing API key"
            })),
        );
    }

    let file_bytes = extract_module_bytes(&headers, &body);
    if file_bytes.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "status": "error",
                "error": "Empty body. Provide .wasm binary or .wat text bytes."
            })),
        );
    }

    let hash = compute_function_id(&file_bytes);

    // 1. Cache in local gateway runner
    if let Err(e) = state.local_runner.deploy_module(&hash, &file_bytes).await {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "status": "error",
                "error": format!("Failed to parse/compile module: {:#}", e)
            })),
        );
    }

    state.raw_byte_cache.write().await.insert(hash.clone(), file_bytes.clone());

    // 2. Broadcast deploy to all connected mDNS worker nodes
    let (reply_tx, reply_rx) = oneshot::channel();
    let deploy_request = ComputeRequest::DeployModule {
        hash: hash.clone(),
        file_bytes,
    };

    let _ = state.cmd_tx.send(SwarmCommand::BroadcastDeploy {
        request: deploy_request,
        reply: reply_tx,
    }).await;

    let broadcast_count = reply_rx.await.unwrap_or(0);

    println!(
        "[{}] Module '{}' successfully deployed to {} worker node(s)",
        chrono_timestamp(),
        hash,
        broadcast_count
    );

    (
        StatusCode::OK,
        Json(json!({
            "function_id": hash,
            "status": "success",
            "bytes_size": body.len(),
            "broadcast_workers_count": broadcast_count,
            "invoke_endpoint": format!("POST /invoke/{}?func=<FUNC>", hash)
        })),
    )
}

/// Legacy /execute?file=<PATH>&func=<FUNC>
#[derive(Deserialize)]
struct ExecuteQuery {
    func: Option<String>,
    file: String,
    #[serde(default)]
    fuel: Option<u64>,
    #[serde(default)]
    max_memory_mb: Option<usize>,
}

async fn execute_handler(
    State(state): State<GatewayState>,
    Query(query): Query<ExecuteQuery>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    // Auth check
    let api_key = extract_api_key(&headers);
    if !verify_api_key(api_key.as_deref()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "status": "error",
                "error": "Invalid or missing API key"
            })),
        );
    }

    let file_path = PathBuf::from(&query.file);
    if !file_path.exists() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "status": "error",
                "error": format!("File not found on gateway: '{}'", query.file)
            })),
        );
    }

    let file_bytes = match std::fs::read(&file_path) {
        Ok(b) => b,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))),
    };

    let hash = compute_function_id(&file_bytes);
    let _ = state.local_runner.deploy_module(&hash, &file_bytes).await;

    invoke_handler(
        State(state),
        AxumPath(hash),
        Query(InvokeQuery {
            func: query.func,
            fuel: query.fuel,
            max_memory_mb: query.max_memory_mb,
        }),
        headers,
        body,
    ).await
}

async fn peers_handler(State(state): State<GatewayState>) -> Json<Value> {
    let peers = state.active_peers.read().await;
    let list: Vec<String> = peers.iter().map(|p| p.to_string()).collect();
    Json(json!({
        "status": "ok",
        "count": list.len(),
        "active_workers": list,
        "local_gateway_peer_id": state.local_peer_id.to_string()
    }))
}

async fn run_gateway(
    http_host: &str,
    http_port: u16,
    p2p_port: u16,
    bootstrap: Option<Multiaddr>,
) -> Result<()> {
    println!("============================================================");
    println!("     MAKO HTTP Edge Ingress Gateway (Content-Addressed)     ");
    println!("============================================================");

    let key_path = Path::new(".mako_gateway.key");
    let id_keys = get_or_create_identity(key_path)?;
    let local_peer_id = PeerId::from(id_keys.public());

    println!("[Gateway Identity]");
    println!("  * Gateway PeerId: {}", local_peer_id);

    let active_peers: Arc<RwLock<Vec<PeerId>>> = Arc::new(RwLock::new(Vec::new()));
    let local_runner = Arc::new(IsolatedWasmRunner::new()?);
    let raw_byte_cache = Arc::new(RwLock::new(HashMap::new()));

    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SwarmCommand>(64);

    let mut swarm = SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|keypair| {
            let local_peer_id = keypair.public().to_peer_id();
            let mdns = mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                local_peer_id,
            )?;
            let request_response = cbor::Behaviour::<ComputeRequest, ComputeResponse>::new(
                [(COMPUTE_PROTOCOL, ProtocolSupport::Full)],
                request_response::Config::default(),
            );
            let store = MemoryStore::new(local_peer_id);
            let kad = KadBehaviour::new(local_peer_id, store);

            Ok(MakoBehaviour {
                request_response,
                mdns,
                kad,
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();

    let p2p_listen_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", p2p_port).parse()?;
    swarm.listen_on(p2p_listen_addr)?;

    let active_peers_clone = Arc::clone(&active_peers);
    let bootstrap_clone = bootstrap.clone();

    // Spawn Background libp2p Swarm actor
    tokio::spawn(async move {
        let mut pending_queries: HashMap<OutboundRequestId, oneshot::Sender<Result<ComputeResponse>>> =
            HashMap::new();

        // Handle initial gateway bootstrap seed if provided
        if let Some(ref b_addr) = bootstrap_clone {
            println!("[Gateway Bootstrap] Connecting to seed peer: {}", b_addr);
            match parse_peer_multiaddr(b_addr) {
                Ok((seed_peer_id, transport_addr)) => {
                    let clean_addr = strip_peer_id(transport_addr);
                    swarm.behaviour_mut().kad.add_address(&seed_peer_id, clean_addr.clone());
                    swarm.add_peer_address(seed_peer_id, clean_addr);
                    {
                        let mut peers = active_peers_clone.write().await;
                        if !peers.contains(&seed_peer_id) && seed_peer_id != local_peer_id {
                            peers.push(seed_peer_id);
                        }
                    }

                    if let Err(e) = swarm.dial(b_addr.clone()) {
                        eprintln!("[Gateway Bootstrap] Dial error: {:#}", e);
                    } else {
                        match swarm.behaviour_mut().kad.bootstrap() {
                            Ok(_) => println!("[Gateway Bootstrap] Kademlia DHT bootstrap discovery running..."),
                            Err(e) => eprintln!("[Gateway Bootstrap] Kademlia bootstrap note: {:?}", e),
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[Gateway Bootstrap] Invalid multiaddress: {}. Attempting dial...", e);
                    let _ = swarm.dial(b_addr.clone());
                }
            }
        }

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        SwarmCommand::SendToPeer { target_peer, request, reply } => {
                            let _ = swarm.dial(target_peer);
                            let req_id = swarm.behaviour_mut().request_response.send_request(&target_peer, request);
                            pending_queries.insert(req_id, reply);
                        }
                        SwarmCommand::BroadcastDeploy { request, reply } => {
                            let peers = active_peers_clone.read().await;
                            let count = peers.len();
                            for peer in peers.iter() {
                                let _ = swarm.dial(*peer);
                                let _ = swarm.behaviour_mut().request_response.send_request(peer, request.clone());
                            }
                            let _ = reply.send(count);
                        }
                    }
                }
                event = swarm.select_next_some() => {
                    match event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            println!("[P2P Mesh] Gateway listening on: {}", address);
                        }
                        SwarmEvent::Behaviour(MakoBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                            for (peer_id, multiaddr) in list {
                                if peer_id == local_peer_id {
                                    continue;
                                }
                                let clean_addr = strip_peer_id(multiaddr);
                                println!("[mDNS] Discovered worker node: {} at {}", peer_id, clean_addr);
                                swarm.add_peer_address(peer_id, clean_addr.clone());
                                swarm.behaviour_mut().kad.add_address(&peer_id, clean_addr);
                                let mut peers = active_peers_clone.write().await;
                                if !peers.contains(&peer_id) {
                                    peers.push(peer_id);
                                }
                            }
                        }
                        SwarmEvent::Behaviour(MakoBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                            for (peer_id, multiaddr) in list {
                                println!("[mDNS] Worker node expired: {} at {}", peer_id, multiaddr);
                                active_peers_clone.write().await.retain(|p| p != &peer_id);
                            }
                        }
                        SwarmEvent::Behaviour(MakoBehaviourEvent::Kad(kad_event)) => {
                            match kad_event {
                                kad::Event::RoutingUpdated { peer, addresses, .. } => {
                                    if peer != local_peer_id {
                                        for addr in addresses.iter() {
                                            let clean_addr = strip_peer_id(addr.clone());
                                            swarm.add_peer_address(peer, clean_addr);
                                        }
                                        let mut peers = active_peers_clone.write().await;
                                        if !peers.contains(&peer) {
                                            peers.push(peer);
                                        }
                                    }
                                }
                                kad::Event::OutboundQueryProgressed { result, .. } => {
                                    if let kad::QueryResult::Bootstrap(Ok(kad::BootstrapOk { peer, .. })) = result {
                                        if peer != local_peer_id {
                                            let mut peers = active_peers_clone.write().await;
                                            if !peers.contains(&peer) {
                                                peers.push(peer);
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        SwarmEvent::Behaviour(MakoBehaviourEvent::RequestResponse(request_response::Event::Message {
                            message: request_response::Message::Response { request_id, response },
                            ..
                        })) => {
                            if let Some(reply) = pending_queries.remove(&request_id) {
                                let _ = reply.send(Ok(response));
                            }
                        }
                        SwarmEvent::Behaviour(MakoBehaviourEvent::RequestResponse(request_response::Event::OutboundFailure {
                            request_id,
                            error,
                            ..
                        })) => {
                            if let Some(reply) = pending_queries.remove(&request_id) {
                                let _ = reply.send(Err(anyhow::anyhow!("P2P wire failure: {:?}", error)));
                            }
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                            if peer_id != local_peer_id {
                                let remote_addr = strip_peer_id(endpoint.get_remote_address().clone());
                                swarm.add_peer_address(peer_id, remote_addr.clone());
                                swarm.behaviour_mut().kad.add_address(&peer_id, remote_addr);
                                let mut peers = active_peers_clone.write().await;
                                if !peers.contains(&peer_id) {
                                    peers.push(peer_id);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    // Configure Axum HTTP router
    let gateway_state = GatewayState {
        cmd_tx,
        active_peers,
        rr_index: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        local_runner,
        local_peer_id,
        raw_byte_cache,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Body size limit layer (2MB)
    let body_limit = RequestBodyLimitLayer::new(MAX_PAYLOAD_SIZE);

    let app = Router::new()
        .route("/deploy", post(deploy_handler))
        .route("/invoke/:hash", post(invoke_handler))
        .route("/execute", post(execute_handler))
        .route("/peers", get(peers_handler))
        .layer(cors)
        .layer(body_limit)
        .with_state(gateway_state);

    let bind_addr = format!("{}:{}", http_host, http_port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await
        .with_context(|| format!("Failed to bind HTTP server to {}", bind_addr))?;

    println!("\n[HTTP Ingress Online]");
    println!("  * Deploy Endpoint: POST http://{}/deploy", bind_addr);
    println!("  * Invoke Endpoint: POST http://{}/invoke/:hash?func=<FUNC>", bind_addr);
    println!("  * Active Peers:    GET  http://{}/peers", bind_addr);
    println!("  * Status: Ready to receive content-addressed compute traffic...\n");

    axum::serve(listener, app).await?;
    Ok(())
}

// ============================================================================
// 7. CLI DEPLOY & DISPATCH
// ============================================================================

async fn run_cli_deploy(
    wasm_file: PathBuf,
    gateway_url: String,
    peer: Option<Multiaddr>,
) -> Result<()> {
    if !wasm_file.exists() {
        bail!("Target WebAssembly file not found: '{}'", wasm_file.display());
    }

    let file_bytes = std::fs::read(&wasm_file)
        .with_context(|| format!("Failed to read file: '{}'", wasm_file.display()))?;
    let hash = compute_function_id(&file_bytes);

    println!("============================================================");
    println!("     MAKO Content-Addressed Module Deployment               ");
    println!("============================================================");
    println!("  * File: '{}' ({} bytes)", wasm_file.display(), file_bytes.len());
    println!("  * SHA-256 Hash ID: {}", hash);

    if let Some(target_peer_addr) = peer {
        let mut dial_addr = target_peer_addr.clone();
        let target_peer_id = match dial_addr.pop() {
            Some(libp2p::multiaddr::Protocol::P2p(id)) => id,
            _ => target_peer_addr.iter().find_map(|p| match p {
                libp2p::multiaddr::Protocol::P2p(id) => Some(id),
                _ => None,
            }).context("Multiaddress must include /p2p/<PeerId>")?,
        };

        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
            .with_behaviour(|_| {
                cbor::Behaviour::<ComputeRequest, ComputeResponse>::new(
                    [(COMPUTE_PROTOCOL, ProtocolSupport::Full)],
                    request_response::Config::default(),
                )
            })?
            .build();

        println!(">>> Broadcasting to peer: {}...", target_peer_id);
        swarm.dial(target_peer_addr)?;

        let req = ComputeRequest::DeployModule {
            hash: hash.clone(),
            file_bytes,
        };
        let req_id = swarm.behaviour_mut().send_request(&target_peer_id, req);

        let timeout = tokio::time::sleep(Duration::from_secs(15));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                _ = &mut timeout => bail!("Timeout waiting for peer deployment confirmation"),
                event = swarm.select_next_some() => {
                    if let SwarmEvent::Behaviour(request_response::Event::Message {
                        message: request_response::Message::Response { request_id, response },
                        ..
                    }) = event {
                        if request_id == req_id {
                            if let Some(err) = response.error {
                                bail!("Remote deployment error: {}", err);
                            }
                            break;
                        }
                    }
                }
            }
        }
    } else {
        let mut deployed_via_gateway = false;
        println!(">>> Broadcasting deploy via Gateway at: {}...", gateway_url);
        match post_bytes_to_gateway(&gateway_url, &file_bytes).await {
            Ok(body) => {
                println!("  * Gateway Response: {}", body.trim());
                deployed_via_gateway = true;
            }
            Err(e) => {
                println!("  * Gateway not currently running at '{}' ({})", gateway_url, e);
                println!("  * Attempting direct mDNS broadcast over libp2p...");
            }
        }

        if !deployed_via_gateway {
            let mut swarm = SwarmBuilder::with_new_identity()
                .with_tokio()
                .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
                .with_behaviour(|keypair| {
                    let local_peer_id = keypair.public().to_peer_id();
                    let mdns = mdns::tokio::Behaviour::new(
                        mdns::Config::default(),
                        local_peer_id,
                    )?;
                    let request_response = cbor::Behaviour::<ComputeRequest, ComputeResponse>::new(
                        [(COMPUTE_PROTOCOL, ProtocolSupport::Full)],
                        request_response::Config::default(),
                    );
                    let store = MemoryStore::new(local_peer_id);
                    let kad = KadBehaviour::new(local_peer_id, store);

                    Ok(MakoBehaviour {
                        request_response,
                        mdns,
                        kad,
                    })
                })?
                .build();

            swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

            let discover_timeout = tokio::time::sleep(Duration::from_millis(1500));
            tokio::pin!(discover_timeout);

            let mut discovered = Vec::new();
            println!("  * Scanning local network for worker peers via mDNS...");
            loop {
                tokio::select! {
                    _ = &mut discover_timeout => break,
                    event = swarm.select_next_some() => {
                        if let SwarmEvent::Behaviour(MakoBehaviourEvent::Mdns(mdns::Event::Discovered(list))) = event {
                            for (peer_id, multiaddr) in list {
                                swarm.add_peer_address(peer_id, multiaddr);
                                discovered.push(peer_id);
                            }
                        }
                    }
                }
            }

            if discovered.is_empty() {
                println!("  * Note: No active worker peers discovered on local network.");
            } else {
                let req = ComputeRequest::DeployModule {
                    hash: hash.clone(),
                    file_bytes: file_bytes.clone(),
                };
                for peer_id in &discovered {
                    println!("  * Broadcasted module to worker: {}", peer_id);
                    swarm.behaviour_mut().request_response.send_request(peer_id, req.clone());
                }
            }
        }
    }

    println!("\n============================================================");
    println!("Deployment successful!");
    println!("Function ID: {}", hash);
    println!("Invoke via: POST http://localhost:8080/invoke/{}?func=<FUNC>", hash);
    println!("============================================================");

    Ok(())
}

async fn run_dispatch(
    peer_addr: Multiaddr,
    wasm_file: PathBuf,
    func: String,
    args: Vec<i32>,
    fuel: u64,
    max_memory_mb: usize,
) -> Result<()> {
    if !wasm_file.exists() {
        bail!("Target WebAssembly file not found: '{}'", wasm_file.display());
    }

    let mut dial_addr = peer_addr.clone();
    let target_peer_id = match dial_addr.pop() {
        Some(libp2p::multiaddr::Protocol::P2p(peer_id)) => peer_id,
        _ => peer_addr.iter().find_map(|p| match p {
            libp2p::multiaddr::Protocol::P2p(id) => Some(id),
            _ => None,
        }).context("Multiaddress must include peer ID")?,
    };

    let file_bytes = std::fs::read(&wasm_file)?;
    let hash = compute_function_id(&file_bytes);

    let compute_request = ComputeRequest::ExecuteRaw {
        file_bytes,
        func_name: func,
        args,
        fuel_limit: fuel,
        max_memory_mb,
    };

    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        .with_behaviour(|_| {
            cbor::Behaviour::<ComputeRequest, ComputeResponse>::new(
                [(COMPUTE_PROTOCOL, ProtocolSupport::Full)],
                request_response::Config::default(),
            )
        })?
        .build();

    println!(">>> Connecting to remote peer {}...", target_peer_id);
    swarm.dial(peer_addr)?;

    let req_id = swarm.behaviour_mut().send_request(&target_peer_id, compute_request);
    let timeout = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => bail!("Timeout waiting for peer response"),
            event = swarm.select_next_some() => {
                if let SwarmEvent::Behaviour(request_response::Event::Message {
                    message: request_response::Message::Response { request_id, response },
                    ..
                }) = event {
                    if request_id == req_id {
                        if let Some(err) = response.error {
                            bail!("Remote computation failed: {}", err);
                        } else {
                            println!("\n============================================================");
                            println!("             REMOTE EXECUTION SUMMARY                       ");
                            println!("============================================================");
                            println!("  * Function ID: {}", hash);
                            println!("  * Output: {}", response.results.join(", "));
                            println!("  * Execution Time: {} \u{00B5}s ({:.3} ms)",
                                response.execution_micros,
                                response.execution_micros as f64 / 1000.0
                            );
                            println!("  * Fuel Consumed: {} units\n", response.fuel_consumed);
                            println!("============================================================");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// 8. MAIN ENTRY POINT
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Daemon { port, host, bootstrap }) => {
            run_daemon(&host, port, bootstrap).await?;
        }
        Some(Commands::Gateway { port, host, p2p_port, bootstrap }) => {
            run_gateway(&host, port, p2p_port, bootstrap).await?;
        }
        Some(Commands::Deploy { wasm_file, gateway_url, peer }) => {
            run_cli_deploy(wasm_file, gateway_url, peer).await?;
        }
        Some(Commands::Dispatch {
            peer_addr,
            wasm_file,
            func,
            args,
            fuel,
            max_memory_mb,
        }) => {
            run_dispatch(peer_addr, wasm_file, func, args, fuel, max_memory_mb).await?;
        }
        Some(Commands::Run {
            wasm_file,
            func,
            args,
            fuel,
            max_memory_mb,
        }) => {
            println!("============================================================");
            println!("     MAKO Local WebAssembly Runner (wasmtime)               ");
            println!("============================================================");

            let runner = IsolatedWasmRunner::new()?;
            let metrics = runner.execute_file(&wasm_file, &func, &args, fuel, max_memory_mb).await?;

            println!("[Execution Metrics]");
            println!("  * Output: {}", if metrics.results.is_empty() { "(void)".to_string() } else { metrics.results.join(", ") });
            println!("  * Cold-Start Execution Time: {} \u{00B5}s ({:.3} ms)", 
                metrics.cold_start_micros, 
                metrics.cold_start_micros as f64 / 1000.0
            );
            println!("  * Fuel Consumed: {} units", metrics.fuel_consumed);
            println!("  * Fuel Remaining: {} units", metrics.fuel_remaining);
            println!("============================================================");
        }
        None => {
            println!("============================================================");
            println!("     MAKO: Sovereign Edge Compute Engine (CLI)              ");
            println!("============================================================");
            println!("Usage:");
            println!("  1. Start Worker Daemon (with in-memory module cache):");
            println!("     cargo run -- daemon --port 4001\n");
            println!("  2. Start HTTP Gateway (with mDNS & Kademlia mesh routing):");
            println!("     cargo run -- gateway --port 8080\n");
            println!("  3. Deploy Wasm Module (Content-Addressed):");
            println!("     cargo run -- deploy fixtures/multiply.wat\n");
            println!("  4. Invoke Cached Module via HTTP:");
            println!("     curl -X POST http://localhost:8080/invoke/<HASH>?func=multiply -d '[6, 7]'\n");
            println!("See 'cargo run -- --help' for all options.");
            println!("============================================================");
        }
    }

    Ok(())
}
