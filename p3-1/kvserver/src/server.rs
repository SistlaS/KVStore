#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use core::fmt;
use log::{debug, error, info};
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tonic::{transport::Channel, Request};
pub mod mngr {
    tonic::include_proto!("mngr");
}
use mngr::cluster_manager_service_client::ClusterManagerServiceClient;
use mngr::{RegisterServerReq, RegisterServerResp};
use std::net::{IpAddr, UdpSocket};

pub const RETRY_DELAY_SEC: u64 = 5;
pub const MAX_RETRIES: u32 = 10;

fn get_local_ip() -> Option<IpAddr> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    socket.local_addr().ok().map(|addr| addr.ip())
}

fn djb2_hash(key: &str) -> u64 {
    let mut hash: u64 = 5381;
    for byte in key.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(u64::from(byte));
    }
    hash
}

#[derive(Debug)]
pub struct ServerConfig {
    pub part_id: u32,
    pub rep_id: u32,
    pub manager_addrs: Vec<String>,
    pub api_listen: String,
    pub p2p_listen: String,
    pub peers: Vec<String>,
    pub backer_path: String,
    pub use_rocksdb: bool,
    pub n_key_partitions: u32,
}

impl ServerConfig {
    // ./yourserver --partition_id 0 --replica_id 0
    // --manager_addrs 1.2.3.4:3666,8.7.6.5:3667,12.11.10.9:3668
    // --api_listen 0.0.0.0:3777 --p2p_listen 0.0.0.0:3707
    // --peer_addrs 5.6.7.8:3708,9.10.11.12:3709 --backer_path ./backer.s0.0
    pub fn from_args() -> Result<ServerConfig, String> {
        let args: Vec<String> = env::args().collect();
        if args.len() < 15 {
            error!(
                "Usage: {} \
                 --partition_id <ID> \
                --replica_id <ID> \
                --manager_addrs <IP:PORT,IP:PORT,...> \
                --api_listen <IP:PORT> \
                --p2p_listen <IP:PORT> \
                --peer_addrs <IP:PORT,IP:PORT,...> \
                --backer_path <PATH>",
                args[0]
            );
            std::process::exit(1);
        }

        let mut manager_addrs: Vec<String> = Vec::new();
        let mut part_id = 0;
        let mut rep_id = 0;
        let mut api_listen: String = "".to_string();
        let mut p2p_listen: String = "".to_string();
        let mut peers: Vec<String> = Vec::new();
        let mut backer_path: String = "".to_string();
        let mut use_rocksdb = false;

        for i in 0..args.len() {
            match args[i].as_str() {
                "--partition_id" => {
                    let part_id_str = args[i + 1].clone();
                    part_id = part_id_str
                        .parse::<u32>()
                        .map_err(|_| format!("Invalid partition ID: {}", part_id_str))?;
                }
                "--replica_id" => {
                    let rep_id_str = args[i + 1].clone();
                    rep_id = rep_id_str
                        .parse::<u32>()
                        .map_err(|_| format!("Invalid replica ID: {}", rep_id_str))?;
                }
                "--manager_addrs" => {
                    let addresses = args[i + 1].split(',').collect::<Vec<&str>>();
                    // verify that the addresses are valid IP:PORT
                    for addr in addresses {
                        if addr.parse::<SocketAddr>().is_err() {
                            return Err(format!("Invalid address: {}, expected IP:PORT", addr));
                        }
                        manager_addrs.push(addr.to_string());
                    }
                }
                "--api_listen" => {
                    let api_str = args[i + 1].clone();

                    if api_str.parse::<SocketAddr>().is_err() {
                        return Err(format!("Invalid address: {}, expected IP:PORT", api_str));
                    }
                    api_listen = api_str;
                }
                "--p2p_listen" => {
                    let p2p_str = args[i + 1].clone();
                    if p2p_str.parse::<SocketAddr>().is_err() {
                        return Err(format!("Invalid address: {}, expected IP:PORT", p2p_str));
                    }
                    p2p_listen = p2p_str;
                }
                "--peer_addrs" => {
                    let addresses = args[i + 1].clone();
                    if addresses == "none" {
                        continue; // no peers, replication factor is 1
                    }
                    let addresses = addresses.split(',').collect::<Vec<&str>>();
                    // verify that the addresses are valid IP:PORT
                    for addr in addresses {
                        if addr.parse::<SocketAddr>().is_err() {
                            return Err(format!("Invalid address: {}, expected IP:PORT", addr));
                        }
                        peers.push(addr.to_string());
                    }
                }
                "--backer_path" => {
                    backer_path = args[i + 1].clone();
                }
                "--rocks" => {
                    use_rocksdb = true;
                }
                _ => {}
            }
        }

        // validate the parsed arguments
        if api_listen.is_empty() {
            return Err("api_listen cannot be empty".to_string());
        }
        if p2p_listen.is_empty() {
            return Err("p2p_listen cannot be empty".to_string());
        }
        if backer_path.is_empty() {
            return Err("backer_path cannot be empty".to_string());
        }
        if manager_addrs.is_empty() {
            return Err("managers cannot be empty".to_string());
        }

        // create backer_path dir if it doesn't exist
        std::fs::create_dir_all(&backer_path)
            .map_err(|_| format!("Failed to create backer_path directory: {}", backer_path))?;

        let server_config = ServerConfig {
            part_id,
            rep_id,
            manager_addrs,
            api_listen,
            p2p_listen,
            peers,
            backer_path,
            use_rocksdb,
            n_key_partitions: 0,
        };
        Ok(server_config)
    }

    pub fn key_valid(&self, key: &String) -> bool {
        djb2_hash(key) % self.n_key_partitions as u64 == self.part_id as u64
    }
}

// ---------------------------------------------------------------
// ----------------- COMMUNICATION WITH MANAGER -----------------
// ---------------------------------------------------------------

pub async fn connect_to_manager(
    addr: &str,
) -> Result<ClusterManagerServiceClient<Channel>, String> {
    let addr = format!("http://{}", addr);
    debug!("Connecting to manager {}", addr);

    for attempt in 1..=MAX_RETRIES {
        let conn = ClusterManagerServiceClient::connect(addr.clone()).await;
        match conn {
            Ok(client) => {
                info!("Connected to manager {}!", addr);
                return Ok(client);
            }
            Err(e) => {
                debug!(
                    "Retrying connect to manager {}, attempt {}: {}",
                    addr, attempt, e
                );
                sleep(Duration::from_secs(RETRY_DELAY_SEC)).await;
            }
        }
    }
    Err(format!(
        "Failed to connect to manager after {} attempts",
        MAX_RETRIES
    ))
}

pub async fn register_with_manager(
    manager_addrs: Vec<String>,
    part_id: u32,
    rep_id: u32,
    api_listen: String,
) -> Result<u32, String> {
    debug!("Registering with manager...");
    // TODO: what if there are multiple manager addresses?
    let manager_addr = manager_addrs.get(0).ok_or("No manager address found")?;

    let mut mngr_client = connect_to_manager(manager_addr).await?;

    // register the server with the manager
    let server_addr = api_listen;
    let mut n_parts = None;
    for attempt in 1..=MAX_RETRIES {
        let req = Request::new(RegisterServerReq {
            part_id: part_id,
            rep_id: rep_id,
            server_address: server_addr.clone(),
        });
        let resp = mngr_client.register_server(req).await;
        match resp {
            Ok(response) => {
                let resp = response.into_inner();
                debug!("Response: {:?}", resp);
                n_parts = Some(resp.n_parts);
                break;
            }
            Err(e) => {
                debug!(
                    "Retrying registration with manager {}, attempt {}: {}",
                    manager_addr, attempt, e
                );
                sleep(Duration::from_secs(RETRY_DELAY_SEC)).await;
            }
        }
    }

    if n_parts.is_none() {
        let err_msg = format!(
            "Failed to register with manager {} after {} attempts",
            manager_addr, MAX_RETRIES
        );
        return Err(err_msg);
    }
    info!("Registered with manager {}!", manager_addr);
    eprintln!("Registered with manager {}!", manager_addr);
    eprintln!("Server ID: {} {}", part_id, rep_id);
    eprintln!("Server address: api {}", server_addr);
    Ok(n_parts.unwrap())
}
