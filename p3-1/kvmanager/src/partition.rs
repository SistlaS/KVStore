#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use log::{debug, error, info, warn};
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::net::SocketAddr;

const ALPHANUMERIC: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const KEY_LENGTH: usize = 3;
const DEFAULT_SEED: u64 = 42;

pub fn get_seeded_rng(seed: Option<u64>) -> StdRng {
    StdRng::seed_from_u64(seed.unwrap_or(DEFAULT_SEED))
}

fn number_to_string(mut num: usize, length: usize) -> String {
    let mut result = vec![ALPHANUMERIC[0]; length];
    let n = ALPHANUMERIC.len();
    for i in (0..length).rev() {
        result[i] = ALPHANUMERIC[num % n];
        num /= n;
    }
    String::from_utf8(result).unwrap()
}

#[derive(Debug, Clone)]
pub struct ManagerConfig {
    pub rep_id: u32,
    pub man_listen: String,
    pub p2p_listen: String,
    pub peers: Vec<String>,
    pub n_parts: u32,
    pub server_rf: u32,
    pub part_servers: Vec<Vec<String>>, // each inner vec is a list of replicas for a partition
    pub backer_path: String,
}

impl ManagerConfig {
    pub fn from_args() -> Result<ManagerConfig, String> {
        let args: Vec<String> = std::env::args().collect();
        if args.len() < 15 {
            error!(
                "Usage: {} \
            --replica_id <replica_id> \
            --man_listen <IP:PORT> \
            --p2p_listen <IP:PORT> \
            --peer_addrs <IP:PORT,IP:PORT,...> \
            --server_rf <replication_factor> \
            --server_addrs <IP:PORT,IP:PORT,...> \
            --backer_path <PATH>",
                args[0]
            );
            std::process::exit(1);
        }

        let mut rep_id = 0;
        let mut man_listen: String = "".to_string();
        let mut p2p_listen: String = "".to_string();
        let mut peers: Vec<String> = Vec::new();
        let mut server_rf = 0;
        let mut servers: Vec<String> = Vec::new();
        let mut backer_path: String = "".to_string();

        for i in 0..args.len() {
            match args[i].as_str() {
                "--replica_id" => {
                    let rep_id_str = args[i + 1].clone();
                    rep_id = rep_id_str
                        .parse::<u32>()
                        .map_err(|_| format!("Invalid replica ID: {}", rep_id_str))?;
                }
                "--man_listen" => {
                    let man_str = args[i + 1].clone();

                    if man_str.parse::<SocketAddr>().is_err() {
                        return Err(format!("Invalid address: {}, expected IP:PORT", man_str));
                    }
                    man_listen = man_str
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
                "--server_rf" => {
                    let rf_str = args[i + 1].clone();
                    server_rf = rf_str
                        .parse::<u32>()
                        .map_err(|_| format!("Invalid replication factor: {}", rf_str))?;
                }
                "--server_addrs" => {
                    let addresses = args[i + 1].clone();
                    let addresses = addresses.split(',').collect::<Vec<&str>>();
                    // verify that the addresses are valid IP:PORT
                    for addr in addresses {
                        if addr.parse::<SocketAddr>().is_err() {
                            return Err(format!("Invalid address: {}, expected IP:PORT", addr));
                        }
                        servers.push(addr.to_string());
                    }
                }
                "--backer_path" => {
                    backer_path = args[i + 1].clone();
                }
                _ => continue, // ignore other arguments
            }
        }

        let n_parts = servers.len() / server_rf as usize;
        if n_parts == 0 {
            return Err("Invalid replication factor".to_string());
        }

        let mut parts_replicas = Vec::new();
        let mut k = 0;
        for i in 0..n_parts {
            let mut replicas = Vec::new();
            for j in 0..server_rf {
                if k >= servers.len() {
                    break;
                }
                replicas.push(servers[k].clone());
                k += 1;
            }
            parts_replicas.push(replicas);
        }

        let config = ManagerConfig {
            rep_id,
            man_listen,
            p2p_listen,
            peers,
            server_rf,
            n_parts: n_parts as u32,
            part_servers: parts_replicas,
            backer_path,
        };
        Ok(config)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionManager {
    pub config: ManagerConfig,
    pub registered: Vec<Vec<bool>>,
}

impl PartitionManager {
    pub fn new(config: ManagerConfig) -> Self {
        let mut registered = Vec::new();
        for i in 0..config.n_parts {
            registered.push(Vec::new());
            for _ in 0..config.server_rf {
                registered[i as usize].push(false);
            }
        }
        let rng = get_seeded_rng(None);

        PartitionManager {
            config,
            registered: registered,
        }
    }

    pub fn register_server(
        &mut self,
        part_id: u32,
        rep_id: u32,
        server_addr: String,
    ) -> Result<(), String> {
        let server_addr = server_addr.clone();

        let part_id = part_id as usize;
        let rep_id = rep_id as usize;
        let server_port = server_addr.split(':').last().unwrap();

        let expected_addr = self.config.part_servers[part_id][rep_id].clone();
        if expected_addr != server_addr {
            let expected_port = expected_addr.split(':').last().unwrap();
            if expected_port != server_port {
                let errmsg = format!(
                    "Server {} is not expected to be registered at partition {} replica {}. ",
                    server_addr, part_id, rep_id
                );
                error!("{}", errmsg);
                return Err(errmsg);
            }
        }
        if self.registered[part_id][rep_id] {
            info!(
                "Server {} is already registered at partition {} replica {}",
                server_addr, part_id, rep_id
            );
        }
        self.registered[part_id][rep_id] = true;
        Ok(())
    }

    pub fn all_servers_registered(&self) -> bool {
        for part_no in 0..self.config.n_parts as usize {
            for rep_no in 0..self.config.server_rf as usize {
                if !self.registered[part_no][rep_no] {
                    return false;
                }
            }
        }
        eprintln!("All servers registered");
        true
    }
}
