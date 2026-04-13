use kvrpc::{kv_service_client::KvServiceClient, DeleteReq, GetReq, PutReq, ScanReq, SwapReq};
use mngr::{cluster_manager_service_client::ClusterManagerServiceClient, Partition, PartitionReq};
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic::Request;

pub mod kvrpc {
    tonic::include_proto!("kvrpc");
}

pub mod mngr {
    tonic::include_proto!("mngr");
}

fn djb2_hash(key: &str) -> u64 {
    let mut hash: u64 = 5381;
    for byte in key.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(u64::from(byte));
    }
    hash
}

#[derive(Debug, Clone)]
struct PartitionInfo {
    n_parts: u64,
    partitions: Vec<Partition>,
}

const RETRY_DELAY: Duration = Duration::from_secs(3);

#[derive(Debug)]
struct KVClient {
    manager_addr: String,
    partitions: Arc<Mutex<Option<PartitionInfo>>>,
    kv_clients: Arc<Mutex<HashMap<String, KvServiceClient<Channel>>>>,
    leader_cache: Arc<Mutex<HashMap<usize, String>>>, // part_idx -> leader_addr
    verbose: bool,
    quiet: bool,
}

impl KVClient {
    async fn new(manager_addr: String, debug: bool, quiet: bool) -> Self {
        let client = KVClient {
            manager_addr,
            partitions: Arc::new(Mutex::new(None)),
            kv_clients: Arc::new(Mutex::new(HashMap::new())),
            leader_cache: Arc::new(Mutex::new(HashMap::new())),
            verbose: debug,
            quiet: quiet,
        };
        client.fetch_partitions_with_retry().await;
        client
    }

    async fn verbose(&self, msg: String) {
        if self.quiet {
            return;
        }
        if self.verbose {
            eprintln!("{}", msg);
        }
    }

    async fn pinfo(&self, msg: String) {
        if self.quiet {
            return;
        }
        eprintln!("{}", msg);
    }

    async fn fetch_partitions_with_retry(&self) {
        loop {
            self.verbose(format!("Connecting to manager: {}", self.manager_addr))
                .await;

            match ClusterManagerServiceClient::connect(self.manager_addr.clone()).await {
                Ok(mut cluster_client) => loop {
                    let request = Request::new(PartitionReq {
                        client_id: "client-1".to_string(),
                    });

                    match cluster_client.get_partition_assignments(request).await {
                        Ok(response) => {
                            let resp_parts = response.into_inner().partitions;
                            if resp_parts.is_empty() {
                                self.pinfo(format!("Empty partition list, retrying..."))
                                    .await;
                                sleep(RETRY_DELAY).await;
                                continue;
                            }
                            let partition_info = PartitionInfo {
                                n_parts: resp_parts.len() as u64,
                                partitions: resp_parts,
                            };

                            let mut partitions = self.partitions.lock().await;
                            *partitions = Some(partition_info);
                            self.pinfo(format!("Partitions fetched successfully."))
                                .await;
                            return;
                        }
                        Err(e) => {
                            let err_msg = e.message();
                            self.pinfo(format!(
                                "Failed to fetch partitions: {}. Retrying...",
                                err_msg
                            ))
                            .await;
                            sleep(RETRY_DELAY).await;
                        }
                    }
                },
                Err(e) => {
                    let err_msg = e.to_string();
                    self.pinfo(format!(
                        "Failed to connect to manager: {}. Retrying...",
                        err_msg
                    ))
                    .await;
                    sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    async fn get_or_connect_client(&self, server_addr: &str) -> KvServiceClient<Channel> {
        let mut kv_clients = self.kv_clients.lock().await;

        if !kv_clients.contains_key(server_addr) {
            let full_addr = format!("http://{}", server_addr);
            loop {
                match KvServiceClient::connect(full_addr.clone()).await {
                    Ok(client) => {
                        kv_clients.insert(server_addr.to_string(), client);
                        break;
                    }
                    Err(e) => {
                        self.pinfo(format!(
                            "Retrying connection to server {}: {}",
                            server_addr, e
                        ))
                        .await;
                        sleep(RETRY_DELAY).await;
                    }
                }
            }
        }

        kv_clients.get(server_addr).unwrap().clone()
    }

    async fn find_partition(&self, key: &str) -> Option<(usize, Partition)> {
        let hash = djb2_hash(key);
        let partitions_guard = self.partitions.lock().await;
        let partition_info = partitions_guard.as_ref()?;
        let part_idx = (hash % partition_info.n_parts as u64) as usize;
        partition_info
            .partitions
            .get(part_idx)
            .cloned()
            .map(|p| (part_idx, p))
    }

    async fn extract_leader_replica_id(&self, errmsg: &str) -> Option<u32> {
        if !errmsg.contains("NotLeader") {
            return None;
        }
        errmsg.split(':').nth(1)?.trim().parse::<u32>().ok()
    }

    async fn find_working_leader_addr(
        &self,
        part_idx: usize,
        partition: &Partition,
    ) -> Option<String> {
        {
            let leader_cache = self.leader_cache.lock().await;
            if let Some(addr) = leader_cache.get(&part_idx) {
                return Some(addr.clone()); // use cached leader if available
            }
        }

        // find the leader by pinging replicas
        self.verbose(format!(
            "Finding working leader for partition {}...",
            part_idx
        ))
        .await;
        loop {
            for replica in &partition.replicas {
                self.verbose(format!(
                    "\tpinging replica {} for partition {}",
                    replica.addr, part_idx
                ))
                .await;
                let mut client = self.get_or_connect_client(&replica.addr).await;
                let req = GetReq {
                    key: format!("__ping__{}", part_idx),
                };
                let res = client.get(Request::new(req)).await;
                if res.is_ok()
                    || res
                        .as_ref()
                        .err()
                        .map(|e| e.message().contains("NotLeader"))
                        .unwrap_or(false)
                {
                    let mut cache = self.leader_cache.lock().await;
                    cache.insert(part_idx, replica.addr.clone());
                    self.pinfo(format!(
                        "\tfind_working_leader_addr: Partition {} leader {}",
                        part_idx, replica.addr
                    ))
                    .await;
                    return Some(replica.addr.clone());
                }
            }
        }
    }

    async fn handle_command(&self, command: &str, key: String, value: Option<String>) {
        self.verbose(format!(
            "handling {} {} {}",
            command,
            key,
            value.as_deref().unwrap_or("")
        ))
        .await;
        if let Some((part_idx, partition)) = self.find_partition(&key).await {
            let mut server_addr = self.find_working_leader_addr(part_idx, &partition).await;

            loop {
                let mut attempt = 0;
                while let Some(addr) = server_addr.clone() {
                    attempt += 1;
                    let mut client = self.get_or_connect_client(&addr).await;
                    self.verbose(format!("\tsending {} to {}...", command, addr))
                        .await;
                    let result = match command {
                        "PUT" => {
                            let req = PutReq {
                                key: key.clone(),
                                value: value.clone().unwrap_or_default(),
                            };
                            client.put(Request::new(req)).await.map(|r| {
                                let r = r.into_inner();
                                let found = r.found;
                                println!(
                                    "PUT {} {}",
                                    key,
                                    if found { "found" } else { "not_found" }
                                );

                                if self.verbose {
                                    eprintln!("\tresponse: {:?}", r);
                                }
                            })
                        }
                        "GET" => {
                            let req = GetReq { key: key.clone() };
                            client.get(Request::new(req)).await.map(|r| {
                                let r = r.into_inner();
                                let val = r.clone().value;
                                println!(
                                    "GET {} {}",
                                    key,
                                    if val == "null" { "null" } else { &val }
                                );

                                if self.verbose {
                                    eprintln!("\tresponse: {:?}", r);
                                }
                            })
                        }
                        "DELETE" => {
                            let req = DeleteReq { key: key.clone() };
                            client.delete(Request::new(req)).await.map(|r| {
                                let r = r.into_inner();
                                let found = r.found;
                                println!(
                                    "DELETE {} {}",
                                    key,
                                    if found { "found" } else { "not_found" }
                                );

                                if self.verbose {
                                    eprintln!("\tresponse: {:?}", r);
                                }
                            })
                        }
                        "SWAP" => {
                            let req = SwapReq {
                                key: key.clone(),
                                value: value.clone().unwrap_or_default(),
                            };
                            client.swap(Request::new(req)).await.map(|r| {
                                let r = r.into_inner();
                                let old = r.clone().old_value;
                                println!(
                                    "SWAP {} {}",
                                    key,
                                    if old == "null" { "null" } else { &old }
                                );

                                if self.verbose {
                                    eprintln!("\tresponse: {:?}", r);
                                }
                            })
                        }
                        _ => {
                            eprintln!("Unknown command: {}", command);
                            return;
                        }
                    };

                    match result {
                        Ok(_) => {
                            return;
                        }
                        Err(err) => {
                            if err.message().contains("NotLeader") {
                                // extract the new leader ID from the error message
                                if let Some(leader_id) =
                                    self.extract_leader_replica_id(err.message()).await
                                {
                                    server_addr = partition
                                        .replicas
                                        .iter()
                                        .find(|r| r.rep_id == leader_id)
                                        .map(|r| r.addr.clone());

                                    // update leader cache
                                    if let Some(addr) = &server_addr {
                                        let mut cache = self.leader_cache.lock().await;
                                        cache.insert(part_idx, addr.clone());
                                        self.verbose(format!(
                                            "\tupdated leader cache for partition {}: new leader {}",
                                            part_idx, addr
                                        )).await;
                                        drop(cache);
                                        self.pinfo(format!("\tRetrying {} {}...", command, key))
                                            .await;
                                        continue;
                                    }
                                }
                            } else {
                                if !err.message().contains("raft_write") {
                                    let err_msg = err.message();
                                    self.pinfo(format!(
                                        "\terror {} part {} ({}): {}",
                                        command, part_idx, addr, err_msg
                                    ))
                                    .await;
                                }
                                if attempt > 3 {
                                    break; // too many attempts on this server
                                }
                            }
                        }
                    }
                }

                // no clear leader, try next replica or retry
                {
                    let mut cache = self.leader_cache.lock().await;
                    cache.remove(&part_idx);
                    drop(cache);
                }
                server_addr = self.find_working_leader_addr(part_idx, &partition).await;
                if server_addr.is_none() {
                    self.verbose(format!(
                        "\tNo working leader found for partition {}",
                        part_idx
                    ))
                    .await;
                }
                eprintln!("\tRetrying {} {}...", command, key);
                sleep(RETRY_DELAY).await;
            }
        }
    }

    async fn scan(&self, start_key: String, end_key: String) {
        self.verbose(format!("SCAN {} {}", start_key, end_key))
            .await;
        println!("SCAN {} {} BEGIN", start_key, end_key);

        let mut all_items = vec![];

        loop {
            all_items.clear();
            let part_info = self.partitions.lock().await;
            let part_info = match &*part_info {
                Some(info) => info.clone(),
                None => {
                    eprintln!("No partition info available.");
                    sleep(RETRY_DELAY).await;
                    continue;
                }
            };

            let mut success = true;
            for (part_idx, partition) in part_info.partitions.iter().enumerate() {
                let mut server_addr = self.find_working_leader_addr(part_idx, partition).await;

                let mut partition_scanned = false;

                while let Some(addr) = server_addr.clone() {
                    let mut client = self.get_or_connect_client(&addr).await;
                    let req = ScanReq {
                        start_key: start_key.clone(),
                        end_key: end_key.clone(),
                    };

                    match client.scan(Request::new(req)).await {
                        Ok(resp) => {
                            let resp = resp.into_inner();
                            self.verbose(format!(
                                "\tSCAN response from partition {} ({}): {:?}",
                                part_idx, addr, resp.items
                            ))
                            .await;
                            all_items.extend(resp.items);
                            partition_scanned = true;
                            break;
                        }
                        Err(err) => {
                            if err.message().contains("NotLeader") {
                                // extract the new leader ID from the error message
                                if let Some(leader_id) =
                                    self.extract_leader_replica_id(err.message()).await
                                {
                                    server_addr = partition
                                        .replicas
                                        .iter()
                                        .find(|r| r.rep_id == leader_id)
                                        .map(|r| r.addr.clone());

                                    // update leader cache
                                    if let Some(addr) = &server_addr {
                                        let mut cache = self.leader_cache.lock().await;
                                        cache.insert(part_idx, addr.clone());
                                        self.verbose(format!(
                                            "\tupdated leader cache for partition {}: new leader {}",
                                            part_idx, addr
                                        )).await;
                                        drop(cache);
                                        self.pinfo(format!(
                                            "\tRetrying {} {}...",
                                            start_key, end_key
                                        ))
                                        .await;
                                        sleep(RETRY_DELAY).await;
                                        continue;
                                    }
                                }
                            } else {
                                if !err.message().contains("raft_write") {
                                    let err_msg = err.message();
                                    self.pinfo(format!(
                                        "\t{} error on partition {} ({}): {}",
                                        "SCAN".to_string(),
                                        part_idx,
                                        addr,
                                        err_msg
                                    ))
                                    .await;
                                }

                                // no clear leader, try next replica or retry
                                let mut cache = self.leader_cache.lock().await;
                                cache.remove(&part_idx);
                                drop(cache);
                                server_addr =
                                    self.find_working_leader_addr(part_idx, &partition).await;
                                if server_addr.is_none() {
                                    self.verbose(format!(
                                        "\tNo working leader found for partition {}",
                                        part_idx
                                    ))
                                    .await;
                                }
                            }
                        }
                    }

                    eprintln!("\tRetrying partition {}...", part_idx);
                    sleep(RETRY_DELAY).await;
                }

                if !partition_scanned {
                    success = false;
                    eprintln!("Partition {} scan failed. Retrying whole scan...", part_idx);
                    break; // retry the whole scan
                }
            }

            if success {
                break;
            }
            sleep(RETRY_DELAY).await;
        }

        // Sort after gathering all
        all_items.sort_by(|a, b| a.key.cmp(&b.key));

        for kv in all_items {
            println!("  {} {}", kv.key, kv.value);
        }

        println!("SCAN END");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    // if args.len() == 4 && args[1] == "--run-tests" && args[2] == "--manager_addrs" {
    //     let manager_addr = args[3].clone();
    //     return run_tests(manager_addr).await;
    // }

    if args.len() < 3 || args[1] != "--manager_addrs" {
        eprintln!(
            "Usage: kvclient --manager_addrs <IP:PORT,IP:PORT,...> [--dump_partitions] [--verbose]"
        );
        eprintln!("       kvclient --run-tests --manager_addrs <IP:PORT>");
        std::process::exit(1);
    }

    let mut manager_addrs: Vec<String> = Vec::new();
    let mut verbose = false;
    let mut quiet = false;
    for i in 0..args.len() {
        match args[i].as_str() {
            "--manager_addrs" => {
                let addresses = args[i + 1].split(',').collect::<Vec<&str>>();
                // verify that the addresses are valid IP:PORT
                for addr in addresses {
                    if addr.parse::<SocketAddr>().is_err() {
                        eprintln!("Invalid address: {}, expected IP:PORT", addr);
                        std::process::exit(1);
                    }
                    manager_addrs.push(addr.to_string());
                }
            }
            "--verbose" => {
                verbose = true;
                eprintln!("Verbose mode enabled.");
            }
            "--quiet" => {
                quiet = true;
                eprintln!("Quiet mode enabled.");
            }
            _ => {}
        }
    }
    if manager_addrs.len() < 1 {
        eprintln!("No manager addresses provided.");
        std::process::exit(1);
    }
    let mut manager_addr = manager_addrs[0].clone(); // TODO: handle multiple addresses
    if !manager_addr.starts_with("http://") {
        manager_addr = format!("http://{}", manager_addr);
    }

    if args.len() == 4 && args[3] == "--dump_partitions" {
        dump_partitions(manager_addr.clone()).await?;
        return Ok(());
    }

    let client = KVClient::new(manager_addr.clone(), verbose, quiet).await;
    let stdin = io::stdin();

    eprintln!("Client Ready. Type commands:");
    for line in stdin.lock().lines() {
        let line = line?;
        let parts: Vec<&str> = line.trim().split_whitespace().collect();

        if parts.is_empty() {
            continue;
        }

        // match parts[0] {
        //     "PUT" if parts.len() == 3 => client.put(parts[1].into(), parts[2].into()).await,
        //     "GET" if parts.len() == 2 => client.get(parts[1].into()).await,
        //     "DELETE" if parts.len() == 2 => client.delete(parts[1].into()).await,
        //     "SWAP" if parts.len() == 3 => client.swap(parts[1].into(), parts[2].into()).await,
        //     "SCAN" if parts.len() == 3 => client.scan(parts[1].into(), parts[2].into()).await,
        //     "REFRESH" => client.fetch_partitions_with_retry().await,
        //     "STOP" => break,
        //     _ => eprintln!("Invalid command: {}", line),
        // }
        match parts[0] {
            "PUT" | "SWAP" if parts.len() == 3 => {
                client
                    .handle_command(parts[0], parts[1].into(), Some(parts[2].into()))
                    .await;
            }
            "GET" | "DELETE" if parts.len() == 2 => {
                client.handle_command(parts[0], parts[1].into(), None).await;
            }
            "SCAN" if parts.len() == 3 => client.scan(parts[1].into(), parts[2].into()).await,
            "REFRESH" => client.fetch_partitions_with_retry().await,
            "STOP" => {
                println!("STOP");
                break;
            }
            _ => eprintln!("Invalid command: {}", line),
        }
    }
    eprintln!("Exiting client...");
    Ok(())
}

// async fn run_tests(manager_addr: String) -> Result<(), Box<dyn std::error::Error>> {
//     println!("Running unit tests against manager: {}", manager_addr);

//     let manager_url = if !manager_addr.starts_with("http://") {
//         format!("http://{}", manager_addr)
//     } else {
//         manager_addr
//     };

//     let client = KVClient::new(manager_url.clone()).await;

//     // Test Case 1: Basic PUT + GET
//     println!("\n== Test 1: PUT + GET ==");
//     client.put("testkey".into(), "val1".into()).await;
//     client.get("testkey".into()).await;

//     // Test Case 2: SWAP
//     println!("\n== Test 2: SWAP ==");
//     client.swap("testkey".into(), "val2".into()).await;
//     client.get("testkey".into()).await;

//     // Test Case 3: DELETE + GET (should return null)
//     println!("\n== Test 3: DELETE + GET ==");
//     client.delete("testkey".into()).await;
//     client.get("testkey".into()).await;

//     // Test Case 4: SWAP non-existent key
//     println!("\n== Test 4: SWAP non-existent key ==");
//     client.swap("ghost".into(), "value".into()).await;

//     // Test Case 5: GET nonexistent
//     println!("\n== Test 5: GET nonexistent key ==");
//     client.get("ghost".into()).await;

//     println!("\n All test cases completed.\n");
//     Ok(())
// }

async fn dump_partitions(manager_addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster_client = ClusterManagerServiceClient::connect(manager_addr.clone()).await?;
    let request = Request::new(PartitionReq {
        client_id: "client-dump".to_string(),
    });
    let response = cluster_client.get_partition_assignments(request).await?;
    let resp = response.into_inner();
    let partitions = resp.partitions;
    println!("Partitions:");
    for part in partitions {
        println!("  Replicas: {:?}", part.replicas);
    }
    println!("Total partitions: {}", resp.n_parts);
    Ok(())
}
