/**
 * This is for testing the raft server.
 * It will create a server and start it.
 */
use raft::node;
use std::env;
use std::net::SocketAddr;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        let program_name = env::args().next().unwrap_or_else(|| "program".to_string());
        return Err(format!(
            "Missing required argument. Usage: {} <rf> <my_id>",
            program_name
        )
        .into());
    }

    let rf: u32 = args[1]
        .parse()
        .map_err(|_| "Invalid rf. Expected a number".to_string())?;
    println!("Replication factor: {}", rf);
    let my_id: u32 = args[2]
        .parse()
        .map_err(|_| "Invalid id. Expected a number".to_string())?;
    println!("My id: {}", my_id);

    let mut peer_addrs = Vec::new();
    let mut my_addr = String::new();
    let config_path = "config.txt";
    // config.txt looks like following:
    // server_id,ip_addr:port
    // 0,127.0.0.1:3777
    // 1,127.0.0.1:3778
    // 2,127.0.0.1:3779
    // read config.txt, extract peer_addrs and server_id
    let file = File::open(config_path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut i = 0;
    while let Some(line) = lines.next_line().await? {
        if i == 0 {
            i += 1;
            continue; // skip the csv header
        }
        if i - 1 == rf {
            // i-1 because we skip the header
            break; // stop reading after `rf` lines
        }

        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() != 2 {
            return Err("Invalid config file format. Expected format: <id,ipaddr:port>".into());
        }
        let addr = parts[1].to_string();
        addr.parse::<SocketAddr>()
            .map_err(|_| "Invalid ip addr inside config. Expected format: <IP:PORT>".to_string())?;

        if parts[0] == my_id.to_string() {
            my_addr = addr; // Assign the address to `my_addr`
        } else {
            peer_addrs.push(addr); // Add the address to `peer_addrs`
        }

        i += 1;
    }

    if my_addr.is_empty() {
        return Err(format!("Error: My id {} not found in config file", my_id).into());
    }

    println!("My addr {}", my_addr);
    println!("Peer addrs {:?}", peer_addrs);

    let dir_path = format!("./backer.s{}", my_id);
    std::fs::create_dir_all(&dir_path)
        .map_err(|e| format!("Error creating directory {}: {}", dir_path, e.to_string()))?;
    node::start_raft_service(dir_path, rf, my_addr, my_id, peer_addrs).await?;
    println!("Server started");

    println!("Press Ctrl+C to stop the server...");
    tokio::signal::ctrl_c().await?;
    println!("Shutdown signal received. Stopping server...");

    Ok(())
}
