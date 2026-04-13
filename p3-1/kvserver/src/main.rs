#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod server;
mod service;
use log::{debug, error, info};
use raft::*;
use server::ServerConfig;
use service::MyKVService;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    info!("Starting kvserver");

    let mut config = ServerConfig::from_args()?;
    debug!("Parsed config: {:?}", config);

    // start raft server
    let replication_factor = config.peers.len() as u32 + 1;
    let dir_path = config.backer_path.clone();
    let addr = config.p2p_listen.clone();
    let peer_addrs = config.peers.clone();
    let raft_node = raft::node::start_raft_service(
        dir_path,
        replication_factor,
        addr,
        config.rep_id,
        peer_addrs,
    )
    .await
    .expect("Failed to start raft service");

    // register with cluster manager
    let partitions = server::register_with_manager(
        config.manager_addrs.clone(),
        config.part_id,
        config.rep_id,
        config.api_listen.clone(),
    )
    .await?;
    config.n_key_partitions = partitions;
    info!(
        "Registered with cluster manager, partitions: {:?}",
        partitions
    );

    // start kvstore server
    let server_addr = config.api_listen.clone();
    let server_addr = server_addr.parse::<SocketAddr>()?;
    info!("Starting kvstore server at {:?}", server_addr);
    match tonic::transport::Server::builder()
        .add_service(MyKVService::get_service_server(config, raft_node).await)
        .serve(server_addr)
        .await
    {
        Ok(_) => info!("KVStore server started successfully"),
        Err(e) => error!("Failed to start KVStore server at {}: {:?}", server_addr, e),
    }
    Ok(())
}
