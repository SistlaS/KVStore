#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod partition;
mod service;
use log::{debug, error, info};
use partition::{ManagerConfig, PartitionManager};
use service::MyClusterManagerService;
use std::net::SocketAddr;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    debug!("Starting cluster manager");

    let mngr_config = ManagerConfig::from_args()?;
    debug!("Manager config: {:?}", mngr_config);
    let mngr_addr = mngr_config.man_listen.clone();
    let part_mngr = PartitionManager::new(mngr_config);
    debug!("Partition manager created: {:?}", part_mngr);

    info!("Starting cluster manager at {:?}", mngr_addr);
    let mngr_addr: SocketAddr = mngr_addr.parse().expect("Invalid manager listen address");
    Server::builder()
        .add_service(MyClusterManagerService::get_service_server(part_mngr).await)
        .serve(mngr_addr)
        .await?;

    Ok(())
}
