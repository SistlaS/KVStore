use crate::partition::{self, ManagerConfig, PartitionManager};
use log::{debug, error};
pub mod mngr_rpc {
    tonic::include_proto!("mngr");
}
use mngr_rpc::cluster_manager_service_server::ClusterManagerServiceServer;
use mngr_rpc::{
    Partition, PartitionReq, PartitionResp, RegisterServerReq, RegisterServerResp, Replica,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Response, Status};

pub struct MyClusterManagerService {
    part_mngr: Arc<RwLock<PartitionManager>>,
}

impl MyClusterManagerService {
    pub fn new(part_mngr: PartitionManager) -> Self {
        let part_mngr = Arc::new(RwLock::new(part_mngr));
        MyClusterManagerService { part_mngr }
    }

    pub async fn get_service_server(
        part_mngr: PartitionManager,
    ) -> ClusterManagerServiceServer<MyClusterManagerService> {
        let myservice = MyClusterManagerService::new(part_mngr);
        return ClusterManagerServiceServer::new(myservice);
    }
}

#[tonic::async_trait]
impl mngr_rpc::cluster_manager_service_server::ClusterManagerService for MyClusterManagerService {
    async fn get_partition_assignments(
        &self,
        _request: tonic::Request<PartitionReq>,
    ) -> Result<Response<PartitionResp>, tonic::Status> {
        debug!("Request: get_partition_assignments");

        let part_mngr = self.part_mngr.read().await;
        if !part_mngr.all_servers_registered() {
            return Err(Status::failed_precondition(
                "Not all servers are registered".to_string(),
            ));
        }

        let n_parts = part_mngr.config.n_parts;
        let mut partitions = Vec::new();
        {
            // get lock
            let part_mngr = self.part_mngr.read().await;
            for i in 0..n_parts {
                let mut replicas = Vec::new();
                for j in 0..part_mngr.config.server_rf {
                    let server_addr = part_mngr.config.part_servers[i as usize][j as usize].clone();
                    replicas.push(Replica {
                        addr: server_addr,
                        part_id: i,
                        rep_id: j,
                    });
                }
                let part = Partition { replicas: replicas };
                partitions.push(part);
            }
            // drop lock
        }
        let resp = PartitionResp {
            partitions: partitions,
            n_parts: n_parts,
        };
        debug!("Response: {:?}", resp);
        Ok(Response::new(resp))
    }

    async fn register_server(
        &self,
        request: tonic::Request<RegisterServerReq>,
    ) -> Result<Response<RegisterServerResp>, tonic::Status> {
        let req = request.into_inner();
        debug!("Request: register_server {:?}", req);

        let (res, n_parts) = {
            // get lock
            let mut part_mngr = self.part_mngr.write().await;
            let res = part_mngr.register_server(req.part_id, req.rep_id, req.server_address);
            (res, part_mngr.config.n_parts)
            // drop lock
        };

        match res {
            Ok(()) => {
                let resp = RegisterServerResp {
                    success: true,
                    n_parts: n_parts,
                };
                debug!("Response: {:?}", resp);
                Ok(Response::new(resp))
            }
            Err(err) => return Err(Status::invalid_argument(err)),
        }
    }
}
