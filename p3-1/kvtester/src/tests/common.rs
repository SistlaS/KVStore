use color_print::cprintln;

#[macro_export]
macro_rules! test_passed {
    ($status:expr) => {
        use color_print::cprintln;
        if $status {
            cprintln!("{}", PASSED);
        } else {
            cprintln!("{}", FAILED);
        }
    };
}

pub fn print_test_status(test_name: String, passed: bool) {
    print!("Test: {test_name}: ");
    if passed {
        cprintln!("<s><green>passed</></s>");
    } else {
        cprintln!("<s><red>failed</></s>");
    }
}

use rand::distr::{Alphanumeric, SampleString};
use rand::{rngs::StdRng, SeedableRng};

const DEFAULT_SEED: u64 = 42;
pub const DEFAULT_KEYSIZE: usize = 10;
pub const DEFAULT_VALSIZE: usize = 5;

pub fn get_seeded_rng(seed: Option<u64>) -> StdRng {
    StdRng::seed_from_u64(seed.unwrap_or(DEFAULT_SEED))
}

pub fn gen_rand_string(len: usize, rng: &mut StdRng) -> String {
    debug_assert_ne!(len, 0);
    Alphanumeric.sample_string(rng, len)
}

// Reusable dependencies
pub mod kvrpc {
    tonic::include_proto!("kvrpc");
}
pub use kvrpc::{
    kv_service_client::KvServiceClient, DeleteReq, GetReq, LengthReq, PutReq, ScanReq, SwapReq,
};
pub use tonic::{transport::Channel, Request};

pub async fn delete_kvstore(client: &mut KvServiceClient<Channel>) {
    let scan_req = tonic::Request::new(ScanReq {
        start_key: "".to_string(),
        end_key: "\u{10FFFF}".to_string(),
    });
    let scan_resp = client
        .scan(scan_req)
        .await
        .expect("SCAN failed")
        .into_inner();

    for item in scan_resp.items {
        let delete_request = tonic::Request::new(DeleteReq { key: item.key });
        client.delete(delete_request).await.expect("DELETE failed");
    }

    let length_req = Request::new(LengthReq {});
    let length_resp = client
        .length(length_req)
        .await
        .expect("LENGTH failed")
        .into_inner();
    assert_eq!(length_resp.length, 0);
    println!("Reset kvstore");
}
