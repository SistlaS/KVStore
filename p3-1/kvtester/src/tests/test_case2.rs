use crate::tests::common::*;
use rand::Rng;
use std::collections::HashMap;
use tonic::Request;

async fn validate_scan(
    client: &mut KvServiceClient<Channel>,
    expected_kv: &HashMap<String, String>,
) {
    let scan_req = ScanReq {
        start_key: "".to_string(),
        end_key: "\u{10FFFF}".to_string(),
    };

    let scan_resp = client
        .scan(scan_req)
        .await
        .expect("SCAN failed")
        .into_inner();

    // Convert SCAN response to a HashMap
    let scanned_kv: HashMap<String, String> = scan_resp
        .items
        .iter()
        .map(|item| (item.key.clone(), item.value.clone()))
        .collect();

    // Ensure SCAN matches the expected model
    assert_eq!(
        scanned_kv, *expected_kv,
        "SCAN does not match expected state"
    );
}

async fn test_random_operations(client: &mut KvServiceClient<Channel>, num_ops: usize) {
    // Init kvstore
    delete_kvstore(client).await;

    let test_name = format!("single client, random {num_ops} operations with key repetition");
    println!("\nRunning test: {test_name}...");

    let mut shadow_kvstore: HashMap<String, String> = HashMap::new(); // For verifying results
    let mut rng = get_seeded_rng(None);

    let mut prev_key: String = gen_rand_string(DEFAULT_KEYSIZE, &mut rng);

    for i in 0..num_ops {
        let op_type = rng.random_range(0..5); // 0 = PUT, 1 = GET, 2 = DELETE, 3 = SWAP, 4 = SCAN
        let reuse_key = rng.random_bool(0.1); // 10% prob of reusing prev key

        let (key, value) = if reuse_key {
            (prev_key.clone(), gen_rand_string(DEFAULT_VALSIZE, &mut rng))
        } else {
            let new_key = gen_rand_string(DEFAULT_KEYSIZE, &mut rng);
            let new_value = gen_rand_string(DEFAULT_VALSIZE, &mut rng);
            prev_key = new_key.clone();
            (new_key, new_value)
        };

        match op_type {
            0 => {
                // PUT
                let put_req = PutReq {
                    key: key.clone(),
                    value: value.clone(),
                };
                let put_resp = client
                    .put(Request::new(put_req))
                    .await
                    .unwrap()
                    .into_inner();

                if shadow_kvstore.contains_key(&key) {
                    assert!(put_resp.found);
                } else {
                    assert!(!put_resp.found);
                }

                shadow_kvstore.insert(key, value);
            }
            1 => {
                // GET
                let get_req = GetReq { key: key.clone() };
                let get_resp = client
                    .get(Request::new(get_req))
                    .await
                    .unwrap()
                    .into_inner();

                if let Some(expected_value) = shadow_kvstore.get(&key) {
                    assert!(get_resp.found);
                    assert_eq!(get_resp.value, *expected_value);
                } else {
                    assert!(!get_resp.found);
                }
            }
            2 => {
                // DELETE
                let delete_req = DeleteReq { key: key.clone() };
                let delete_resp = client
                    .delete(Request::new(delete_req))
                    .await
                    .unwrap()
                    .into_inner();

                if shadow_kvstore.contains_key(&key) {
                    assert!(delete_resp.found);
                    shadow_kvstore.remove(&key);
                } else {
                    assert!(!delete_resp.found);
                }
            }
            3 => {
                // SWAP
                let swap_req = SwapReq {
                    key: key.clone(),
                    value: value.clone(),
                };
                let swap_resp = client
                    .swap(Request::new(swap_req))
                    .await
                    .unwrap()
                    .into_inner();

                if let Some(old_value) = shadow_kvstore.get_mut(&key) {
                    assert!(swap_resp.found);
                    assert_eq!(swap_resp.old_value, *old_value);
                    *old_value = value;
                } else {
                    assert!(!swap_resp.found);
                    shadow_kvstore.insert(key, value);
                }
            }

            4 => {
                // SCAN
                let start_key = key;
                let end_key = gen_rand_string(DEFAULT_KEYSIZE, &mut rng);

                let scan_req = ScanReq {
                    start_key: start_key.clone(),
                    end_key: end_key.clone(),
                };
                let scan_resp = client
                    .scan(Request::new(scan_req))
                    .await
                    .unwrap()
                    .into_inner();
                let mut got: HashMap<String, String> = HashMap::new();
                for item in scan_resp.items {
                    got.insert(item.key, item.value);
                }

                let mut expected: HashMap<String, String> = HashMap::new();
                for (key, value) in shadow_kvstore.iter() {
                    if *key >= start_key && *key <= end_key {
                        expected.insert(key.clone(), value.clone());
                    }
                }

                assert_eq!(got, expected);
            }
            _ => unreachable!(),
        }

        // SCAN every 100 ops
        if i % 100 == 0 {
            validate_scan(client, &shadow_kvstore).await;
        }
    }

    // Final scan verification
    validate_scan(client, &shadow_kvstore).await;

    print_test_status(test_name.to_string(), true);
}

pub async fn run_tests(addr: String) {
    // Connect to server
    let mut client = KvServiceClient::connect(addr)
        .await
        .expect("Failed to connect");

    println!("\nStarting Test Case 2 Stress testing ...");
    test_random_operations(&mut client, 10000).await;
    println!("\nTest Case 2 stress testing complete");
}
