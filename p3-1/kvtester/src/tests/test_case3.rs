use crate::tests::common::*;
use rand::Rng;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use tokio::task;
use tonic::Request;

pub async fn run_tests(addr: String) {
    // Init kvstore
    let mut client = KvServiceClient::connect(addr.clone())
        .await
        .expect("Failed to connect");
    delete_kvstore(&mut client).await;

    let n_clients = 10;
    let n_ops = 10000;
    let test_name = format!("{n_clients} clients, random {n_ops} ops per client");
    println!("\nRunning test: {test_name}...");

    // Generate unique keys
    let mut rng = get_seeded_rng(None);
    let mut keys = HashSet::new();
    while keys.len() < (n_clients * n_ops / 2) {
        let key = gen_rand_string(DEFAULT_KEYSIZE, &mut rng);
        keys.insert(key);
    }

    // Divide keys among clients
    let mut keys: Vec<String> = keys.into_iter().collect();
    keys.sort();
    let mut key_chunks: Vec<Vec<String>> = vec![Vec::new(); n_clients];
    for (i, key) in keys.into_iter().enumerate() {
        key_chunks[i % n_clients].push(key);
    }

    let mut handles = Vec::new();
    for i in 0..n_clients {
        let client_task = task::spawn(client_task(i, addr.clone(), n_ops, key_chunks[i].clone()));
        handles.push(client_task);
    }

    for handle in handles {
        handle.await.expect("Task failed");
    }

    print_test_status(test_name.to_string(), true);
}

async fn client_task(client_id: usize, addr: String, num_ops: usize, assigned_keys: Vec<String>) {
    let mut client = KvServiceClient::connect(addr)
        .await
        .expect("Failed to connect");

    let filename = format!("client_{client_id}.log");
    let mut logfile = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        // .append(true)
        .open(&filename)
        .expect("Failed to open log file");
    // log all keys
    for key in assigned_keys.clone() {
        let log_entry = format!("{},", key);
        logfile
            .write_all(log_entry.as_bytes())
            .expect("Failed to write log");
        logfile.flush().expect("Failed to flush log");
    }
    logfile
        .write_all("\n".as_bytes())
        .expect("Failed to write log");
    logfile.flush().expect("Failed to flush log");

    let mut shadow_kvstore: HashMap<String, String> = HashMap::new(); // For verifying results
    let seed: u64 = 42 + (client_id as u64);
    let mut rng = get_seeded_rng(Some(seed));

    let mut k = 0;
    let mut prev_key: String = assigned_keys[k % assigned_keys.len()].clone();
    for _ in 0..num_ops {
        let op_type = rng.random_range(0..5); // 0 = PUT, 1 = GET, 2 = DELETE, 3 = SWAP, 4 = SCAN
        let reuse_key = rng.random_bool(0.1); // 10% prob of reusing prev key

        let (key, value) = if reuse_key {
            (prev_key.clone(), gen_rand_string(DEFAULT_VALSIZE, &mut rng))
        } else {
            let new_key = assigned_keys[k % assigned_keys.len()].clone(); // Use assigned keys
            k = k + 1;
            let new_value = gen_rand_string(DEFAULT_VALSIZE, &mut rng);
            prev_key = new_key.clone();
            (new_key, new_value)
        };

        // write!("client {client_id}: {op_type} {key} {value}");
        // writeln!(writer, "PUT {} {}", key, value)?

        match op_type {
            0 => {
                // PUT
                let log_entry = format!("[PUT] {} = {}\n", key, value);
                logfile
                    .write_all(log_entry.as_bytes())
                    .expect("Failed to write log");

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
                let log_entry = format!("[GET] {}\n", key);
                logfile
                    .write_all(log_entry.as_bytes())
                    .expect("Failed to write log");

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
                let log_entry = format!("[DELETE] {}\n", key);
                logfile
                    .write_all(log_entry.as_bytes())
                    .expect("Failed to write log");

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
                let log_entry = format!("[SWAP] {} -> {}\n", key, value);
                logfile
                    .write_all(log_entry.as_bytes())
                    .expect("Failed to write log");

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
                    assert!(!swap_resp.found, "{client_id}: problematic key {key}");
                    shadow_kvstore.insert(key, value);
                }
            }

            4 => {
                // SCAN
                let start_key = key;
                let end_key = gen_rand_string(DEFAULT_KEYSIZE, &mut rng);
                // println!("\tclient {client_id}: {op_type} {start_key} {end_key}");

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

                // Check if our shadow key scan results found in server kvstore
                for (key, value) in shadow_kvstore.iter() {
                    if *key >= start_key && *key <= end_key {
                        let got_val = got.get(key);
                        assert!(got_val.is_some());
                        assert_eq!(got_val.unwrap(), value);
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    // Final scan verification

    println!("client {client_id} done.");
}
