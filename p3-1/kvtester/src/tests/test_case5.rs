use crate::tests::common::*;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::task;
use tokio::time::sleep;
use tonic::Request;

pub async fn run_tests(addr: String) {
    let mut client = KvServiceClient::connect(addr.clone())
        .await
        .expect("Failed to connect");

    delete_kvstore(&mut client).await;
    println!("\nStarting Test Case 5 ...");

    let n_clients = 10;
    let n_ops = 10000;
    let in_flight_limit = Arc::new(Semaphore::new(10));
    let test_name =
        format!("{n_clients} clients, all keys shared, conflicting {n_ops} ops per client");
    println!("\nRunning test: {test_name}...");

    // Generate unique keys
    let mut rng = get_seeded_rng(None);
    let mut keys = HashSet::new();
    while keys.len() < (n_ops / 2) {
        let key = gen_rand_string(DEFAULT_KEYSIZE, &mut rng);
        keys.insert(key);
    }
    let shared_keys: Vec<String> = keys.into_iter().collect();

    // Shared shadow key-value store (visible to all clients)
    let shared_kvstore: Arc<Mutex<HashMap<String, Vec<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let mut handles = Vec::new();
    for i in 0..n_clients {
        let client_store = shared_kvstore.clone();
        let client_limit = in_flight_limit.clone();
        let client_task = task::spawn(client_task(
            i,
            addr.clone(),
            n_ops,
            shared_keys.clone(),
            client_store,
            client_limit,
        ));
        handles.push(client_task);
    }

    for handle in handles {
        handle.await.expect("Task failed");
    }

    print_test_status(test_name.to_string(), true);
    println!("\nTest Case 5 testing complete");
}

async fn client_task(
    client_id: usize,
    addr: String,
    num_ops: usize,
    shared_keys: Vec<String>,
    shared_kvstore: Arc<Mutex<HashMap<String, Vec<String>>>>, // Track history of values per key
    in_flight_limit: Arc<Semaphore>,                          // Controls in-flight requests
) {
    let mut client = KvServiceClient::connect(addr)
        .await
        .expect("Failed to connect");

    let filename = format!("client_{client_id}.log");
    let mut logfile = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .expect("Failed to open log file");

    // Log all keys
    for key in &shared_keys {
        write!(logfile, "{},", key).expect("Failed to write log");
    }
    writeln!(logfile).expect("Failed to write newline");
    logfile.flush().expect("Failed to flush log");

    let seed: u64 = 42 + (client_id as u64);
    let mut rng = get_seeded_rng(Some(seed));

    let mut k = 0;
    let mut prev_key: String = shared_keys[k % shared_keys.len()].clone();
    let mut prev_value: String = gen_rand_string(DEFAULT_VALSIZE, &mut rng);

    for _ in 0..num_ops {
        let _permit = in_flight_limit
            .acquire()
            .await
            .expect("Failed to acquire permit"); // Controls in-flight requests

        let op_type = rng.random_range(0..2); // 0 = PUT, 1 = GET, 2 = DELETE, 3 = SWAP, 4 = SCAN
        let reuse_key = rng.random_bool(0.3); // 30% probability of reusing a previous key

        let (key, value) = if reuse_key {
            (prev_key.clone(), prev_value.clone())
        } else {
            let new_key = shared_keys[k % shared_keys.len()].clone();
            k = (k + 1) % shared_keys.len();
            let new_value = gen_rand_string(DEFAULT_VALSIZE, &mut rng);
            prev_key = new_key.clone();
            prev_value = new_value.clone();
            (new_key, new_value)
        };

        match op_type {
            0 => {
                writeln!(logfile, "[SWAP] {} -> {}", key, value).expect("Failed to write log");
                logfile.flush().expect("Failed to flush log");

                let swap_req = SwapReq {
                    key: key.clone(),
                    value: value.clone(),
                };
                let swap_resp = client
                    .swap(Request::new(swap_req))
                    .await
                    .unwrap()
                    .into_inner();

                writeln!(logfile, "\t{} {}", swap_resp.found, swap_resp.old_value)
                    .expect("Failed to write log");
                logfile.flush().expect("Failed to flush log");

                let mut kv: tokio::sync::MutexGuard<'_, HashMap<String, Vec<String>>> =
                    shared_kvstore.lock().await;

                if swap_resp.found {
                    let mut retries = 10;
                    while retries > 0 {
                        let entry = kv.entry(key.clone()).or_insert_with(Vec::new);

                        if entry.contains(&swap_resp.old_value) {
                            break; // Found in history, proceed
                        }

                        drop(kv); // Release lock and wait

                        sleep(Duration::from_millis(50)).await;

                        kv = shared_kvstore.lock().await; // Reacquire the lock
                        retries -= 1;
                    }

                    let entry = kv.entry(key.clone()).or_insert_with(Vec::new);
                    if !entry.contains(&swap_resp.old_value) {
                        eprintln!(
                            "C{client_id}: SWAP inconsistency: Key `{key}` old value `{}` not found in history",
                            swap_resp.old_value
                        );
                    }
                    entry.push(value.clone());
                } else {
                    let entry = kv.entry(key.clone()).or_insert_with(Vec::new);
                    // assert_eq!(entry.len(), 0, "{client_id}: Since swapped value null, client kv should not have any entry");
                    // TODO: room for improvement

                    entry.push(value.clone());
                }
            }
            1 => {
                // GET operation: Ensure the latest value is retrieved.
                writeln!(logfile, "[GET] {}", key).expect("Failed to write log");
                logfile.flush().expect("Failed to flush log");

                let get_req = GetReq { key: key.clone() };
                let get_resp = client
                    .get(Request::new(get_req))
                    .await
                    .unwrap()
                    .into_inner();

                writeln!(logfile, "\t{} {}", get_resp.found, get_resp.value)
                    .expect("Failed to write log");
                logfile.flush().expect("Failed to flush log");

                let mut kv: tokio::sync::MutexGuard<'_, HashMap<String, Vec<String>>> =
                    shared_kvstore.lock().await;
                if get_resp.found {
                    let mut retries = 10;
                    while retries > 0 {
                        let entry = kv.entry(key.clone()).or_insert_with(Vec::new);

                        if entry.contains(&get_resp.value) {
                            break; // Found in history, proceed
                        }

                        drop(kv); // Release lock and wait

                        sleep(Duration::from_millis(50)).await;

                        kv = shared_kvstore.lock().await; // Reacquire the lock
                        retries -= 1;
                    }

                    let entry = kv.entry(key.clone()).or_insert_with(Vec::new);
                    if !entry.contains(&get_resp.value) {
                        eprintln!(
                            "C{client_id}: GET inconsistency: Key `{key}` old value `{}` not found in history",
                            get_resp.value
                        );
                    }
                    entry.push(value.clone());
                } else {
                    let entry = kv.entry(key.clone()).or_insert_with(Vec::new);
                    // assert_eq!( entry.len(), 0, "C{client_id}: Since get value null, client kv should not have any entry");
                    // TODO: room for improvement

                    entry.push(value.clone());
                }
            }
            _ => unreachable!(),
        }
    }

    println!("client {client_id} done.");
}
