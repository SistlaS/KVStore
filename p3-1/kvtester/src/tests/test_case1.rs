use crate::tests::common::*;
use kvrpc::{
    kv_service_client::KvServiceClient, DeleteReq, GetReq, LengthReq, PutReq, ScanReq, SwapReq,
};
use tonic::Request;

// Test Case 1 - Single client, basic test case

pub async fn run_tests(addr: String) {
    // let addr = "http://[::1]:5739";
    let mut client = KvServiceClient::connect(addr)
        .await
        .expect("Failed to connect");

    // PUT Test (put key 1 in key value store)
    let put_req = Request::new(PutReq {
        key: "key1".to_string(),
        value: "value1".to_string(),
    });
    let put_resp = client.put(put_req).await.expect("PUT failed").into_inner();
    assert_eq!(put_resp.found, false);

    // GET Test (retrieve key1 from store)
    let get_req = Request::new(GetReq {
        key: "key1".to_string(),
    });
    let get_resp = client.get(get_req).await.expect("GET failed").into_inner();
    assert_eq!(get_resp.value, "value1");

    // SWAP Test
    let swap_req = Request::new(SwapReq {
        key: "key1".to_string(),
        value: "value2".to_string(),
    });
    let swap_resp = client
        .swap(swap_req)
        .await
        .expect("SWAP failed")
        .into_inner();
    assert_eq!(swap_resp.old_value, "value1");

    // DELETE Test
    let delete_req = Request::new(DeleteReq {
        key: "key1".to_string(),
    });
    let delete_resp = client
        .delete(delete_req)
        .await
        .expect("DELETE failed")
        .into_inner();
    assert_eq!(delete_resp.found, true);

    // SCAN Test
    let scan_req = Request::new(ScanReq {
        start_key: "a".to_string(),
        end_key: "z".to_string(),
    });
    let scan_resp = client
        .scan(scan_req)
        .await
        .expect("SCAN failed")
        .into_inner();
    assert!(scan_resp.items.is_empty());

    // LENGTH Test
    let length_req = Request::new(LengthReq {});
    let length_resp = client
        .length(length_req)
        .await
        .expect("LENGTH failed")
        .into_inner();
    assert_eq!(length_resp.length, 0);
    println!("Test: run each operation once: passed");

    //  Step 1: Insert multiple key-value pairs
    let mut put_keys = vec![("keyA", "valueA"), ("keyB", "valueB"), ("keyC", "valueC")];
    for (key, value) in &put_keys {
        let put_req = Request::new(PutReq {
            key: key.to_string(),
            value: value.to_string(),
        });
        let put_resp = client.put(put_req).await.expect("PUT failed").into_inner();
        assert_eq!(put_resp.found, false);
    }
    println!("Test: put multiple key-value pairs: passed");

    //  Step 2: Verify GET for inserted keys
    for (key, expected_value) in &put_keys {
        let get_req = Request::new(GetReq {
            key: key.to_string(),
        });
        let get_resp = client.get(get_req).await.expect("GET failed").into_inner();
        assert_eq!(get_resp.value, *expected_value);
    }
    println!("Test: get all inserted key-value pairs: passed");

    //  Step 3: Swap values for some keys
    let swap_req = Request::new(SwapReq {
        key: "keyA".to_string(),
        value: "valueX".to_string(),
    });
    let swap_resp = client
        .swap(swap_req)
        .await
        .expect("SWAP failed")
        .into_inner();
    assert_eq!(swap_resp.old_value, "valueA");
    println!("Test: swap existing key: passed");

    // Edge Case: Swap on a non-existing key
    let swap_req = Request::new(SwapReq {
        key: "nonExistingKey".to_string(),
        value: "randomValue".to_string(),
    });
    let swap_resp = client
        .swap(swap_req)
        .await
        .expect("SWAP failed")
        .into_inner();
    assert_eq!(swap_resp.old_value, "null"); // Should return "null"
                                             // println!("SWAP nonExistingKey -> randomValue correctly returned null");
    println!("Test: swap non-existing key: passed");

    //  Step 4: Scan within a range and validate
    let scan_req = Request::new(ScanReq {
        start_key: "a".to_string(),
        end_key: "z".to_string(),
    });
    let scan_resp = client
        .scan(scan_req)
        .await
        .expect("SCAN failed")
        .into_inner();

    // Check if the expected number of items is found
    assert_eq!(
        scan_resp.items.len(),
        4,
        "SCAN test failed: Expected 4 items, but found {}",
        scan_resp.items.len()
    );
    println!("Test: scan a range covering all existing keys: passed");

    //  Step 5: Verify LENGTH before and after deletions
    let length_req = Request::new(LengthReq {});
    let length_resp = client
        .length(length_req)
        .await
        .expect("LENGTH failed")
        .into_inner();
    assert_eq!(length_resp.length, 4);

    put_keys.push(("nonExistingKey", "randomValue")); // Added nonExistingKey to delete

    //  Step 6: Delete keys and verify state
    for (key, _) in &put_keys {
        let delete_req = Request::new(DeleteReq {
            key: key.to_string(),
        });
        let delete_resp = client
            .delete(delete_req)
            .await
            .expect("DELETE failed")
            .into_inner();
        assert_eq!(delete_resp.found, true);
    }
    println!("Test: delete all existing keys: passed");

    // Edge Case: Try deleting an already deleted key
    let delete_req = Request::new(DeleteReq {
        key: "keyA".to_string(),
    });
    let delete_resp = client
        .delete(delete_req)
        .await
        .expect("DELETE failed")
        .into_inner();
    assert_eq!(delete_resp.found, false);
    println!("Test: delete non-existing keys: passed");

    //  Step 7: Check final LENGTH after deletions
    let length_req = Request::new(LengthReq {});
    let length_resp = client
        .length(length_req)
        .await
        .expect("LENGTH failed")
        .into_inner();
    assert_eq!(length_resp.length, 0);
    println!("Verify kvstore is empty: passed");

    println!("\nStarting Test Case 1 edge case testing ...");

    test_put_new_key_and_get(&mut client).await;
    test_put_existing_key_get(&mut client).await;
    test_swap_existing_key(&mut client).await;
    test_swap_new_key(&mut client).await;
    test_delete_existing_key(&mut client).await;
    test_delete_new_key(&mut client).await;
    test_scan_full_range(&mut client).await;
    test_scan_partial_range(&mut client).await;
    test_scan_nonexisting_range(&mut client).await;
    test_scan_empty_kvstore(&mut client).await;
    test_scan_startkey_bigger_than_endkey(&mut client).await;
    println!("\nTest Case 1 edge case testing complete");
}

async fn test_put_new_key_and_get(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("put new key and get");
    // PUT a key
    let put_req = PutReq {
        key: "apple".to_string(),
        value: "fruit".to_string(),
    };
    let put_resp = client
        .put(Request::new(put_req))
        .await
        .unwrap()
        .into_inner();
    assert!(!put_resp.found, "Key should not exist");

    // GET the key
    let get_req = GetReq {
        key: "apple".to_string(),
    };
    let get_resp = client
        .get(Request::new(get_req))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_resp.value, "fruit".to_string());
    assert!(get_resp.found, "Key should exist");
    print_test_status(test_name.to_string(), true);
}

async fn test_put_existing_key_get(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("put existing key and get");
    // PUT a new key
    let put_req = PutReq {
        key: "apple".to_string(),
        value: "fruit".to_string(),
    };
    let put_resp = client
        .put(Request::new(put_req))
        .await
        .unwrap()
        .into_inner();
    assert!(!put_resp.found, "Key should not exist");

    // PUT an existing key
    let put_req = PutReq {
        key: "apple".to_string(),
        value: "red".to_string(),
    };
    let put_resp = client
        .put(Request::new(put_req))
        .await
        .unwrap()
        .into_inner();
    assert!(put_resp.found, "Key should exist");

    // GET the key
    let get_req = GetReq {
        key: "apple".to_string(),
    };
    let get_resp = client
        .get(Request::new(get_req))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_resp.value, "red".to_string());
    assert!(get_resp.found, "Key should exist");
    print_test_status(test_name.to_string(), true);
}

async fn test_swap_existing_key(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("swap existing key");
    // PUT a key
    client
        .put(Request::new(PutReq {
            key: "banana".to_string(),
            value: "fruit".to_string(),
        }))
        .await
        .unwrap();

    // SWAP the key
    let swap_req = SwapReq {
        key: "banana".to_string(),
        value: "yellow".to_string(),
    };
    let swap_resp = client
        .swap(Request::new(swap_req))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(swap_resp.old_value, "fruit".to_string());
    assert!(swap_resp.found);

    // GET the swapped key
    let get_resp = client
        .get(Request::new(GetReq {
            key: "banana".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_resp.value, "yellow");
    print_test_status(test_name.to_string(), true);
}

async fn test_swap_new_key(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("swap new key");
    // SWAP new key
    let swap_req = SwapReq {
        key: "banana".to_string(),
        value: "yellow".to_string(),
    };
    let swap_resp = client
        .swap(Request::new(swap_req))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(swap_resp.old_value, "null".to_string());
    assert!(!swap_resp.found);

    // GET the swapped key
    let get_resp = client
        .get(Request::new(GetReq {
            key: "banana".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_resp.value, "yellow");
    print_test_status(test_name.to_string(), true);
}

async fn test_delete_existing_key(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("delete existing key");
    // PUT a key
    client
        .put(Request::new(PutReq {
            key: "apple".to_string(),
            value: "fruit".to_string(),
        }))
        .await
        .unwrap();

    // DELETE the key
    let delete_req = DeleteReq {
        key: "apple".to_string(),
    };
    let delete_resp = client
        .delete(Request::new(delete_req))
        .await
        .unwrap()
        .into_inner();
    assert!(delete_resp.found);

    // Ensure key is deleted
    let get_resp = client
        .get(Request::new(GetReq {
            key: "apple".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_resp.value, "null".to_string());
    assert!(!get_resp.found);
    print_test_status(test_name.to_string(), true);
}

async fn test_delete_new_key(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("delete new key");
    // DELETE new key
    let delete_req = DeleteReq {
        key: "apple".to_string(),
    };
    let delete_resp = client
        .delete(Request::new(delete_req))
        .await
        .unwrap()
        .into_inner();
    assert!(!delete_resp.found);

    // Ensure key is not found
    let get_resp = client
        .get(Request::new(GetReq {
            key: "apple".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_resp.value, "null".to_string());
    assert!(!get_resp.found);
    print_test_status(test_name.to_string(), true);
}

async fn test_scan_full_range(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("scan full range");
    // PUT multiple keys
    let test_data = vec![
        ("apple".to_string(), "red".to_string()),
        ("banana".to_string(), "yellow".to_string()),
        ("carrot".to_string(), "orange".to_string()),
        ("date".to_string(), "brown".to_string()),
        ("berry".to_string(), "blue".to_string()),
        ("0".to_string(), "digit".to_string()),
    ];
    for (key, value) in &test_data {
        client
            .put(Request::new(PutReq {
                key: key.to_string(),
                value: value.to_string(),
            }))
            .await
            .unwrap();
    }

    // SCAN full range
    let scan_req = ScanReq {
        start_key: "0".to_string(),
        end_key: "zzzzzzzzzzzzzzzzzzzzzzzz".to_string(),
    };
    let scan_resp = client
        .scan(Request::new(scan_req))
        .await
        .unwrap()
        .into_inner();
    let scan_results: Vec<(String, String)> = scan_resp
        .items
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();

    let mut expected = test_data.clone();
    expected.sort_by_key(|k| k.0.clone()); // Result should be sorted
    assert_eq!(scan_results, expected);
    print_test_status(test_name.to_string(), true);
}

async fn test_scan_partial_range(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("scan partial range");
    // PUT multiple keys
    let test_data = vec![
        ("apple".to_string(), "red".to_string()),
        ("banana".to_string(), "yellow".to_string()),
        ("carrot".to_string(), "orange".to_string()),
        ("date".to_string(), "brown".to_string()),
        ("berry".to_string(), "blue".to_string()),
        ("0".to_string(), "digit".to_string()),
    ];
    for (key, value) in &test_data {
        client
            .put(Request::new(PutReq {
                key: key.to_string(),
                value: value.to_string(),
            }))
            .await
            .unwrap();
    }

    // SCAN full range
    let scan_req = ScanReq {
        start_key: "berry".to_string(),
        end_key: "orange".to_string(),
    };
    let scan_resp = client
        .scan(Request::new(scan_req))
        .await
        .unwrap()
        .into_inner();
    let scan_results: Vec<(String, String)> = scan_resp
        .items
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();

    let expected = vec![
        ("berry".to_string(), "blue".to_string()),
        ("carrot".to_string(), "orange".to_string()),
        ("date".to_string(), "brown".to_string()),
    ];
    assert_eq!(scan_results, expected);
    print_test_status(test_name.to_string(), true);
    print_test_status(test_name.to_string(), true);
}

async fn test_scan_nonexisting_range(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("scan non-existing range");
    // PUT multiple keys
    let test_data = vec![
        ("apple".to_string(), "red".to_string()),
        ("banana".to_string(), "yellow".to_string()),
        ("carrot".to_string(), "orange".to_string()),
        ("date".to_string(), "brown".to_string()),
        ("berry".to_string(), "blue".to_string()),
    ];
    for (key, value) in &test_data {
        client
            .put(Request::new(PutReq {
                key: key.to_string(),
                value: value.to_string(),
            }))
            .await
            .unwrap();
    }

    // SCAN non-existing range
    let scan_req = ScanReq {
        start_key: "0".to_string(),
        end_key: "99".to_string(),
    };
    let scan_resp = client
        .scan(Request::new(scan_req))
        .await
        .unwrap()
        .into_inner();
    let scan_results: Vec<(String, String)> = scan_resp
        .items
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();

    let expected = vec![];
    assert_eq!(scan_results, expected);
    print_test_status(test_name.to_string(), true);
}

async fn test_scan_empty_kvstore(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("scan empty kvstore");
    // SCAN empty kvstore
    let scan_req = ScanReq {
        start_key: "0".to_string(),
        end_key: "99".to_string(),
    };
    let scan_resp = client
        .scan(Request::new(scan_req))
        .await
        .unwrap()
        .into_inner();
    let scan_results: Vec<(String, String)> = scan_resp
        .items
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();

    let expected = vec![];
    assert_eq!(scan_results, expected);
    print_test_status(test_name.to_string(), true);
}

async fn test_scan_startkey_bigger_than_endkey(client: &mut KvServiceClient<Channel>) {
    delete_kvstore(client).await;
    let test_name = format!("scan (start key > end key)");
    // PUT multiple keys
    let test_data = vec![
        ("apple".to_string(), "red".to_string()),
        ("banana".to_string(), "yellow".to_string()),
        ("carrot".to_string(), "orange".to_string()),
        ("date".to_string(), "brown".to_string()),
        ("berry".to_string(), "blue".to_string()),
    ];
    for (key, value) in &test_data {
        client
            .put(Request::new(PutReq {
                key: key.to_string(),
                value: value.to_string(),
            }))
            .await
            .unwrap();
    }
    let scan_req = ScanReq {
        start_key: "date".to_string(),
        end_key: "apple".to_string(),
    };
    let scan_resp = client
        .scan(Request::new(scan_req))
        .await
        .unwrap()
        .into_inner();
    let scan_results: Vec<(String, String)> = scan_resp
        .items
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();

    let expected = vec![];
    assert_eq!(scan_results, expected);
    print_test_status(test_name.to_string(), true);
}
