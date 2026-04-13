use kvrpc::{kv_service_client::KvServiceClient, DeleteReq, GetReq, PutReq, SwapReq};
use std::time::Duration;
use tokio::task;
use tonic::Request;

pub mod kvrpc {
    tonic::include_proto!("kvrpc");
}

pub async fn run_tests(addr: String) {
    test_1(addr.clone()).await;
    test_2(addr).await;
}

pub async fn test_1(addr: String) {
    // let addr = "http://[::1]:5739"; // Adjust as needed

    let client1 = task::spawn(client_task1(
        "Client1",
        "shared_key",
        "valueA",
        addr.to_string(),
        0,
    ));
    let client2 = task::spawn(client_task1(
        "Client2",
        "shared_key",
        "valueB",
        addr.to_string(),
        100,
    ));

    client1.await.expect("Client1 failed");
    client2.await.expect("Client2 failed");

    println!("Test: 2 clients, concurrent put same key: passed");
}

pub async fn test_2(addr: String) {
    let key1 = "swap_key1".to_string();
    let key2 = "swap_key2".to_string();

    //  Clients performing concurrent swaps and deletes
    let client1 = task::spawn(client_task2(
        "Client1",
        key1.clone(),
        "initial",
        "valueA",
        addr.clone(),
        0,
    ));
    let client2 = task::spawn(client_task2(
        "Client2",
        key1.clone(),
        "valueA",
        "valueB",
        addr.clone(),
        50,
    ));
    let client3 = task::spawn(client_task2(
        "Client3",
        key2.clone(),
        "initial",
        "valueX",
        addr.clone(),
        100,
    ));
    let client4 = task::spawn(delete_task("Client4", key1.clone(), addr.clone(), 150));
    let client5 = task::spawn(client_task2(
        "Client5",
        key2.clone(),
        "valueX",
        "valueY",
        addr.clone(),
        200,
    ));

    client1.await.expect("Client1 failed");
    client2.await.expect("Client2 failed");
    client3.await.expect("Client3 failed");
    client4.await.expect("Client4 failed");
    client5.await.expect("Client5 failed");

    println!("Test: 5 clients, concurrent swap and delete same key: passed");
}

async fn client_task1(client_name: &str, key: &str, value: &str, addr: String, delay: u64) {
    let mut client = KvServiceClient::connect(addr.clone())
        .await
        .expect(&format!("{} failed to connect", client_name));

    let put_req = Request::new(PutReq {
        key: key.to_string(),
        value: value.to_string(),
    });
    let _ = client.put(put_req).await.expect("PUT failed").into_inner();

    tokio::time::sleep(Duration::from_millis(delay)).await;

    println!("[{}] GET {}", client_name, key);
    let get_req = Request::new(GetReq {
        key: key.to_string(),
    });
    let get_resp = client.get(get_req).await.expect("GET failed").into_inner();
    if !(get_resp.value == "valueA" || get_resp.value == "valueB") {
        assert!(false);
    }
}

async fn client_task2(
    client_name: &str,
    key: String,
    old_value: &str,
    new_value: &str,
    addr: String,
    delay: u64,
) {
    let mut client = KvServiceClient::connect(addr.clone())
        .await
        .expect(&format!("{} failed to connect", client_name));

    //  Initial PUT to ensure key exists
    println!("[{}] PUT {} -> {}", client_name, key, old_value);
    let put_req = Request::new(PutReq {
        key: key.clone(),
        value: old_value.to_string(),
    });
    client.put(put_req).await.expect("PUT failed");

    tokio::time::sleep(Duration::from_millis(50)).await;

    let swap_req = Request::new(SwapReq {
        key: key.clone(),
        value: new_value.to_string(),
    });
    let swap_resp = client
        .swap(swap_req)
        .await
        .expect("SWAP failed")
        .into_inner();

    println!(
        "[{}] SWAP response: Old value = {}, New value = {}, Found = {}",
        client_name,
        swap_resp.old_value,
        new_value.to_string(),
        swap_resp.found
    );

    println!("[{}] Sleeping for {}ms", client_name, delay);
    tokio::time::sleep(Duration::from_millis(delay)).await;

    let get_req = Request::new(GetReq { key: key.clone() });
    let get_resp = client.get(get_req).await.expect("GET failed").into_inner();

    println!("[{}] GET {}", client_name, key);
    println!(
        "[{}] Observed final value for {} -> {}",
        client_name, key, get_resp.value
    );
}

async fn delete_task(client_name: &str, key: String, addr: String, delay: u64) {
    let mut client = KvServiceClient::connect(addr.clone())
        .await
        .expect(&format!("{} failed to connect", client_name));

    println!("[{}] Sleeping before DELETE for {}ms", client_name, delay);
    tokio::time::sleep(Duration::from_millis(delay)).await;

    println!("[{}] DELETE {}", client_name, key);
    let delete_req = Request::new(DeleteReq { key: key.clone() });
    let delete_resp = client
        .delete(delete_req)
        .await
        .expect("DELETE failed")
        .into_inner();

    println!(
        "[{}] DELETE response: {}",
        client_name,
        if delete_resp.found {
            "Deleted"
        } else {
            "Not found"
        }
    );
}
