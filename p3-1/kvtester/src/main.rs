use std::env;
use std::net::SocketAddr;
mod tests;
use tests::*;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: test_runner <test_case_number> <address:port>");
        std::process::exit(1);
    }

    let test_case = &args[1];
    let addr: SocketAddr = match args[2].parse() {
        Ok(a) => a,
        Err(_) => {
            eprintln!("Invalid address format. Use: <ip:port>");
            std::process::exit(1);
        }
    };

    let addr_str = format!("http://{}", addr);

    match test_case.as_str() {
        "1" => {
            test_case1::run_tests(addr_str.clone()).await;
        }
        "2" => {
            test_case2::run_tests(addr_str.clone()).await;
        }
        "3" => {
            test_case3::run_tests(addr_str.clone()).await;
        }
        "4" => {
            test_case4::run_tests(addr_str.clone()).await;
        }
        "5" => {
            test_case5::run_tests(addr_str).await;
        }
        _ => {
            eprintln!(
                "Invalid test case number: {}. Choose 1, 2, 3, 4, or 5.",
                test_case
            );
            std::process::exit(1);
        }
    }
}
