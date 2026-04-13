import subprocess
import time
import random
import sys
import os
import argparse
from collections import defaultdict

CLIENT_BIN = "target/release/kvclient"
MANAGER_BIN = "target/release/kvmanager"
SERVER_BIN = "target/release/kvserver"

TIMEOUT_SEC = 5
DATA_DIR = "./data"
PARTITIONS_FILE = "./parts.info"  # consistent with manager save file

def cleanup(processes):
    print("Cleaning up...")
    for proc in processes:
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except Exception:
            proc.kill()

def run_cmd(cmd, timeout=None, input_text=None):
    try:
        result = subprocess.run(
            cmd,
            input=input_text.encode() if input_text else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        stdout = result.stdout.decode().strip()
        stderr = result.stderr.decode().strip()
        return stdout, stderr, result.returncode
    except subprocess.TimeoutExpired:
        return None, "Timeout", -1

def start_manager(server_addrs, manager_ip):
    print("Starting manager...")
    servers_arg = ",".join(server_addrs)
    manager_addr = f"{manager_ip}:3666"
    proc = subprocess.Popen(
        [
            MANAGER_BIN,
            "--man_listen",
            manager_addr,
            "--servers",
            servers_arg,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(1)
    return proc

def get_servers(num_servers, server_ip):
    server_addrs = []
    for i in range(num_servers):
        addr = f"{server_ip}:{5000 + i}"
        server_addrs.append(addr)
    return server_addrs

def start_servers(num_servers, manager_ip, server_ip):
    server_addrs = []
    server_procs = []
    manager_addr = f"{manager_ip}:3666"

    for i in range(num_servers):
        addr = f"{server_ip}:{5000 + i}"
        server_addrs.append(addr)

        backer_path = os.path.join(DATA_DIR, f"server_{i}")
        os.makedirs(backer_path, exist_ok=True)

        proc = subprocess.Popen(
            [
                SERVER_BIN,
                "--manager_addr",
                manager_addr,
                "--api_listen",
                addr,
                "--server_id",
                str(i),
                "--backer_path",
                backer_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        server_procs.append(proc)

    time.sleep(2)
    return server_addrs, server_procs

def trigger_partition_save(manager_ip):
    """
    Run the REFRESH command via kvclient to trigger get_partition_assignments.
    This causes the manager to save the partition info file.
    """
    print("Triggering partition save by running REFRESH on kvclient...")
    manager_addr = f"{manager_ip}:3666"
    cmd = [CLIENT_BIN, "--manager_addr", manager_addr]
    input_cmd = "REFRESH\nSTOP\n"

    stdout, stderr, code = run_cmd(cmd, input_text=input_cmd)
    if code != 0:
        print(f"Failed to trigger partition save via REFRESH: {stderr}")
        sys.exit(1)

    print("Partition save triggered successfully via REFRESH.")

def dump_partitions_from_file(partitions_file_path):
    print(f"Reading partitions from file: {partitions_file_path}")

    partitions = []
    current_server = None

    try:
        with open(partitions_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                # If the line describes a server
                if line.startswith("server id"):
                    parts = line.split()
                    if len(parts) >= 5:
                        current_server = parts[-1]  # server address
                        print(f"Found server: {current_server}")
                else:
                    # Partition range line (start_key end_key)
                    if current_server is None:
                        print("Error: partition entry without server context!")
                        continue

                    partition_keys = line.split()
                    if len(partition_keys) != 2:
                        print(f"Skipping invalid partition line: {line}")
                        continue

                    start_key, end_key = partition_keys
                    partitions.append((current_server, start_key, end_key))

    except FileNotFoundError:
        print(f"Partition file not found: {partitions_file_path}")
        sys.exit(1)

    print("\nPartitions fetched from file:")
    for p in partitions:
        print(p)

    return partitions

def map_keys_to_servers(keys, partitions):
    server_for_key = {}

    for key in keys:
        for server_addr, start_key, end_key in partitions:
            if (key >= start_key) and (key < end_key):
                server_for_key[key] = server_addr
                break
        else:
            print(f"Warning: key {key} not mapped to any server!")

    print("\nKey -> Server mapping:")
    for key, server in server_for_key.items():
        print(f"{key} -> {server}")

    return server_for_key

def put_keys(keys, manager_ip):
    manager_addr = f"{manager_ip}:3666"
    print("\n=== Putting keys ===")
    for key in keys:
        value = str(random.randint(1, 1000))
        cmd = [CLIENT_BIN, "--manager_addr", manager_addr]
        input_cmd = f"PUT {key} {value}\nSTOP\n"

        stdout, stderr, code = run_cmd(cmd, input_text=input_cmd)
        if code != 0:
            print(f"PUT {key} failed: {stderr}")
        else:
            print(f"PUT {key}: {stdout}")

def kill_random_servers(server_addrs, server_procs):
    print("Killing random servers...")
    num_fail = random.randint(0, len(server_addrs) // 2)
    fail_indices = random.sample(range(len(server_addrs)), num_fail)

    failed_servers = []
    for idx in fail_indices:
        proc = server_procs[idx]
        if proc is not None:
            proc.kill()
            server_procs[idx] = None
            failed_servers.append(server_addrs[idx])
            print(f"Killed server {server_addrs[idx]}")

    return failed_servers

def get_keys(keys, server_for_key, failed_servers, manager_ip):
    manager_addr = f"{manager_ip}:3666"
    print("\n=== Getting keys ===")
    for key in keys:
        server_addr = server_for_key.get(key)

        if not server_addr:
            print(f"Skipping unmapped key {key}")
            continue

        cmd = [CLIENT_BIN, "--manager_addr", manager_addr]
        input_cmd = f"GET {key}\nSTOP\n"

        print(f"GET {key} from {server_addr}...")
        if server_addr in failed_servers:
            stdout, stderr, code = run_cmd(cmd, timeout=TIMEOUT_SEC, input_text=input_cmd)
            if stdout is not None:
                print(f"ERROR: Expected timeout but GET {key} succeeded: {stdout}")
                sys.exit(1)
            else:
                print(f"SUCCESS: GET {key} timed out as expected")
        else:
            stdout, stderr, code = run_cmd(cmd, timeout=TIMEOUT_SEC, input_text=input_cmd)
            if code != 0:
                print(f"ERROR: GET {key} failed: {stderr}")
                sys.exit(1)
            else:
                print(f"GET {key}: {stdout}")

def main():
    parser = argparse.ArgumentParser(description="Distributed KV Store Test Script")
    parser.add_argument('--manager-ip', required=True, help='Manager node IP address')
    parser.add_argument('--server-ip', required=True, help='Server node IP address')

    args = parser.parse_args()

    manager_ip = args.manager_ip
    server_ip = args.server_ip

    processes = []

    try:
        num_servers = random.randint(1, 30)
        print(f"\n=== Starting {num_servers} servers ===")

        # Start manager
        manager_proc = start_manager(get_servers(num_servers, server_ip), manager_ip)
        processes.append(manager_proc)

        time.sleep(2)

        # Start servers
        print("Starting servers...")
        server_addrs, server_procs = start_servers(num_servers, manager_ip, server_ip)
        processes.extend([p for p in server_procs if p])

        # Trigger the manager to save the partition file by running REFRESH on kvclient
        trigger_partition_save(manager_ip)

        # Dump partitions from the file after triggering save
        print("Getting partitions from parts.info...")
        partitions = dump_partitions_from_file(PARTITIONS_FILE)

        # Example keys to PUT and GET
        keys = [
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
            "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi", "rho",
            "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega"
        ]

        print("Putting keys...")
        put_keys(keys, manager_ip)

        server_for_key = map_keys_to_servers(keys, partitions)

        failed_servers = kill_random_servers(server_addrs, server_procs)

        get_keys(keys, server_for_key, failed_servers, manager_ip)

        print("\n=== Test completed successfully! ===")

    finally:
        cleanup(processes)

if __name__ == "__main__":
    main()
