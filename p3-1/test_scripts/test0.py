import os
import time
import subprocess
import signal
import sys
import random
import string
import glob
import shutil

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
PARTS_INFO_PATH = "parts.info"  # Change this to your actual file path
MANAGER_IP_ADDR = "127.0.0.1"
SERVER_IP_ADDR = "127.0.0.1"
MANAGER_PORT = "3666"
MANAGER_ADDR = f"{MANAGER_IP_ADDR}:{MANAGER_PORT}"
SERVER_ADDRS = [
    f"{SERVER_IP_ADDR}:5001",
    f"{SERVER_IP_ADDR}:5002",
    f"{SERVER_IP_ADDR}:5003",
]
DATA_DIR = "./.testdir"
RED = "\033[91m"
RESET = "\033[0m"
GREEN = "\033[92m"
ORANGE = "\033[1;93m" #"\033[93m"


def remove_backer_files():
    os.remove("./parts.info")
    if os.path.exists(DATA_DIR):
        try:
            shutil.rmtree(DATA_DIR)  # Recursively delete the directory
            print(f"deleted {DATA_DIR}")
        except Exception as e:
            print(f"Failed to delete")

def error():
    return f"{RED}ERROR{RESET}"

def success():
    return f"{GREEN}SUCCESS{RESET}"

def warning():
    return f"{ORANGE}WARNING{RESET}"

def parse_partition_file(filepath):
    server_map = {}  # store server_id -> {addr, partitions}
    current_server = None

    with open(filepath, "r") as file:
        for line in file:
            line = line.strip()
            if line.startswith("server id"):
                parts = line.split()
                server_id = int(parts[2])
                server_addr = parts[4]
                server_map[server_id] = {"addr": server_addr, "partitions": []}
                current_server = server_id  # Set current server context

            elif line and current_server is not None:
                start_key, end_key = line.split()
                server_map[current_server]["partitions"].append(
                    # {"start_key": start_key, "end_key": end_key}
                    (start_key, end_key)
                )

    for key, value in server_map.items():
        print(f"Server: id {key} addr {value["addr"]}")
        for part in value["partitions"]:
            print(part)
    return server_map


ALPHANUMERIC = string.ascii_letters + string.digits
KEY_SPACE = sorted(ALPHANUMERIC)


def gen_random_key(start_key, end_key, key_len=3):
    random_chars = []
    for i in range(min(len(start_key), len(end_key))):
        if i >= key_len:
            break
        start, end = ord(start_key[i]), ord(end_key[i]) - 1
        if start >= end:
            random_chars.append(start_key[i])
            continue
        while True:
            random_char = chr(random.randint(start, end))
            if random_char in ALPHANUMERIC:
                break
        random_chars.append(random_char)
    return "".join(random_chars)


def gen_kvs_per_server(server_map, num_kv_pairs=2):
    key_value_map = {}
    for server_id, server_info in server_map.items():
        key_value_map[server_id] = {
            "addr": server_info["addr"],
            "key_values": [],
        }
        for start_key, end_key in server_info["partitions"]:
            print(start_key, end_key)
            for _ in range(num_kv_pairs):
                random_key = gen_random_key(start_key, end_key)
                random_value = str(random.randint(1, 1000))
                key_value_map[server_id]["key_values"].append(
                    (random_key, random_value)
                )
        print(server_id, start_key, end_key, "\n\tkvs:", key_value_map[server_id])
    return key_value_map


def cleanup():
    print("Cleaning up..")
    try:
        subprocess.run(["just", "p2", "kill"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running just p2 kill: {e}")


def perform_puts_and_swaps(manager_addr, kv_map):
    for server_id, server_info in kv_map.items():
        print(f"\nServer id {server_id}:")
        commands = ""
        for key, value in server_info["key_values"]:
            commands += f"PUT {key} {value}\n"
            commands += (
                f"SWAP {key} {value * 2}\n"
            )
        commands += "STOP\n"
        print('command', commands)
        subprocess.run(["just", "p2::client", manager_addr], input=commands, text=True)


def perform_gets_and_scans(manager_addr, kv_map):
    commands = ""
    all_keys = []
    for server_id, server_info in kv_map.items():
        print(f"\nServer id {server_id}:")
        keys = [key for key, _ in server_info["key_values"]]
        all_keys.extend(keys)
        # GET for each key
        for key in keys:
            commands += f"GET {key}\n"
        commands += "STOP\n"
        print('command', commands)
        subprocess.run(["just", "p2::client", manager_addr], input=commands, text=True)

    commands = ""
    all_keys.sort()
    for i in range(len(all_keys) - 2, 2):
        commands += f"SCAN {all_keys[i]} {all_keys[i + 2]}\n"
    if all_keys:
        commands += f"SCAN {all_keys[0]} {all_keys[-1]}\n"
    commands += "STOP\n"
    print('command', commands)
    subprocess.run(["just", "p2::client", manager_addr], input=commands, text=True)


def start_manager():
    print("Starting manager")
    manager_process = subprocess.Popen(
        ["just", "p2::manager", MANAGER_PORT, ",".join(SERVER_ADDRS)],
        stdout=open("mngr.log", "w"),
        stderr=subprocess.STDOUT,
        start_new_session=True  # Fully detaches from the parent process
    )
    print(f"Manager PID: {manager_process.pid}")
    time.sleep(1)
    return manager_process


def start_servers():
    server_processes = {}
    for i, addr in enumerate(SERVER_ADDRS):
        server_id = str(i)
        backer_path = os.path.join(DATA_DIR, f"server_{i}")
        os.makedirs(backer_path, exist_ok=True)
        port = addr.split(":")[1]
        print(f"Starting server {addr} (ID={server_id})..")
        server_process = subprocess.Popen(
            ["just", "p2::server", server_id, MANAGER_ADDR, port, backer_path],
            stdout=open(f"server_{server_id}.log", "w"),
            stderr=subprocess.STDOUT,
            start_new_session=True  # Fully detaches from the parent process
        )
        server_processes[i] = server_process.pid
        print(f"Server {server_id} {port} PID: {server_process.pid}")

    time.sleep(2)
    return server_processes

def is_process_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
    
def all_process_alive(server_processes):
    for server_id in server_processes:
        pid = server_processes[server_id]
        if is_process_alive(pid):
            print(f"Server {server_id} (PID {pid}) alive")

def find_process_by_port(port):
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True,
            check=True
        )
        for line in result.stdout.splitlines():
            if f"--api_listen 0.0.0.0:{port}" in line and "kvserver" in line:
                pid = int(line.split()[1])  # Extract PID
                return pid
    except Exception as e:
        print(f"Error finding process by port {port}: {e}")
    return None

def kill_server(server_id):
    port = SERVER_ADDRS[server_id].split(":")[1]  # Get the correct port
    pid = find_process_by_port(port)

    if pid:
        try:
            os.kill(pid, signal.SIGTERM)  # Send termination signal
            time.sleep(2)  # Allow process to terminate
            if find_process_by_port(port):  # Double-check if process is still running
                os.kill(pid, signal.SIGKILL)  # Force kill if still alive
            print(f"Server {server_id} (PID {pid}) killed")
        except Exception as e:
            print(f"{error()} killing server {server_id} (PID {pid}): {e}")
            exit(1)
    else:
        print(f"{warning()}Server {server_id} not found")
        exit(1)



def test_unaffected_operations(manager_addr, server_partitions, kv_map, killed_sid):
    commands = ""
    for server_id, server_info in kv_map.items():
        if server_id == killed_sid: 
            continue
        print(server_id, server_info)
        unaffected_keys = []
        unaffected_keys.extend([key for key, _ in server_info["key_values"]])
        print(unaffected_keys)
        if unaffected_keys:
            for key in unaffected_keys[:3]:  # Pick a few keys for testing
                commands += f"GET {key}\n"
    commands += "STOP\n"
    print('commands', commands)
    sys.stdout.flush()
    os.sync()
    subprocess.run(["timeout", "5", "just", "p2::client", manager_addr], input=commands, text=True)
    sys.stdout.flush()
    os.sync()

    # exit()
    for server_id, info in server_partitions.items():
        if server_id == killed_sid: 
            continue
        print(f"\n\nServer {server_id}")
        for start_k, end_k in info["partitions"]:
            _end = f"{start_k}{end_k}"
            if _end >= start_k and _end < end_k:
                commands = f"SCAN {start_k} {_end}\n"
                commands += "STOP\n"
                print('commands', commands)
                sys.stdout.flush()
                os.sync()
                subprocess.run(["just", "p2::client", manager_addr], input=commands, text=True)
    sys.stdout.flush()
    os.sync()

def force_flush():
    sys.stdout.flush() 
    # os.fsync(sys.stdout.fileno())  
    os.sync()  


def test_failed_partition(manager_addr, kv_map, killed_server_id):
    failed_keys = [
        key for key, _ in kv_map.get(killed_server_id, {}).get("key_values", [])
    ]
    if not failed_keys:
        print("No keys found for the failed server")
        return
    force_flush()
    failed_key = failed_keys[0]  # Pick the first key
    scan_end_key = failed_keys[-1] if len(failed_keys) > 1 else failed_key
    try:
        command = f"GET {failed_key}\nSTOP\n"
        print('command', command)
        subprocess.run(
            ["just", "p2::client", manager_addr],
            input=command,
            text=True,
            timeout=2, 
        )
        print(f"{error()}: Expected timeout but GET {failed_key} succeeded!")
    except subprocess.TimeoutExpired:
        print(f"{success()}: GET {failed_key} timed out as expected")


def restart_server(server_id):
    backer_path = os.path.join(DATA_DIR, f"server_{server_id}")
    os.makedirs(backer_path, exist_ok=True)
    addr = SERVER_ADDRS[server_id]
    port = addr.split(":")[1]

    server_process = subprocess.Popen(
        ["just", "p2::server", str(server_id), MANAGER_ADDR, port, backer_path],
        stdout=open(f"server_{server_id}_restarted.log", "w"),
        stderr=subprocess.STDOUT,
    )

    server_processes[server_id] = server_process.pid
    print(f"Server {server_id} restarted with PID: {server_process.pid}")
    time.sleep(2)


def test_recovered_partition(manager_addr, kv_map, recovered_server_id):
    recovered_keys = [
        key for key, _ in kv_map.get(recovered_server_id, {}).get("key_values", [])
    ]
    if not recovered_keys:
        print("No keys found for the recovered server. Skipping final GET test")
        return
    recovered_key = recovered_keys[0]  # Pick one key to test recovery
    commands = f"GET {recovered_key}\nSTOP\n"
    print(commands)
    subprocess.run(["just", "p2::client", manager_addr], input=commands, text=True)
    print("\nTest completed successfully!")


# signal.signal(signal.SIGINT, lambda sig, frame: cleanup() or sys.exit(0))
# signal.signal(signal.SIGTERM, lambda sig, frame: cleanup() or sys.exit(0))

print(f"\n{ORANGE}=== 1. Starting Partitioned Cluster ==={RESET}")
cleanup()
remove_backer_files()
manager_process = start_manager()
server_processes = start_servers()
commands="STOP"
subprocess.run(["just", "p2::client", MANAGER_ADDR], input=commands, text=True)
time.sleep(10)

print(success())

server_partitions = parse_partition_file(PARTS_INFO_PATH)
random_kv_data = gen_kvs_per_server(server_partitions, 1)

print(f"\n{ORANGE}=== 2. Doing PUTs and SWAPs ==={RESET}")
perform_puts_and_swaps(MANAGER_ADDR, random_kv_data)
print(success())

print(f"\n{ORANGE}=== 3. Doing GETs and SCANs ==={RESET}")
perform_gets_and_scans(MANAGER_ADDR, random_kv_data)
print(success())

all_process_alive(server_processes)
# exit()
print(f"\n{ORANGE}=== 4. Killing Server 1  ==={RESET}")
KILLED_SID = 1
kill_server(KILLED_SID)
all_process_alive(server_processes)
print(success())

for key, value in server_partitions.items():
    if key != KILLED_SID:
        continue
    print(f"Server: id {key} addr {value["addr"]}")
    for part in value["partitions"]:
        print(part)

print(f"{ORANGE}=== 5. GET and SCAN on unaffected partition (should succeed) ==={RESET}")
force_flush()
test_unaffected_operations(MANAGER_ADDR, server_partitions, random_kv_data, KILLED_SID)
print(success())
sys.stdout.flush()
os.sync()
time.sleep(5)

print(f"\n{ORANGE}=== 6. GET and SCAN on failed partition (should timeout) ==={RESET}")
force_flush()
test_failed_partition(MANAGER_ADDR, random_kv_data, KILLED_SID)
print(success())

print(f"\n{ORANGE}=== 7. Restarting Server 1 ==={RESET}")
force_flush()
restart_server(KILLED_SID)
print(success())

print(f"{ORANGE}=== 8. GET from recovered partition (should succeed) ==={RESET}")
force_flush()
test_recovered_partition(MANAGER_ADDR, random_kv_data, KILLED_SID)
print(success())

print(f"\n{GREEN}Test completed successfully!{RESET}")
force_flush()
cleanup()


