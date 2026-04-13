"""
Extract CPU and memory data from sar logs and generate plot
"""

import os
import re
import pandas as pd
import matplotlib.pyplot as plt

SERVER_LOG_DIR = "perf_logs"

# Find the latest benchmark dir
latest_dir = sorted(os.listdir(SERVER_LOG_DIR))[-1]
log_dir = os.path.join(SERVER_LOG_DIR, latest_dir)

# Output directory for plots
plot_dir = os.path.join(log_dir, "plots")
os.makedirs(plot_dir, exist_ok=True)

workloads = ["a", "b", "c", "d", "e", "f"]
n_clients_list = [1, 10, 25, 40, 55, 70, 85]

def parse_sar_log(file_path):
    cpu_data, mem_data = [], []
    
    with open(file_path, "r") as file:
        for line in file:
            # cpu usage
            cpu_match = re.match(r'(\d{2}:\d{2}:\d{2} [APM]{2})\s+all\s+([\d.]+)\s+[\d.]+\s+([\d.]+)\s+[\d.]+\s+[\d.]+\s+([\d.]+)', line)
            # memory usage
            mem_match = re.match(r'(\d{2}:\d{2}:\d{2} [APM]{2})\s+\d+\s+\d+\s+(\d+)', line)
            
            if cpu_match:
                time, user, system, idle = cpu_match.groups()
                total_cpu = float(user) + float(system)
                cpu_data.append([time, total_cpu])

            if mem_match:
                time, mem_used = mem_match.groups()
                mem_data.append([time, int(mem_used) / (1024 ** 2)])  # Convert KB to GB

    df_cpu = pd.DataFrame(cpu_data, columns=["Time", "CPU Usage (%)"])
    df_mem = pd.DataFrame(mem_data, columns=["Time", "Memory Usage (GB)"])
    df_cpu["Time"] = pd.to_datetime(df_cpu["Time"], format="%I:%M:%S %p")
    df_mem["Time"] = pd.to_datetime(df_mem["Time"], format="%I:%M:%S %p")

    return df_cpu, df_mem

# Gen subplots for each workload
for workload in workloads:
    fig_cpu, axes_cpu = plt.subplots(len(n_clients_list), 1, figsize=(10, len(n_clients_list) * 3), sharex=True)
    fig_mem, axes_mem = plt.subplots(len(n_clients_list), 1, figsize=(10, len(n_clients_list) * 3), sharex=True)

    for idx, n_clients in enumerate(n_clients_list):
        log_file = os.path.join(log_dir, f"sar_{n_clients}_{workload}.txt")
        
        if os.path.exists(log_file):
            df_cpu, df_mem = parse_sar_log(log_file)

            if not df_cpu.empty:
                axes_cpu[idx].plot(df_cpu["Time"], df_cpu["CPU Usage (%)"], color="r", label=f"{n_clients} Clients")
                axes_cpu[idx].set_ylabel("CPU (%)")
                axes_cpu[idx].legend()
                axes_cpu[idx].grid()

            if not df_mem.empty:
                axes_mem[idx].plot(df_mem["Time"], df_mem["Memory Usage (GB)"], color="b", label=f"{n_clients} Clients")
                axes_mem[idx].set_ylabel("Memory (GB)")
                axes_mem[idx].legend()
                axes_mem[idx].grid()

    fig_cpu.suptitle(f"CPU Usage for Workload {workload.upper()}")
    fig_mem.suptitle(f"Memory Usage for Workload {workload.upper()}")

    fig_cpu.autofmt_xdate()
    fig_mem.autofmt_xdate()

    cpu_plot_path = os.path.join(plot_dir, f"workload_{workload}_cpu.png")
    mem_plot_path = os.path.join(plot_dir, f"workload_{workload}_mem.png")
    
    fig_cpu.savefig(cpu_plot_path)
    fig_mem.savefig(mem_plot_path)

    plt.close(fig_cpu)
    plt.close(fig_mem)

print("All plots generated!")
