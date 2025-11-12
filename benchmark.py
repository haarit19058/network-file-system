import os
import subprocess
import time
import json
import pandas as pd
import matplotlib.pyplot as plt


chunk_sizes = [
    # 1024,        # 1 KB
    # 8192,        # 8 KB
    # 65536,       # 64 KB
    # 131072,      # 128 KB
    524288,      # 512 KB
    # 1048576,     # 1 MB
    # 4194304,     # 4 MB
    # 8388608      # 8 MB
]
concurrent_connections = [ 
    # 4,
    8, 
    # 16,
    # 32 ,
    ]
cache_sizes = [
    # 4 * 1024 * 1024,    # 4 MB
    16 * 1024 * 1024,   # 16 MB
    # 64 * 1024 * 1024,   # 64 MB
    # 256 * 1024 * 1024   # 256 MB
]
read_ahead_sizes = [
    131072,    # 128 KB
    262144,    # 256 KB
    524288,    # 512 KB
    1048576,    # 1 MB
    4 * 1024 * 1024,   # 4 MB
    8 * 1024 * 1024   # 8 MB
]
batch_write_thresholds = [
    # 256 * 1024,    # 256 KB
    512 * 1024,    # 512 KB
    # 1 * 1024 * 1024,   # 1 MB
    # 4 * 1024 * 1024    # 4 MB
]

# Fixed parameter
cache_block_size = 4096  # Fixed at 4 KB

# --- Script Setup ---
MOUNT_DIR = "./mntdir"
RESULTS_FILE = "benchmark_results.csv"
PLOTS_DIR = "benchmark_plots"
results_list = []

# Ensure mount directory and plot directory exist
# os.makedirs(MOUNT_DIR, exist_ok=True)
os.makedirs(PLOTS_DIR, exist_ok=True)

# Function to parse FIO JSON output
def parse_fio_json(fio_output):
    """Parses FIO JSON output and returns read/write bandwidth in KiB/s."""
    try:
        data = json.loads(fio_output)
        # 'bw' is the average bandwidth in KiB/s
        read_bw = data['jobs'][0]['read']['bw']
        write_bw = data['jobs'][0]['write']['bw']
        return read_bw, write_bw
    except (json.JSONDecodeError, KeyError, IndexError):
        print("Error parsing FIO output.")
        return 0, 0

# --- Main Test Loop ---
total_runs = len(chunk_sizes) * len(concurrent_connections) * len(cache_sizes) * len(read_ahead_sizes) * len(batch_write_thresholds)
current_run = 0

try:
    for chunk_size in chunk_sizes:
        for connections in concurrent_connections:
            for cache_size in cache_sizes:
                for read_ahead in read_ahead_sizes:
                    for write_threshold in batch_write_thresholds:
                        current_run += 1
                        print(f"\n--- Running Test {current_run} / {total_runs} ---")
                        print(f"Chunk: {chunk_size}, Conns: {connections}, Cache: {cache_size}, ReadAhead: {read_ahead}, WriteThresh: {write_threshold}")

                        # 1. Write constants file
                        with open("constants.cpp", "w") as f:
                            f.write(f"#pragma once\n")
                            f.write(f"#define CHUNK_SIZE {chunk_size}\n")
                            f.write(f"#define POOL_SIZE {connections}\n")
                            f.write(f"#define CACHE_BLOCK_SIZE {cache_block_size}\n")
                            f.write(f"#define CACHE_CAPACITY_BYTES {cache_size}\n")
                            f.write(f"#define READAHEAD_SIZE {read_ahead}\n")
                            f.write(f"#define BATCH_WRITE_THRESHOLD {write_threshold}\n")

                        # 2. Compile the code
                        print("Compiling...")
                        compile_result = subprocess.run(["g++", "-o", "client", "client.cpp", "-lfuse3"], capture_output=True, text=True)
                        if compile_result.returncode != 0:
                            print(f"Compile FAILED:\n{compile_result.stderr}")
                            continue  # Skip this test

                        # 3. Mount the client in the background
                        print("Mounting FUSE client...")
                        client_process = subprocess.Popen(["./client", MOUNT_DIR, "127.0.0.1", "3030"])
                        
                        # Wait for mount to complete
                        time.sleep(2) 

                        read_bw = 0
                        write_bw = 0

                        try:
                            # 4. Run FIO tests
                            print("Running FIO write test...")
                            fio_write_cmd = [
                                "fio", "--name=nfs_write_test", f"--directory={MOUNT_DIR}",
                                "--rw=write", "--size=10M", "--bs=1M", "--numjobs=1",
                                "--direct=1", "--runtime=10", "--time_based",
                                "--output-format=json"
                            ]
                            write_result = subprocess.run(fio_write_cmd, capture_output=True, text=True, check=True)
                            
                            # Parse write result (FIO JSON includes both read/write sections, but only 'write' will have data)
                            _, write_bw_val = parse_fio_json(write_result.stdout)
                            write_bw = write_bw_val / 1024 # Convert KiB/s to MiB/s
                            print(f"Write throughput: {write_bw:.2f} MiB/s")


                            print("Running FIO read test...")
                            fio_read_cmd = [
                                "fio", "--name=nfs_read_test", f"--directory={MOUNT_DIR}",
                                "--rw=read", "--size=10M", "--bs=1M", "--numjobs=1",
                                "--direct=1", "--runtime=10", "--time_based",
                                "--output-format=json"
                            ]
                            read_result = subprocess.run(fio_read_cmd, capture_output=True, text=True, check=True)

                            # Parse read result
                            read_bw_val, _ = parse_fio_json(read_result.stdout)
                            read_bw = read_bw_val / 1024 # Convert KiB/s to MiB/s
                            print(f"Read throughput: {read_bw:.2f} MiB/s")

                        except subprocess.CalledProcessError as e:
                            print(f"FIO test FAILED:\n{e.stderr}")
                        
                        finally:
                            time.sleep(10)
                            # 5. Unmount and clean up
                            print("Unmounting...")
                            subprocess.run(["fusermount3", "-u", MOUNT_DIR], check=True)
                            time.sleep(1) # Give time to unmount
                            client_process.terminate() # Terminate the client process
                            client_process.wait() # Wait for it to exit

                        # 6. Save results
                        results_list.append({
                            "chunk_size": chunk_size,
                            "connections": connections,
                            "cache_size": cache_size,
                            "read_ahead": read_ahead,
                            "write_threshold": write_threshold,
                            "read_throughput_mib": read_bw,
                            "write_throughput_mib": write_bw
                        })

except KeyboardInterrupt:
    print("\nBenchmark interrupted by user. Saving partial results...")

finally:
    # --- Data Processing ---
    if results_list:
        print(f"\nBenchmark finished. Saving results to {RESULTS_FILE}")
        df = pd.DataFrame(results_list)
        df.to_csv(RESULTS_FILE, index=False)
        
        print(df.head())

        # --- Plotting ---
        print(f"Generating plots in {PLOTS_DIR}...")
        
        # Define parameters to plot against
        params_to_plot = [
            'chunk_size', 
            'connections', 
            'cache_size', 
            'read_ahead', 
            'write_threshold'
        ]
        
        # Convert sizes to MB for cleaner plot labels
        for col in ['chunk_size', 'cache_size', 'read_ahead', 'write_threshold']:
            if col in df.columns:
                df[f'{col}_mb'] = df[col] / (1024 * 1024)
        
        param_map_mb = {
            'chunk_size': 'chunk_size_mb',
            'connections': 'connections',
            'cache_size': 'cache_size_mb',
            'read_ahead': 'read_ahead_mb',
            'write_threshold': 'write_threshold_mb'
        }

        for param in params_to_plot:
            plot_param = param_map_mb[param]
            unit = "MB" if "mb" in plot_param else "count"
            
            # Group by the parameter and calculate the mean throughput
            # This averages out the effects of all *other* parameters
            grouped_data = df.groupby(plot_param)[['read_throughput_mib', 'write_throughput_mib']].mean()

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), sharex=True)

            # Read Throughput Plot
            grouped_data['read_throughput_mib'].plot(ax=ax1, marker='o', grid=True)
            ax1.set_title(f'Average Read Throughput vs {param.replace("_", " ").title()}')
            ax1.set_ylabel('Avg. Read Throughput (MiB/s)')
            
            # Write Throughput Plot
            grouped_data['write_throughput_mib'].plot(ax=ax2, marker='o', color='orange', grid=True)
            ax2.set_title(f'Average Write Throughput vs {param.replace("_", " ").title()}')
            ax2.set_ylabel('Avg. Write Throughput (MiB/s)')
            ax2.set_xlabel(f'{param.replace("_", " ").title()} ({unit})')

            # Use log scale for size-based parameters
            if "mb" in plot_param:
                ax2.set_xscale('log')

            plt.tight_layout()
            plot_filename = os.path.join(PLOTS_DIR, f'throughput_vs_{param}.png')
            plt.savefig(plot_filename)
            plt.close(fig)
            print(f"Saved plot: {plot_filename}")

    else:
        print("No results to save or plot.")

print("Done.")