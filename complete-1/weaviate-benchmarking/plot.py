import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

def plot_cpu_usage(filename, title_suffix=""):
    if not os.path.exists(filename):
        print(f"File {filename} not found, skipping...")
        return
    
    try:
        # if 'time_offset' in open(filename).readline():
        with open(filename) as f:
            first_line = f.readline()
        if 'time_offset' in first_line:
            df = pd.read_csv(filename)
            time_data = df['time_offset']
            cpu_data = df['cpu_percent']
        else:
            with open(filename, 'r') as f:
                cpu_data = [float(line.strip()) for line in f if line.strip()]
            time_data = np.arange(0, len(cpu_data) * 0.1, 0.1)[:len(cpu_data)]
        
        plt.figure(figsize=(8, 5))
        plt.plot(time_data, cpu_data, marker='o', markersize=2)
        plt.title(f"CPU Usage {title_suffix}")
        plt.xlabel("Time (seconds)")
        plt.ylabel("CPU Usage (%)")
        plt.grid(True)
        
        # output_filename = f"cpu_usage_{title_suffix.lower().replace(' ', '_')}.png"
        output_filename = f"{os.path.splitext(filename)[0]}.png"
        plt.savefig(output_filename)
        plt.close()
        
        print(f"CPU Usage plot saved: {output_filename}")
        print(f"  Average CPU: {np.mean(cpu_data):.2f}%")
        print(f"  Peak CPU: {np.max(cpu_data):.2f}%")
        
    except Exception as e:
        print(f"Error plotting CPU usage from {filename}: {e}")

def plot_memory_usage_mb(filename, title_suffix=""):
    """Plot memory usage in MB from log file"""
    if not os.path.exists(filename):
        print(f"File {filename} not found, skipping...")
        return
    
    try:
        df = pd.read_csv(filename)
        
        if 'timestamp' in df.columns:
            start_time = df['timestamp'].iloc[0]
            time_data = df['timestamp'] - start_time
        else:
            time_data = np.arange(len(df)) * 0.1
        
        memory_mb = df['memory_mb']
        
        plt.figure(figsize=(8, 5))
        plt.plot(time_data, memory_mb, marker='o', markersize=2)
        plt.title(f"Memory Usage (MB) {title_suffix}")
        plt.xlabel("Time (seconds)")
        plt.ylabel("Memory Usage (MB)")
        plt.grid(True)
        
        # output_filename = f"memory_mb_{title_suffix.lower().replace(' ', '_')}.png"
        output_filename = f"{os.path.splitext(filename)[0]}.png"
        plt.savefig(output_filename)
        plt.close()
        
        print(f"Memory Usage (MB) plot saved: {output_filename}")
        print(f"  Average Memory: {np.mean(memory_mb):.2f} MB")
        print(f"  Peak Memory: {np.max(memory_mb):.2f} MB")
        
    except Exception as e:
        print(f"Error plotting memory usage from {filename}: {e}")

def plot_memory_usage_percent(filename, title_suffix=""):
    """Plot memory usage in percentage from log file"""
    if not os.path.exists(filename):
        print(f"File {filename} not found, skipping...")
        return
    
    try:
        df = pd.read_csv(filename)
        
        if 'timestamp' in df.columns:
            start_time = df['timestamp'].iloc[0]
            time_data = df['timestamp'] - start_time
        else:
            time_data = np.arange(len(df)) * 0.1
        
        memory_percent = df['memory_percent']
        
        plt.figure(figsize=(8, 5))
        plt.plot(time_data, memory_percent, marker='o', markersize=2)
        plt.title(f"Memory Usage (%) {title_suffix}")
        plt.xlabel("Time (seconds)")
        plt.ylabel("Memory Usage (%)")
        plt.grid(True)
        
        # output_filename = f"memory_percent_{title_suffix.lower().replace(' ', '_')}.png"
        output_filename = f"{os.path.splitext(filename)[0]}.png"
        plt.savefig(output_filename)
        plt.close()
        
        print(f"Memory Usage (%) plot saved: {output_filename}")
        print(f"  Average Memory: {np.mean(memory_percent):.2f}%")
        print(f"  Peak Memory: {np.max(memory_percent):.2f}%")
        
    except Exception as e:
        print(f"Error plotting memory usage from {filename}: {e}")

def main():
    """Generate all simple plots"""
    print("=== Generating Simple Log Plots ===\n")
    
    log_files = [
        ("cpu_usage_log.txt", "Benchmark"),
        ("ingest_cpu_usage_log.txt", "Ingestion"),
        ("weaviate_memory_log.txt", "Weaviate Benchmark"),
        ("ingest_weaviate_memory_log.txt", "Weaviate Ingestion"),
        ("python_memory_log.txt", "Python Benchmark"),
        ("ingest_python_memory_log.txt", "Python Ingestion")
    ]
    
    # Generate CPU plots
    for filename, title in log_files:
        if "cpu" in filename:
            plot_cpu_usage(filename, title)
            print()
    
    # Generate memory plots (MB)
    for filename, title in log_files:
        if "memory" in filename:
            plot_memory_usage_mb(filename, title)
            print()
    
    # Generate memory plots (%)
    for filename, title in log_files:
        if "memory" in filename:
            plot_memory_usage_percent(filename, title)
            print()
    
    print("=== All plots generated successfully! ===")
    print("Check the current directory for all generated PNG files.")

if __name__ == "__main__":
    main()