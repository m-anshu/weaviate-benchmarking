# benchmark.py - Weaviate-focused memory monitoring
import time
import psutil
import threading
import os

# Global variables for logging
cpu_usage_log = []
weaviate_memory_log = []
python_memory_log = []

def log_process_metrics():
    """Log CPU and memory usage for both Weaviate and Python processes"""
    global cpu_usage_log, weaviate_memory_log, python_memory_log
    
    python_process = psutil.Process()
    system_memory = psutil.virtual_memory()
    
    while True:
        try:
            # CPU usage (system-wide)
            cpu_percent = psutil.cpu_percent(interval=0.01)
            cpu_usage_log.append(cpu_percent)
            
            # Python process memory
            python_memory_info = python_process.memory_info()
            python_memory_mb = python_memory_info.rss / (1024 * 1024)
            python_memory_percent = (python_memory_info.rss / system_memory.total) * 100
            
            python_memory_log.append({
                'memory_mb': python_memory_mb,
                'memory_percent': python_memory_percent,
                'timestamp': time.time()
            })
            
            # Weaviate process memory (primary focus)
            weaviate_memory = get_weaviate_memory_usage()
            if weaviate_memory:
                weaviate_memory_log.append({
                    'memory_mb': weaviate_memory['memory_mb'],
                    'memory_percent': weaviate_memory['memory_percent'],
                    'timestamp': time.time()
                })
            
            time.sleep(0.1)  # Sample every 100ms
            
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            break

def get_weaviate_memory_usage():
    """
    Get Weaviate server memory usage - PRIMARY MONITORING TARGET
    Returns memory in MB and percentage
    """
    try:
        system_memory = psutil.virtual_memory()
        for proc in psutil.process_iter(['pid', 'name', 'memory_info', 'cpu_percent']):
            proc_name = proc.info['name'].lower()
            if 'weaviate' in proc_name or 'weaviate-server' in proc_name:
                memory_info = proc.info['memory_info']
                memory_mb = memory_info.rss / (1024 * 1024)
                memory_percent = (memory_info.rss / system_memory.total) * 100
                return {
                    'pid': proc.info['pid'],
                    'name': proc.info['name'],
                    'memory_mb': memory_mb,
                    'memory_percent': memory_percent
                }
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    
    return None

def get_python_process_stats():
    """Get current Python process stats - SECONDARY MONITORING"""
    try:
        process = psutil.Process()
        system_memory = psutil.virtual_memory()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / (1024 * 1024)
        memory_percent = (memory_info.rss / system_memory.total) * 100
        
        return {
            'pid': process.pid,
            'memory_mb': memory_mb,
            'memory_percent': memory_percent
        }
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None

def benchmark_query(query_func, *args, **kwargs):
    """Benchmark function with Weaviate-focused monitoring"""
    global cpu_usage_log, weaviate_memory_log, python_memory_log
    
    # Reset logs
    cpu_usage_log = []
    weaviate_memory_log = []
    python_memory_log = []
    
    # Get initial state - Weaviate (PRIMARY)
    initial_weaviate = get_weaviate_memory_usage()
    if not initial_weaviate:
        print("WARNING: Weaviate process not found! Make sure Weaviate is running.")
    
    # Get initial state - Python (SECONDARY)
    initial_python = get_python_process_stats()
    
    # Start monitoring thread
    monitor_thread = threading.Thread(target=log_process_metrics, daemon=True)
    monitor_thread.start()
    
    # Execute the query
    start_time = time.time()
    result = query_func(*args, **kwargs)
    duration = time.time() - start_time
    
    # Wait a bit to capture post-execution metrics
    time.sleep(0.5)
    
    # Get final state
    final_weaviate = get_weaviate_memory_usage()
    final_python = get_python_process_stats()
    
    # Calculate Weaviate statistics (PRIMARY METRICS)
    weaviate_stats = {}
    if initial_weaviate and final_weaviate and weaviate_memory_log:
        weaviate_stats = {
            "initial_mb": initial_weaviate['memory_mb'],
            "initial_percent": initial_weaviate['memory_percent'],
            "final_mb": final_weaviate['memory_mb'],
            "final_percent": final_weaviate['memory_percent'],
            "delta_mb": final_weaviate['memory_mb'] - initial_weaviate['memory_mb'],
            "delta_percent": final_weaviate['memory_percent'] - initial_weaviate['memory_percent'],
            "peak_mb": max(entry['memory_mb'] for entry in weaviate_memory_log),
            "peak_percent": max(entry['memory_percent'] for entry in weaviate_memory_log),
            "average_mb": sum(entry['memory_mb'] for entry in weaviate_memory_log) / len(weaviate_memory_log),
            "average_percent": sum(entry['memory_percent'] for entry in weaviate_memory_log) / len(weaviate_memory_log),
            "pid": final_weaviate['pid']
        }
    
    # Calculate Python statistics (SECONDARY METRICS)
    python_stats = {}
    if initial_python and final_python and python_memory_log:
        python_stats = {
            "initial_mb": initial_python['memory_mb'],
            "initial_percent": initial_python['memory_percent'],
            "final_mb": final_python['memory_mb'],
            "final_percent": final_python['memory_percent'],
            "delta_mb": final_python['memory_mb'] - initial_python['memory_mb'],
            "delta_percent": final_python['memory_percent'] - initial_python['memory_percent'],
            "peak_mb": max(entry['memory_mb'] for entry in python_memory_log),
            "peak_percent": max(entry['memory_percent'] for entry in python_memory_log),
            "average_mb": sum(entry['memory_mb'] for entry in python_memory_log) / len(python_memory_log),
            "average_percent": sum(entry['memory_percent'] for entry in python_memory_log) / len(python_memory_log)
        }
    
    # Calculate CPU statistics
    avg_cpu = sum(cpu_usage_log) / len(cpu_usage_log) if cpu_usage_log else 0
    peak_cpu = max(cpu_usage_log) if cpu_usage_log else 0
    
    # Save logs to files
    save_process_logs()
    
    benchmark_results = {
        "result": result,
        "duration": duration,
        "weaviate_memory_stats": weaviate_stats,  # PRIMARY METRICS
        "python_memory_stats": python_stats,     # SECONDARY METRICS
        "cpu_stats": {
            "average_percent": avg_cpu,
            "peak_percent": peak_cpu
        },
        "logs": {
            "cpu_log": cpu_usage_log,
            "weaviate_memory_log": weaviate_memory_log,
            "python_memory_log": python_memory_log
        }
    }
    
    return benchmark_results

def benchmark_crud_operation(operation_func, *args):
    """CRUD benchmark with Weaviate-focused monitoring"""
    global cpu_usage_log, weaviate_memory_log, python_memory_log
    
    # Reset logs
    cpu_usage_log = []
    weaviate_memory_log = []
    python_memory_log = []
    
    # Get initial state
    initial_weaviate = get_weaviate_memory_usage()
    initial_python = get_python_process_stats()
    
    if not initial_weaviate:
        print("WARNING: Weaviate process not found! Make sure Weaviate is running.")
    
    # Start monitoring
    monitor_thread = threading.Thread(target=log_process_metrics, daemon=True)
    monitor_thread.start()
    
    # Execute operation
    start_time = time.time()
    operation_func(*args)
    duration = time.time() - start_time
    
    # Wait to capture post-operation metrics
    time.sleep(1.0)
    
    # Get final state
    final_weaviate = get_weaviate_memory_usage()
    final_python = get_python_process_stats()
    
    print(f"CRUD Operation Time: {duration:.4f} seconds")
    
    # Print Weaviate stats (PRIMARY)
    if initial_weaviate and final_weaviate:
        weaviate_delta_mb = final_weaviate['memory_mb'] - initial_weaviate['memory_mb']
        weaviate_delta_percent = final_weaviate['memory_percent'] - initial_weaviate['memory_percent']
        weaviate_peak_mb = max(entry['memory_mb'] for entry in weaviate_memory_log) if weaviate_memory_log else final_weaviate['memory_mb']
        weaviate_peak_percent = max(entry['memory_percent'] for entry in weaviate_memory_log) if weaviate_memory_log else final_weaviate['memory_percent']
        
        print(f"\n=== WEAVIATE MEMORY (PRIMARY) ===")
        print(f"Initial: {initial_weaviate['memory_mb']:.2f} MB ({initial_weaviate['memory_percent']:.2f}%)")
        print(f"Final: {final_weaviate['memory_mb']:.2f} MB ({final_weaviate['memory_percent']:.2f}%)")
        print(f"Delta: {weaviate_delta_mb:.2f} MB ({weaviate_delta_percent:.2f}%)")
        print(f"Peak: {weaviate_peak_mb:.2f} MB ({weaviate_peak_percent:.2f}%)")
    
    # Print Python stats (SECONDARY)
    if initial_python and final_python:
        python_delta_mb = final_python['memory_mb'] - initial_python['memory_mb']
        python_delta_percent = final_python['memory_percent'] - initial_python['memory_percent']
        python_peak_mb = max(entry['memory_mb'] for entry in python_memory_log) if python_memory_log else final_python['memory_mb']
        python_peak_percent = max(entry['memory_percent'] for entry in python_memory_log) if python_memory_log else final_python['memory_percent']
        
        print(f"\n=== PYTHON CLIENT (SECONDARY) ===")
        print(f"Initial: {initial_python['memory_mb']:.2f} MB ({initial_python['memory_percent']:.2f}%)")
        print(f"Final: {final_python['memory_mb']:.2f} MB ({final_python['memory_percent']:.2f}%)")
        print(f"Delta: {python_delta_mb:.2f} MB ({python_delta_percent:.2f}%)")
        print(f"Peak: {python_peak_mb:.2f} MB ({python_peak_percent:.2f}%)")
    
    # CPU stats
    avg_cpu = sum(cpu_usage_log) / len(cpu_usage_log) if cpu_usage_log else 0
    print(f"\nAverage CPU: {avg_cpu:.2f}%")
    
    # Save logs
    save_process_logs()
    
    return duration

def save_process_logs():
    """Save process monitoring logs to files"""
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../weaviate-benchmarking'))
    log_path = os.path.join(BASE_DIR, "cpu_usage_log.txt")
    # CPU usage log
    with open(log_path, "w") as f:
        f.write("time_offset,cpu_percent\n")
        for i, usage in enumerate(cpu_usage_log):
            f.write(f"{i*0.1:.1f},{usage}\n")
    
    log_path = os.path.join(BASE_DIR, "weaviate_memory_log.txt")
    # Weaviate memory usage log (PRIMARY)
    with open(log_path, "w") as f:
        f.write("timestamp,memory_mb,memory_percent\n")
        for entry in weaviate_memory_log:
            f.write(f"{entry['timestamp']:.2f},{entry['memory_mb']:.2f},{entry['memory_percent']:.2f}\n")
    
    log_path = os.path.join(BASE_DIR, "python_memory_log.txt")
    # Python memory usage log (SECONDARY)
    with open(log_path, "w") as f:
        f.write("timestamp,memory_mb,memory_percent\n")
        for entry in python_memory_log:
            f.write(f"{entry['timestamp']:.2f},{entry['memory_mb']:.2f},{entry['memory_percent']:.2f}\n")
    
    print(f"\nProcess monitoring logs saved:")
    print(f"  - CPU usage: cpu_usage_log.txt")
    print(f"  - Weaviate memory (PRIMARY): weaviate_memory_log.txt")
    print(f"  - Python memory (SECONDARY): python_memory_log.txt")
