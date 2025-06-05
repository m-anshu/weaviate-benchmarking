import csv
import weaviate
import time
from sentence_transformers import SentenceTransformer
import psutil
import threading

cpu_usage_log = []
weaviate_memory_log = []
python_memory_log = []

def log_process_metrics():
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

def save_process_logs():
    """Save process monitoring logs to files"""
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../weaviate-benchmarking'))
    log_path = os.path.join(BASE_DIR, "ingest_cpu_usage_log.txt")
    # CPU usage log
    with open(log_path, "w") as f:
        f.write("time_offset,cpu_percent\n")
        for i, usage in enumerate(cpu_usage_log):
            f.write(f"{i*0.1:.1f},{usage}\n")
    
    log_path = os.path.join(BASE_DIR, "ingest_weaviate_memory_log.txt")
    # Weaviate memory usage log (PRIMARY)
    with open(log_path, "w") as f:
        f.write("timestamp,memory_mb,memory_percent\n")
        for entry in weaviate_memory_log:
            f.write(f"{entry['timestamp']:.2f},{entry['memory_mb']:.2f},{entry['memory_percent']:.2f}\n")
    
    log_path = os.path.join(BASE_DIR, "ingest_python_memory_log.txt")
    # Python memory usage log (SECONDARY)
    with open(log_path, "w") as f:
        f.write("timestamp,memory_mb,memory_percent\n")
        for entry in python_memory_log:
            f.write(f"{entry['timestamp']:.2f},{entry['memory_mb']:.2f},{entry['memory_percent']:.2f}\n")
    
    print(f"\nIngestion monitoring logs saved:")
    print(f"  - CPU usage: ingest_cpu_usage_log.txt")
    print(f"  - Weaviate memory (PRIMARY): ingest_weaviate_memory_log.txt")
    print(f"  - Python memory (SECONDARY): ingest_python_memory_log.txt")


client = weaviate.Client("http://localhost:8080")
embed_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

def create_ip_flow_embedding(flow_data):
    flow_text = (
        f"Traffic from IP address {flow_data['source_ip']} to {flow_data['destination_ip']} "
        f"using {flow_data['protocol']} protocol on ports {flow_data['source_port']} -> {flow_data['destination_port']}. "
        f"Packet number {flow_data['frame_number']} was captured at {flow_data['frame_time']} and was {flow_data['frame_length']} bytes long."
    )
    return embed_model.encode(flow_text).tolist()

def safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

def insert_ip_flows(csv_file):
    global cpu_usage_log, weaviate_memory_log, python_memory_log
    
    print("=== STARTING IP FLOW INGESTION WITH COMPREHENSIVE MONITORING ===")
    
    # Reset logs
    cpu_usage_log = []
    weaviate_memory_log = []
    python_memory_log = []
    
    # Get initial state - Weaviate (PRIMARY)
    initial_weaviate = get_weaviate_memory_usage()
    if not initial_weaviate:
        print("WARNING: Weaviate process not found! Make sure Weaviate is running.")
    else:
        print(f"Initial Weaviate Memory: {initial_weaviate['memory_mb']:.2f} MB ({initial_weaviate['memory_percent']:.2f}%)")
    
    # Get initial state - Python (SECONDARY)
    initial_python = get_python_process_stats()
    if initial_python:
        print(f"Initial Python Memory: {initial_python['memory_mb']:.2f} MB ({initial_python['memory_percent']:.2f}%)")
    
    # Start comprehensive monitoring thread
    monitor_thread = threading.Thread(target=log_process_metrics, daemon=True)
    monitor_thread.start()
    
    # Count total rows for progress tracking
    total_rows = 0
    with open(csv_file, mode="r") as file:
        total_rows = sum(1 for line in file) - 1  # Subtract header row
    
    print(f"Total rows to process: {total_rows}")
    
    # Process the CSV file
    processed_rows = 0
    start_time = time.time()
    
    with open(csv_file, mode="r") as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            processed_rows += 1
            
            # Create data object
            data_object = {
                "frame_number": safe_int(row["frame.number"]),
                "frame_time": row["frame.time"],
                "source_ip": row["ip.src"],
                "destination_ip": row["ip.dst"],
                "source_port": safe_int(row["tcp.srcport"]),
                "destination_port": safe_int(row["tcp.dstport"]),
                "protocol": row["_ws.col.protocol"].strip().upper(),
                "frame_length": safe_int(row["frame.len"])
            }
            
            # Create vector embedding
            vector_embedding = create_ip_flow_embedding(data_object)
            
            # Insert into Weaviate
            client.data_object.create(data_object, "IPFlow", vector=vector_embedding)
            
            # Progress reporting every 100 rows
            if processed_rows % 100 == 0:
                elapsed_time = time.time() - start_time
                progress_percent = (processed_rows / total_rows) * 100
                rows_per_second = processed_rows / elapsed_time if elapsed_time > 0 else 0
                print(f"Progress: {processed_rows}/{total_rows} ({progress_percent:.1f}%) - {rows_per_second:.1f} rows/sec")
    
    # Calculate total duration
    duration = time.time() - start_time
    
    # Calculate throughput
    throughput = processed_rows / duration if duration > 0 else 0
    
    time.sleep(2.0)
    
    # Get final state
    final_weaviate = get_weaviate_memory_usage()
    final_python = get_python_process_stats()
    
    print(f"\n=== INGESTION COMPLETED ===")
    print(f"Total Ingestion Time: {duration:.4f} seconds")
    print(f"Total Rows Processed: {processed_rows}")
    print(f"Average Rows/Second: {throughput:.2f}")
    print("IP Flows ingested successfully with vector embeddings!")
    
    # Print Weaviate stats (PRIMARY)
    if initial_weaviate and final_weaviate:
        weaviate_delta_mb = final_weaviate['memory_mb'] - initial_weaviate['memory_mb']
        weaviate_delta_percent = final_weaviate['memory_percent'] - initial_weaviate['memory_percent']
        weaviate_peak_mb = max(entry['memory_mb'] for entry in weaviate_memory_log) if weaviate_memory_log else final_weaviate['memory_mb']
        weaviate_peak_percent = max(entry['memory_percent'] for entry in weaviate_memory_log) if weaviate_memory_log else final_weaviate['memory_percent']
        weaviate_avg_mb = sum(entry['memory_mb'] for entry in weaviate_memory_log) / len(weaviate_memory_log) if weaviate_memory_log else final_weaviate['memory_mb']
        weaviate_avg_percent = sum(entry['memory_percent'] for entry in weaviate_memory_log) / len(weaviate_memory_log) if weaviate_memory_log else final_weaviate['memory_percent']
        
        print(f"\n=== WEAVIATE MEMORY ANALYSIS (PRIMARY) ===")
        print(f"Initial: {initial_weaviate['memory_mb']:.2f} MB ({initial_weaviate['memory_percent']:.2f}%)")
        print(f"Final: {final_weaviate['memory_mb']:.2f} MB ({final_weaviate['memory_percent']:.2f}%)")
        print(f"Delta: {weaviate_delta_mb:.2f} MB ({weaviate_delta_percent:.2f}%)")
        print(f"Peak: {weaviate_peak_mb:.2f} MB ({weaviate_peak_percent:.2f}%)")
        print(f"Average: {weaviate_avg_mb:.2f} MB ({weaviate_avg_percent:.2f}%)")
        print(f"PID: {final_weaviate['pid']}")
    
    # Print Python stats (SECONDARY)
    if initial_python and final_python:
        python_delta_mb = final_python['memory_mb'] - initial_python['memory_mb']
        python_delta_percent = final_python['memory_percent'] - initial_python['memory_percent']
        python_peak_mb = max(entry['memory_mb'] for entry in python_memory_log) if python_memory_log else final_python['memory_mb']
        python_peak_percent = max(entry['memory_percent'] for entry in python_memory_log) if python_memory_log else final_python['memory_percent']
        python_avg_mb = sum(entry['memory_mb'] for entry in python_memory_log) / len(python_memory_log) if python_memory_log else final_python['memory_mb']
        python_avg_percent = sum(entry['memory_percent'] for entry in python_memory_log) / len(python_memory_log) if python_memory_log else final_python['memory_percent']
        
        print(f"\n=== PYTHON CLIENT MEMORY ANALYSIS (SECONDARY) ===")
        print(f"Initial: {initial_python['memory_mb']:.2f} MB ({initial_python['memory_percent']:.2f}%)")
        print(f"Final: {final_python['memory_mb']:.2f} MB ({final_python['memory_percent']:.2f}%)")
        print(f"Delta: {python_delta_mb:.2f} MB ({python_delta_percent:.2f}%)")
        print(f"Peak: {python_peak_mb:.2f} MB ({python_peak_percent:.2f}%)")
        print(f"Average: {python_avg_mb:.2f} MB ({python_avg_percent:.2f}%)")
    
    # CPU stats
    if cpu_usage_log:
        avg_cpu = sum(cpu_usage_log) / len(cpu_usage_log)
        peak_cpu = max(cpu_usage_log)
        min_cpu = min(cpu_usage_log)
        
        print(f"\n=== CPU USAGE ANALYSIS ===")
        print(f"Average CPU: {avg_cpu:.2f}%")
        print(f"Peak CPU: {peak_cpu:.2f}%")
        print(f"Minimum CPU: {min_cpu:.2f}%")
    
    # Performance metrics
    print(f"\n=== PERFORMANCE METRICS ===")
    print(f"Total Processing Time: {duration:.2f} seconds")
    print(f"Rows Processed: {processed_rows}")
    print(f"Processing Rate: {throughput:.2f} rows/second")
    if weaviate_memory_log:
        print(f"Memory Samples Collected: {len(weaviate_memory_log)} (Weaviate), {len(python_memory_log)} (Python)")
        print(f"CPU Samples Collected: {len(cpu_usage_log)}")
    
    save_process_logs()
    
    # Return metrics for programmatic use
    return {
        "duration": duration,
        "processed_rows": processed_rows,
        "throughput": throughput
    }

