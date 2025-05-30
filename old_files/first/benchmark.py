import time
import psutil
import threading

cpu_usage_log = []

def log_cpu_usage():
    global cpu_usage_log
    while True:
        cpu_percent = psutil.cpu_percent(interval=0.01)
        cpu_usage_log.append(cpu_percent)

def benchmark_query(query_func, *args, **kwargs):
    global cpu_usage_log
    cpu_usage_log = []  

    process = psutil.Process()

    initial_memory = process.memory_info().rss/(1024*1024)

    cpu_thread = threading.Thread(target=log_cpu_usage, daemon=True)
    cpu_thread.start()

    start_time = time.time()
    result = query_func(*args, **kwargs)
    cpu_usage = process.cpu_percent(interval=0.1)/psutil.cpu_count(logical=True)
    final_memory = process.memory_info().rss/(1024*1024)
    duration = time.time() - start_time

    
    time.sleep(0.5)

    memory_delta = final_memory-initial_memory
    memory_percent_change = (memory_delta/initial_memory*100) 



    # Save CPU usage log to file
    with open("cpu_usage_log.txt", "w") as f:
        for usage in cpu_usage_log:
            f.write(f"{usage}\n")

    print(f"CPU Usage Log written to cpu_usage_log.txt")

    return {
        "result": result,
        "duration": duration,
        "cpu_usage": cpu_usage,
        "memory_delta_MB": memory_delta,
        "memory_percent_change": memory_percent_change,
        "cpu_log": cpu_usage_log
    }

def benchmark_crud_operation(operation_func, *args):
    global cpu_usage_log
    cpu_usage_log = []
    cpu_thread = threading.Thread(target=log_cpu_usage, daemon=True)
    cpu_thread.start()
    start_time = time.time()
    operation_func(*args)
    duration = time.time() - start_time
    print(f"CRUD Operation Time: {duration:.4f} seconds")

    # Save CPU usage log to file
    with open("cpu_usage_log.txt", "w") as f:
        for usage in cpu_usage_log:
            f.write(f"{usage}\n")
    print(f"CPU Usage Log written to cpu_usage_log.txt")
    
    return duration