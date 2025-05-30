import csv
import weaviate
import time
from sentence_transformers import SentenceTransformer

import psutil
import threading

cpu_usage_log = []

def log_cpu_usage():
    global cpu_usage_log
    while True:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        cpu_usage_log.append(cpu_percent)

client = weaviate.Client("http://localhost:8080")
embed_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

def create_ip_flow_embedding(flow_data):
    # flow_text = (
    #     f"Frame_Number:{flow_data['frame_number']} "
    #     f"Frame_Time:{flow_data['frame_time']} "
    #     f"Source_IP:{flow_data['source_ip']} "
    #     f"Destination_IP:{flow_data['destination_ip']} "
    #     f"Source_Port:{flow_data['source_port']} "
    #     f"Destination_Port:{flow_data['destination_port']} "
    #     f"Protocol:{flow_data['protocol']} "
    #     f"Frame_Length/Packet_Size:{flow_data['frame_length']}"
    # )
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
    global cpu_usage_log
    cpu_usage_log = []
    cpu_thread = threading.Thread(target=log_cpu_usage, daemon=True)
    cpu_thread.start()
    with open(csv_file, mode="r") as file:
        reader = csv.DictReader(file)
        start_time = time.time()
        for row in reader:
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
            vector_embedding = create_ip_flow_embedding(data_object)
            client.data_object.create(data_object, "IPFlow", vector=vector_embedding)
        duration = time.time() - start_time
    print(f"Ingestion Time: {duration:.4f} seconds")
    print("IP Flows ingested successfully with vector embeddings!")
    # Save CPU usage log to file
    with open("cpu_usage_log.txt", "w") as f:
        for usage in cpu_usage_log:
            f.write(f"{usage}\n")

    print(f"CPU Usage Log written to cpu_usage_log.txt")