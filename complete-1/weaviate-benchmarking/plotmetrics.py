import matplotlib.pyplot as plt
import pandas as pd
import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # this points to weaviate-benchmarking
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "screenshots")

if not os.path.exists(SCREENSHOTS_DIR):
    os.makedirs(SCREENSHOTS_DIR)

# Define the data
data = {
    "Record Count": [2500, 5000, 7500, 10000],
    "Ingestion Time (s)": [133.2, 253.94, 401.854, 489.622],
    "Query Latency (s)": [0.1544, 0.1276, 0.1539, 0.2076],
    "Query Throughput (q/s)": [6.47, 7.8759, 6.49, 4.8162],
    "Similarity Score": [0.5721, 0.594, 0.595, 0.6044],
    "Update Time (s)": [5.67, 12.19, 17.56, 31.36],
    "Update Throughput": [440.7, 406.31, 424.14, 311.95],
    "Delete Time (s)": [3.85, 7.63, 12.65, 16.78],
    "Delete Throughput": [649.09, 649.14, 588.77, 583.01],
}

df = pd.DataFrame(data)
df["Ingestion Throughput"] = df["Record Count"] / df["Ingestion Time (s)"]

def save_plot(x, y, title, xlabel, ylabel, filename):
    plt.figure(figsize=(8, 5))
    plt.plot(x, y, marker='o')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.savefig(os.path.join(SCREENSHOTS_DIR, filename))


save_plot(df["Record Count"], df["Ingestion Throughput"], "Ingestion Throughput vs Record Count", "Record Count", "Ingestion Throughput (records/s)", "ingestion_throughput.png")
save_plot(df["Record Count"], df["Ingestion Time (s)"], "Ingestion Time vs Record Count", "Record Count", "Ingestion Time (s)", "ingestion_time.png")
save_plot(df["Record Count"], df["Query Latency (s)"], "Query Latency vs Record Count", "Record Count", "Latency (s)", "query_latency.png")
save_plot(df["Record Count"], df["Update Time (s)"], "Update Time vs Record Count", "Record Count", "Update Time (s)", "update_time.png")
save_plot(df["Record Count"], df["Delete Time (s)"], "Delete Time vs Record Count", "Record Count", "Delete Time (s)", "delete_time.png")
save_plot(df["Record Count"], df["Query Throughput (q/s)"], "Query Throughput vs Record Count", "Record Count", "Query Throughput (queries/s)", "query_throughput.png")
save_plot(df["Record Count"], df["Update Throughput"], "Update Throughput vs Record Count", "Record Count", "Update Throughput", "update_throughput.png")
save_plot(df["Record Count"], df["Delete Throughput"], "Delete Throughput vs Record Count", "Record Count", "Delete Throughput", "delete_throughput.png")
