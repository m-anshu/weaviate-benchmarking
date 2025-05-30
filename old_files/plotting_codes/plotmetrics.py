import matplotlib.pyplot as plt
import pandas as pd

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

# Create DataFrame
df = pd.DataFrame(data)

df["Ingestion Throughput"] = df["Record Count"] / df["Ingestion Time (s)"]

# Plot Ingestion Throughput
plt.figure(figsize=(8, 5))
plt.plot(df["Record Count"], df["Ingestion Throughput"], marker='o')
plt.title("Ingestion Throughput vs Record Count")
plt.xlabel("Record Count")
plt.ylabel("Ingestion Throughput (records/s)")
plt.grid(True)
plt.savefig("./screenshots/ingestion_throughput.png")

# Individual operation time plots
plt.figure(figsize=(8, 5))
plt.plot(df["Record Count"], df["Ingestion Time (s)"], marker='o')
plt.title("Ingestion Time vs Record Count")
plt.xlabel("Record Count")
plt.ylabel("Ingestion Time (s)")
plt.grid(True)
plt.savefig("./screenshots/ingestion_time.png")

plt.figure(figsize=(8, 5))
plt.plot(df["Record Count"], df["Query Latency (s)"], marker='o')
plt.title("Query Latency vs Record Count")
plt.xlabel("Record Count")
plt.ylabel("Latency (s)")
plt.grid(True)
plt.savefig("./screenshots/query_latency.png")

plt.figure(figsize=(8, 5))
plt.plot(df["Record Count"], df["Update Time (s)"], marker='o')
plt.title("Update Time vs Record Count")
plt.xlabel("Record Count")
plt.ylabel("Update Time (s)")
plt.grid(True)
plt.savefig("./screenshots/update_time.png")

plt.figure(figsize=(8, 5))
plt.plot(df["Record Count"], df["Delete Time (s)"], marker='o')
plt.title("Delete Time vs Record Count")
plt.xlabel("Record Count")
plt.ylabel("Delete Time (s)")
plt.grid(True)
plt.savefig("./screenshots/delete_time.png")

# Individual throughput plots
plt.figure(figsize=(8, 5))
plt.plot(df["Record Count"], df["Query Throughput (q/s)"], marker='o')
plt.title("Query Throughput vs Record Count")
plt.xlabel("Record Count")
plt.ylabel("Query Throughput (queries/s)")
plt.grid(True)
plt.savefig("./screenshots/query_throughput.png")

plt.figure(figsize=(8, 5))
plt.plot(df["Record Count"], df["Update Throughput"], marker='o')
plt.title("Update Throughput vs Record Count")
plt.xlabel("Record Count")
plt.ylabel("Update Throughput")
plt.grid(True)
plt.savefig("./screenshots/update_throughput.png")

plt.figure(figsize=(8, 5))
plt.plot(df["Record Count"], df["Delete Throughput"], marker='o')
plt.title("Delete Throughput vs Record Count")
plt.xlabel("Record Count")
plt.ylabel("Delete Throughput")
plt.grid(True)
plt.savefig("./screenshots/delete_throughput.png")
