import streamlit as st
import os
import glob
from sentence_transformers import SentenceTransformer
import weaviate
import pandas as pd
import matplotlib.pyplot as plt
import importlib.util
import json
from datetime import datetime
import matplotlib.dates as mdates

# --- Utility to dynamically import modules from the CLI app ---
def import_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# --- Paths ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../weaviate-benchmarking'))
DATASETS = glob.glob(os.path.join(BASE_DIR, '*.csv'))
PLOT_LOGS = [
    'cpu_usage_log.txt',
    'ingest_cpu_usage_log.txt',
    'weaviate_memory_log.txt',
    'ingest_weaviate_memory_log.txt',
    'python_memory_log.txt',
    'ingest_python_memory_log.txt',
]

METRICS_FILE = os.path.join(os.path.dirname(__file__), 'operation_metrics.json')

def append_operation_metrics(op_type, metrics):
    # Load existing
    if os.path.exists(METRICS_FILE):
        with open(METRICS_FILE, 'r') as f:
            data = json.load(f)
    else:
        data = []
    # Add timestamp and op_type
    metrics_entry = {"timestamp": datetime.now().isoformat(), "operation": op_type}
    metrics_entry.update(metrics)
    data.append(metrics_entry)
    with open(METRICS_FILE, 'w') as f:
        json.dump(data, f)

# --- Import CLI modules ---
ingest = import_module_from_path('ingest', os.path.join(BASE_DIR, 'ingest.py'))
query_mod = import_module_from_path('query', os.path.join(BASE_DIR, 'query.py'))
benchmark = import_module_from_path('benchmark', os.path.join(BASE_DIR, 'benchmark.py'))
delete_mod = import_module_from_path('delete', os.path.join(BASE_DIR, 'delete.py'))
schema_mod = import_module_from_path('schema', os.path.join(BASE_DIR, 'schema.py'))
plot_mod = import_module_from_path('plot', os.path.join(BASE_DIR, 'plot.py'))

# --- Streamlit UI ---
st.set_page_config(page_title="Weaviate Benchmarking Dashboard", layout="wide")
st.title("Weaviate Benchmarking & Analytics Dashboard")

# Sidebar: Dataset and Model Selection
st.sidebar.header("Configuration")
dataset = st.sidebar.selectbox("Select dataset to ingest", [os.path.basename(f) for f in DATASETS])

# List available sentence-transformers models (basic list, can be expanded)
EMBED_MODELS = [
    "sentence-transformers/all-MiniLM-L6-v2",
    "sentence-transformers/all-mpnet-base-v2",
    "sentence-transformers/paraphrase-MiniLM-L6-v2",
    "sentence-transformers/distiluse-base-multilingual-cased-v2"
]
selected_model = st.sidebar.selectbox("Select embedding model", EMBED_MODELS)

# Connect to Weaviate
client = weaviate.Client("http://localhost:8080")
embed_model = SentenceTransformer(selected_model)

# --- Tabs for features ---
tabs = st.tabs(["Ingest Data", "Query", "Update", "Delete", "Benchmark", "Plots", "Metrics", "Analysis"])

# --- Ingest Data Tab ---
with tabs[0]:
    st.header("Ingest Data into Weaviate")
    st.write(f"Selected dataset: `{dataset}`")
    st.write(f"Selected embedding model: `{selected_model}`")
    if st.button("Ingest Data"):
        with st.spinner("Ingesting data and monitoring resources..."):
            ingest.embed_model = embed_model
            csv_path = os.path.join(BASE_DIR, dataset)
            metrics = ingest.insert_ip_flows(csv_path)
            append_operation_metrics("ingest", metrics)
        st.success(f"Ingestion complete! Time: {metrics['duration']:.2f}s, Rows: {metrics['processed_rows']}, Throughput: {metrics['throughput']:.2f} rows/sec")
        with st.expander("Show Detailed Stats"):
            st.subheader("Weaviate Memory Stats")
            st.table({
                'Initial (MB)': metrics.get('weaviate_initial_mb'),
                'Final (MB)': metrics.get('weaviate_final_mb'),
                'Delta (MB)': metrics.get('weaviate_delta_mb'),
                'Peak (MB)': metrics.get('weaviate_peak_mb'),
                'Average (MB)': metrics.get('weaviate_avg_mb'),
                'Initial (%)': metrics.get('weaviate_initial_percent'),
                'Final (%)': metrics.get('weaviate_final_percent'),
                'Delta (%)': metrics.get('weaviate_delta_percent'),
                'Peak (%)': metrics.get('weaviate_peak_percent'),
                'Average (%)': metrics.get('weaviate_avg_percent'),
                'PID': metrics.get('weaviate_pid'),
                'Samples': metrics.get('weaviate_memory_samples'),
            })
            st.subheader("Python Memory Stats")
            st.table({
                'Initial (MB)': metrics.get('python_initial_mb'),
                'Final (MB)': metrics.get('python_final_mb'),
                'Delta (MB)': metrics.get('python_delta_mb'),
                'Peak (MB)': metrics.get('python_peak_mb'),
                'Average (MB)': metrics.get('python_avg_mb'),
                'Initial (%)': metrics.get('python_initial_percent'),
                'Final (%)': metrics.get('python_final_percent'),
                'Delta (%)': metrics.get('python_delta_percent'),
                'Peak (%)': metrics.get('python_peak_percent'),
                'Average (%)': metrics.get('python_avg_percent'),
                'Samples': metrics.get('python_memory_samples'),
            })
            st.subheader("CPU Stats")
            st.table({
                'Average (%)': metrics.get('cpu_avg'),
                'Peak (%)': metrics.get('cpu_peak'),
                'Min (%)': metrics.get('cpu_min'),
                'Samples': metrics.get('cpu_samples'),
            })

# --- Query Tab ---
with tabs[1]:
    st.header("Semantic Query")
    query_text = st.text_input("Enter your query text:")
    limit = st.number_input("Number of results", min_value=1, max_value=20, value=5)
    if st.button("Run Query"):
        with st.spinner("Querying Weaviate..."):
            query_mod.embed_model = embed_model
            result = query_mod.semantic_query_ip_flow(query_text, limit=limit)
            hits = result.get("data", {}).get("Get", {}).get("IPFlow", [])
            if hits:
                table = []
                for obj in hits:
                    distance = obj.get("_additional", {}).get("distance", 0)
                    similarity = 1 - distance if distance is not None else None
                    row = obj.copy()
                    row["similarity"] = similarity
                    table.append(row)
                st.dataframe(table)
            else:
                st.info("No results found.")

# --- Update Tab ---
with tabs[2]:
    st.header("Update IP Flow Records")
    protocol = st.text_input("Protocol to update (e.g., TCP)")
    new_size = st.number_input("New frame length", min_value=1, value=1500)
    batch_size = st.number_input("Batch size", min_value=1, value=100)
    if st.button("Update Records"):
        with st.spinner("Updating records with benchmarking..."):
            result = benchmark.benchmark_crud_operation(query_mod.update_ip_flow, protocol, new_size, batch_size)
            summary = f"Update complete! Time: {result['duration']:.2f}s"
            if 'records_affected' in result:
                summary += f", Records: {result['records_affected']}"
            if 'throughput' in result:
                summary += f", Throughput: {result['throughput']:.2f} recs/sec"
            op_metrics= {
                "duration": result.get("duration"),
                "records_affected": result.get("records_affected"),
                "throughput": result.get("throughput")
            }
            append_operation_metrics("update", op_metrics)
        st.success(summary)
        with st.expander("Show Detailed Stats"):
            st.subheader("Weaviate Memory Stats")
            st.json(result.get('weaviate_memory_stats', {}))
            st.subheader("Python Memory Stats")
            st.json(result.get('python_memory_stats', {}))
            st.subheader("CPU Stats")
            st.json(result.get('cpu_stats', {}))

# --- Delete Tab ---
with tabs[3]:
    st.header("Delete IP Flow Records by Protocol")
    protocol = st.text_input("Protocol to delete (e.g., TCP)", key="delete_protocol")
    batch_size = st.number_input("Batch size", min_value=1, value=100, key="delete_batch")
    if st.button("Delete Records"):
        with st.spinner("Deleting records with benchmarking..."):
            result = benchmark.benchmark_crud_operation(query_mod.delete_ip_flow, protocol, batch_size)
            summary = f"Delete complete! Time: {result['duration']:.2f}s"
            if 'records_affected' in result:
                summary += f", Records: {result['records_affected']}"
            if 'throughput' in result:
                summary += f", Throughput: {result['throughput']:.2f} recs/sec"
            op_metrics= {
                "duration": result.get("duration"),
                "records_affected": result.get("records_affected"),
                "throughput": result.get("throughput")
            }                
            append_operation_metrics("delete", op_metrics)
        st.success(summary)
        with st.expander("Show Detailed Stats"):
            st.subheader("Weaviate Memory Stats")
            st.json(result.get('weaviate_memory_stats', {}))
            st.subheader("Python Memory Stats")
            st.json(result.get('python_memory_stats', {}))
            st.subheader("CPU Stats")
            st.json(result.get('cpu_stats', {}))
    st.divider()
    st.header("Delete All Schema (Danger Zone)")
    if st.button("Delete All Schema"):
        with st.spinner("Deleting all schema and objects..."):
            delete_mod.delete_all_schema(client)
        st.success("All schema deleted!")

# --- Benchmark Tab ---
with tabs[4]:
    st.header("Benchmark Queries")
    queries = st.text_area("Enter queries (one per line)")
    if st.button("Run Benchmark"):
        query_list = [q.strip() for q in queries.splitlines() if q.strip()]
        if query_list:
            with st.spinner("Running benchmarks..."):
                results = []
                metrics_table = []
                detailed_stats = []
                for q in query_list:
                    benchmark_result = benchmark.benchmark_query(query_mod.semantic_query_ip_flow, q)
                    duration = benchmark_result["duration"]
                    hits = benchmark_result["result"].get("data", {}).get("Get", {}).get("IPFlow", [])
                    similarities = [1 - (obj.get("_additional", {}).get("distance", 0) or 0) for obj in hits]
                    avg_similarity = sum(similarities)/len(similarities) if similarities else None
                    metrics = {
                        "query": q,
                        "duration (s)": duration,
                        "num_results": len(hits),
                        "avg_similarity": avg_similarity,
                        "throughput (qps)": 1/duration if duration > 0 else None
                    }
                    metrics_table.append(metrics)
                    append_operation_metrics("benchmark", metrics)
                    results.append(benchmark_result)
                    detailed_stats.append({
                        'query': q,
                        'weaviate_memory_stats': benchmark_result.get('weaviate_memory_stats'),
                        'python_memory_stats': benchmark_result.get('python_memory_stats'),
                        'cpu_stats': benchmark_result.get('cpu_stats'),
                    })
                st.dataframe(metrics_table)
                for stat in detailed_stats:
                    with st.expander(f"Details for query: {stat['query']}"):
                        st.subheader("Weaviate Memory Stats")
                        st.json(stat['weaviate_memory_stats'])
                        st.subheader("Python Memory Stats")
                        st.json(stat['python_memory_stats'])
                        st.subheader("CPU Stats")
                        st.json(stat['cpu_stats'])
        else:
            st.warning("Please enter at least one query.")

# --- Plots Tab ---
with tabs[5]:
    st.header("Resource Usage Plots")
    for log_file in PLOT_LOGS:
        log_path = os.path.join(BASE_DIR, log_file)
        if os.path.exists(log_path):
            st.subheader(log_file)
            fig = plot_mod.plot_cpu_usage(log_path, log_file) if "cpu" in log_file else plot_mod.plot_memory_usage_mb(log_path, log_file)
            st.image(f"{log_path.replace('.txt', '.png')}")
        else:
            st.info(f"Log file {log_file} not found.")

# --- Metrics Tab ---
with tabs[6]:
    st.header("Benchmark Metrics & Graphs")
    # Re-run plotmetrics.py logic and show images inline
    screenshots_dir = os.path.join(BASE_DIR, "screenshots")
    if not os.path.exists(screenshots_dir):
        os.makedirs(screenshots_dir)
    # Dynamically import and run plotmetrics.py
    plotmetrics = import_module_from_path('plotmetrics', os.path.join(BASE_DIR, 'plotmetrics.py'))
    # List all PNGs in screenshots
    pngs = glob.glob(os.path.join(screenshots_dir, '*.png'))
    for img in pngs:
        st.image(img, caption=os.path.basename(img))
    if not pngs:
        st.info("No metric plots found. Run some benchmarks first.")

# --- Analysis Tab ---
with tabs[7]:
    st.header("Dynamic Operation Analysis")
    if os.path.exists(METRICS_FILE):
        with open(METRICS_FILE, 'r') as f:
            metrics_data = json.load(f)
        df = pd.DataFrame(metrics_data)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Ingest Throughput")
                ingest_df = df[df["operation"] == "ingest"]
                if not ingest_df.empty:
                    st.line_chart(ingest_df.set_index("timestamp")["throughput"])
                else:
                    st.info("No ingest operations yet.")
                st.subheader("Update Throughput")
                update_df = df[df["operation"] == "update"]
                if not update_df.empty:
                    st.line_chart(update_df.set_index("timestamp")["throughput"])
                else:
                    st.info("No update operations yet.")
                st.subheader("Delete Throughput")
                delete_df = df[df["operation"] == "delete"]
                if not delete_df.empty:
                    st.line_chart(delete_df.set_index("timestamp")["throughput"])
                else:
                    st.info("No delete operations yet.")
            with col2:
                st.subheader("Benchmark Query Latency")
                bench_df = df[df["operation"] == "benchmark"]
                if not bench_df.empty:
                    st.line_chart(bench_df.set_index("timestamp")["duration (s)"])
                else:
                    st.info("No benchmark operations yet.")
            # st.subheader("All Operation Metrics Table")
            # st.dataframe(df)
        else:
            st.info("No operation metrics recorded yet.")
    else:
        st.info("No operation metrics file found. Run some operations first.")