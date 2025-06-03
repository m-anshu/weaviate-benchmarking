import streamlit as st
import os
import glob
from sentence_transformers import SentenceTransformer
import weaviate
import pandas as pd
import matplotlib.pyplot as plt
import importlib.util

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
tabs = st.tabs(["Ingest Data", "Query", "Update", "Delete", "Benchmark", "Plots", "Metrics"])

# --- Ingest Data Tab ---
with tabs[0]:
    st.header("Ingest Data into Weaviate")
    st.write(f"Selected dataset: `{dataset}`")
    st.write(f"Selected embedding model: `{selected_model}`")
    if st.button("Ingest Data"):
        with st.spinner("Ingesting data and monitoring resources..."):
            # Patch ingest.py to use selected model
            ingest.embed_model = embed_model
            csv_path = os.path.join(BASE_DIR, dataset)
            ingest.insert_ip_flows(csv_path)
        st.success("Ingestion complete!")

# --- Query Tab ---
with tabs[1]:
    st.header("Semantic Query")
    query_text = st.text_input("Enter your query text:")
    limit = st.number_input("Number of results", min_value=1, max_value=20, value=5)
    if st.button("Run Query"):
        with st.spinner("Querying Weaviate..."):
            query_mod.embed_model = embed_model
            result = query_mod.semantic_query_ip_flow(query_text, limit=limit)
            st.write(result)

# --- Update Tab ---
with tabs[2]:
    st.header("Update IP Flow Records")
    protocol = st.text_input("Protocol to update (e.g., TCP)")
    new_size = st.number_input("New frame length", min_value=1, value=1500)
    batch_size = st.number_input("Batch size", min_value=1, value=100)
    if st.button("Update Records"):
        with st.spinner("Updating records..."):
            query_mod.update_ip_flow(protocol, new_size, batch_size=batch_size)
        st.success("Update complete!")

# --- Delete Tab ---
with tabs[3]:
    st.header("Delete IP Flow Records by Protocol")
    protocol = st.text_input("Protocol to delete (e.g., TCP)", key="delete_protocol")
    batch_size = st.number_input("Batch size", min_value=1, value=100, key="delete_batch")
    if st.button("Delete Records"):
        with st.spinner("Deleting records..."):
            query_mod.delete_ip_flow(protocol, batch_size=batch_size)
        st.success("Delete complete!")
    st.divider()
    st.header("Delete All Schema (Danger Zone)")
    if st.button("Delete All Schema"):
        with st.spinner("Deleting all schema and objects..."):
            delete_mod.delete_all_schema()
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
                for q in query_list:
                    benchmark_result = benchmark.benchmark_query(query_mod.semantic_query_ip_flow, q)
                    results.append(benchmark_result)
                st.write(results)
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
            st.image(f"{log_file.replace('.txt', '.png')}")
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