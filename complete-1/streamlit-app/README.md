# Weaviate Benchmarking Streamlit App

This Streamlit app provides a user-friendly interface for benchmarking, querying, and visualizing Weaviate vector database performance. It wraps all the original CLI features and adds interactive controls for dataset selection, embedding model choice, ingestion, querying, updating, deleting, benchmarking, and graphing.

## Features
- **Dataset Selection:** Choose from available CSV datasets for ingestion.
- **Embedding Model Selection:** Select a SentenceTransformer model for vectorization.
- **Ingestion:** Ingest data into Weaviate with resource monitoring.
- **Semantic Query:** Run semantic queries and view results.
- **Update/Delete:** Update or delete records by protocol.
- **Benchmarking:** Run and visualize query benchmarks.
- **Resource Plots:** View CPU and memory usage plots.
- **Metric Graphs:** Visualize benchmark metrics and throughput.

## Setup
1. Ensure Weaviate is running locally (see `../weaviate-benchmarking/setup.txt`).
2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Running the App
From the `streamlit-app` directory, run:
```bash
streamlit run app.py
```

## Notes
- The app dynamically loads and reuses logic from the original CLI scripts in `../weaviate-benchmarking`.
- All plots and metrics are displayed inline in the UI.
- You can add more CSV datasets to the `weaviate-benchmarking` folder for ingestion. 