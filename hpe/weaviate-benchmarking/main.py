# Updated main.py with Weaviate-focused reporting
import argparse
from ingest import insert_ip_flows
from query import semantic_query_ip_flow, update_ip_flow, delete_ip_flow
from benchmark import benchmark_query, benchmark_crud_operation
import warnings

def main():
    warnings.simplefilter("ignore", ResourceWarning)
    
    parser = argparse.ArgumentParser(description="IP Flow Analysis CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subparser for ingesting IP flows
    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("csv_file")

    # Subparser for semantic querying
    query_parser = subparsers.add_parser("query")
    query_parser.add_argument("query_text")

    # Subparser for benchmarking
    bench_parser = subparsers.add_parser("benchmark")
    bench_parser.add_argument("queries", nargs="+")
    
    update_parser = subparsers.add_parser("update")
    update_parser.add_argument("protocol", help="Protocol for which to be updated")
    update_parser.add_argument("new_packet_size", type=int, help="New packet size")
    
    delete_parser = subparsers.add_parser("delete")
    delete_parser.add_argument("protocol_number", help="Delete flows with protocol number")
    
    args = parser.parse_args()

    if args.command == "ingest":
        insert_ip_flows(args.csv_file)
        print("IP Flow ingestion complete!")
        
    elif args.command == "query":
        result = semantic_query_ip_flow(args.query_text)
        print("Semantic Query Results:")
        for obj in result.get("data", {}).get("Get", {}).get("IPFlow", []):
            print(obj)
            
    elif args.command == "benchmark":
        query_results = []
        total_time = 0
        
        # Weaviate memory stats (PRIMARY)
        weaviate_memory_totals = {
            'delta_mb': 0,
            'delta_percent': 0,
            'peak_mb': 0,
            'peak_percent': 0,
            'average_mb': 0,
            'average_percent': 0
        }
        
        # Python memory stats (SECONDARY)
        python_memory_totals = {
            'delta_mb': 0,
            'delta_percent': 0,
            'peak_mb': 0,
            'peak_percent': 0
        }
        
        # CPU stats
        cpu_totals = {
            'average_percent': 0,
            'peak_percent': 0
        }
        
        valid_weaviate_results = 0
        valid_python_results = 0
        
        for query_text in args.queries:
            print(f"\nBenchmarking query: '{query_text}'")
            benchmark = benchmark_query(semantic_query_ip_flow, query_text)
            query_results.append(benchmark["result"])
            
            # Accumulate metrics
            total_time += benchmark["duration"]
            
            # Weaviate metrics (PRIMARY)
            if benchmark["weaviate_memory_stats"]:
                weaviate_stats = benchmark["weaviate_memory_stats"]
                weaviate_memory_totals['delta_mb'] += weaviate_stats["delta_mb"]
                weaviate_memory_totals['delta_percent'] += weaviate_stats["delta_percent"]
                weaviate_memory_totals['peak_mb'] = max(weaviate_memory_totals['peak_mb'], weaviate_stats["peak_mb"])
                weaviate_memory_totals['peak_percent'] = max(weaviate_memory_totals['peak_percent'], weaviate_stats["peak_percent"])
                weaviate_memory_totals['average_mb'] += weaviate_stats["average_mb"]
                weaviate_memory_totals['average_percent'] += weaviate_stats["average_percent"]
                valid_weaviate_results += 1
            
            # Python metrics (SECONDARY)
            if benchmark["python_memory_stats"]:
                python_stats = benchmark["python_memory_stats"]
                python_memory_totals['delta_mb'] += python_stats["delta_mb"]
                python_memory_totals['delta_percent'] += python_stats["delta_percent"]
                python_memory_totals['peak_mb'] = max(python_memory_totals['peak_mb'], python_stats["peak_mb"])
                python_memory_totals['peak_percent'] = max(python_memory_totals['peak_percent'], python_stats["peak_percent"])
                valid_python_results += 1
            
            # CPU metrics
            cpu_totals['average_percent'] += benchmark["cpu_stats"]["average_percent"]
            cpu_totals['peak_percent'] = max(cpu_totals['peak_percent'], benchmark["cpu_stats"]["peak_percent"])
            
            # Print individual query results
            print(f"  Duration: {benchmark['duration']:.4f}s")
            print(f"  CPU Avg: {benchmark['cpu_stats']['average_percent']:.2f}%")
            
            # Print Weaviate stats (PRIMARY)
            if benchmark["weaviate_memory_stats"]:
                w_stats = benchmark["weaviate_memory_stats"]
                print(f"   Weaviate Memory Delta: {w_stats['delta_mb']:.2f} MB ({w_stats['delta_percent']:.2f}%)")
                print(f"   Weaviate Peak: {w_stats['peak_mb']:.2f} MB ({w_stats['peak_percent']:.2f}%)")
            else:
                print(f"    Weaviate process not monitored")
            
            # Print Python stats (SECONDARY)
            if benchmark["python_memory_stats"]:
                p_stats = benchmark["python_memory_stats"]
                print(f"   Python Memory Delta: {p_stats['delta_mb']:.2f} MB ({p_stats['delta_percent']:.2f}%)")
                print(f"   Python Peak: {p_stats['peak_mb']:.2f} MB ({p_stats['peak_percent']:.2f}%)")
        
        # Calculate averages
        num_queries = len(args.queries)
        avg_time = total_time / num_queries
        avg_throughput = 1 / avg_time
        avg_cpu = cpu_totals['average_percent'] / num_queries
        
        print("\n" + "="*80)
        print("BENCHMARK RESULTS SUMMARY")
        print("="*80)
        print(f"Queries Executed: {num_queries}")
        print(f"Average Duration: {avg_time:.4f} seconds")
        print(f"Throughput: {avg_throughput:.4f} queries/second")
        print(f"")
        
        # Weaviate results (PRIMARY FOCUS)
        if valid_weaviate_results > 0:
            print(f" WEAVIATE MEMORY METRICS (PRIMARY):")
            print(f"   Average Memory Delta: {weaviate_memory_totals['delta_mb']/valid_weaviate_results:.2f} MB ({weaviate_memory_totals['delta_percent']/valid_weaviate_results:.2f}%)")
            print(f"   Overall Peak Memory: {weaviate_memory_totals['peak_mb']:.2f} MB ({weaviate_memory_totals['peak_percent']:.2f}%)")
            print(f"   Average Memory Usage: {weaviate_memory_totals['average_mb']/valid_weaviate_results:.2f} MB ({weaviate_memory_totals['average_percent']/valid_weaviate_results:.2f}%)")
        else:
            print(f" WEAVIATE MEMORY METRICS: Not available (process not found)")
        
        print(f"")
        
        # Python results (SECONDARY)
        if valid_python_results > 0:
            print(f" PYTHON CLIENT METRICS (SECONDARY):")
            print(f"   Average Memory Delta: {python_memory_totals['delta_mb']/valid_python_results:.2f} MB ({python_memory_totals['delta_percent']/valid_python_results:.2f}%)")
            print(f"   Overall Peak Memory: {python_memory_totals['peak_mb']:.2f} MB ({python_memory_totals['peak_percent']:.2f}%)")
        
        print(f"")
        print(f" CPU METRICS:")
        print(f"   Average CPU Usage: {avg_cpu:.2f}%")
        print(f"   Peak CPU Usage: {cpu_totals['peak_percent']:.2f}%")
        
        print(f"\n" + "="*80)
        print("QUERY RESULTS")
        print("="*80)
        for i, result in enumerate(query_results):
            print(f"\nQuery {i+1} Results:")
            for obj in result.get("data", {}).get("Get", {}).get("IPFlow", []):
                distance = obj.get("_additional", {}).get("distance", 0)
                similarity = 1 - distance
                print(f"  {obj}")
                print(f"  Similarity Score: {similarity:.4f}")
                
    elif args.command == "update":
        print("Starting CRUD operation benchmark (UPDATE)...")
        benchmark_crud_operation(update_ip_flow, args.protocol, args.new_packet_size)
        
    elif args.command == "delete":
        print("Starting CRUD operation benchmark (DELETE)...")
        benchmark_crud_operation(delete_ip_flow, args.protocol_number)

if __name__ == "__main__":
    main()