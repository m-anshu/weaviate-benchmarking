import argparse
from ingest import insert_ip_flows
from query import semantic_query_ip_flow, update_ip_flow, delete_ip_flow
from benchmark import benchmark_query, benchmark_crud_operation
import warnings


def main():
    warnings.simplefilter("ignore", ResourceWarning)
    cpu_usage_log = []

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
        total_time, total_cpu, total_memory = 0, 0, 0
        total_memory_delta_MB, total_memory_percent_change = 0,0
        for query_text in args.queries:
            benchmark = benchmark_query(semantic_query_ip_flow, query_text)
            query_results.append(benchmark["result"])
            total_time += benchmark["duration"]
            total_cpu += benchmark["cpu_usage"]
            total_memory_delta_MB += benchmark["memory_delta_MB"]
            total_memory_percent_change += benchmark["memory_percent_change"]
            

        avg_time = total_time / len(args.queries)
        avg_cpu = total_cpu / len(args.queries)
        avg_memory_delta = total_memory_delta_MB / len(args.queries)
        avg_memory_percent_change = len(args.queries)
        avg_throughput=1/avg_time

        print("\nBenchmark Results (Averages):")
        print(f"Query Duration: {avg_time:.4f} seconds")
        print(f"Throughput : {avg_throughput:.4f} queries/second")
        print(f"CPU Usage: {avg_cpu:.2f}%")
        print(f"Memory Usage (Delta): {avg_memory_delta:.2f} MB")
        print(f"Memory Percent Change : {avg_memory_percent_change:.2f}%")

        print("\nQuery Results:")
        for result in query_results:
            for obj in result.get("data", {}).get("Get", {}).get("IPFlow", []):
                print(obj)
                distance=obj["_additional"]["distance"] 
                similarity=1-distance
                print(f"Similarity Score: {similarity:.4f}")
    elif args.command == "update":
        benchmark_crud_operation(update_ip_flow, args.protocol, args.new_packet_size)

        
    elif args.command == "delete":
        benchmark_crud_operation(delete_ip_flow, args.protocol_number)


if __name__ == "__main__":
    main()