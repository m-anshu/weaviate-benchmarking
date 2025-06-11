from sentence_transformers import SentenceTransformer
import weaviate
import time


client = weaviate.Client("http://localhost:8080")
embed_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

def semantic_query_ip_flow(query_text, limit=5):
    query_vector = embed_model.encode(query_text).tolist()
    result = (
        client.query
        .get("IPFlow", ["frame_number", "frame_time", "source_ip", "destination_ip",
                        "source_port", "destination_port", "protocol", "frame_length"])
        .with_near_vector({"vector": query_vector})
        .with_additional(["distance"])
        .with_limit(limit)
        .do()
    )
    return result


def update_ip_flow(protocol, new_frame_length, batch_size=100):
    normalized_protocol = protocol.strip().upper()
    offset = 0
    update_count = 0
    start_time = time.time()
    while True:
        try:
            response = client.query.get(
                "IPFlow",
                ["protocol", "frame_length", "_additional { id }"]
            ).with_where({
                "path": ["protocol"],
                "operator": "Equal",
                "valueString": normalized_protocol
            }).with_limit(batch_size).with_offset(offset).do()

            matching_records = response.get("data", {}).get("Get", {}).get("IPFlow", [])
            if not matching_records:
                break  # No more records

            print(f"Batch at offset {offset}: Found {len(matching_records)} records")

            for record in matching_records:
                record_id = record["_additional"]["id"]
                current_length = record.get("frame_length")

                if current_length != new_frame_length:
                    try:
                        client.data_object.update(
                            {"frame_length": new_frame_length},
                            class_name="IPFlow",
                            uuid=record_id
                        )
                        update_count += 1
                        print(f"Updated record {record_id} from {current_length} to {new_frame_length}")
                    except Exception as e:
                        print(f"Error updating record {record_id}: {e}")

            offset += batch_size

        except Exception as e:
            print(f"Error during batch update at offset {offset}: {e}")
            break

    print(f"Total records updated: {update_count}")
    duration = time.time() - start_time
    throughput = update_count / duration if duration > 0 else 0
    return {
        "duration": duration,
        "records_affected": update_count,
        "throughput": throughput
    }


def delete_ip_flow(protocol_name, batch_size=100):
    normalized_protocol = protocol_name.strip().upper()
    offset = 0
    delete_count = 0
    start_time = time.time()
    while True:
        try:
            response = client.query.get(
                "IPFlow",
                ["protocol", "_additional { id }"]
            ).with_where({
                "path": ["protocol"],
                "operator": "Equal",
                "valueString": normalized_protocol
            }).with_limit(batch_size).with_offset(offset).do()

            matching_records = response.get("data", {}).get("Get", {}).get("IPFlow", [])
            if not matching_records:
                break 

            print(f" Found {len(matching_records)} records")

            for record in matching_records:
                record_id = record["_additional"]["id"]
                try:
                    client.data_object.delete(
                        record_id,
                        class_name="IPFlow"
                    )
                    delete_count += 1
                    print(f"Deleted record {record_id}")
                except Exception as e:
                    print(f"Error deleting record {record_id}: {e}")

        except Exception as e:
            print(f"Error during batch delete at offset {offset}: {e}")
            break

    print(f"Total records deleted: {delete_count}")
    duration = time.time() - start_time
    throughput = delete_count / duration if duration > 0 else 0
    return {
        "duration": duration,
        "records_affected": delete_count,
        "throughput": throughput
    }
