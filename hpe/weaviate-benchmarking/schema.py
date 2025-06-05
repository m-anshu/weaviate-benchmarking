import weaviate

client = weaviate.Client("http://localhost:8080")

print("Schema recreation logic triggered...")

ip_flow_schema = {
    "class": "IPFlow",
    # "vectorIndexType": "flat",
    "vectorizer": "none",
    # "vectorIndexConfig": {
    # "filterable": True,
    # "filterStrategy": "acorn"
    # },
    "properties": [
        {"name": "frame_number", "dataType": ["int"]},
        {"name": "frame_time", "dataType": ["string"]},
        {"name": "source_ip", "dataType": ["string"]},
        {"name": "destination_ip", "dataType": ["string"]},
        {"name": "source_port", "dataType": ["int"]},
        {"name": "destination_port", "dataType": ["int"]},
        {"name": "protocol", "dataType": ["string"]},
        {"name": "frame_length", "dataType": ["int"]},
    ]
}

existing_schema = client.schema.get()
if any(cls["class"] == "IPFlow" for cls in existing_schema.get("classes", [])):
    print("The 'IPFlow' class already exists.")
else:
    client.schema.create_class(ip_flow_schema)
    print("The 'IPFlow' class has been created successfully!")