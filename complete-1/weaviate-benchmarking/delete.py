import weaviate


client = weaviate.Client("http://localhost:8080")

def delete_all_schema(client):
    schema = client.schema.get()
    classes = schema.get("classes", [])
    
    if not classes:
        print("No classes found in the schema. Nothing to delete.")
        return

    for cls in classes:
        class_name = cls.get("class")
        print(f"Deleting class: {class_name}")
        client.schema.delete_class(class_name)
    print("All classes and associated objects have been deleted. Schema reset complete.")

if __name__ == "__main__":
    confirm = input("WARNING: This will delete all schema classes and associated objects from Weaviate. Continue? (y/n): ")
    if confirm.lower() == 'y':
        delete_all_schema(client)
    else:
        print("Operation cancelled.")

del client