from tqdm import tqdm
from config import get_redis_connection, get_extractor_config
from redis_extractor import extract_database
from key_parser import group_keys, build_nested_structure
from schema_inference import extract_schema
from schema_processor import group_schema_variations, combine_schema_variations
from utils import write_json_file

def main():
    config = get_extractor_config()
    conn = get_redis_connection()

    try:
        ## Extract data from Redis database
        kv_data = extract_database(conn, config['database'], config['batch_size'])
        conn.close()

        ## Group keys by entity instance
        print("\nGrouping keys...")
        grouped_keys = group_keys(kv_data)
        print(f"Created {len(grouped_keys)} groups")

        ## Build object structures
        print("\nBuilding object structures...")
        object_instances = [
            build_nested_structure(group_id, pairs) 
            for group_id, pairs in tqdm(grouped_keys.items())
        ]

        ## Extract schemas
        print("\nExtracting schemas...")
        schema_variations = [extract_schema(obj) for obj in tqdm(object_instances)]

        ## Group schemas by entity
        print("\nGrouping schema variations...")
        entity_variations = group_schema_variations(schema_variations)

        ## Combine schemas
        print("\nCombining schemas...")
        combined_schemas = {}
        
        for entity, variations in entity_variations.items():
            print(f"Entity '{entity}': {len(variations)} variations")

            combined = combine_schema_variations(variations)
            combined_schemas[entity] = combined
        
        ## Export results
        if config['export_variations']:
            write_json_file('output_schema_variations.json', entity_variations)
            print("\nSchema variations written to 'output_schema_variations.json'")
        
        final_schema = {"type": "object", "properties": combined_schemas}
        write_json_file('output_schema.json', final_schema)
        print("\nCombined schema written to 'output_schema.json'")
        
    except Exception as e:
        print(f"Error during extraction: {e}")
        conn.close()
    
if __name__ == "__main__":
    main()