from collections import defaultdict
from tqdm import tqdm
from utils import get_schema_hash

def group_schema_variations(schemas): ## more efficient
    grouped = defaultdict(dict) # entity -> {hash: (schema, count)}
    
    for schema_obj in tqdm(schemas):
        entity = next(iter(schema_obj.keys()))
        schema = schema_obj[entity]
        
        # Create hash of the normalized JSON string
        schema_hash = get_schema_hash(schema)
        
        if schema_hash in grouped[entity]:
            # Increment count for existing schema
            existing_schema, count = grouped[entity][schema_hash]
            grouped[entity][schema_hash] = (existing_schema, count + 1)
        else:
            # Add new schema variation
            grouped[entity][schema_hash] = (schema, 1)
    
    # Convert back to the original format
    result = {}
    for entity, hash_dict in grouped.items():
        result[entity] = list(hash_dict.values())
    
    return result

def combine_schema_variations(schemas):
    if not schemas:
        return {"type": "string"}
    
    variation_types = {schema.get("type") for schema, _ in schemas if "type" in schema}
    
    if len(variation_types) == 1:
        schema_type = next(iter(variation_types))
        
        if schema_type == "object":
            return _merge_object_schemas(schemas)
        elif schema_type == "array":
            return _merge_array_schemas(schemas)
        else:
            return {"type": schema_type}
    
    return {"type": _find_dominant_type(schemas)}

def _find_dominant_type(schemas):
    type_counts = defaultdict(int)
    
    for schema, count in schemas:
        schema_type = schema.get("type")
        if schema_type:
            type_counts[schema_type] += count
    
    return max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else "string"

def _merge_object_schemas(schemas):
    total_instances = sum(count for _, count in schemas)
    property_stats = defaultdict(lambda: {"count": 0, "variations": []})
    
    for schema, count in schemas:
        properties = schema.get("properties", {})
        for prop_name, prop_schema in properties.items():
            stats = property_stats[prop_name]
            stats["count"] += count
            stats["variations"].append((prop_schema, count))
    
    merged_properties = {}
    required_properties = []
    
    for prop_name, stats in property_stats.items():
        merged_properties[prop_name] = combine_schema_variations(stats["variations"])
        
        if stats["count"] == total_instances:
            required_properties.append(prop_name)
    
    result = {"type": "object", "properties": merged_properties}
    if required_properties:
        result["required"] = sorted(required_properties)
    
    return result

def _merge_array_schemas(schemas):
    array_items = []
    for schema, count in schemas:
        if "items" in schema:
            array_items.append((schema["items"], count))
    
    return {"type": "array", "items": _simplify_array_items(array_items)}

def _simplify_array_items(item_variations):
    if not item_variations:
        return {"type": "string"}
    
    flattened_items = []
    for item, count in item_variations:
        if "oneOf" in item:
            for sub_item in item["oneOf"]:
                flattened_items.append((sub_item, count))
        else:
            flattened_items.append((item, count))
    
    object_items = [(v, c) for v, c in flattened_items if v.get("type") == "object"]
    simple_type_counts = defaultdict(int)
    
    for variation, count in flattened_items:
        if variation.get("type") != "object" and "type" in variation:
            simple_type_counts[variation["type"]] += count
    
    if object_items:
        combined = combine_schema_variations(object_items)
        if combined.get("properties"):
            return combined
    
    if simple_type_counts:
        dominant_type = max(simple_type_counts.items(), key=lambda x: x[1])[0]
        return {"type": dominant_type}
    
    return {"type": "string"}