import json
import random
import uuid
from faker import Faker

fake = Faker()

def generate_fake_value(type_def):
    if type_def == "string":
        return fake.text(max_nb_chars=20)
    elif type_def == "integer":
        return fake.random_int(min=1, max=10000)
    elif type_def == "number":
        return round(fake.random.uniform(0.01, 9999.99), 2)
    elif type_def == "boolean":
        return fake.boolean()
    else:
        return f"unknown_type_{type_def}"

def format_array_value(value, value_type):
    if value_type == "string":
        escaped_value = str(value).replace("'", "\\'")
        return f"'{escaped_value}'"
    elif value_type == "boolean":
        return str(value).lower()
    else:
        return str(value)

def generate_array_keys(path, items_schema, min_items=0, delimiter=":", is_first_instance=False, id_counters=None, generate_all_props=False):
    keys = []
    items_type = items_schema.get("type")
    
    if generate_all_props:
        num_items = 1
    elif items_type != "object":
        num_items = max(1, min_items)
    else:
        if is_first_instance:
            num_items = 1 if min_items > 0 else 0
        else:
            num_items = random.randint(min_items, 3)
    
    if items_type == "object":
        for i in range(num_items):
            object_keys = generate_object_keys(f"{path}[{i}]", items_schema, delimiter=delimiter, is_first_instance=is_first_instance, id_counters=id_counters, generate_all_props=generate_all_props)
            keys.extend(object_keys)
    else:
        if num_items > 0:
            array_values = []
            for _ in range(num_items):
                value = generate_fake_value(items_type)
                formatted_value = format_array_value(value, items_type)
                array_values.append(formatted_value)
            keys.append((path, f"[{', '.join(array_values)}]"))
        else:
            keys.append((path, "[]"))
    
    return keys

def generate_id_value(id_type, base_path, id_counters):
    if id_type == "integer":
        id_key = base_path if base_path else "root"
        if id_counters is None:
            id_counters = {}
        if id_key not in id_counters:
            id_counters[id_key] = 1
        else:
            id_counters[id_key] += 1
        return str(id_counters[id_key])
    elif id_type == "string":
        return str(uuid.uuid4())
    else:
        id_key = base_path if base_path else "root"
        if id_counters is None:
            id_counters = {}
        if id_key not in id_counters:
            id_counters[id_key] = 0
        else:
            id_counters[id_key] += 1
        return str(id_counters[id_key])

def generate_object_keys(base_path, schema, instance_id=None, delimiter=":", include_id_in_path=True, is_first_instance=False, id_counters=None, generate_all_props=False):
    keys = []
    properties = schema.get("properties", {})
    required_fields = schema.get("required", [])
    
    id_value = None
    use_id_in_path = "id" in properties and include_id_in_path
    
    if use_id_in_path:
        id_schema = properties.get("id", {})
        id_type = id_schema.get("type", "integer")
        
        if instance_id is not None:
            id_value = str(instance_id)
        else:
            id_value = generate_id_value(id_type, base_path, id_counters)
        
        if base_path:
            base_path = f"{base_path}{delimiter}{id_value}"
        else:
            base_path = id_value
    
    for prop_name, prop_schema in properties.items():
        if prop_name == "id" and use_id_in_path:
            continue
        
        is_required = prop_name in required_fields
        
        if generate_all_props:
            should_include = True
        elif is_first_instance:
            should_include = is_required  ## first instance: ONLY required fields
        else:
            should_include = is_required or (random.random() < 0.7)  ## other instances: required + random optional
        
        if not should_include:
            continue
        
        prop_type = prop_schema.get("type")
        
        if base_path:
            prop_path = f"{base_path}{delimiter}{prop_name}"
        else:
            prop_path = prop_name
        
        if prop_type == "object":
            nested_keys = generate_object_keys(prop_path, prop_schema, delimiter=delimiter, include_id_in_path=include_id_in_path, is_first_instance=is_first_instance, id_counters=id_counters, generate_all_props=generate_all_props)
            keys.extend(nested_keys)
        elif prop_type == "array":
            min_items = 1 if is_required else 0
            array_keys = generate_array_keys(prop_path, prop_schema["items"], min_items=min_items, delimiter=delimiter, is_first_instance=is_first_instance, id_counters=id_counters, generate_all_props=generate_all_props)
            keys.extend(array_keys)
        else:
            value = generate_fake_value(prop_type)
            keys.append((prop_path, str(value)))
    
    return keys

def generate_keys_from_schema(schema, num_instances=3, delimiter=":", include_id_in_path=True, generate_all_props=False):
    redis_commands = []
    
    if schema.get("type") == "object" and "properties" in schema:
        root_properties = schema["properties"]
        
        for entity_name, entity_schema in root_properties.items():
            if entity_schema.get("type") == "object":
                entity_id_counters = {}
                
                for i in range(num_instances):
                    is_first_instance = (i == 0) and not generate_all_props
                    
                    entity_properties = entity_schema.get("properties", {})
                    id_schema = entity_properties.get("id", {})
                    id_type = id_schema.get("type", "integer")
                    
                    if include_id_in_path and "id" in entity_properties:
                        if id_type == "integer":
                            instance_id = i + 1
                        else:
                            instance_id = str(uuid.uuid4())
                    else:
                        instance_id = None
                    
                    entity_keys = generate_object_keys(
                        entity_name, 
                        entity_schema, 
                        instance_id=instance_id,
                        delimiter=delimiter, 
                        include_id_in_path=include_id_in_path, 
                        is_first_instance=is_first_instance,
                        id_counters=entity_id_counters,
                        generate_all_props=generate_all_props
                    )
                    
                    for key, value in entity_keys:
                        escaped_value = str(value).replace('"', '\\"')
                        redis_commands.append(f'SET "{key}" "{escaped_value}"')
    
    return redis_commands

def load_schema_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Schema file '{file_path}' not found.")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in schema file: {e}")
        return {}

def main():
    ## Input schema file
    schema_file = input("Schema file (default: input_schema.json): ").strip() or "input_schema.json"
    
    schema = load_schema_from_file(schema_file)
    if not schema:
        return
    
    ## Delimiter definition
    print("\nDelimiter options: 1=: 2=/ 3=.")
    delimiter_choice = input("Choose delimiter (1/2/3, default: 1): ").strip() or "1"
    delimiters = {"1": ":", "2": "/", "3": "."}
    delimiter = delimiters.get(delimiter_choice, ":")
    
    ## Include ID as path segment
    include_id = input("Include ID in path? (y/n, default: y): ").strip().lower()
    include_id_in_path = include_id != "n"
    
    ## Generation modes
    print("\nGeneration modes:")
    print("1. Multiple instances (1st=required only, rest=required+optional)")
    print("2. One instance with required and optional properties")
    mode = input("Choose mode (1/2, default: 1): ").strip() or "1"
    
    if mode == "2":
        num_instances = 1
        generate_all_props = True
    else:
        try:
            num_instances = int(input("Number of instances per entity (default: 100): ").strip() or "100")
        except ValueError:
            num_instances = 100
        generate_all_props = False
    
    commands = generate_keys_from_schema(schema, num_instances, delimiter, include_id_in_path, generate_all_props)
    
    if not commands:
        print("No commands generated. Check schema structure.")
        return
    
    print(f"\nGenerated {len(commands)} Redis SET commands")
    
    output_file = input("Output file (default: redis_commands.txt): ").strip()
    
    if output_file == "":
        output_file = "redis_commands.txt"
    
    try:
        with open(output_file, 'w') as f:
            for cmd in commands:
                f.write(cmd + '\n')
        print(f"Saved {len(commands)} commands to {output_file}")
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    main()