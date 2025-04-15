import re
import json
import yaml
from redis import Redis
from random import randint
from typing import Any
from thefuzz.fuzz import WRatio
from collections import defaultdict
from classes import Key_Value, Entity_Object
from connection import redis_connection
from schema import generate_relational_model, generate_sql_schema
############################################################################################
def get_keys(conn: Redis, db: int, count: int = 1000) -> list[Key_Value]:
    """Get all keys from the Redis database, non-blocking (cursor)"""
    conn.select(db)
    cursor = 0
    next_cursor = -1
    keys = []
    while next_cursor:
        next_cursor, items = conn.scan(cursor, count = count)
        cursor = next_cursor
        keys.extend(items)
    return keys
############################################################################################
def get_value(key: str, conn: Redis):
    """Get the value for a Redis key"""
    # BITMAP, BITSTREAM, HYPERLOGLOG and GEOSPATIAL make no sense without previous knowledge
    key_type = conn.type(key)
    match key_type:
        case "string": ## Only works for string
            try:
                key_value = conn.get(key)
                if not key_value.isprintable(): ## BITMAP bytes are returned on conn.get
                    key_value = None
            except: ## BITSTREAM and HYPERLOGLOG are string but "ungettable"
                key_value = None
        case "list":
            key_value = conn.lrange(key, 0 , -1)
        case "set":
            key_value = list(conn.smembers(key))
        case "hash":
            key_value = conn.hgetall(key)
        case "zset":
            key_value = conn.zrange(key, 0, -1, withscores=True)
            if key_value[0][1] > 10**13: ## GEO coords are float > 10^13
                key_value = None
            else:
                key_value = list(k for k,v in key_value)
        case "ReJSON-RL":
            key_value = conn.json().get(key, "$")[0] ## ReJSON-RE always returns a list with 1 item
        case _: ## STREAM, any other
            key_value = None

    return key_value
############################################################################################
def check_for_structured_string(string):
    """Parses a string value to check for strucuted value"""
    try:
        parsed = json.loads(string)
        if isinstance(parsed, (int, float, str, bool)) or parsed is None:
            return string
        else:
            return parsed
    except:
        return string
############################################################################################
def get_json_key_values(data, parent_key: str = '', result=None) -> list[tuple[str, Any]]:
    """Extract attributes and key path in nested JSON objects"""
    if result is None:
        result = []
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            get_json_key_values(value, new_key, result)
    elif isinstance(data, list):
        for index, item in enumerate(data):
            new_key = f"{parent_key}[{index}]"
            get_json_key_values(item, new_key, result)
    else:
        result.append((parent_key, data))
    return result
############################################################################################
def group_keys_into_object(keys: list[Key_Value]) -> defaultdict[list[Key_Value]]:
    """Group Key_Values by entity and ID"""
    grouped_keys = defaultdict(list[Key_Value])
    for key in keys:
        group_key = f'{key.components["entity"]}.{key.components["id"]}'
        grouped_keys[group_key].append(key)
    return grouped_keys
############################################################################################
def load_regex_patterns(file_path: str) -> list[tuple[str, str]]:
    """Load regex patterns from YAML file"""
    with open(file_path, "r") as file: ## Load patterns file
        data = yaml.safe_load(file)

    def replace_component(match): ## Replaces id, index (components that are INT)
        component = match.group(1)
        if component.lower() in ["id","index"]:
            return r'(?P<{}>\d+)'.format(component) ## d+ [0-9]
        else:
            return r'(?P<{}>\w+)'.format(component) ## w+ [A-Za-z0-9_]

    regex_patterns = []
    for pattern in data["patterns"]:
        regex_pattern = re.sub(r'\{(\w+)\}', replace_component, pattern["pattern"])
        regex_pattern = regex_pattern.replace(r'[', '\\[').replace(r']', '\\]').replace(r'.', '\\.')  ## Replaces escape chars
        regex_pattern = "^" + regex_pattern + "$"
        regex_patterns.append((regex_pattern, pattern["label"]))
    return regex_patterns
############################################################################################
def extract_components(string: str, patterns: list[tuple[str, str]]) -> tuple[dict,str]:
    """Extract key components using regex patterns"""
    for regex, pattern_type in patterns:
        match = re.match(regex, string)
        if match:
            return match.groupdict(), pattern_type
    return {"entity": string}, "Primitive"
############################################################################################
def parse_entity_object(group: list[Key_Value]) -> Entity_Object:
    """Parse Key_Values groups into an Entity_Object"""
    new_object = Entity_Object(group[0].components["entity"], group[0].components["id"])
    for key in group:
        comps = key.components
        match key.key_type:
            case "ArrProp": ## Array[].Attribute -> Primitive
                new_object.add_aggregate_array_attribute(comps["property"], f'{comps["id"]}.{comps["index"]}', comps["aggregate_property"], key.value)
            case "Arr": ## Array[] -> REF
                new_object.add_aggregate_array_attribute(comps["property"], f'{comps["id"]}.{comps["index"]}', None, key.value)
            case "AggProp": ## Aggregate.Attribute -> Value
                new_object.add_aggregate_attribute(comps["property"], comps["aggregate_property"], key.value)
            case "Prop" | "Primitive": ## Atribute -> Value
                new_object.add_entity_attribute(comps["property"], key.value)
    
    return new_object
############################################################################################
def parse_objects(parsed_keys: list[Key_Value]):
    """Groups Key_Values attributes and aggregates into Entity_Objects"""
    entity_groups = group_keys_into_object(parsed_keys)

    parsed_objects = [
        parse_entity_object(group)
        for group in entity_groups.values()
    ]

    return parsed_objects
############################################################################################
def database_extraction(conn: Redis, db: int):
    """Extracts keys and their values ((k,v) tuples) from Redis database"""
    conn.select(db)
    keys = conn.keys() ## Blocks database, single call to DB
    r_db = [(key, get_value(key, conn)) for key in keys]
    return r_db
############################################################################################
def flatten_object(object, components = None):
    entity = components.get("entity", False)
    id = components.get("id", False)
    property = components.get("property", False)
    index = components.get("index", False)

    pairs = get_json_key_values(object)

    if not id: ## Assures every entity has an ID
        for item in pairs: ## Finds ID in the pairs
            if WRatio(f"{entity}ID", item[0]) > 75:
                id = item[1]
                break
        else:
            id = str(randint(100, 999999999))

    for idx, (k, v) in enumerate(pairs):
        string = f"{entity}:{id}"
        if property:
            string += f":{property}"
            if isinstance(object, list):
                string += k
            else:
                if index:
                    string += f"[{index}]"
                string += f".{k}"
        else:
            string += f":{k}"
        pairs[idx] = (string, str(v))

    return pairs
############################################################################################
def flatten_structured_values(key: Key_Value):
    objects = []
    comps = key.components

    if isinstance(key.value, dict):
        if all(isinstance(v, dict) for v in key.value.values()): ## DICT(DICT) -> Value: {ent:{properties}, ent:{properties}}
            for obj_name, object in key.value.items():
                objects.append(flatten_object(object=object, components={"entity":obj_name}))
        else: ## DICT -> Value: {properties}
            objects.append(flatten_object(object=key.value, components=comps))
        
    elif isinstance(key.value, list):
        if all(isinstance(v, dict) for v in key.value):
            for item in key.value:
                if all(isinstance(v, dict) for v in item.values()): ## LIST[DICT(DICT)] -> Value: {ent:{properties}, ent:{properties}}
                    for obj_name, object in item.items():
                        objects.append(flatten_object(object=object, components={"entity":obj_name}))
                else: ## LIST[DICT] -> Value: {props}
                    objects.append(flatten_object(object=item, components=comps))
        else: ## LIST[] -> Value: REFS
            objects.append(flatten_object(object=key.value, components=comps))
        
    else:
        objects.append(flatten_object(object=key.value, components=comps))

    return objects
############################################################################################
def parse_key_values(r_db):
    patterns = load_regex_patterns("./patterns.yaml")
    parsed_keys = []
    for key, value in r_db:

        ## Checks for structured string value
        if isinstance(value, str):
            value = check_for_structured_string(value)

        ## Extracts key components and pattern
        components, key_type = extract_components(key, patterns)

        ## Creates initial Key/Value
        k_V = Key_Value(key=key, value=value, key_type=key_type, components=components)

        ## Flatten structured values (JSON, HASH, LIST, SET, ZSET) -> Generates new Key_Values for each property
        if not isinstance(value, str):
            objects = flatten_structured_values(k_V)
            for object in objects:
                for key, value in object:
                    components, key_type = extract_components(key, patterns)
                    parsed_keys.append(Key_Value(key=key, value=value, key_type=key_type, components=components))
        else:
            if key_type == "Primitive": ## Primitive key with Primitive value has no ID nor Property
                k_V.components["id"] = str(randint(1000, 999999999))
                k_V.components["property"] = "value"
            
            parsed_keys.append(k_V)

    return parsed_keys
############################################################################################
def main():
    db = 3 ## Redis DB: default 0
    F_THRESHOLD = 75 ## Minimum score to stablish relationship and detect "ID" in attribute

    with redis_connection() as conn:
        ## Extract data from Redis database
        r_db = database_extraction(conn, db)

        ## Parse rdb tuples into Key_Values
        parsed_keys = parse_key_values(r_db)

        ## Parse Key_Values into Entity_Objects
        parsed_objects = parse_objects(parsed_keys)

        ## Parse Entity_Objects into relational model (logical)
        relational_model = generate_relational_model(parsed_objects, F_THRESHOLD)

        ## Generate SQL statements
        sql_schema = generate_sql_schema(relational_model)

        ## Print SQL statements
        for statement in sql_schema:
            print(statement)

        ## Export SQL to file
        with open("schema.sql", "w") as f:
            f.write("\n\n".join(sql_schema))
############################################################################################
if __name__ == "__main__":
    main()