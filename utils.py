import json
import re
import hashlib

UUID_REGEX = re.compile(r"^[0-9A-Fa-f]{8}(-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}")
KEY_SEPARATORS = r'[:/.]'

def parse_value(value):
    if not value or (isinstance(value, str) and len(value) == 0):
        return None
    
    if isinstance(value, str):
        if value.count("'") >= 2:
            value = value.replace("'", '"')
        
        try:
            return json.loads(value)
        except:
            if value == "True":
                return True
            elif value == "False":
                return False
    
    return value

def is_id_token(token):
    return token.isdigit() or UUID_REGEX.match(token)

def remove_empty_containers(obj):
    if isinstance(obj, dict):
        return {k: remove_empty_containers(v) for k, v in obj.items() 
                if not (isinstance(v, (dict, list)) and not remove_empty_containers(v))}
    elif isinstance(obj, list):
        return [remove_empty_containers(v) for v in obj 
                if not (isinstance(v, (dict, list)) and not remove_empty_containers(v))]
    return obj

def write_json_file(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def get_schema_hash(schema):
    json_str = json.dumps(schema, sort_keys=True, separators=(',', ':'))
    return hashlib.md5(json_str.encode()).hexdigest()