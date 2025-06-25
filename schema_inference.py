import json
from utils import parse_value

def _infer_schema(value):
    value = parse_value(value) if isinstance(value, str) else value
    
    type_map = {
        str: {"type": "string"},
        bool: {"type": "boolean"},
        int: {"type": "integer"},
        float: {"type": "number"},
        type(None): {"type": "null"}
    }
    
    if type(value) in type_map:
        return type_map[type(value)]
    elif isinstance(value, (list, set)):
        return {"type": "array", "items": _merge_array_schemas([_infer_schema(v) for v in value])}
    elif isinstance(value, dict):
        return {"type": "object", "properties": {k: _infer_schema(v) for k, v in value.items()}}
    else:
        return {"type": "string"}

def _merge_array_schemas(schemas):
    if not schemas:
        return {"type": "string"}
    
    unique_schemas = {json.dumps(s, sort_keys=True) for s in schemas}
    if len(unique_schemas) == 1:
        return schemas[0]
    
    return {"oneOf": [json.loads(s) for s in unique_schemas]}

def extract_schema(obj):
    entity = next(iter(obj.keys()))
    return {entity: _infer_schema(obj[entity])}