from utils import parse_value
from tqdm import tqdm

def _get_redis_value_batch(keys, conn, db, batch_size):
    conn.select(db)
    results = {}
    
    for i in tqdm(range(0, len(keys), batch_size)):
        batch_keys = keys[i:i + batch_size]
        batch_results = _process_key_batch(batch_keys, conn)
        results.update(batch_results)
    
    return results

def _process_key_batch(keys, conn):
    pipe = conn.pipeline()
    for key in keys:
        pipe.type(key)
    key_types = pipe.execute()
    
    type_groups = {}
    for key, key_type in zip(keys, key_types):
        if key_type not in type_groups:
            type_groups[key_type] = []
        type_groups[key_type].append(key)
    
    results = {}
    
    if 'string' in type_groups:
        string_keys = type_groups['string']
        string_values = conn.mget(string_keys)
        
        for key, value in zip(string_keys, string_values):
            if value is not None:
                try:
                    if value.isprintable():
                        results[key] = parse_value(value)
                    else:
                        results[key] = None
                except:
                    results[key] = None
            else:
                results[key] = None
    
    non_string_keys = []
    for key_type, keys_of_type in type_groups.items():
        if key_type != 'string':
            non_string_keys.extend([(key, key_type) for key in keys_of_type])
    
    if non_string_keys:
        pipe = conn.pipeline()
        for key, key_type in non_string_keys:
            _add_to_pipeline(pipe, key, key_type)
        
        non_string_values = pipe.execute()
        
        for (key, key_type), value in zip(non_string_keys, non_string_values):
            if key_type == "ReJSON-RL": ## REJSON always returns a list with 1 value (document root)
                value = value[0]
            results[key] = value
    
    return results

def _add_to_pipeline(pipe, key, key_type):
    handlers = {
        "list": lambda: pipe.lrange(key, 0, -1),
        "set": lambda: pipe.smembers(key),
        "hash": lambda: pipe.hgetall(key),
        "zset": lambda: pipe.zrange(key, 0, -1, withscores=True),
        "ReJSON-RL": lambda: pipe.json().get(key, "$")
    }
    
    handler = handlers.get(key_type)
    if handler:
        handler()
    else:
        pipe.exists(key)

def _get_all_keys(conn, db, batch_size):
    conn.select(db)
    keys, cursor = [], 0
    
    while True:
        cursor, batch = conn.scan(cursor, count=batch_size)
        keys.extend(batch)
        if cursor == 0:
            break
    
    return keys

def extract_database(conn, db, batch_size=10000):
    print("Collecting keys...")
    keys = _get_all_keys(conn, db, batch_size)
    print(f"Number of keys collected: {len(keys)}\nGetting values...")
    key_value_dict = _get_redis_value_batch(keys, conn, db, batch_size)
    return sorted([(key, key_value_dict[key]) for key in keys])