import re
from tqdm import tqdm
from collections import defaultdict
from utils import is_id_token, parse_value, remove_empty_containers, KEY_SEPARATORS

def group_keys(kv_pairs):
    groups = defaultdict(list)
    
    for key, value in tqdm(kv_pairs):
        segments = re.split(KEY_SEPARATORS, key)
        id_path = _find_id_path(segments)
        groups[id_path].append((key, value))
    
    return dict(groups)
    
def _find_id_path(segments):
    for i, segment in enumerate(segments, 1):
        if is_id_token(segment):
            return tuple(segments[:i] + [i, True])
    return tuple([segments[0], 1, False])

def build_nested_structure(group_id, key_pairs):
    has_id, id_level = group_id[-1], group_id[-2] if group_id[-1] else None
    entity_id = group_id[-3] if has_id else None
    
    obj = {}
    
    for key, value in key_pairs:
        segments = re.split(KEY_SEPARATORS, key)
        current = obj
        
        for i, segment in enumerate(segments, 1):
            if has_id and i == id_level:
                if 'id' not in current:
                    current['id'] = parse_value(entity_id)
                
                if i == len(segments):
                    _add_terminal_value(current, value)
                
                continue
            
            current = _process_segment(current, segment, value, i, len(segments))
    
    return remove_empty_containers(obj)

def _add_terminal_value(current, value):
    if isinstance(value, dict):
        current.update(value)
    else:
        current["value"] = value

def _process_segment(current, segment, value, index, total_segments):
    array_match = re.match(r'(\w+)\[(\d+)\]', segment)
    
    if array_match:
        return _handle_array_segment(current, array_match, value, index, total_segments)
    else:
        return _handle_object_segment(current, segment, value, index, total_segments)

def _handle_array_segment(current, match, value, index, total_segments):
    key, idx = match.group(1), int(match.group(2))
    
    if key not in current:
        current[key] = []
    
    while len(current[key]) <= idx:
        current[key].append({})
    
    if index == total_segments:
        current[key][idx] = value
        return current
    else:
        if not isinstance(current[key][idx], dict):
            current[key][idx] = {}
        return current[key][idx]

def _handle_object_segment(current, segment, value, index, total_segments):
    if index == total_segments:
        current[segment] = value
        return current
    else:
        if segment not in current or not isinstance(current[segment], dict):
            current[segment] = {}
        return current[segment]