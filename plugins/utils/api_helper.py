from typing import List, Dict
    
def find_key_value(data:Dict|List, target_key:str, max_depth:int = 5, current_depth:int = 0):
    if current_depth > max_depth:
        return "", False

    if isinstance(data, dict):
        for k, v in data.items():
            if k == target_key:
                return v, True
            val, found = find_key_value(v, target_key, max_depth, current_depth + 1)
            if found:
                return val, True

    elif isinstance(data, list):
        for item in data:
            val, found = find_key_value(item, target_key, max_depth, current_depth + 1)
            if found:
                return val, True

    return "", False