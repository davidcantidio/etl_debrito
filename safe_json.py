# safe_json.py
import math
import numpy as np

def _to_json_safe(x):
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, str)):
        return x
    if isinstance(x, float):
        return None if math.isnan(x) or math.isinf(x) else x
    if isinstance(x, np.integer):
        return int(x)
    if isinstance(x, np.floating):
        return None if math.isnan(x) or math.isinf(x) else float(x)
    if isinstance(x, dict):
        return {k: _to_json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set)):
        cls = list if isinstance(x, (list, tuple)) else set
        return cls(_to_json_safe(v) for v in x)
    return str(x)

def json_safe(obj):
    """Recursivamente converte obj em tipos JSON‐nativos (int, float, str, None, list, dict)."""
    return _to_json_safe(obj)
