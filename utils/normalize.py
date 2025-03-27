# utils/normalize.py

def normalize_campaign_name(value):
    if not isinstance(value, str):
        return value
    return value.strip().upper()
