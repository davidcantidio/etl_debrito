# treat/settings.py
import os
import datetime as dt

# data mínima padrão (pode vir do .env / var de ambiente)
_MIN_DATE_STR = os.getenv("MIN_DATE", "2025-06-01")
MIN_DATE: dt.date = dt.date.fromisoformat(_MIN_DATE_STR)
