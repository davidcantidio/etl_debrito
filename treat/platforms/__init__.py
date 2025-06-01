# File: treat/platforms/__init__.py

from importlib import import_module

def dispatch(sheet_name: str):
    """Aplica transformações específicas da plataforma."""
    lower = sheet_name.lower()
    if lower.startswith("meta"):
        mod = "meta"
    elif lower.startswith("tiktok"):
        mod = "tiktok"
    elif lower.startswith("pinterest"):
        mod = "pinterest"
    elif lower.startswith("linkedin"):
        mod = "linkedin"
    elif lower.startswith("gageral"):
        mod = "ga"
    else:
        # fallback: nenhuma transformação específica
        return lambda df, lookup=None: df

    # importa dinamicamente o módulo e retorna transform_<mod>
    return getattr(
        import_module(f"treat.platforms.{mod}"),
        f"transform_{mod}"
    )
