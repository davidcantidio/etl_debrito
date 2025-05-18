# check_unused.py
"""
Varre todo o repositório e reporta .py não alcançados a partir de
'treat/treat_runner.py' e 'treat/treat_pipeline.py'.
"""
import ast, os, sys, importlib.util, pathlib, networkx as nx
from tqdm import tqdm
from rich import print as rprint

ROOT = pathlib.Path(__file__).resolve().parent
ENTRY = [
    ROOT / "treat" / "treat_runner.py",
    ROOT / "treat" / "treat_pipeline.py",
]

def module_name(path: pathlib.Path) -> str:
    rel = path.relative_to(ROOT).with_suffix("")
    parts = rel.parts
    return ".".join(parts)

def iter_py_files():
    SKIP_TOP = {"env", "venv", ".venv", "tests", ".git", "Documentos", "__pycache__"}
    for p in ROOT.rglob("*.py"):
        rel = p.relative_to(ROOT)
        # se o 1º diretório for um destes, ignora
        if rel.parts and rel.parts[0] in SKIP_TOP:
            continue
        yield p

# Build import graph -------------------------------------------------
G = nx.DiGraph()
files = {module_name(p): p for p in iter_py_files()}

def add_edges(name: str, path: pathlib.Path):
    if not path.is_file(): return
    G.add_node(name)
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                mod = alias.name.split(".")[0]
                if mod in files:  # only project-local
                    G.add_edge(name, mod)

# cria arestas
for mod, path in files.items():
    add_edges(mod, path)

# Marcação de alcançáveis
reachable = set()
for entry in ENTRY:
    mod = module_name(entry)
    if mod not in G: continue
    reachable |= nx.descendants(G, mod) | {mod}

unused = sorted(set(files) - reachable)

# Relatório
rprint(f"[bold green]Arquivos analisados:[/bold green] {len(files)}")
rprint(f"[bold yellow]Alcançáveis:[/bold yellow] {len(reachable)}")
rprint(f"[bold red]Não usados:[/bold red] {len(unused)}\n")
for mod in unused:
    rprint(f"  • {files[mod].relative_to(ROOT)}")
