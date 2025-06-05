import os

def coletar_codigo_projeto(root_dir: str,
                           output_file: str,
                           include_dirs: list,
                           include_root_files: list,
                           exts=None) -> None:
    """
    Percorre apenas as pastas em 'include_dirs' (relativas a root_dir) e
    inclui também arquivos específicos em 'include_root_files' (no nível raiz).
    Grava em 'output_file' uma seção para cada .py encontrado, no formato:

    === FILE: caminho/relativo/para/arquivo.py ===
    <conteúdo do arquivo>

    - root_dir: pasta onde está a raiz do projeto (por exemplo, ".")
    - output_file: caminho do .txt que vai receber TODO o código filtrado
    - include_dirs: lista de subpastas (relativas a root_dir) que devem ser varridas
    - include_root_files: lista de nomes de arquivos (apenas no nível root_dir) a incluir
    - exts: lista de extensões (incluindo ponto). Se None, usa ['.py'] por padrão.
    """
    if exts is None:
        exts = ['.py']

    with open(output_file, 'w', encoding='utf-8') as out:
        # 1) Primeiro, inclui apenas os arquivos que estão no nível raiz e batem com include_root_files
        for fname in sorted(os.listdir(root_dir)):
            fullpath = os.path.join(root_dir, fname)
            if os.path.isfile(fullpath) and fname in include_root_files:
                relpath = os.path.relpath(fullpath, root_dir)
                out.write(f"=== FILE: {relpath} ===\n")
                try:
                    with open(fullpath, 'r', encoding='utf-8') as f:
                        out.write(f.read())
                except Exception as e:
                    out.write(f"[ERRO AO LER: {e}]\n")
                out.write("\n\n")

        # 2) Depois, para cada subpasta em include_dirs, faz um os.walk limitado a ela
        for subdir in include_dirs:
            pasta = os.path.join(root_dir, subdir)
            if not os.path.isdir(pasta):
                # Se por algum motivo a pasta não existir, pular
                continue

            for dirpath, dirnames, filenames in os.walk(pasta):
                for filename in sorted(filenames):
                    if any(filename.lower().endswith(ext) for ext in exts):
                        filepath = os.path.join(dirpath, filename)
                        relpath = os.path.relpath(filepath, root_dir)
                        out.write(f"=== FILE: {relpath} ===\n")
                        try:
                            with open(filepath, 'r', encoding='utf-8') as f:
                                out.write(f.read())
                        except Exception as e:
                            out.write(f"[ERRO AO LER: {e}]\n")
                        out.write("\n\n")


if __name__ == "__main__":
    # --- Defina aqui, com base na árvore do seu projeto, o que incluir ---
    ROOT = "."  # ⟵ execute o script a partir da raiz do projeto (onde está este .py)

    # Pastas que contêm código-fonte ativo (subpastas da raiz)
    include_dirs = [
        "extract",
        "load",
        "main",        # esta é a pasta "main/" (contém append_only_new_*.py em main/main/)
        "scripts",
        "tests",
        "treat",
        "utils"
    ]
    

    # Arquivos .py que ficam no nível raiz e que você quer trazer (exatamente esses nomes)
    include_root_files = [
        "main.py",
        "parametrizar.py",
        "planilha_to_pi.py",
        "subs.py",
        "safe_json.py"
    ]

    coletar_codigo_projeto(
        root_dir=ROOT,
        output_file="project_code_filtrado.txt",
        include_dirs=include_dirs,
        include_root_files=include_root_files,
        exts=[".py"]
    )

    print("Coleta finalizada: todo o código do projeto está em project_code_filtrado.txt")
