import os


def concatenar_todos_codigos(
    destino: str = "transform/utils/concatenado_transform.txt",
    raiz: str = "treat",
    excluir_dirs=None,
):
    """
    Percorre 'treat/' recursivamente e concatena todos os .py
    em um único arquivo .txt, ignorando caches e ambientes virtuais.
    """
    if excluir_dirs is None:
        excluir_dirs = {"__pycache__", "venv", ".venv", "env", ".git"}

    with open(destino, "w", encoding="utf-8") as out:
        for root, dirs, files in os.walk(raiz):
            # Filtra diretórios de exclusão
            dirs[:] = [d for d in dirs if d not in excluir_dirs]

            for file in sorted(files):
                if not file.endswith(".py"):
                    continue
                caminho = os.path.join(root, file)
                out.write(f"\n# ==== {caminho} ====\n\n")
                try:
                    with open(caminho, "r", encoding="utf-8") as f:
                        out.write(f.read())
                        out.write("\n\n")
                except Exception as e:
                    print(f"Erro ao ler {caminho}: {e}")

    print(f"Todos os .py em '{raiz}/' foram concatenados em '{destino}'")


if __name__ == "__main__":
    concatenar_todos_codigos()
