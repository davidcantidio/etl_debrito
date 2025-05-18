import os

def concatenar_todos_codigos(destino='concatenado_projeto.txt', raiz='.', incluir_dirs=None, excluir_dirs=None):
    """
    Percorre os diretórios relevantes e concatena os .py em um único arquivo .txt,
    ignorando ambientes virtuais e pastas de cache.
    """
    if incluir_dirs is None:
        incluir_dirs = {'main', 'scripts', 'tests', 'utils'}
    if excluir_dirs is None:
        excluir_dirs = {'__pycache__', 'venv', '.venv', 'env', '.git'}

    with open(destino, 'w', encoding='utf-8') as out:
        for root, dirs, files in os.walk(raiz):
            # Remove diretórios que não nos interessam
            dirs[:] = [d for d in dirs if d not in excluir_dirs]

            # Mantém apenas os diretórios permitidos ou arquivos na raiz
            caminho_relativo = os.path.relpath(root, raiz)
            if caminho_relativo != '.' and caminho_relativo.split(os.sep)[0] not in incluir_dirs:
                continue

            for file in sorted(files):
                if file.endswith('.py') and not file.startswith('test_') or file.startswith('test_'):
                    caminho = os.path.join(root, file)
                    out.write(f"\n# ==== {caminho} ====\n\n")
                    try:
                        with open(caminho, 'r', encoding='utf-8') as f:
                            out.write(f.read())
                            out.write("\n\n")
                    except Exception as e:
                        print(f"Erro ao ler {caminho}: {e}")

    print(f"Todos os .py relevantes foram concatenados em '{destino}'")

if __name__ == "__main__":
    concatenar_todos_codigos()
