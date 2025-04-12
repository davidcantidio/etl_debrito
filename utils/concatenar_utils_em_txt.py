import os

def concatenar_arquivos_em_txt(diretorio_base, nome_arquivo_saida='concatenado.txt', extensao='.py'):
    caminho_saida = os.path.join(diretorio_base, nome_arquivo_saida)

    with open(caminho_saida, 'w', encoding='utf-8') as outfile:
        for nome_arquivo in sorted(os.listdir(diretorio_base)):
            caminho_completo = os.path.join(diretorio_base, nome_arquivo)

            if os.path.isfile(caminho_completo) and nome_arquivo.endswith(extensao):
                outfile.write(f'# utils/{nome_arquivo}\n\n')
                with open(caminho_completo, 'r', encoding='utf-8') as f:
                    outfile.write(f.read())
                    outfile.write('\n\n')
    
    print(f"Arquivos concatenados com sucesso em: {caminho_saida}")

if __name__ == '__main__':
    diretorio = '/home/debrito/Documentos/ETL/ELET_ETL_Projeto/utils'
    concatenar_arquivos_em_txt(diretorio)
