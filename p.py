import os
import glob
from PyPDF2 import PdfMerger
import argparse

def concatenate_pdfs(input_dir, output_file):
    # Criar um objeto PdfMerger
    merger = PdfMerger()
    
    # Encontrar todos os arquivos PDF na pasta
    pdf_files = sorted(glob.glob(os.path.join(input_dir, "*.pdf")))
    
    if not pdf_files:
        print("Nenhum arquivo PDF encontrado na pasta especificada.")
        return
    
    # Adicionar cada PDF ao merger
    for pdf in pdf_files:
        print(f"Adicionando: {os.path.basename(pdf)}")
        merger.append(pdf)
    
    # Criar diretório de saída se não existir
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Salvar o PDF concatenado
    merger.write(output_file)
    merger.close()
    
    print(f"\nPDF concatenado salvo em: {output_file}")
    print(f"Total de arquivos concatenados: {len(pdf_files)}")

def main():
    parser = argparse.ArgumentParser(description="Concatena todos os PDFs em uma pasta")
    parser.add_argument(
        "-i", 
        "--input",
        default="/home/david/Documentos/arbitragem/",
        help="Diretório contendo os arquivos PDF"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="/home/david/Documentos/arbitragem/concatenado.pdf",
        help="Caminho do arquivo PDF de saída"
    )
    
    args = parser.parse_args()
    
    # Verificar se o diretório de entrada existe
    if not os.path.exists(args.input):
        print(f"Erro: O diretório {args.input} não existe.")
        return
    
    concatenate_pdfs(args.input, args.output)

if __name__ == "__main__":
    main()