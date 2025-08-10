#!/bin/bash
# 🧠 Script de Geração Automática de Mindmap - ETL Debrito

echo "🧠 Gerando mindmap ETL Debrito..."

# Verificar se mermaid-cli está instalado
if ! command -v mmdc &> /dev/null; then
    echo "❌ Mermaid CLI não encontrado!"
    echo "💡 Instale com: npm install -g @mermaid-js/mermaid-cli"
    echo "📋 Ou siga o guia em: VISUALIZATION_GUIDE.md"
    exit 1
fi

# Verificar se mindmap.md existe
if [ ! -f "mindmap.md" ]; then
    echo "❌ Arquivo mindmap.md não encontrado!"
    echo "💡 Execute este script no diretório do projeto ETL Debrito"
    exit 1
fi

# Criar diretório docs se não existir
mkdir -p docs
echo "📁 Diretório docs/ criado/verificado"

# Gerar PNG otimizado para GitHub/apresentações
echo "📸 Gerando PNG..."
mmdc -i mindmap.md -o docs/mindmap.png \
    --backgroundColor white \
    --width 2400 \
    --height 1600 \
    --scale 2

# Gerar SVG para alta qualidade
echo "🎨 Gerando SVG..."
mmdc -i mindmap.md -o docs/mindmap.svg \
    --backgroundColor white

# Gerar PDF para impressão
echo "📄 Gerando PDF..."
mmdc -i mindmap.md -o docs/mindmap.pdf \
    --backgroundColor white \
    --width 2400 \
    --height 1600

# Verificar se arquivos foram criados
echo ""
echo "✅ Mindmap gerado com sucesso:"
if [ -f "docs/mindmap.png" ]; then
    echo "   📸 docs/mindmap.png ($(du -h docs/mindmap.png | cut -f1))"
fi
if [ -f "docs/mindmap.svg" ]; then
    echo "   🎨 docs/mindmap.svg ($(du -h docs/mindmap.svg | cut -f1))"
fi
if [ -f "docs/mindmap.pdf" ]; then
    echo "   📄 docs/mindmap.pdf ($(du -h docs/mindmap.pdf | cut -f1))"
fi

echo ""
echo "🚀 Para visualizar:"
echo "   🌐 Online: https://mermaid.live/ (cole conteúdo de mindmap.md)"
echo "   💻 Local: code mindmap.md (com extensão Mermaid Preview)"
echo "   📸 Imagem: xdg-open docs/mindmap.png"

echo ""
echo "📋 Próximos passos:"
echo "   1. Ver imagens em docs/"
echo "   2. Adicionar ao README.md do projeto"
echo "   3. Commitar arquivos: git add docs/ mindmap.md"