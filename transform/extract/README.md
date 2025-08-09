# Extract Layer 📥

**Extração de Dados das Fontes (Data Extraction)**

Esta camada contém a extração de dados das fontes originais sem transformações significativas.

## Responsabilidades:

- **Extração**: Buscar dados das fontes (Google Sheets API, etc.)
- **Ingestão**: Carregar dados brutos preservando formato original
- **Cache**: Implementar cache para otimizar performance
- **Validação Básica**: Verificações mínimas de integridade

## Estrutura:

- `sheets_fetcher.py`: Cliente principal para extração do Google Sheets
- `__init__.py`: Configuração do módulo extract

## Princípios:

- ✅ Dados preservados exatamente como na fonte
- ✅ Mínima transformação aplicada
- ✅ Foco em performance e confiabilidade de extração
- ✅ Tratamento de erros robusto
EOF < /dev/null
