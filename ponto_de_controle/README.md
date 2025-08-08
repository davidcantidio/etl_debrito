# Módulo Ponto de Controle

Pipeline ETL para atualização incremental de dados no Google Sheets.

## Estrutura

```
ponto_de_controle/
├── __init__.py          # Módulo Python
├── __main__.py          # Entry point para execução CLI
├── constants.py         # Importa configurações de config.py
├── origin.py            # Leitura e preparação dos dados de origem
├── destination.py       # Leitura dos dados de destino
├── transform.py         # Transformação para formato de destino
├── diff.py              # Cálculo de diferenças
├── writer.py            # Gravação no Google Sheets
├── debug.py             # Funções auxiliares de debug
└── ponto_de_controle.ipynb  # Notebook para exploração
```

## Execução

### Via CLI

```bash
# Modo dry-run (apenas loga, não grava)
python -m ponto_de_controle --dry-run

# Modo produção (grava no Google Sheets)
python -m ponto_de_controle
```

### Via Python

```python
from ponto_de_controle import main

# Executa em modo dry-run
main(dry_run=True)

# Executa em modo produção
main(dry_run=False)
```

## Configuração

O módulo usa variáveis de ambiente definidas em `.env` ou `config.py`:

- `ORIGIN_SHEET_ID`: ID da planilha de origem
- `ORIGIN_TAB`: Aba de origem (padrão: "modeloGeral")
- `DEST_SHEET_ID`: ID da planilha de destino
- `DEST_TAB`: Aba de destino (padrão: "IMPULSIONAMENTOS 2025")
- `HEAD_ROW_DEST`: Linha do cabeçalho no destino (padrão: 4)
- `MIN_DATE`: Data mínima para filtros (padrão: "2025-06-01")
- `GOOGLE_CREDS_PATH`: Caminho para credenciais Google (padrão: "creds.json")

## Fluxo de Dados

1. **Extração**: Lê dados da planilha de origem
2. **Filtro**: Aplica filtro de data mínima
3. **Transformação**: Converte para formato de destino
4. **Deduplicação**: Identifica apenas linhas novas
5. **Carga**: Grava linhas novas no destino

## Testes

```bash
pytest tests/ponto_de_controle/
```

## Manutenção

- Para adicionar novas colunas, edite `DEFAULT_DEST_COLUMNS` em `treat/utils/campos_calculados.py`
- Para mudar mapeamentos, edite `transform.py`
- Para alterar validações, edite `origin.py` e `destination.py`