# ETL Debrito

![CI](https://github.com/davidcantidio/etl_debrito/actions/workflows/ci.yml/badge.svg)

Projeto de ETL para unificação e tratamento de dados de marketing digital.

## 📦 Estrutura
- `extract/`: scripts de extração de dados (ex: Google Sheets)
- `treat/`: transformações e normalizações
- `load/`: escrita nos destinos (planilhas, banco de dados)
- `tests/`: testes automatizados com `pytest`
- `.github/workflows/`: workflows de CI (formatação, lint, notebooks)

## 🚀 Executando localmente

### Instalação das dependências
```bash
poetry install
```

### Executando o pipeline
```bash
python main.py
