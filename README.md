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
```

## 📚 Documentação

- **[CLAUDE.md](CLAUDE.md)** - Documentação técnica completa do pipeline ETL
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Guia de debugging e resolução de problemas
- **[prject_critic.md](prject_critic.md)** - Análise crítica do pré-projeto vs implementação atual
- **[docs/scrum/](docs/scrum/)** - Documentação do sistema de warnings em desenvolvimento

## 🔍 Sistema de Warnings (Em Desenvolvimento)

Este projeto inclui planejamento para um **Sistema Interativo de Warnings** documentado em `/docs/scrum/`. A análise crítica da viabilidade está disponível em `prject_critic.md`.
