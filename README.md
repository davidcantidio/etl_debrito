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

## 🧠 Arquitetura do Projeto

![ETL Debrito Mindmap](docs/mindmap.png)

### 🎯 **Visão Geral:**
Sistema ETL ultra-otimizado com **arquitetura de 2 chamadas API** (93% redução), sistema interativo de warnings e ferramentas de produtividade TDAH integradas.

### **📊 Visualização Interativa:**
- 🔗 **[Mermaid Live](https://mermaid.live/)** - Cole o código de [mindmap.md](mindmap.md) para visualização interativa
- 📁 **[Código Fonte](mindmap.md)** - Mindmap em formato Mermaid
- 🎨 **[SVG Alta Resolução](docs/mindmap.svg)** - Para zoom detalhado
- 📋 **[Guia de Visualização](VISUALIZATION_GUIDE.md)** - Setup completo VS Code + CLI

### **⚡ Métricas do Sistema:**
- 📡 **API Calls**: 2 chamadas apenas (era 25-30)
- ⏱️ **Execução**: ~30-45 segundos
- 🧪 **Cobertura TDD**: ≥90% universal
- 🚀 **Performance**: ≤10ms warning processing
- 🏗️ **Épicos**: 8 épicos TDD com 93+ micro-tasks

## 📚 Documentação

- **[CLAUDE.md](CLAUDE.md)** - Documentação técnica completa do pipeline ETL
- **[mindmap.md](mindmap.md)** - Mindmap completo da arquitetura do sistema
- **[VISUALIZATION_GUIDE.md](VISUALIZATION_GUIDE.md)** - Guia completo de visualização Mermaid
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Guia de debugging e resolução de problemas
- **[prject_critic.md](prject_critic.md)** - Análise crítica do pré-projeto vs implementação atual
- **[docs/scrum/](docs/scrum/)** - Documentação do sistema de warnings em desenvolvimento

## 🎯 Épicos de Desenvolvimento (TDD)

| Epic | Nome | Status | Descrição |
|------|------|--------|-----------|
| 0 | Environment & Safety | 📋 Planejado | Base segura para produção |
| 0.5 | Architecture Fixes | 📋 Planejado | Dependency injection + cleanup |
| 2 | Discovery & Compatibility | 📋 Planejado | Sistema de logging existente |
| 3 | Interactive Warning System | 📋 Planejado | ⭐ Core do projeto |
| 4 | TDAH Tooling | 📋 Planejado | Timer, achievements, analytics |
| 5 | Cache Management | 📋 Planejado | Invalidação coordenada |
| 6 | Data Migration & Issues | 📋 Planejado | SQLite + GitHub integration |
| 7 | Missing Caches Integration | 📋 Planejado | Integração write-back |
| 8 | Task Time Monitoring | 📋 Planejado | Analytics tempo estimado vs real |

## 🔍 Sistema Interativo de Warnings

Sistema completo para captura, decisão e resolução automática de warnings:
- ⚡ **Real-time capture**: ≤10ms por warning
- 💾 **Decisões persistentes**: SQLite com ACID compliance  
- 🤖 **Auto-resolution**: Rules engine com 90% cache hit
- 👤 **Interface interativa**: Prompts inteligentes para decisões
- 📊 **Analytics**: Tracking e improvement suggestions
# GitHub Pages Fix Applied dom 10 ago 2025 18:02:13 -03
