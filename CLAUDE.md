# CLAUDE.md - ETL Debrito Project

## 🏗️ Arquitetura ETL Ultra-Otimizada

Este projeto implementa um **pipeline ETL ultra-otimizado** para dados de marketing digital, reduzindo de ~25-30 chamadas API para **apenas 2 chamadas** (93% de redução).

### 📊 Estrutura do Projeto

```
transform/                    # 🔄 Módulo ETL Principal
├── extract/                  # 📥 Camada de Extração
│   ├── sheets_fetcher.py    # Cliente Google Sheets otimizado
│   └── CLAUDE.md           # Documentação específica Extract
├── transform/              # 🔄 Camada de Transformação
│   ├── transform_pipeline.py  # Pipeline principal
│   ├── platforms/           # Transformações por plataforma
│   ├── utils/              # Utilitários (validações, renomeações)
│   └── CLAUDE.md          # Documentação específica Transform
└── load/                   # 📤 Camada de Carga
    ├── dest_writer.py      # Escritor para destinos finais
    ├── origin_writer.py    # Escritor para origens
    └── CLAUDE.md          # Documentação específica Load
```

## 🚀 Pipeline Ultra-Otimizado (2 Chamadas API)

### Fluxo Completo:
1. **Extract**: `1x batchGet` consolidado → carrega TODAS as abas necessárias
2. **Transform**: Processamento **em memória** → aplica todas as transformações
3. **Load**: `1x batchUpdate` consolidado → escreve TODAS as mudanças

### Otimizações Críticas Implementadas:

#### 🔧 Threading & Connection Pool
- **Safe Connection Pooling**: Elimina erros "unlikely to be threadsafe"
- **Ultra-safe fallbacks**: Múltiplos níveis de proteção contra crashes
- **Smart reload**: Apenas módulos alterados são recarregados

#### 📡 API Optimizations  
- **Consolidated batchGet**: Uma única chamada para todas as abas
- **Consolidated batchUpdate**: Uma única chamada para todas as escritas
- **Metadata consolidation**: Cache inteligente de worksheets (5min TTL)
- **Rate limiting conditional**: Aplicado apenas quando necessário

#### 🔄 Processing Optimizations
- **In-memory processing**: Zero I/O entre Extract e Load
- **Early exit strategies**: Skip abas sem novos dados
- **Smart deduplication**: Evita registros já processados
- **Schema validation**: Validação antecipada de estruturas

## 🛠️ Comandos Importantes

### 🖥️ CLI Modular - Execução por Plataforma

**NEW**: Pipeline pode ser executado modularmente por plataforma via CLI:

```bash
# Executar apenas Meta (Facebook/Instagram)
python main.py --platform meta

# Executar apenas LinkedIn
python main.py --platform linkedin

# Executar TikTok + Pinterest
python main.py --platform tiktok,pinterest

# Executar todas as plataformas (default)
python main.py --platform all
python main.py  # equivalente

# Ver ajuda
python main.py --help
```

#### 📊 Plataformas Suportadas:
- **`meta`**: Facebook/Instagram (metaGeral, metaIdade, metaGenero, metaRegiao, metaAlcance)
- **`linkedin`**: LinkedIn campaigns (linkedinGeral)  
- **`tiktok`**: TikTok ads (tiktokGeral)
- **`Parametrizacao`**: BI_PARAMETRIZACAO (lookups e enrichment)

#### 🎯 Vantagens da Execução Modular:
- **Debugging focado**: Isolar problemas por plataforma
- **Performance**: Processar apenas dados necessários
- **Desenvolvimento**: Testar mudanças em plataforma específica
- **Production**: Executar pipelines independentes via cron

### 📦 Instalação das Dependências:
```bash
# Instalar dependências com Poetry
poetry install

# Ativar ambiente virtual (opcional, Poetry faz automaticamente)
poetry shell
```

### Executar Pipeline Completo:
```bash
# Via Notebook (recomendado para desenvolvimento)
jupyter notebook testar_pipeline_real.ipynb

# Via Python script (pipeline completo)
python testar_pipeline_real.py
```

### Testes:
```bash
# Executar todos os testes
python -m pytest tests/
# ou com Poetry:
poetry run pytest tests/

# Teste específico de performance
python -m pytest tests/test_no_extra_gets.py
```

## 🐛 Debugging & Troubleshooting

### Logs Importantes:
- **Pipeline**: `/logs/pipeline_debug.log`
- **Performance**: Procurar por "🚀" e "📡" nos logs
- **Erros**: Procurar por "❌" e "ERROR"

### Problemas Comuns & Soluções:

#### 1. Threading Issues
**Erro**: `"Iteration over this class is unlikely to be threadsafe"`
**Solução**: `ultra_safe_pooling` já implementado em `transform/transform/utils/safe_connection_pool.py`

#### 2. Import Errors
**Erro**: `ModuleNotFoundError` após reorganização
**Solução**: Verificar imports usando estrutura `transform.{layer}.module`

#### 3. API Rate Limits
**Erro**: HTTP 429
**Solução**: Rate limiting já implementado e conditional

#### 4. CLI Platform Issues
**Erro**: `Plataforma 'X' não encontrada em sheets_config.yaml`
**Solução**: Verificar plataformas disponíveis em `sheets_config.yaml` ou usar `python main.py --help`

**Erro**: `Missing required parameter "spreadsheetId"`
**Solução**: Configurar `SPREADSHEET_ID` environment variable ou verificar se `creds.json` existe

## 📈 Performance Metrics

### Before Optimization:
- **API Calls**: ~25-30 chamadas
- **Time**: ~2-3 minutos
- **Reliability**: Crashes frequentes por threading

### After Optimization:
- **API Calls**: 2 chamadas (93% redução)
- **Time**: ~30-45 segundos  
- **Reliability**: Zero crashes com ultra-safe pooling

## 🔑 Configurações Críticas

### Environment Variables:
```bash
GOOGLE_CREDS_PATH="creds.json"
GOOGLE_SHEET_ID="1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE"
ETL_QUIET_MODE="normal"  # normal|quiet|minimal
```

### API Optimizations (testar_pipeline_real.py):
```python
API_OPTIMIZATIONS = {
    "ultra_mode": True,                # ⭐ Arquitetura 2 chamadas
    "consolidate_batch_get": True,     # batchGet consolidado  
    "consolidate_batch_update": True,  # batchUpdate consolidado
    "cache_worksheets": True,          # Cache de worksheets
    "rate_limiting": True,             # Rate limiting condicional
}
```

## 🎯 Principais Classes & Funções

### Extract Layer:
- `SheetsFetcher`: Cliente principal Google Sheets
- `get_cached_worksheets()`: Cache inteligente de worksheets

### Transform Layer:
- `TreatPipeline`: Pipeline principal de transformação
- `dispatch()`: Roteamento por plataforma (Meta, LinkedIn, TikTok, etc)
- `validate_consistent_dates_across_models()`: Validação de consistência

### Load Layer:
- `consolidated_write_back()`: Escrita consolidada ultra-otimizada
- `prepare_dest_payload()`: Preparação de payloads para destinos
- `prepare_origin_payload()`: Preparação de payloads para origens

## ⚠️ Notas Importantes

### Threading Safety:
- **NUNCA** iterar sobre `adapter.poolmanager.pools` (causa crashes)
- **SEMPRE** usar `ultra_safe_pooling` nos imports
- **EVITAR** operações não thread-safe em connection stats

### API Usage:
- **Manter** arquitetura de 2 chamadas API
- **Usar** consolidação de batchGet/batchUpdate
- **Aplicar** rate limiting apenas quando necessário

### Development:
- **Hot-reload** configurado para dev (ENABLE_HOT_RELOAD = True)
- **Smart reload** implementado - apenas módulos alterados
- **Production mode** disponível via environment vars

## 🔄 Histórico de Otimizações

1. **Eliminação de threading crashes** - ultra_safe_pooling
2. **Redução API calls** - 93% redução (30→2 chamadas)
3. **Consolidação metadata** - 3 calls→1 call  
4. **Hot-reload inteligente** - 16 modules→apenas alterados
5. **Reorganização ETL** - Arquitetura profissional Extract/Transform/Load

## 🚧 **Projetos em Desenvolvimento**

### **Sistema Interativo de Warnings** *(Pre-Projeto)*
**Status**: 📋 Planejamento completo documentado  
**Objetivo**: Resolver warnings do ETL de forma interativa durante execução  
**Benefício**: Eliminar edição manual de CSVs, persistir decisões  

**Documentação completa disponível em:**
- [`docs/scrum/PRE_PROJETO_WARNINGS.md`](docs/scrum/PRE_PROJETO_WARNINGS.md) - Visão geral do projeto
- [`docs/scrum/SPRINT_PLANNING.md`](docs/scrum/SPRINT_PLANNING.md) - 93 micro-tasks detalhadas  
- [`docs/scrum/ARQUITETURA_WARNINGS.md`](docs/scrum/ARQUITETURA_WARNINGS.md) - Especificação técnica
- [`docs/scrum/TDAH_OPTIMIZATION.md`](docs/scrum/TDAH_OPTIMIZATION.md) - Estratégias TDAH específicas

**Análise crítica disponível**: [`prject_critic.md`](prject_critic.md) - Comparação detalhada entre pré-projeto e implementação atual

**Próximos passos**: Refinamento do planejamento → Início da implementação

## 🔍 **Riscos Identificados e Quick Wins**

### **Riscos Técnicos** *(identificados na análise crítica)*
1. **Estado Global (builtins)**: Uso de `builtins.fetcher` e caches globais pode causar problemas de concorrência
2. **TTL Hardcoded**: Cache fixo em 300s pode expirar durante execuções longas
3. **Ordem de Execução Pinterest**: Dependência rígida entre pinterestGeral e demográficos
4. **Thread Safety**: Caches globais sem proteção para acesso paralelo

### **Quick Wins Recomendados**
- ✅ **TTL Configurável**: Expor TTL do cache em arquivo de configuração
- ✅ **Exportar Warnings**: Salvar relatórios de validação em CSV/JSON
- ✅ **Checkpoints nos Logs**: Adicionar timestamps por etapa do pipeline
- ✅ **Testes de Regressão**: Criar suite básica com dados de amostra

---

**Última atualização**: 2025-01-09  
**Status**: Pipeline completamente funcional e otimizado  
**Performance**: 93% redução em API calls, zero crashes  
**Projetos futuros**: Sistema de warnings interativo planejado e documentado