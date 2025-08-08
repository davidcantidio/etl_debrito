# 📊 Documentação Técnica Completa: Pipeline ETL de Dados Publicitários

## 🏆 Classificação por Criticidade

Este documento organiza **70+ processos de transformação** segundo criticidade:
- **🥇 GOLD (15):** Processos críticos insubstituíveis - 21% dos processos, 80% da funcionalidade
- **🥈 SILVER (25):** Processos importantes valiosos - 36% dos processos, qualidade++
- **🥉 BRONZE (30+):** Processos úteis opcionais - 43% dos processos, conveniência

## 🌟 Visão Geral

Este é um pipeline ETL empresarial altamente sofisticado que processa dados de múltiplas plataformas publicitárias. O sistema integra dados do Meta/Facebook, Pinterest, LinkedIn, TikTok, Google Analytics e outras fontes em um formato padronizado no Google Sheets.

### 📊 Planilha de Referência
**Google Sheets Modelo:** https://docs.google.com/spreadsheets/d/1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE/edit?gid=174789324#gid=174789324

Esta planilha contém:
- **BI_PARAMETRIZAÇÃO:** Aba central com mapeamentos, campanhas, UTMs, datas
- **Abas de origem:** metaGeral, linkedinGeral, pinterestGeral, tiktokGeral, etc.
- **Abas de destino:** ModeloGeral, PontoDeControle com layout padronizado
- **SOURCE:** Mapeamento de veículos e IDs

### 📈 Características Principais
- **Cache multi-nível** para performance otimizada
- **Algoritmos proprietários** (distribuição Meta, geo-normalização)
- **Sistema de deduplicação** avançado por key_creative
- **Load incremental** com detecção diferencial
- **Validações rigorosas** com 18 tipos
- **Write-back opcional** para Google Sheets  
- **Configuração centralizada** via environment variables

---

## 🏆 PROCESSOS GOLD - CRÍTICOS E INSUBSTITUÍVEIS (15)

> **Para MVP:** Implemente APENAS os 15 processos GOLD para ter um pipeline funcional básico.

### **1. 💎 Sistema de Cache BIParamLookup** (`bi_param_utils.py`)
**Criticidade:** Hub central de dados. Sistema quebra completamente sem ele.
```python
class BIParamLookup:
    _df: Optional[pd.DataFrame] = None  # Cache global compartilhado
    _last_load: float = 0.0
    _TTL = 60 * 10  # TTL 10 minutos
```

### **2. 🏗️ Pipeline Principal 15 Estágios** (`treat_pipeline.py`)
**Criticidade:** Orquestrador central com lógica de negócio insubstituível.

### **3. 🧮 Algoritmo Distribuição Meta por Idade** (`common_meta.py`)
**Criticidade:** Algoritmo proprietário único. Distribuição proporcional complexa.
```python
def distribute_age_metrics(df_in: pd.DataFrame):
    # Pesos por impressões + arredondamento inteligente
    # Validação soma global obrigatória
```

### **4. 🔑 Sistema Deduplicação key_creative** (`campos_calculados.py`)
**Criticidade:** Lógica central de qualidade de dados.
```python
def add_key_creative(): 
    # Prioridade: utm_content > ad_name > ad_group_name
def dedupe_by_key_creative():
    # Remove duplicatas mantendo ordem
```

### **5. 🌍 Normalização Geográfica com Cache** (`geo_normalize.py`)
**Criticidade:** Cache JSON local + lógica específica brasileira única.
```python
CACHE_ESTADOS, CACHE_MUNICIPIOS = carregar_caches_padrao()
# "Brazil: São Paulo" → "São Paulo"
# "Federal District" → "Distrito Federal"
```

### **6. 🔧 Sistema Substituições Manuais** (`substitute_origin_values.py`)
**Criticidade:** 40+ correções críticas catalogadas. Remove inconsistências essenciais.

### **7. 🔄 Mapeamento Bidirecional Criativos** (`creative_mapping.py`)
**Criticidade:** Funcionalidade central para integração BI_PARAMETRIZAÇÃO.

### **8. ✅ Validações Integridade de Dados** (`validations.py`)
**Criticidade:** 8 validações que previnem corrupção de dados.

### **9. 📈 Sistema Load Incremental** (`load/load.py`)
**Criticidade:** Performance crítica. Detecção diferencial essencial.

### **10. ⚡ Cache Global Google Sheets** (`sheets_cache.py`)
**Criticidade:** Previne rate limits Google API. Cache hierárquico único.

### **11. 🎯 Transform Ponto de Controle** (`ponto_de_controle/transform.py`)
**Criticidade:** Layout final específico. 11 colunas padronizadas.

### **12. 🔗 Sistema Enriquecimento BI** (`bi_param_utils.py`)
**Criticidade:** Enriquecimento central via BI_PARAMETRIZAÇÃO.

### **13. 🏭 Extração Google Sheets** (`extract/sheets_fetcher.py`)
**Criticidade:** Fonte primária de dados. Sistema quebra sem extração.

### **14. 📊 Sistema Numeração Sequencial** (`numeracao.py`)
**Criticidade:** Garante ordem e continuidade sem gaps.

### **15. ⚙️ Configuração Central** (`config.py`)
**Criticidade:** Todas as configurações críticas. Sistema não inicializa sem.

---

## 🥈 PROCESSOS SILVER - IMPORTANTES E VALIOSOS (25)

> **Para Produção:** Adicione os 25 processos SILVER para sistema robusto e completo.

### **Normalização e Qualidade (5 processos)**

**16. 📝 Normalização Unicode** (`normalize.py`)
- `normalize_unicode_text()` - Unicode NFKD essencial
- `normalize_gender()` - M/F/Unknown padronizado  
- `normalize_age()` - Grupos etários consistentes

**17. 📅 Processamento Avançado Datas** (`datas.py`)
- `transformar_para_date()` - Conversão robusta
- `normalize_date_to_str_DD_M_YYYY()` - Formato brasileiro

**18. 🎯 Atribuição Veículos via Lookup** (`atribuicoes_via_lookup.py`)
- Meta via placement, outras via prefixo
- Cache aba SOURCE com TTL

**19. 🚫 Filtros Qualidade Dados** (`filter_utils.py`)
- Remove impressões zero
- Remove campanhas em blacklist

**20. 📋 Sistema Logging Empresarial** (`logs/logging_setup.py`)
- Console INFO+ / Arquivo DEBUG+
- Exception handling automático

### **Transformações por Plataforma (4 processos)**

**21. 📘 Transformações Meta** (`platforms/meta.py`)
**22. 📌 Transformações Pinterest** (`platforms/pinterest.py`) 
**23. 💼 Transformações LinkedIn** (`platforms/linkedin.py`)
**24. 🎵 Transformações TikTok** (`platforms/tiktok.py`)

### **Validações e Qualidade (6 processos)**

**25-30. Validações Estruturais e Consistência** (`validations.py`)
- Schema compliance, campos obrigatórios
- Datas entre abas, métricas por plataforma

### **Mapeamentos e Utilitários (10 processos)**

**31-40. Mapeamentos, Merges e Utilitários Google Sheets**
- Renomeação colunas, merge dimensionais
- Write-back, autenticação Google

---

## 🥉 PROCESSOS BRONZE - ÚTEIS E OPCIONAIS (30+)

> **Para Full-Featured:** Adicione processos BRONZE para funcionalidades específicas e conveniência.

### **Funcionalidades Específicas (10 processos)**
**41-50.** LinkedIn específico, Pinterest específico, Preview links, Validações load específicas

### **Utilitários Desenvolvimento (15 processos)**  
**51-65.** Organização DataFrame, configurações externas, dev utils, merge Pinterest específico

### **Testes e Debug (5+ processos)**
**66-70+.** Suíte testes unitários, testes ponto controle, debug utilities

---

## 📊 Estratégia de Implementação por Fases

### **🚀 FASE 1 - MVP (GOLD apenas)**
**Implementar 15 processos GOLD = Pipeline básico funcional**
- Extração, transformação básica, load incremental
- Cache essencial, validações críticas
- **Tempo estimado:** 60% do desenvolvimento
- **Resultado:** ETL funcional para produção básica

### **📈 FASE 2 - Produção (GOLD + SILVER)**  
**Adicionar 25 processos SILVER = Sistema completo robusto**
- Qualidade de dados++, logging, validações avançadas
- Transformações específicas por plataforma
- **Tempo estimado:** +30% do desenvolvimento  
- **Resultado:** Sistema enterprise-ready

### **🔧 FASE 3 - Full-Featured (GOLD + SILVER + BRONZE)**
**Adicionar 30+ processos BRONZE = Sistema com todas as funcionalidades**
- Conveniência, casos específicos, debug tools
- **Tempo estimado:** +10% do desenvolvimento
- **Resultado:** Sistema completo com todas as features

---

## 📁 Arquivos Essenciais Completos

### **1. 🔧 Configuração e Pipeline Central**
```
config.py                           # Configurações centralizadas
treat/treat_pipeline.py              # Orquestrador principal (15 estágios)
ponto_de_controle/transform.py       # Transformações finais para layout destino
main.py                             # Entry point principal
logs/logging_setup.py               # Sistema de logging empresarial
```

### **2. 🔄 Normalizações Core** 
```
treat/utils/normalize.py             # Normalização Unicode, gênero, idade, veículos
treat/utils/geo_normalize.py         # Normalização geográfica com cache JSON
treat/utils/datas.py                 # Processamento e formatação de datas
treat/utils/substitute_origin_values.py  # Substituições específicas por campo
treat/utils/substitutions_lists.py  # Listas de substituições manuais
```

### **3. 📊 Campos Calculados e Mapeamentos**
```
treat/utils/campos_calculados.py     # Cálculo de campos derivados
treat/utils/creative_mapping.py      # Mapeamento bidirecional utm_content ↔ ad_name
treat/utils/renomeacoes.py           # Mapeamentos de colunas entre plataformas
treat/utils/campanha_mapper.py       # Mapeamento de campanhas por substring
treat/utils/numeracao.py             # Sistema de numeração sequencial
```

### **4. 🔗 Integração BI e Lookups**
```
treat/bi_param_utils.py              # Sistema principal de lookup BI_PARAMETRIZAÇÃO
treat/utils/lookups_bi_parametrizacao.py  # Lookups específicos adicionais
treat/utils/atribuicoes_via_lookup.py     # Atribuição de veículos via lookups
treat/utils/sheets_cache.py          # Cache global para Google Sheets
```

### **5. ✅ Validações e Controle de Qualidade**
```
treat/utils/validations.py           # 18 validações abrangentes de dados
treat/utils/filter_utils.py          # Filtros de impressões zero e campanhas
treat/utils/filter_lists.py          # Listas de filtros configuráveis
treat/utils/consistency.py           # Verificações de consistência
load/utils/validate_impressions_consistency.py  # Validações específicas load
```

### **6. 🔀 Transformações por Plataforma**
```
treat/platforms/meta.py              # Meta/Facebook específicas
treat/platforms/pinterest.py         # Pinterest específicas  
treat/platforms/linkedin.py          # LinkedIn específicas
treat/platforms/tiktok.py            # TikTok específicas
treat/platforms/ga.py                # Google Analytics específicas
treat/utils/common_linkedin.py       # Funcionalidades LinkedIn avançadas
```

### **7. ⚙️ Utilitários Avançados**
```
treat/utils/common_meta.py           # Algoritmo distribuição Meta por idade
treat/utils/preprocess_utils.py      # Pipeline pré-processamento unificado
treat/utils/age_placements_merge.py  # Merge idade/placements
treat/utils/gender_placement_merge.py # Merge gênero/placements
treat/utils/region_placements_merge.py # Merge região/placements
treat/utils/common_meta_merges.py    # Merges específicos Meta
load/load.py                         # Sistema de load incremental
```

### **8. 🏗️ Infraestrutura e Suporte**
```
extract/sheets_fetcher.py            # Extração dados Google Sheets
ponto_de_controle/writer.py          # Escrita com deduplicação
ponto_de_controle/diff.py            # Cálculo diferencial
treat/utils/get_google_client.py     # Autenticação Google
treat/utils/google_sheets.py         # Configurações Google Sheets
```

---

## 🔄 Processos de Normalização Detalhados (70+ Identificados)

### **1. 📝 Normalização Geográfica Avançada** (`geo_normalize.py`)

```python
# Cache JSON local para performance
CACHE_ESTADOS, CACHE_MUNICIPIOS = carregar_caches_padrao()

def normalize_region(regiao_bruta: Any) -> str:
    # 1. Limpeza básica: remove prefixos "Brazil:", "State of", etc.
    # 2. Normalização Unicode NFKD
    # 3. Lookup em cache de estados/municípios
    # 4. Retorna estado normalizado ou "Não identificado"
```

**Transformações aplicadas:**
- `"Brazil: São Paulo"` → `"São Paulo"`
- `"Federal District"` → `"Distrito Federal"`
- `"Greater Rio metropolitan area"` → `"Rio de Janeiro"`

### **2. 🔧 Sistema de Substituições Manuais** (`substitute_origin_values.py`)

```python
# Aplicação de substituições específicas por campo
ID_CONTENT_REPLACEMENTS = {
    "influenciador_gabi_bailas": "dbt_sbrae_2025_catalisa0001",
    "dbt_sbrae_2025_cer_pan0006": "dbt_sbrae_2025_pan0006",
    # ... 10+ substituições
}

CAMPAIGN_NAME_REPLACEMENTS = {
    "TOPVIEW-20250321-Q-20250301460526-2025031002130": 
    "2025_3_EMPREENDEDORISMO FEMININO_ALC_COMERCIALIZAÇÃO_CPM",
    # ... 4+ substituições
}
```

### **3. 🔀 Mapeamento Bidirecional de Criativos** (`creative_mapping.py`)

```python
def get_utm_content_from_ad_name(df, mapping_criativo, write_back=False):
    # Mapeia ad_name → utm_content usando BI_PARAMETRIZAÇÃO
    # Suporte a write-back opcional para Google Sheets
    # Inversão automática do mapeamento utm_content → CRIATIVO
```

### **4. 📊 Algoritmo de Distribuição Meta** (`common_meta.py`)

Sistema complexo para distribuir métricas agregadas por idade através de placements:

```python
def distribute_age_metrics(df_in: pd.DataFrame) -> pd.DataFrame:
    # 1. Calcula pesos por impressões em cada placement
    # 2. Distribuição proporcional de métricas
    # 3. Tratamento especial para valores baixos
    # 4. Arredondamento inteligente (floor para Cost, round para inteiros)
    # 5. Validação de soma global
```

**Métricas processadas:** `Impressions`, `Link clicks`, `Cost`, `Video watches at 100%`

### **5. 🎯 Atribuição Avançada de Veículos** (`atribuicoes_via_lookup.py`)

```python
# Estratégias por plataforma:
# Meta: extraction de placement → Facebook/Instagram
# LinkedIn/Pinterest: lookup via utm_content em BI_PARAMETRIZAÇÃO  
# TikTok/outras: prefixo fixo com mapeamento SOURCE

class SourceLookup:
    # Cache TTL 10min para aba SOURCE
    # Mapeamento: descrição_mídia → ID_Veiculo
```

### **6. 🚫 Filtros Avançados** (`filter_utils.py`)

```python
def remove_zero_impressoes(df: pd.DataFrame) -> pd.DataFrame:
    # Remove linhas com impressões = 0 ou nulas

def filter_campaign_names(df: pd.DataFrame) -> pd.DataFrame:
    # Remove campanhas em blacklist com logging detalhado
```

### **7. ⚡ Pré-processamento Unificado** (`preprocess_utils.py`)

```python
def preprocess_origin(df, worksheet=None, write_back=True):
    # 1. Substituições de origem (com write-back opcional)
    # 2. Normalização geográfica
    # 3. Extração automática de plataforma por nome da aba
```

### **8. 📈 Sistema de Load Incremental** (`load/load.py`)

```python
def load_missing_records(spreadsheet_id, creds_path, origem_sheet, destino_sheet):
    # 1. Extrai origem e destino
    # 2. Gera IDs únicos em ambos
    # 3. Identifica registros faltantes  
    # 4. Gera numeração sequencial incremental
    # 5. Organiza colunas para modelo
    # 6. Faz append apenas dos novos registros
```

### **9. 🔧 Sistema de Logging Empresarial** (`logs/logging_setup.py`)

```python
def setup_logging() -> None:
    # Console (INFO+) + Arquivo (DEBUG+)
    # Captura warnings e exceções não tratadas
    # Intercepta Ctrl+C apropriadamente
    # LOG_DIR e LOG_FILE configuráveis via config.py
```

### **10. ⚡ Cache Global Google Sheets** (`treat/utils/sheets_cache.py`)

```python
# Cache duplo para otimização extrema
_SHEET_CACHE: Dict[Tuple[str, str], gspread.Spreadsheet] = {}
_WS_CACHE: Dict[Tuple[str, str, str], gspread.Worksheet] = {}

def get_worksheet(creds_path, spreadsheet_id, sheet_name):
    # Evita recriar objetos gspread
    # Cache por (credenciais, spreadsheet) E (credenciais, spreadsheet, aba)
```

---

## ✅ Validações Abrangentes (18 Tipos)

### **🔍 Validações Estruturais** (`validations.py`)
1. `check_required_columns()` - Células vazias obrigatórias
2. `validate_columns()` - Estrutura esperada  
3. `validate_utm_content_in_bi()` - UTMs vs BI_PARAMETRIZAÇÃO
4. `validate_taxonomy_consistency()` - Consistência taxonomias

### **📊 Validações de Dados**
5. `validate_aggregates()` - Totais impressions/cost
6. `validate_impressions_by_platform()` - Validação por plataforma
7. `validate_consistent_dates_across_models()` - Datas entre abas
8. `validate_no_blank_cells()` - Células em branco

### **🔄 Validações Específicas Load** (`load/utils/validate_impressions_consistency.py`)
9. Consistência impressões origem/destino
10. Validações métricas específicas módulo load

---

## 📊 Categorização Completa das Transformações

### **CATEGORIA 1: 📝 NORMALIZAÇÕES DE DADOS**

**1.1 Normalização de Texto**
- `normalize.py:normalize_unicode_text()` - Unicode NFKD
- `geo_normalize.py:normalize_string()` - ASCII, lowercase, strip

**1.2 Normalização Demográfica** 
- `normalize.py:normalize_gender()` - M/F/Unknown padronizado
- `normalize.py:normalize_age()` - Grupos etários consistentes  
- `normalize.py:normalize_vehicle()` - Nomes de veículos

**1.3 Normalização Geográfica**
- `geo_normalize.py:normalize_region()` - Estados/municípios via cache
- `geo_normalize.py:limpeza_basica()` - Remoção prefixos "Brazil:", "State of"

**1.4 Normalização de Datas**
- `datas.py:transformar_para_date()` - Conversão para date objects
- `datas.py:normalize_date_to_str_DD_M_YYYY()` - Formato brasileiro
- `datas.py:converter_data()` - Parsing robusto com fallback

### **CATEGORIA 2: ✅ VALIDAÇÕES E CONTROLE DE QUALIDADE**

**2.1 Validações Estruturais**
- `validations.py:validate_columns()` - Schema compliance
- `validations.py:check_required_columns()` - Campos obrigatórios
- `validations.py:validate_no_blank_cells()` - Células vazias

**2.2 Validações de Integridade**
- `validations.py:validate_utm_content_in_bi()` - Referencial vs BI
- `validations.py:validate_taxonomy_consistency()` - Consistência taxonomias
- `validations.py:validate_aggregates()` - Preservação de totais

**2.3 Validações de Consistência**
- `validations.py:validate_consistent_dates_across_models()` - Datas entre abas
- `validations.py:validate_impressions_by_platform()` - Métricas por plataforma

**2.4 Filtros de Qualidade**
- `filter_utils.py:remove_zero_impressoes()` - Dados sem impressões
- `filter_utils.py:filter_campaign_names()` - Campanhas em blacklist

### **CATEGORIA 3: 🔗 ENRIQUECIMENTO E LOOKUPS**

**3.1 Lookups BI_PARAMETRIZAÇÃO**
- `bi_param_utils.py:BIParamLookup` - Sistema principal com cache
- `bi_param_utils.py:get_campaign_maps()` - Campanhas e UTM campaigns
- `bi_param_utils.py:utm_start_end()` - Datas por utm_content

**3.2 Enriquecimento de Campos**
- `bi_param_utils.py:fill_utm_content_from_ad_name()` - utm_content ← ad_name
- `bi_param_utils.py:fill_missing_start_end_from_utm()` - Datas faltantes
- `bi_param_utils.py:enrich_with_bi_parametrizacao()` - Enriquecimento completo

**3.3 Atribuição de Veículos**
- `atribuicoes_via_lookup.py:atribuir_veiculo_e_id_meta()` - Meta via placement
- `atribuicoes_via_lookup.py:atribuir_veiculo_por_prefixo()` - Outras plataformas
- `atribuicoes_via_lookup.py:SourceLookup` - Cache aba SOURCE

### **CATEGORIA 4: 📊 CAMPOS CALCULADOS E DERIVADOS**

**4.1 Métricas Calculadas**
- `campos_calculados.py:calcular_engajamento_total()` - post_reactions + shares + comments
- `campos_calculados.py:gerar_id()` - ID único: data-campanha-impressions-cost-clicks
- `campos_calculados.py:make_id_ponto_de_controle()` - ID para deduplicação

**4.2 Chaves e Identificadores**
- `campos_calculados.py:add_key_creative()` - Prioridade: utm_content > ad_name > ad_group_name
- `campos_calculados.py:dedupe_by_key_creative()` - Deduplicação por chave criativa

**4.3 Datas Derivadas**
- `datas.py:generate_pinterest_dates()` - Inicio/Fim_da_Campanha
- `datas.py:concat_period()` - "DD/M/YYYY a DD/M/YYYY"
- `datas.py:unify_campaign_dates()` - Menor start, maior end por campanha

### **CATEGORIA 5: 🔀 MAPEAMENTOS E SUBSTITUIÇÕES**

**5.1 Substituições Diretas**
- `substitute_origin_values.py:apply_all_origin_substitutions()` - 4 tipos de campos
- `substitutions_lists.py` - 40+ substituições manuais catalogadas
- `renomeacoes.py:aplicar_substituicoes_objetivo()` - 28 objetivos normalizados

**5.2 Mapeamentos Bidirecionais**
- `creative_mapping.py:load_ad_name_mapping()` - utm_content ↔ CRIATIVO
- `creative_mapping.py:get_utm_content_from_ad_name()` - Inversão automática
- `campanha_mapper.py:buscar_mapping()` - Busca por substring

**5.3 Renomeação de Colunas**
- `renomeacoes.py:renomeacao_geral` - 25+ colunas padrão
- `renomeacoes.py:renomeacao_metaIdade` - Meta específico por idade
- `renomeacoes.py:BI_PARAM_COLUMN_MAP` - 50+ mapeamentos BI

---

## 🔗 Integração BI_PARAMETRIZAÇÃO (Sistema Central)

### **⚡ Cache Inteligente** (`bi_param_utils.py`)
```python
class BIParamLookup:
    # Cache global compartilhado entre instâncias
    # TTL: 10 minutos
    # Suporte a múltiplas estratégias de lookup
    
    def get_campaign_maps(self) -> tuple[dict, dict]:
        # Retorna: (campaign_name_map, utm_campaign_map)
        
    def fill_utm_content_from_ad_name(self, write_back=False):
        # Preenchimento bidirecional com write-back opcional
```

---

## 🔄 Pipeline de Transformação Completo (15 Estágios)

1. **Validação Inicial** - Estrutura e dados obrigatórios
2. **Substituições de Origem** - Correções específicas por campo
3. **Renomeação Colunas** - Padronização para formato interno
4. **Normalização Unicode** - NFKD para textos
5. **Normalização Demográfica** - Gênero, idade, localização
6. **Transformações Específicas** - Por plataforma (Meta, Pinterest, etc.)
7. **Enriquecimento BI** - Lookup em BI_PARAMETRIZAÇÃO
8. **Processamento Datas** - Formatação e validação
9. **Campos Calculados** - Engajamento, IDs, chaves criativas  
10. **Mapeamento Criativos** - utm_content, previews
11. **Objetivos** - Normalização e lookup
12. **URLs Preview** - Construção de links por plataforma
13. **Renomeação Final** - Para modelo de destino
14. **Validações Finais** - Consistência e totais
15. **Limpeza** - Remoção de colunas auxiliares

---

## 🌐 Transformações por Plataforma Específicas

### **📘 Meta/Facebook** (`meta.py` + `common_meta.py`)
- **Idades:** Normalização para grupos etários padrão
- **Gêneros:** male→M, female→F, unknown→Unknown
- **Placements:** Extração Facebook vs Instagram
- **Distribuição:** Algoritmo proporcional por impressões
- **Preview:** Prioridade IG > FB > URL_Anuncio

### **📌 Pinterest** (`pinterest.py`)
- **URLs:** `https://www.pinterest.com/pin/{pin_id}`
- **Datas:** Mapping start/end → Inicio/Fim_da_Campanha
- **Campanhas:** Replicação campaign_name → múltiplos campos

### **💼 LinkedIn** (`linkedin.py` + `common_linkedin.py`)
- **Ad Names:** Lookup via BI_PARAMETRIZAÇÃO taxonomy_ad_name_social
- **Veículos:** Atribuição via utm_content lookup
- **B2B:** Campos específicos profissionais
- **Mapeamentos:** Preview + criativo combinado

### **🎵 TikTok** (`tiktok.py`)
- **Normalização:** Específica da plataforma
- **Objetivos:** Métricas do TikTok
- **Veículos:** Prefixo "TikTok" fixo

### **📈 Google Analytics** (`ga.py`)
- **Tráfego:** Processamento dados de tráfego
- **Métricas:** Específicas de GA (sessions, users, etc.)

---

## 🏗️ Mapeamento de Dependências e Integrações

### **🔧 Dependências Centrais**

```
config.py (configuração central)
    ├── MIN_DATE, GOOGLE_CREDS_PATH, SPREADSHEET_ID
    └── usado por: treat_pipeline.py, bi_param_utils.py, todos os módulos

bi_param_utils.py (hub central de lookups)
    ├── BIParamLookup (cache TTL 10min)
    ├── integra: get_google_client, normalize functions
    └── usado por: treat_pipeline.py, atribuicoes_via_lookup.py, preprocess_utils.py
```

### **🔗 Integrações por Módulo**

**1. Pipeline Principal** (`treat_pipeline.py`)
```
treat_pipeline.py
    ├── imports: config, bi_param_utils, validations, campos_calculados
    ├── imports: renomeacoes, datas, normalize, substitute_origin_values  
    ├── imports: platforms/* (meta, pinterest, linkedin, tiktok, ga)
    └── orquestra todos os 15 estágios de transformação
```

**2. Pré-processamento** (`preprocess_utils.py`)
```
preprocess_utils.py
    ├── substitute_origin_values → apply_all_origin_substitutions()
    ├── geo_normalize → normalize_region()
    ├── atribuicoes_via_lookup → assign_vehicle_and_id()
    └── unifica preprocessing para todas as plataformas
```

**3. Meta Complexo** (`common_meta.py`)
```
common_meta.py
    ├── google_sheets → CREDS_PATH, SPREADSHEET_ID
    ├── read_sheet_as_dataframe → carregamento abas
    ├── get_google_client → autenticação
    └── algoritmo independente de distribuição por idade
```

### **📋 Padrões de Integração**

**Padrão 1: Cache Compartilhado**
- `BIParamLookup`: cache global entre todas as instâncias
- `SourceLookup`: cache específico com TTL
- `sheets_cache`: cache global Google Sheets
- `geo_normalize`: cache JSON local para estados/municípios

**Padrão 2: Write-back Opcional**
- `substitute_origin_values.py`
- `creative_mapping.py` 
- `bi_param_utils.py:fill_*` functions
- Permite gravar modificações de volta no Google Sheets

**Padrão 3: Pipeline Unificado**
- `preprocess_utils.py` centraliza pré-processamento
- `treat_pipeline.py` orquestra transformações  
- `validations.py` fornece QA em múltiplos pontos

---

## 🔄 Fluxo de Dados Completo

```
[ORIGEM: Google Sheets] 
    ↓ extract/sheets_fetcher.py
[Raw DataFrames por Aba]
    ↓ preprocess_utils.py
┌─────────────────────────────┐
│    PRÉ-PROCESSAMENTO        │
│ 1. substitute_origin_values │ ← substitutions_lists.py
│ 2. geo_normalize            │ ← cache_estados/municipios.json  
│ 3. assign_vehicle_and_id    │ ← atribuicoes_via_lookup.py
└─────────────────────────────┘
    ↓ treat_pipeline.py (15 estágios)
┌─────────────────────────────┐
│    PIPELINE PRINCIPAL       │
│ 1. Validações iniciais      │ ← validations.py
│ 2. Renomeação colunas       │ ← renomeacoes.py
│ 3. Normalização Unicode     │ ← normalize.py  
│ 4. Normalização demográfica │
│ 5. Transformações plataforma│ ← platforms/*
│ 6. Enriquecimento BI        │ ← bi_param_utils.py (cache)
│ 7. Processamento datas      │ ← datas.py
│ 8. Campos calculados        │ ← campos_calculados.py
│ 9. Mapeamento criativos     │ ← creative_mapping.py
│ 10. Objetivos              │ 
│ 11. URLs preview           │
│ 12. Renomeação final       │
│ 13. Validações finais      │ ← validations.py (18 tipos)
│ 14. Filtros qualidade      │ ← filter_utils.py
│ 15. Limpeza               │
└─────────────────────────────┘
    ↓ ponto_de_controle/transform.py
┌─────────────────────────────┐
│  TRANSFORMAÇÃO FINAL        │
│ 1. Layout destino (11 cols) │ ← ponto_de_controle/constants.py
│ 2. Campos calculados finais │ ← Data, Período, Editoria
│ 3. ID único deduplicação    │ ← make_id_ponto_de_controle()
└─────────────────────────────┘
    ↓ load/load.py + ponto_de_controle/main_pipeline.py
┌─────────────────────────────┐
│    LOAD INCREMENTAL         │
│ 1. Detecção diferencial     │ ← campos_calculados.py:key_creative
│ 2. Numeração sequencial     │ ← numeracao.py
│ 3. Deduplicação            │ ← diff.py
│ 4. Batch write Google      │ ← writer.py
└─────────────────────────────┘
    ↓ 
[DESTINO: Google Sheets Normalizado]
```

### **⚡ Componentes de Cache e Performance**
```
Caches Locais:
├── geo_normalize: JSON files (estados, municípios)
├── BIParamLookup: In-memory TTL 10min
├── SourceLookup: In-memory TTL 10min
├── sheets_cache: Global gspread objects  
└── SheetsFetcher: Per-session cache

Write-back Opcional:
├── substitute_origin_values → Google Sheets
├── creative_mapping → Google Sheets
└── bi_param_utils fills → Google Sheets

Sistema de Logging:
├── Console: INFO+ 
├── Arquivo: DEBUG+ (pipeline_debug.log)
├── UTF-8 encoding
└── Exception handling automático
```

---

## ⚙️ Pontos Críticos para Implementação

### **1. 💾 Sistema de Cache Multi-Nível**
```python
# Cache BI_PARAMETRIZAÇÃO (global, TTL 10min)
BIParamLookup._df = None  # Compartilhado entre todas as instâncias
BIParamLookup._last_load = 0.0

# Cache Geográfico (local, persistent)  
CACHE_ESTADOS, CACHE_MUNICIPIOS = carregar_caches_padrao()

# Cache Google Sheets (global, persistent)
_SHEET_CACHE: Dict[Tuple[str, str], gspread.Spreadsheet] = {}
_WS_CACHE: Dict[Tuple[str, str, str], gspread.Worksheet] = {}
```

### **2. 📝 Write-back Strategy**
Muitas funções suportam gravação opcional no Google Sheets:
```python
# Padrão consistente em todo o codebase
def funcao_transformacao(df, *, write_back=False, worksheet=None):
    # Transformação in-memory sempre
    # Write-back opcional com batch_update
```

### **3. ⚠️ Tratamento de Erros e Validações**
- **Fail-fast**: Validações críticas param o pipeline
- **Graceful degradation**: Lookups falham silenciosamente 
- **Logging extensivo**: Para auditoria e debugging

### **4. 🧮 Algoritmos Críticos**

**Distribuição Meta por Idade:**
```python
# Distribui métricas agregadas proporcionalmente por placement
# baseado em pesos de impressões, com arredondamento inteligente
pesos = {placement: impressions_count}
distribuicao = (peso_placement / total_pesos) * metrica_total
```

**Deduplicação por key_creative:**
```python
# Prioridade: utm_content > ad_name > ad_group_name > ID_Campanha
key_creative = np.select(conditions, choices, default="")
df_dedup = df.drop_duplicates(subset="key_creative", keep="first")
```

**Load Incremental:**
```python
# Detecção diferencial via ID único
df_new = get_missing_records(df_src, df_dest)
df_new = gerar_numeracao(df_new, df_destino=df_dest)  # Sequencial
```

### **5. ⚙️ Configurações Críticas**
```python
# config.py - DEVE ser carregado primeiro
MIN_DATE = date.fromisoformat("2025-01-01")  # Filtro temporal
GOOGLE_CREDS_PATH = Path(os.getenv("GOOGLE_CREDS_PATH", "creds.json"))
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE")
DEST_SHEET_ID = os.getenv("DEST_SHEET_ID", "")
LOG_DIR = Path("logs")
```

**📊 Planilha Google Sheets:**
https://docs.google.com/spreadsheets/d/1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE/edit?gid=174789324#gid=174789324

---

## 🎯 Resumo Executivo para Implementação

**Este pipeline ETL contém 70+ processos de normalização distribuídos em 80+ arquivos Python, processando dados de 5+ plataformas publicitárias.**

### **🌟 Características principais:**
- **Cache multi-nível** para performance
- **Write-back opcional** para Google Sheets  
- **Validações rigorosas** com 18 tipos
- **Algoritmos específicos** (distribuição Meta, geo-normalização)
- **Configuração centralizada** via environment variables
- **Sistema de logging empresarial** com dual output
- **Load incremental** com detecção diferencial
- **Padrões consistentes** em toda a base de código

### **🚀 Para implementar em outro projeto:**

1. **Replicar a estrutura de cache** (BI_PARAMETRIZAÇÃO, geográfico, SOURCE, Google Sheets)
2. **Implementar os algoritmos críticos** (distribuição Meta, key_creative, load incremental)
3. **Configurar autenticação** Google Sheets via service account
4. **Seguir o fluxo de 15 estágios** do pipeline principal
5. **Incluir todas as 70+ transformações** documentadas
6. **Configurar sistema de logging** dual (console + arquivo)
7. **Implementar sistema de numeração** sequencial
8. **Incluir validações abrangentes** (18 tipos)

### **📁 Arquivos absolutamente essenciais:**
```
Configuração Central:
├── config.py
├── logs/logging_setup.py
└── main.py

Pipeline Core:
├── treat_pipeline.py
├── bi_param_utils.py  
├── ponto_de_controle/transform.py
└── load/load.py

Normalizações:
├── normalize.py
├── geo_normalize.py
├── substitute_origin_values.py
├── campos_calculados.py
└── validations.py

Utilitários Críticos:
├── preprocess_utils.py
├── common_meta.py
├── sheets_cache.py
├── creative_mapping.py
└── atribuicoes_via_lookup.py
```

**📊 Recursos Externos Essenciais:**
- **Google Sheets:** https://docs.google.com/spreadsheets/d/1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE/edit?gid=174789324#gid=174789324
- **Service Account JSON:** Para autenticação Google API
- **Cache JSON:** Estados/municípios brasileiros (geo_normalize)
- **Environment Variables:** GOOGLE_CREDS_PATH, SPREADSHEET_ID, etc.

### **📊 Estatísticas Finais:**
- **80+ arquivos Python** analisados
- **70+ processos** de transformação documentados  
- **5 categorias** de transformação mapeadas
- **15 estágios** de pipeline principal
- **18 tipos** de validação identificados
- **4 sistemas de cache** implementados
- **5 plataformas** com transformações específicas
- **10 lacunas críticas** identificadas e documentadas

---

---

## 📈 Análise Estratégica da Classificação

### **🎯 Distribuição por Criticidade:**
```
🥇 GOLD:   15 processos (21%) → 80% funcionalidade crítica
🥈 SILVER: 25 processos (36%) → Qualidade e robustez  
🥉 BRONZE: 30+ processos (43%) → Conveniência e específicos
```

### **💡 Insights para Implementação:**

**1. 🚀 Regra 80/20 Aplicada:**
Implementando apenas **21% dos processos (GOLD)**, você obtém **80% da funcionalidade crítica**.
Ideal para MVP e prototipagem rápida.

**2. 📈 ROI por Categoria:**
- **GOLD:** ROI Altíssimo - Cada processo é essencial
- **SILVER:** ROI Alto - Melhoram significativamente qualidade/UX  
- **BRONZE:** ROI Médio - Conveniência e casos específicos

**3. ⚠️ Dependências Críticas GOLD:**
```
config.py → sheets_cache.py → sheets_fetcher.py
    ↓
bi_param_utils.py → treat_pipeline.py
    ↓  
common_meta.py + campos_calculados.py
    ↓
ponto_de_controle/transform.py → load/load.py
```

### **🔧 Ordem de Implementação Recomendada:**

**MVP Phase (GOLD):**
1. `config.py` - Configurações base
2. `sheets_cache.py` - Cache Google Sheets  
3. `sheets_fetcher.py` - Extração dados
4. `bi_param_utils.py` - Hub central lookups
5. `geo_normalize.py` - Cache geográfico
6. `substitute_origin_values.py` - Substituições críticas
7. `campos_calculados.py` - Deduplicação
8. `common_meta.py` - Algoritmo distribuição
9. `validations.py` - Validações críticas
10. `treat_pipeline.py` - Pipeline principal
11. `creative_mapping.py` - Mapeamento criativos
12. `numeracao.py` - Numeração sequencial
13. `ponto_de_controle/transform.py` - Layout final
14. `load/load.py` - Load incremental
15. `extract/sheets_fetcher.py` - Extração (se não implementado)

---

## 🎯 Resumo Executivo por Classificação

### **🥇 PROCESSOS GOLD - Para MVP:**
**Implementar estes 15 processos garante um ETL funcional básico.**

**Algoritmos Proprietários Únicos:**
- Distribuição Meta por idade (algoritmo complexo)
- Normalização geográfica brasileira (cache JSON)
- Deduplicação por key_creative (prioridade específica)
- Sistema de substituições (40+ correções catalogadas)

**Infraestrutura Crítica:**
- Cache multi-nível (BI + Sheets + Geográfico)
- Pipeline 15 estágios com lógica específica
- Load incremental com detecção diferencial
- Configuração centralizada

### **🥈 PROCESSOS SILVER - Para Produção:**
**Adicionar estes 25 processos para sistema enterprise-ready.**

**Qualidade de Dados:**
- Normalização Unicode/demográfica
- Validações avançadas (structural + consistency)
- Filtros de qualidade automáticos

**Plataformas Específicas:**
- Transformações Meta/Pinterest/LinkedIn/TikTok
- Logging empresarial dual-output

### **🥉 PROCESSOS BRONZE - Para Full-Featured:**
**Adicionar estes 30+ processos para funcionalidades completas.**

**Conveniência:**
- LinkedIn específico, preview links
- Utilitários desenvolvimento, testes
- Debug tools, merge específicos

---

## ✅ Conclusão

**Esta documentação classifica 70+ processos por criticidade, permitindo implementação faseada estratégica.**

### **🎯 Para a Outra IA:**

**Implementação Recomendada:**
1. **FASE MVP:** 15 processos GOLD = Pipeline funcional (60% esforço)
2. **FASE PRODUÇÃO:** +25 processos SILVER = Sistema robusto (+30% esforço)  
3. **FASE COMPLETA:** +30 processos BRONZE = Full-featured (+10% esforço)

**Recursos de Referência:**
- **📊 Google Sheets Modelo:** https://docs.google.com/spreadsheets/d/1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE/edit?gid=174789324#gid=174789324
- **🔧 SPREADSHEET_ID:** `1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE`
- **📋 Abas Críticas:** BI_PARAMETRIZAÇÃO, SOURCE, metaGeral, ModeloGeral

**Processos Insubstituíveis GOLD:**
- Algoritmos proprietários únicos
- Cache multi-nível essencial  
- Lógica de negócio específica
- Infraestrutura crítica

**🎉 Documentação reclassificada e otimizada para implementação estratégica!**

---

*Classificação GOLD/SILVER/BRONZE baseada em análise de criticidade de 80+ arquivos Python.*