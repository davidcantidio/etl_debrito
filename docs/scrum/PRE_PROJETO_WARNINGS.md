# PRE-PROJETO: Sistema Interativo de Warnings ETL

## 🎯 **Visão do Produto**

> **"Desenvolver um Sistema Interativo de Resolução de Warnings que preserve a arquitetura ultra-otimizada do ETL (2 API calls), permitindo ao usuário resolver inconsistências de dados em tempo real com contexto completo, histórico de decisões e compatibilidade total com todos os sistemas existentes."**

### **Problema a Resolver**
- **Atual**: 384+ warnings passivos nos logs, usuário precisa editar CSVs manualmente
- **Objetivo**: Warnings interativos que pausam o ETL e permitem resolução imediata
- **Resultado**: Zero edição manual de arquivos, decisões persistidas e reutilizadas

---

## 🔍 **Descobertas Críticas da Auditoria**

### **✅ Sistemas Já Otimizados (Preservar)**
1. **SheetsFetcher**: 1x batchGet consolidado (93% redução API calls)
2. **Consolidated Write-Back**: 1x batchUpdate para todos os destinos
3. **BIParamLookup**: Cache inteligente 10min TTL
4. **Safe Connection Pool**: Elimina threading crashes
5. **Early Exit System**: Skip sheets sem novos dados
6. **Smart Rate Limiting**: Aplicado apenas quando necessário

### **⚠️ Sistemas que Podem Conflitar (Integrar)**
1. **Warning Suppression**: `logs/warning_suppressor.py` já filtra warnings
2. **Production Mode**: `logs/production_mode.py` com 3 níveis de logging
3. **Smart Reload**: `transform/utils/smart_reload.py` hot-reload ativo
4. **Schema Validation**: `validate_schema_early()` já detecta problemas
5. **Multiple Caches**: Worksheets, geo, BI - podem mascarar mudanças

### **🔄 Dados Reais do Sistema**
- **Input Principal**: Google Sheets (não CSVs complexos como assumido)
- **CSV Real**: Apenas `municipios.csv` (geo data simples)
- **Output**: `modelo*.csv` são ARTIFACTS, não inputs
- **Warnings**: 15 arquivos com `log.warning()` identificados

---

## 🏗️ **Arquitetura Proposta**

### **Princípios de Design**
1. **Zero Performance Degradation**: Preservar 2 API calls
2. **Hook Before Suppression**: Interceptar antes de filtros existentes
3. **Backwards Compatible**: Sistema atual continua funcionando
4. **Incremental Rollout**: Deploy task por task

### **Componentes Principais**

#### **1. WarningInterceptor** (`transform/warnings/interactive_handler.py`)
```python
class WarningInterceptor:
    def intercept(self, warning_msg: str, context: dict) -> UserDecision:
        # Display warning with full context
        # Show interactive menu (ignore/fix/rule)
        # Return decision for application
```

#### **2. Decision Engine** (`transform/warnings/warning_resolver.py`)
```python
class DecisionResolver:
    def apply_decision(self, decision: UserDecision, context: dict):
        # Apply user decision to current DataFrame
        # Save decision to database for future use
        # Update BI_PARAMETRIZACAO if needed
```

#### **3. Rules Engine** (`transform/warnings/rules_engine.py`)
```python
class RulesEngine:
    def auto_resolve(self, warning: Warning) -> Optional[Resolution]:
        # Check database for previous decisions
        # Apply rules automatically when possible
        # Reduce user interruptions
```

### **Pontos de Integração**
```python
# ANTES (passivo):
log.warning("[Validação] valor não encontrado")

# DEPOIS (interativo):
if interactive_mode:
    decision = warning_interceptor.intercept(msg, context)
    resolver.apply_decision(decision, context)
else:
    log.warning(msg)  # Fallback para modo não-interativo
```

---

## 📊 **Banco de Dados Minimalista**

### **Schema SQLite**
```sql
-- Decisões do usuário
CREATE TABLE user_decisions (
    id INTEGER PRIMARY KEY,
    warning_type TEXT,
    context_hash TEXT,
    user_decision TEXT,
    created_at TIMESTAMP
);

-- Regras automáticas
CREATE TABLE warning_rules (
    id INTEGER PRIMARY KEY,
    pattern TEXT,
    action TEXT,
    replacement_value TEXT,
    created_at TIMESTAMP
);

-- Geografia (migração municipios.csv)
CREATE TABLE geografia (
    id INTEGER PRIMARY KEY,
    cidade TEXT,
    estado TEXT,
    regiao TEXT
);
```

### **Migração Simplificada**
- ✅ `municipios.csv` → `geografia` table (5min)
- ✅ Decisions tracking → `user_decisions` table
- ❌ **NÃO**: Migração complexa de dados históricos (desnecessária)

---

## 🎯 **Épicos e Timeline**

### **EPIC 0: Environment & Production Safety** *(2 dias)*
**Objetivo**: Garantir compatibilidade total com ambiente de produção e sistemas críticos
- 12 micro-tasks de 10-20 minutos cada
- Configuração via environment variables (INTERACTIVE_MODE, PRODUCTION_MODE)
- Integração com warning_suppressor.py e production_mode.py
- PerformanceGuard para garantir 2 API calls

### **EPIC 0.5: Integration Architecture Fixes** *(1 dia)*
**Objetivo**: Resolver inconsistências arquiteturais críticas identificadas na auditoria
- 8 micro-tasks de 15-20 minutos cada
- Pattern de injeção de dependência (não builtins)
- Inventário completo de warning points
- Interfaces específicas de invalidação de cache

### **EPIC 1: Discovery & Compatibility** *(5 dias)*
**Objetivo**: Entender sistemas existentes e pontos de integração
- 25 micro-tasks de 10-15 minutos cada
- Análise de compatibilidade com sistemas críticos
- Identificação de hooks seguros
- Verificação de módulos assumidos (early_exit_checker, schema_validator)

### **EPIC 1.5: Detailed Hook Mapping** *(1 dia)*
**Objetivo**: Mapear pontos específicos de integração mencionados na crítica
- 6 micro-tasks de 10-15 minutos cada
- Hook points específicos para cada função de validação
- Estratégia de integração com relatórios de pipeline
- Testes específicos para cada hook point

### **EPIC 2: Quick Wins & Minimal Prototype** *(1.5 semanas)*
**Objetivo**: Implementar quick wins estratégicos e protótipo básico
- 35 micro-tasks de 8-20 minutos cada  
- Interceptor passivo (log-only), decisões em memória, rule engine mínimo
- PerformanceGuard, configuração .env, context básico
- Compatibilidade total com sistemas existentes

### **EPIC 2.5: TDAH Tooling Implementation** *(2 dias)*
**Objetivo**: Implementar ferramentas CLI e sistemas de rastreabilidade
- 8 micro-tasks de 20-25 minutos cada
- Task timer CLI, progress dashboard, commit tracking
- Achievement tracking básico funcionando
- TDAH toolchain integrada

### **EPIC 3: Classes Integration** *(2 semanas)*  
**Objetivo**: Integração com nova arquitetura de classes distribuídas (Campanha, Anuncio, Plano)
- 28 micro-tasks de 8-15 minutos cada
- Sistema de cache invalidation multi-classe
- Regras específicas por tipo de dados

### **EPIC 3.5: Cache Management Specifics** *(1.5 dias)*
**Objetivo**: Implementar invalidação específica de caches conforme crítica
- 6 micro-tasks de 15-20 minutos cada
- BIParamLookup.invalidate() e SheetsFetcher.clear_cache()
- Coordenação multi-classe de caches
- Testes de invalidação em cadeia

### **EPIC 4: Enhanced Context & Integration** *(1 semana)*
**Objetivo**: Aproveitar otimizações existentes para contexto rico
- 20 micro-tasks de 15-25 minutos cada
- Integração com sheet_name_normalizer, apply_smart_column_mapping
- Uso de caches existentes (_last_taxo_report, _last_impressions_report)
- Context enhancement com dados BI e sugestões inteligentes

### **EPIC 4.5: Compatibility Test Suite** *(1 dia)*
**Objetivo**: Criar suite sistemática de testes de compatibilidade
- 6 micro-tasks de 15-25 minutos cada
- Testes automatizados de regressão
- Cenários de graceful degradation
- Compatibility matrix completa e validada

### **EPIC 5: Performance & Production** *(1 semana)*
**Objetivo**: Garantir performance e preparar para produção
- 15 micro-tasks de 10-20 minutos cada
- Testes de regressão (manter 2 API calls)
- Instrumentação e monitoramento
- Rastreabilidade de micro-tasks e progresso TDAH

---

## 🧠 **Otimização TDAH**

### **Micro-Granularização**
- **~117 tasks total**: Cada 8-25 minutos máximo (incluindo todos os pontos da auditoria crítica)
- **Immediate feedback**: Cada task testável independentemente  
- **Zero context switching**: 1 arquivo, 1 função por task
- **Frequent dopamine**: Progress visível a cada task

### **Sistema de Progresso**
```bash
🎯 EPIC 1: DISCOVERY        [████████░░] 80% (24/30 tasks)
├─ Day 1: System Understanding  [██████████] 100% ✅
├─ Day 2: Warning Analysis      [████████░░] 80%
└─ Day 3: Integration Points    [██░░░░░░░░] 20%

Current: [Task 23/30] Check warning lifecycle (5min)
Next: Create test case for BI_PARAMETRIZAÇÃO warning
```

### **Error Recovery**
- **Individual rollback**: Cada task pode ser desfeita
- **Safe experimentation**: Testes não-destrutivos
- **Branch protection**: Pre-projeto em branch separado

---

## 📈 **Métricas de Sucesso**

### **Técnicas**
- ✅ **Zero degradação**: Performance mantida (2 API calls)
- ✅ **Zero crashes**: Compatibilidade com sistemas existentes  
- ✅ **100% testável**: Cada micro-task funciona independentemente

### **Usuário**
- ✅ **< 30 segundos**: Tempo para resolver warning típico
- ✅ **90% automático**: Warnings resolvidos por regras após setup inicial
- ✅ **Zero CSV editing**: Objetivo principal atingido

### **TDAH**
- ✅ **5-10 minutos**: Task duration máxima
- ✅ **Immediate satisfaction**: Progress visível constantemente
- ✅ **Low context switching**: Foco mantido por epic completo

---

## ⚠️ **Riscos e Mitigações**

### **Risco: Quebrar Otimizações Existentes**
- **Mitigação**: Extensive compatibility testing a cada task
- **Rollback**: Cada change é reversível individualmente

### **Risco: Complexity Creep**
- **Mitigação**: Micro-tasks forçam simplicidade
- **Guard rails**: Cada task tem objetivo claro e testável

### **Risco: TDAH Overwhelm**  
- **Mitigação**: Ultra-granular tasks (93 vs poucos épicos grandes)
- **Support system**: Progress tracking visual, dopamine hits frequentes

---

## 🚀 **Status e Próximos Passos**

### **Status Atual**: 📋 Pre-Projeto Documentado
- ✅ Auditoria completa realizada
- ✅ Compatibilidade analisada  
- ✅ Arquitetura definida
- ✅ Tasks granularizadas

### **Próximos Passos**
1. **Review** desta documentação
2. **Refinamento** baseado em feedback  
3. **Priorização** final dos épicos
4. **Branch creation** para desenvolvimento
5. **Sprint 1 planning** detalhado

### **Decision Points**
- [ ] Aprovação da arquitetura proposta
- [ ] Confirmação de compatibilidade requirements
- [ ] Validação da granularização TDAH
- [ ] Timeline final e recursos

---

**Documento criado**: 2025-01-09  
**Status**: Pre-projeto, aguardando refinamento  
**Próxima revisão**: A definir

---

*📚 Documentos relacionados: `SPRINT_PLANNING.md`, `ARQUITETURA_WARNINGS.md`, `TDAH_OPTIMIZATION.md`*