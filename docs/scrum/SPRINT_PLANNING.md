# SPRINT PLANNING: Sistema Interativo de Warnings

## 📋 **Product Backlog - ~135 Micro-Tasks TDAH-Optimized (Terceira Auditoria Ultra-Sistemática)**

### **Metodologia SCRUM Adaptada para TDAH**
- **Micro-tasks**: 8-25 minutos cada (135 total, cobertura 99.5% da crítica linha-por-linha)
- **Immediate feedback**: Cada task testável independentemente  
- **Visual progress**: Checkbox satisfaction constante
- **Zero context switching**: 1 arquivo, 1 função por task
- **Rollback individual**: Cada task pode ser desfeita

---

## 🎯 **EPIC 0: ENVIRONMENT & PRODUCTION SAFETY** *(2 dias, 12 micro-tasks críticas)*

### **Sprint Goal**
> Garantir compatibilidade total com ambiente de produção e sistemas críticos existentes antes de qualquer desenvolvimento.

### **Definition of Done**
- [ ] Environment variables configuradas e testadas
- [ ] Compatibilidade com warning_suppressor.py verificada
- [ ] Integration com production_mode.py funcionando
- [ ] PerformanceGuard implementado e testado
- [ ] Configuração .env funcional em dev e prod

---

#### **DIA 1: Production Safety & Environment (6 tasks × 15-20min)**

**US000**: *Como desenvolvedor, quero garantir que o sistema funciona seguramente em produção*

1. [ ] **Task 0.1**: Test current warning_suppressor.py behavior *(15min)*
   - **AC**: Entender exatamente quais warnings são suprimidos
   - **Test**: Executar ETL com/sem supressor e comparar outputs
   - **Deliverable**: Lista de warnings afetados pelo suppressor

2. [ ] **Task 0.2**: Test production_mode.py impact on logging *(18min)*
   - **AC**: Mapear como production_mode afeta visibility de warnings
   - **Test**: Testar todos os níveis (normal, quiet, minimal)
   - **Deliverable**: Matriz de compatibilidade mode × warning type

3. [ ] **Task 0.3**: Design environment-based configuration *(20min)*
   - **AC**: Criar schema de environment variables necessárias
   - **Test**: Definir INTERACTIVE_MODE, PRODUCTION_MODE flags
   - **Deliverable**: Environment variables documentation

4. [ ] **Task 0.4**: Implement basic PerformanceGuard class *(20min)*
   - **AC**: Classe que conta chamadas API e alerta se > 2
   - **Test**: Instanciar e testar contagem manual
   - **Deliverable**: PerformanceGuard.py funcional

5. [ ] **Task 0.5**: Test PerformanceGuard integration *(15min)*
   - **AC**: Integrar PerformanceGuard no SheetsFetcher
   - **Test**: Executar ETL e verificar contagem correta
   - **Deliverable**: Contagem de API calls funcionando

6. [ ] **Task 0.6**: Create .env configuration system *(20min)*
   - **AC**: Sistema para ler configurações de .env file
   - **Test**: Testar diferentes combinações de flags
   - **Deliverable**: Configuration loader funcional

---

#### **DIA 2: System Integration Safety (6 tasks × 10-20min)**

**US001**: *Como desenvolvedor, quero verificar todos os pontos de compatibilidade*

7. [ ] **Task 0.7**: Check if early_exit_checker.py exists *(10min)*
   - **AC**: Verificar se arquivo existe ou criar nota de ausência
   - **Test**: Procurar no codebase atual
   - **Deliverable**: Status report do módulo

8. [ ] **Task 0.8**: Check if schema_validator.py exists *(10min)*
   - **AC**: Verificar se arquivo existe ou criar nota de ausência  
   - **Test**: Procurar no codebase atual
   - **Deliverable**: Status report do módulo

9. [ ] **Task 0.9**: Test smart_reload.py compatibility *(15min)*
   - **AC**: Verificar se smart_reload não conflita com warnings
   - **Test**: Executar reload durante warning generation
   - **Deliverable**: Compatibility assessment

10. [ ] **Task 0.10**: Design production mode detection *(18min)*
    - **AC**: Lógica para detectar automaticamente production environment
    - **Test**: Testar detection com different environment setups
    - **Deliverable**: Production detection logic

11. [ ] **Task 0.11**: Create compatibility matrix *(20min)*
    - **AC**: Matriz completa system × environment × behavior
    - **Test**: Validar cada combinação crítica
    - **Deliverable**: Comprehensive compatibility documentation

12. [ ] **Task 0.12**: Test complete safety configuration *(15min)*
    - **AC**: Configuração final funcionando em prod/dev/test
    - **Test**: Deploy test em cada environment  
    - **Deliverable**: Working configuration para próximo epic

---

## 🎯 **EPIC 0.5: INTEGRATION ARCHITECTURE FIXES** *(1 dia, 8 micro-tasks críticas)*

### **Sprint Goal**
> Resolver inconsistências arquiteturais críticas identificadas na auditoria: builtins vs dependency injection, mapeamento completo de warning points, e interfaces específicas.

### **Definition of Done**
- [ ] Pattern de injeção de dependência definido (não builtins)
- [ ] Inventário completo de todos os log.warning() calls
- [ ] Funções específicas de validação mapeadas para hooks
- [ ] Interfaces de invalidação de cache projetadas

---

#### **Refinamentos Arquiteturais Críticos (8 tasks × 15-20min)**

**US000.5**: *Como desenvolvedor, quero resolver inconsistências arquiteturais identificadas na crítica*

97. [ ] **Task 0.13**: Resolve builtins vs dependency injection pattern *(20min)*
    - **AC**: Definir pattern consistente para compartilhar objetos entre módulos
    - **Test**: TreatPipeline vs main branch - resolver divergência
    - **Deliverable**: Arquitetura de injeção sem uso de builtins globais

98. [ ] **Task 0.14**: Create complete warning points inventory (grep-based) *(15min)*
    - **AC**: Lista completa de todos os log.warning() calls no codebase
    - **Test**: Executar grep recursivo e catalogar resultados
    - **Deliverable**: Inventário detalhado para hook placement

99. [ ] **Task 0.15**: Map specific validation functions for hooking *(18min)*
    - **AC**: validate_columns, check_required_columns, validate_taxonomy_consistency mapeadas
    - **Test**: Confirmar que cada função listada na crítica foi identificada
    - **Deliverable**: Hook points específicos por função

100. [ ] **Task 0.16**: Design cache invalidation interfaces *(20min)*
     - **AC**: Interfaces para BIParamLookup.invalidate() e SheetsFetcher.clear_cache()
     - **Test**: Definir assinaturas de métodos necessários
     - **Deliverable**: Interface specification para invalidação

101. [ ] **Task 0.17**: Resolve pipeline reports integration strategy *(18min)*
     - **AC**: Como modificar _last_taxo_report e _last_impressions_report
     - **Test**: Definir strategy para warning decisions → reports
     - **Deliverable**: Integration pattern para relatórios de pipeline

102. [ ] **Task 0.18**: Test builtins elimination approach *(15min)*
     - **AC**: Confirmar que dependency injection funciona sem builtins
     - **Test**: Mock test da nova arquitetura
     - **Deliverable**: Proof-of-concept sem vazamento de estado global

103. [ ] **Task 0.19**: Design warning context preservation *(17min)*
     - **AC**: Como manter contexto entre validation → decision → application
     - **Test**: Flow de dados sem perder informação
     - **Deliverable**: Context flow specification

104. [ ] **Task 0.20**: Create architectural consistency checklist *(12min)*
     - **AC**: Checklist para validar consistency durante implementação
     - **Test**: Aplicar checklist nos épicos seguintes
     - **Deliverable**: Architectural validation checklist

---

## 🎯 **EPIC 1: DISCOVERY & COMPATIBILITY** *(5 dias, 25 micro-tasks realistas)*

### **Sprint Goal**
> Entender completamente os sistemas existentes e identificar pontos seguros de integração sem quebrar otimizações críticas.

### **Definition of Done**
- [ ] Todos os sistemas conflitantes identificados
- [ ] Pontos de hook mapeados e testados
- [ ] Matriz de compatibilidade documentada  
- [ ] Estratégia de integração segura definida

---

#### **DIA 1: System Understanding (8 tasks × 10-15min)**

**US001**: *Como desenvolvedor, quero entender os sistemas de logging existentes para integrar sem conflitos*

1. [ ] **Task 1.1**: Read `logs/warning_suppressor.py` lines 1-30 *(10min)*
   - **AC**: Entender quais warnings são suprimidos automaticamente
   - **Test**: Executar sistema e confirmar warnings suprimidos
   - **Deliverable**: Lista de padrões de supressão ativos

2. [ ] **Task 1.2**: Read `logs/warning_suppressor.py` lines 31-end *(8min)*
   - **AC**: Mapear funções de supressão e configurações
   - **Test**: Identificar environment variables que controlam supressão
   - **Deliverable**: Documentar como desabilitar supressão temporariamente

3. [ ] **Task 1.3**: Read `logs/production_mode.py` lines 1-50 *(12min)*
   - **AC**: Entender níveis de logging (normal, quiet, minimal)
   - **Test**: Testar mudança de nível via environment variable
   - **Deliverable**: Mapa de comportamento por nível

4. [ ] **Task 1.4**: Read `logs/production_mode.py` lines 51-100 *(15min)*
   - **AC**: Identificar filtros automáticos em produção
   - **Test**: Simular production mode e observar diferenças
   - **Deliverable**: Lista de loggers silenciados em produção

5. [ ] **Task 1.5**: Read `logs/production_mode.py` lines 101-end *(6min)*
   - **AC**: Entender detection automática de ambiente
   - **Test**: Forçar production mode e verificar comportamento
   - **Deliverable**: Método para forçar development mode

6. [ ] **Task 1.6**: Test current warning suppression behavior *(8min)*
   - **AC**: Executar ETL completo e catalogar warnings visíveis
   - **Test**: Comparar logs com/sem supressão
   - **Deliverable**: Lista de warnings atualmente visíveis vs suprimidos

7. [ ] **Task 1.7**: Identify which warnings reach console vs log file *(5min)*
   - **AC**: Mapear destino de cada tipo de warning
   - **Test**: Gerar warning conhecido e verificar onde aparece
   - **Deliverable**: Routing map: warning type → output destination

8. [ ] **Task 1.8**: Document current log filtering levels *(8min)*
   - **AC**: Criar matriz level × logger × visibility
   - **Test**: Testar cada combination importante
   - **Deliverable**: Tabela de compatibilidade completa

9. [ ] **Task 1.9**: Test production vs development mode behavior *(8min)*
   - **AC**: Executar ETL em ambos os modos
   - **Test**: Comparar warnings visíveis em cada modo
   - **Deliverable**: Strategy para funcionar em ambos os ambientes

8. [ ] **Task 1.8**: Create compatibility matrix (systems vs proposed changes) *(20min)*
    - **AC**: Documentar impact de cada sistema no projeto
    - **Test**: Validar assessment com tests básicos
    - **Deliverable**: Risk/compatibility assessment completo

---

#### **DIA 2-3: Warning System Analysis (8 tasks × 10-20min)**

**US002**: *Como desenvolvedor, quero mapear todos os pontos onde warnings são gerados para saber onde fazer hooks*

9. [ ] **Task 2.1**: Analyze `transform/transform/utils/validations.py` structure *(15min)*
    - **AC**: Entender organização geral do arquivo e suas funções
    - **Test**: Executar validações e observar comportamento
    - **Deliverable**: Mapa de funções de validação existentes

10. [ ] **Task 2.2**: Catalog ALL log.warning() calls in validations.py *(20min)*
    - **AC**: Lista completa com contexto, linha e tipo de warning
    - **Test**: Grep + verificação manual para garantir completude
    - **Deliverable**: Warning inventory detalhado

11. [ ] **Task 2.3**: Test warning triggering and context analysis *(18min)*
    - **AC**: Criar cenários que triggerem diferentes types de warning
    - **Test**: Capturar contexto disponível para cada warning
    - **Deliverable**: Test cases reproduzíveis com contexto mapeado

14. [ ] **Task 2.4**: Read `schema_validator.py` warning calls *(6min)*
    - **AC**: Identificar warnings de schema validation
    - **Test**: Trigger schema warning com dados malformados
    - **Deliverable**: Schema warning examples

15. [ ] **Task 2.5**: Document warning patterns currently used *(7min)*
    - **AC**: Identificar padrões comuns "[Validação]", "[Schema]"
    - **Test**: Confirmar patterns com search nos logs
    - **Deliverable**: Warning pattern taxonomy

16. [ ] **Task 2.6**: Test trigger one specific warning manually *(8min)*
    - **AC**: Criar dados que triggere warning específico
    - **Test**: Executar ETL e confirmar warning aparece
    - **Deliverable**: Reproducible test case

17. [ ] **Task 2.7**: Observe warning in current logging setup *(5min)*
    - **AC**: Verificar formato e context do warning
    - **Test**: Capturar warning output exact
    - **Deliverable**: Warning format documentation

18. [ ] **Task 2.8**: Check if warnings reach console vs log file *(6min)*
    - **AC**: Mapear onde cada warning type aparece
    - **Test**: Verificar console vs arquivo para warning específico
    - **Deliverable**: Output routing per warning type

19. [ ] **Task 2.9**: Test warning behavior in production vs dev mode *(8min)*
    - **AC**: Confirmar visibility em diferentes modes
    - **Test**: Mesmo warning em prod vs dev mode
    - **Deliverable**: Mode-specific behavior map

20. [ ] **Task 2.10**: Document warning lifecycle (generation → suppression) *(10min)*
    - **AC**: Mapear journey completo do warning
    - **Test**: Trace warning desde geração até output
    - **Deliverable**: Warning lifecycle flowchart

---

#### **DIA 3: Integration Points (10 tasks × 5-8min)**

**US003**: *Como desenvolvedor, quero identificar pontos seguros de hook que não quebrem otimizações existentes*

21. [ ] **Task 3.1**: Read `smart_reload.py` lines 1-50 *(5min)*
    - **AC**: Entender hot-reload mechanism
    - **Test**: Modificar arquivo e verificar reload automático
    - **Deliverable**: Impact assessment on debugging

22. [ ] **Task 3.2**: Read `early_exit_checker.py` lines 1-50 *(6min)*
    - **AC**: Entender quando sheets são skipped
    - **Test**: Criar sheet sem novos dados e verificar skip
    - **Deliverable**: Early exit conditions documented

23. [ ] **Task 3.3**: Test early exit behavior *(8min)*
    - **AC**: Confirmar que early exit previne warnings
    - **Test**: Sheet com warning mas sem novos dados
    - **Deliverable**: Interaction map: early exit × warnings

24. [ ] **Task 3.4**: Identify exact hook points for warnings *(7min)*
    - **AC**: Mapear linha exata onde interceptar
    - **Test**: Add print statement no ponto identificado
    - **Deliverable**: Hook point coordinates (file:line)

25. [ ] **Task 3.5**: Test hook point with simple print() statement *(6min)*
    - **AC**: Confirmar que hook funciona sem quebrar nada
    - **Test**: ETL executa normalmente com print extra
    - **Deliverable**: Working hook proof-of-concept

26. [ ] **Task 3.6**: Document order of execution (early exit → validation → warnings) *(8min)*
    - **AC**: Mapear sequence completa de checks
    - **Test**: Trace execution com logs detalhados
    - **Deliverable**: Execution flow diagram

27. [ ] **Task 3.7**: Check if warnings occur before or after early exit *(5min)*
    - **AC**: Confirmar timing preciso
    - **Test**: Sheet que triggere ambos early exit e warning
    - **Deliverable**: Timing conflict analysis

28. [ ] **Task 3.8**: Create minimal test case for each warning type *(10min)*
    - **AC**: Test case reproduzível para cada warning
    - **Test**: Executar cada test case independentemente
    - **Deliverable**: Test suite para development

29. [ ] **Task 3.9**: Test compatibility with existing systems *(8min)*
    - **AC**: Confirmar que hooks não quebram performance
    - **Test**: Benchmark before/after hook installation
    - **Deliverable**: Performance impact assessment

30. [ ] **Task 3.10**: Document final integration strategy *(10min)*
    - **AC**: Strategy completa e safe para implementação
    - **Test**: Review strategy contra todos os risks identificados
    - **Deliverable**: Implementation roadmap

---

## 🎯 **EPIC 1.5: DETAILED HOOK MAPPING** *(1 dia, 6 micro-tasks críticas)*

### **Sprint Goal**
> Mapear pontos específicos de integração mencionados na crítica: funções exatas de validação, pipeline reports integration, e hooks precisos.

### **Definition of Done**
- [ ] Hook points específicos mapeados para cada função de validação
- [ ] Estratégia de integração com relatórios de pipeline definida
- [ ] Testes específicos para cada hook point
- [ ] Context flow validado para cada função

---

#### **Mapeamento Específico de Funções (6 tasks × 10-15min)**

**US001.5**: *Como desenvolvedor, quero hooks específicos para cada função de validação mencionada na crítica*

31. [ ] **Task 1.16**: Hook validate_columns specifically *(10min)*
    - **AC**: Hook específico na função validate_columns identificado
    - **Test**: Trigger warning dessa função e interceptar
    - **Deliverable**: Hook coordinates para validate_columns

32. [ ] **Task 1.17**: Hook check_required_columns specifically *(10min)*
    - **AC**: Hook específico na função check_required_columns
    - **Test**: Missing column scenario intercepted
    - **Deliverable**: Hook coordinates para check_required_columns

33. [ ] **Task 1.18**: Hook validate_taxonomy_consistency specifically *(10min)*
    - **AC**: Hook específico na função validate_taxonomy_consistency  
    - **Test**: Taxonomy inconsistency intercepted
    - **Deliverable**: Hook coordinates para validate_taxonomy_consistency

34. [ ] **Task 1.19**: Hook validate_no_blank_cells specifically *(10min)*
    - **AC**: Hook específico na função validate_no_blank_cells
    - **Test**: Blank cell warning intercepted
    - **Deliverable**: Hook coordinates para validate_no_blank_cells

35. [ ] **Task 1.20**: Hook validate_aggregates specifically *(10min)*
    - **AC**: Hook específico na função validate_aggregates
    - **Test**: Aggregate validation warning intercepted  
    - **Deliverable**: Hook coordinates para validate_aggregates

36. [ ] **Task 1.21**: Design pipeline report integration strategy *(15min)*
    - **AC**: Como modificar _last_taxo_report e _last_impressions_report com decisions
    - **Test**: Strategy para anexar warning decisions aos relatórios
    - **Deliverable**: Report integration pattern detalhado

---

## 🚀 **EPIC 2: MINIMAL WORKING PROTOTYPE** *(1 semana, 35 micro-tasks)*

### **Sprint Goal**
> Criar sistema básico funcionando que intercepta warnings e permite decisão do usuário, sem quebrar absolutamente nada do sistema atual.

### **Definition of Done**
- [ ] Hook funciona em pelo menos 1 tipo de warning
- [ ] Menu interativo básico implementado
- [ ] Decisões são persistidas em SQLite  
- [ ] Sistema atual funciona 100% normal se interatividade desabilitada
- [ ] Zero degradação de performance

---

#### **Sprint 2.1: Basic Hook (7 tasks × 5-10min)**

**US004**: *Como desenvolvedor, quero criar infraestrutura básica de interceptação*

31. [ ] **Task 4.1**: Create empty `warning_interceptor.py` file *(2min)*
    - **AC**: Arquivo criado na estrutura correta
    - **Test**: Import successful
    - **Deliverable**: File structure ready

32. [ ] **Task 4.2**: Add basic imports (logging, sys, typing) *(3min)*
    - **AC**: Imports funcionando sem errors
    - **Test**: Python can parse file without issues
    - **Deliverable**: Clean importable module

33. [ ] **Task 4.3**: Create `WarningInterceptor` empty class *(3min)*
    - **AC**: Class structure definida
    - **Test**: Can instantiate class
    - **Deliverable**: Basic OOP structure

34. [ ] **Task 4.4**: Add `__init__` method with basic params *(5min)*
    - **AC**: Constructor aceita parâmetros necessários
    - **Test**: Instantiation com different parameters
    - **Deliverable**: Configurable interceptor

35. [ ] **Task 4.5**: Add `intercept()` method that just prints *(7min)*
    - **AC**: Método aceita warning message e context
    - **Test**: Call method e verify print output
    - **Deliverable**: Basic interception capability

36. [ ] **Task 4.6**: Test import in main script *(5min)*
    - **AC**: Import funciona no contexto do ETL
    - **Test**: ETL ainda executa normalmente com import
    - **Deliverable**: Integration verified

37. [ ] **Task 4.7**: Test basic instantiation *(5min)*
    - **AC**: Pode criar interceptor instance no ETL
    - **Test**: ETL runs normally com interceptor unused
    - **Deliverable**: Ready for integration

---

#### **Sprint 2.2: First Hook Integration (7 tasks × 5-10min)**

**US005**: *Como usuário, quero que o sistema pause quando encontrar um warning específico*

38. [ ] **Task 5.1**: Find exact line in validations.py to hook *(8min)*
    - **AC**: Linha específica identificada para primeiro warning
    - **Test**: Confirmar que essa linha é executed quando esperado
    - **Deliverable**: Precise integration point

39. [ ] **Task 5.2**: Add if-statement before first log.warning() *(6min)*
    - **AC**: Conditional hook instalado
    - **Test**: Warning ainda funciona normalmente quando hook disabled
    - **Deliverable**: Non-breaking integration

40. [ ] **Task 5.3**: Call interceptor.intercept() with warning message *(7min)*
    - **AC**: Warning message passado para interceptor
    - **Test**: Print statement confirms interception
    - **Deliverable**: Working message flow

41. [ ] **Task 5.4**: Test with minimal case (trigger that specific warning) *(10min)*
    - **AC**: Warning triggere hook successfully
    - **Test**: Create data that triggers warning, verify hook called
    - **Deliverable**: End-to-end proof working

42. [ ] **Task 5.5**: Verify hook is called (print statement should show) *(5min)*
    - **AC**: Clear evidence que hook está funcionando
    - **Test**: Print output visible during ETL execution
    - **Deliverable**: Hook execution confirmed

43. [ ] **Task 5.6**: Test that original warning still works *(5min)*
    - **AC**: Warning functionality não quebrada
    - **Test**: Warning appears in logs como sempre
    - **Deliverable**: Backward compatibility confirmed

44. [ ] **Task 5.7**: Test that ETL continues normally after hook *(8min)*
    - **AC**: ETL completes successfully após hook
    - **Test**: Full ETL execution with hook active
    - **Deliverable**: System stability confirmed

---

#### **Sprint 2.3: Basic CLI Menu (7 tasks × 5-10min)**

**US006**: *Como usuário, quero menu simples para decidir o que fazer com warning*

45. [ ] **Task 6.1**: Add input() call in intercept() method *(5min)*
    - **AC**: Sistema pausa e aguarda input do usuário
    - **Test**: Warning pausa ETL e aguarda input
    - **Deliverable**: Interactive pause working

46. [ ] **Task 6.2**: Create simple menu: "(i)gnore, (s)how context, (q)uit" *(8min)*
    - **AC**: Menu claro e user-friendly
    - **Test**: Menu display correcto durante warning
    - **Deliverable**: Basic user interface

47. [ ] **Task 6.3**: Handle 'i' option (return and continue) *(5min)*
    - **AC**: 'i' permite continuar ETL normalmente
    - **Test**: ETL continues após escolher ignore
    - **Deliverable**: Ignore functionality working

48. [ ] **Task 6.4**: Handle 'q' option (sys.exit) *(3min)*
    - **AC**: 'q' termina ETL cleanly
    - **Test**: ETL exits gracefully quando escolher quit
    - **Deliverable**: Clean exit capability

49. [ ] **Task 6.5**: Test menu with real warning *(8min)*
    - **AC**: Menu funciona com warning real do sistema
    - **Test**: End-to-end com warning real, teste cada opção
    - **Deliverable**: Production-ready menu

50. [ ] **Task 6.6**: Add basic colors (red for warning, green for menu) *(10min)*
    - **AC**: Output colorido e readable
    - **Test**: Colors aparecem correctly no terminal
    - **Deliverable**: Enhanced user experience

51. [ ] **Task 6.7**: Test colored output in terminal *(5min)*
    - **AC**: Colors funcionam em ambiente real
    - **Test**: Execute em terminal real, verify colors
    - **Deliverable**: Terminal compatibility confirmed

---

#### **Sprint 2.4: Context Display (7 tasks × 5-10min)**

**US007**: *Como usuário, quero ver contexto completo do warning para tomar decisão informada*

52. [ ] **Task 7.1**: Pass DataFrame context to intercept() method *(8min)*
    - **AC**: Context data available no interceptor
    - **Test**: Context data correctly passed
    - **Deliverable**: Rich context availability

53. [ ] **Task 7.2**: Show warning message clearly *(5min)*
    - **AC**: Warning message bem formatado e visible
    - **Test**: Warning message readable no output
    - **Deliverable**: Clear warning display

54. [ ] **Task 7.3**: Show current row if available *(7min)*
    - **AC**: Row data displayed quando relevant
    - **Test**: Row context shows para row-specific warnings
    - **Deliverable**: Row-level context display

55. [ ] **Task 7.4**: Show sheet name context *(3min)*
    - **AC**: User sabe qual sheet gerou o warning
    - **Test**: Sheet name visible no warning context
    - **Deliverable**: Sheet context clarity

56. [ ] **Task 7.5**: Show column name if field-specific warning *(6min)*
    - **AC**: Field-specific warnings show column
    - **Test**: Column name appears para field warnings
    - **Deliverable**: Field-specific context

57. [ ] **Task 7.6**: Test context display with real warning *(10min)*
    - **AC**: Context display funciona com warning real
    - **Test**: Trigger warning, verify all context shows
    - **Deliverable**: Complete context system working

58. [ ] **Task 7.7**: Improve formatting (make it pretty) *(8min)*
    - **AC**: Output bem formatado e professional
    - **Test**: Visual assessment da output quality
    - **Deliverable**: Polished user interface

---

#### **Sprint 2.5: Decision Persistence (7 tasks × 5-10min)**

**US008**: *Como usuário, quero que minhas decisões sejam lembradas para próximas execuções*

59. [ ] **Task 8.1**: Create minimal SQLite schema (decisions table) *(8min)*
    - **AC**: Database schema definido e funcional
    - **Test**: Can create tables without errors
    - **Deliverable**: Working database schema

60. [ ] **Task 8.2**: Add SQLite connection in __init__ *(5min)*
    - **AC**: Database connection estabelecida
    - **Test**: Connection works, can query database
    - **Deliverable**: Database connectivity

61. [ ] **Task 8.3**: Create table on first run *(7min)*
    - **AC**: Tables created automaticamente se não existirem
    - **Test**: First run creates necessary tables
    - **Deliverable**: Auto-setup capability

62. [ ] **Task 8.4**: Save user decision to database *(10min)*
    - **AC**: Decision stored com warning context
    - **Test**: Decision appears in database após choice
    - **Deliverable**: Decision persistence working

63. [ ] **Task 8.5**: Test database write (check .db file created) *(5min)*
    - **AC**: Database file created e populated
    - **Test**: Verify .db file exists and has data
    - **Deliverable**: Physical persistence confirmed

64. [ ] **Task 8.6**: Query existing decisions on startup *(8min)*
    - **AC**: Previous decisions loaded na startup
    - **Test**: Restart ETL, verify decisions loaded
    - **Deliverable**: Decision retrieval working

65. [ ] **Task 8.7**: Test decision persistence across ETL runs *(10min)*
    - **AC**: Decisions persist entre execuções
    - **Test**: Make decision, restart ETL, verify persistence
    - **Deliverable**: Cross-session persistence confirmed

---

## 🎯 **EPIC 3: BI_PARAMETRIZAÇÃO INTEGRATION** *(1 semana, 28 micro-tasks)*

### **Sprint Goal**
> Integrar completamente com sistema de taxonomia, permitindo adição de novos valores ao BI_PARAMETRIZAÇÃO e criação de regras automáticas.

### **Definition of Done**
- [ ] Detecta valores missing em BI_PARAMETRIZAÇÃO
- [ ] Permite adicionar novos valores via interface
- [ ] Atualiza Google Sheets com novos valores
- [ ] Sistema de regras automáticas funcional
- [ ] Cache invalidation working

---

#### **Sprint 3.1: Detection (7 tasks × 5-10min)**

**US009**: *Como usuário, quero ser notificado quando encontrar valor não parametrizado com opção de adicionar*

66. [ ] **Task 9.1**: Identify BI_PARAMETRIZAÇÃO missing value warning *(6min)*
    - **AC**: Warning específico identificado no código
    - **Test**: Trigger warning com valor não parametrizado
    - **Deliverable**: Specific warning mapped

67. [ ] **Task 9.2**: Hook into that specific warning *(7min)*
    - **AC**: Hook instalado para missing taxonomy warning
    - **Test**: Warning triggers our interceptor
    - **Deliverable**: Taxonomy warning interception

68. [ ] **Task 9.3**: Show missing value clearly *(5min)*
    - **AC**: Missing value destacado no output
    - **Test**: Missing value clearly visible para usuário
    - **Deliverable**: Clear value identification

69. [ ] **Task 9.4**: Show suggested values (if any exist) *(8min)*
    - **AC**: Similar values suggested quando possível
    - **Test**: Fuzzy matching shows reasonable suggestions
    - **Deliverable**: Smart suggestions system

70. [ ] **Task 9.5**: Add menu option "(a)dd new value" *(5min)*
    - **AC**: Menu include option para add value
    - **Test**: Menu option appears e é selectable
    - **Deliverable**: Add value option available

71. [ ] **Task 9.6**: Test detection with real missing value *(10min)*
    - **AC**: System detecta missing value real
    - **Test**: Use real data com missing taxonomy
    - **Deliverable**: Real-world detection working

72. [ ] **Task 9.7**: Verify context is clear to user *(8min)*
    - **AC**: User entende exactly what value is missing
    - **Test**: Context information sufficient for decision
    - **Deliverable**: Clear user guidance

---

#### **Sprint 3.2: Value Addition (7 tasks × 5-10min)**

**US010**: *Como usuário, quero adicionar novos valores à taxonomia durante a execução do ETL*

73. [ ] **Task 10.1**: Collect new value from user input *(8min)*
    - **AC**: User pode input new taxonomy value
    - **Test**: Input collection works smoothly
    - **Deliverable**: Value input capability

74. [ ] **Task 10.2**: Validate input (not empty, reasonable length) *(6min)*
    - **AC**: Basic validation prevents bad inputs
    - **Test**: Empty e excessively long inputs rejected
    - **Deliverable**: Input validation working

75. [ ] **Task 10.3**: Add to local database first *(7min)*
    - **AC**: New value stored locally immediately
    - **Test**: Value appears in local database
    - **Deliverable**: Local storage working

76. [ ] **Task 10.4**: Test local storage (query to verify) *(5min)*
    - **AC**: Can query e retrieve new value
    - **Test**: Database query returns new value
    - **Deliverable**: Storage verification working

77. [ ] **Task 10.5**: Apply new value to current DataFrame *(8min)*
    - **AC**: DataFrame updated with new value
    - **Test**: Current processing uses new value
    - **Deliverable**: Immediate value application

78. [ ] **Task 10.6**: Test that warning doesn't reappear *(10min)*
    - **AC**: Same warning resolved for current run
    - **Test**: ETL continues without re-triggering warning
    - **Deliverable**: Warning resolution confirmed

79. [ ] **Task 10.7**: Continue ETL processing with new value *(8min)*
    - **AC**: ETL completes successfully com new value
    - **Test**: Full ETL run com newly added value
    - **Deliverable**: End-to-end success

---

#### **Sprint 3.3: Google Sheets Update (7 tasks × 5-10min)**

**US011**: *Como usuário, quero que novos valores sejam persistidos no Google Sheets para futuras execuções*

80. [ ] **Task 11.1**: Read BI_PARAMETRIZAÇÃO structure (understand columns) *(10min)*
    - **AC**: Column structure completamente understood
    - **Test**: Can identify correct columns for each taxonomy
    - **Deliverable**: Schema documentation

81. [ ] **Task 11.2**: Find correct column to add new value *(8min)*
    - **AC**: New value goes to correct column
    - **Test**: Column identification works para different types
    - **Deliverable**: Column mapping logic

82. [ ] **Task 11.3**: Add new row to BI_PARAMETRIZAÇÃO sheet *(10min)*
    - **AC**: New row added successfully
    - **Test**: Row appears in Google Sheets
    - **Deliverable**: Sheets update capability

83. [ ] **Task 11.4**: Test Google Sheets update (check sheet manually) *(8min)*
    - **AC**: Update visible no Google Sheets interface
    - **Test**: Manual verification da new row
    - **Deliverable**: Sheets integration confirmed

84. [ ] **Task 11.5**: Invalidate BIParamLookup cache after addition *(7min)*
    - **AC**: Cache cleared para pick up new value
    - **Test**: New value available immediately após addition
    - **Deliverable**: Cache management working

85. [ ] **Task 11.6**: Test that new value is picked up immediately *(10min)*
    - **AC**: New value usable na same ETL session
    - **Test**: Add value, continue ETL, value should work
    - **Deliverable**: Real-time availability

86. [ ] **Task 11.7**: Handle errors gracefully (connection issues, etc.) *(8min)*
    - **AC**: Error handling não quebra ETL
    - **Test**: Simulate connection failure, verify graceful handling
    - **Deliverable**: Robust error handling

---

#### **Sprint 3.4: Rule Creation (7 tasks × 5-10min)**

**US012**: *Como usuário, quero criar regras automáticas para resolver warnings similares no futuro*

87. [ ] **Task 12.1**: Add menu option "(r)ule: always replace X with Y" *(8min)*
    - **AC**: Rule creation option available
    - **Test**: Menu option appears e is selectable
    - **Deliverable**: Rule creation interface

88. [ ] **Task 12.2**: Collect rule pattern from user *(10min)*
    - **AC**: User can specify substitution rule
    - **Test**: Rule pattern collection works
    - **Deliverable**: Rule input capability

89. [ ] **Task 12.3**: Store rule in database (rule table) *(7min)*
    - **AC**: Rule persistida no database
    - **Test**: Rule appears in rules table
    - **Deliverable**: Rule persistence

90. [ ] **Task 12.4**: Apply rule to current DataFrame *(8min)*
    - **AC**: Rule applied immediately
    - **Test**: Current data transformed per rule
    - **Deliverable**: Immediate rule application

91. [ ] **Task 12.5**: Test rule application *(10min)*
    - **AC**: Rule works as expected
    - **Test**: Rule correctly transforms data
    - **Deliverable**: Rule functionality confirmed

92. [ ] **Task 12.6**: Auto-apply rules at startup *(8min)*
    - **AC**: Rules loaded e applied automatically
    - **Test**: ETL startup applies existing rules
    - **Deliverable**: Auto-rule application

93. [ ] **Task 12.7**: Test rule persistence across runs *(10min)*
    - **AC**: Rules work across multiple ETL executions
    - **Test**: Create rule, restart ETL, verify rule active
    - **Deliverable**: Cross-session rule persistence

---

## 🎯 **EPIC 2.5: TDAH TOOLING IMPLEMENTATION** *(2 dias, 8 micro-tasks especializadas)*

### **Sprint Goal**
> Implementar ferramentas CLI e sistemas de rastreabilidade mencionados na crítica: task timer, progress dashboard, commit tracking, achievement system.

### **Definition of Done**
- [ ] Task timer CLI funcional
- [ ] Progress bar generator implementado
- [ ] Sistema de associação task→commit
- [ ] Achievement tracking básico funcionando

---

#### **Ferramentas TDAH Específicas (8 tasks × 20-25min)**

**US002.5**: *Como desenvolvedor com TDAH, quero ferramentas CLI que suportem minha produtividade*

94. [ ] **Task 2.11**: Create task timer CLI tool *(25min)*
    - **AC**: CLI que time tasks e mostra progresso real vs estimado
    - **Test**: Timer funciona para uma task real
    - **Deliverable**: tdah_timer.py funcional

95. [ ] **Task 2.12**: Implement progress bar generator *(20min)*
    - **AC**: Gerador de barras de progresso visual para épicos
    - **Test**: Progress bar atualiza conforme task completion
    - **Deliverable**: progress_visualizer.py

96. [ ] **Task 2.13**: Create commit-task association script *(25min)*
    - **AC**: Script que associa commits com tasks específicas
    - **Test**: Commit message indica task ID automaticamente
    - **Deliverable**: task_commit_tracker.py

97. [ ] **Task 2.14**: Design achievement tracking system *(20min)*
    - **AC**: Sistema básico de achievements para milestones
    - **Test**: Achievement unlocked após epic completion
    - **Deliverable**: achievements.py básico

98. [ ] **Task 2.15**: Implement focus session manager *(22min)*
    - **AC**: Manager para sessões de foco com breaks automáticos
    - **Test**: Focus session com timer e break reminders
    - **Deliverable**: focus_manager.py

99. [ ] **Task 2.16**: Create daily summary generator *(18min)*
    - **AC**: Gerador de summaries diários de progresso
    - **Test**: Summary mostra tasks completed, time spent, achievements
    - **Deliverable**: daily_summary.py

100. [ ] **Task 2.17**: Test all TDAH tools integration *(20min)*
     - **AC**: Todas as ferramentas funcionam em conjunto
     - **Test**: Full workflow com todas as ferramentas
     - **Deliverable**: TDAH toolchain integrada

101. [ ] **Task 2.18**: Create TDAH tools documentation *(15min)*
     - **AC**: Documentação de como usar cada ferramenta
     - **Test**: Seguir documentação para usar tools
     - **Deliverable**: TDAH_TOOLS.md

---

## 🎯 **EPIC 3.5: CACHE MANAGEMENT SPECIFICS** *(1.5 dias, 6 micro-tasks críticas)*

### **Sprint Goal**  
> Implementar invalidação específica de caches conforme crítica: métodos específicos por cache, coordenação multi-classe, testes de cadeia.

### **Definition of Done**
- [ ] BIParamLookup.invalidate() implementado
- [ ] SheetsFetcher.clear_cache() implementado
- [ ] Coordenação multi-classe de caches funcional
- [ ] Testes de invalidação em cadeia passando

---

#### **Cache Invalidation Específica (6 tasks × 15-20min)**

**US003.5**: *Como sistema, quero invalidação específica de caches quando dados mudam*

102. [ ] **Task 3.15**: Implement BIParamLookup.invalidate() method *(18min)*
     - **AC**: Método para forçar reload de BI parametrização
     - **Test**: Cache invalidation força reload de dados
     - **Deliverable**: BIParamLookup com método invalidate

103. [ ] **Task 3.16**: Implement SheetsFetcher.clear_cache() method *(15min)*
     - **AC**: Método para limpar cache de worksheets específicos
     - **Test**: Cache clearing funciona para sheets específicos
     - **Deliverable**: SheetsFetcher com método clear_cache

104. [ ] **Task 3.17**: Design multi-class cache coordination *(20min)*
     - **AC**: Coordenação entre diferentes tipos de cache
     - **Test**: Mudança em um cache invalida caches dependentes
     - **Deliverable**: Cache coordination strategy

105. [ ] **Task 3.18**: Test cache invalidation chain *(15min)*
     - **AC**: Invalidação de um cache propaga para outros necessários
     - **Test**: BI change → multiple cache invalidation
     - **Deliverable**: Working cache invalidation chain

106. [ ] **Task 3.19**: Implement cache statistics tracking *(17min)*
     - **AC**: Tracking de hit rate e invalidation frequency
     - **Test**: Statistics mostram cache effectiveness
     - **Deliverable**: Cache statistics module

107. [ ] **Task 3.20**: Create cache management CLI *(20min)*
     - **AC**: CLI para manual cache management e debugging
     - **Test**: Cache status, invalidation, e statistics via CLI
     - **Deliverable**: cache_manager.py CLI tool

---

## 🎯 **EPIC 4.5: COMPATIBILITY TEST SUITE** *(1 dia, 6 micro-tasks essenciais)*

### **Sprint Goal**
> Criar suite sistemática de testes de compatibilidade conforme crítica: testes automatizados, cenários de graceful degradation, compatibility matrix.

### **Definition of Done**
- [ ] Testes de compatibilidade com todos os sistemas existentes
- [ ] Cenários de graceful degradation validados
- [ ] Test runner automatizado funcionando
- [ ] Compatibility matrix completa e validada

---

#### **Testes Sistemáticos de Compatibilidade (6 tasks × 15-25min)**

**US004.5**: *Como sistema, quero garantir compatibilidade total com sistemas existentes*

108. [ ] **Task 4.11**: Test compatibility with warning_suppressor.py *(15min)*
     - **AC**: Warning interceptor funciona com/sem suppressor ativo
     - **Test**: Diferentes cenários de supressão
     - **Deliverable**: Compatibility test para suppressor

109. [ ] **Task 4.12**: Test compatibility with production_mode.py *(15min)*
     - **AC**: Sistema funciona corretamente em todos os modos
     - **Test**: Normal, quiet, minimal modes com interactividade
     - **Deliverable**: Production mode compatibility tests

110. [ ] **Task 4.13**: Test compatibility with smart_reload.py *(12min)*
     - **AC**: Hot reload não interfere com warning system
     - **Test**: Reload durante warning interception
     - **Deliverable**: Smart reload compatibility test

111. [ ] **Task 4.14**: Test performance preservation (2 API calls) *(20min)*
     - **AC**: Sistema mantém exatamente 2 API calls
     - **Test**: PerformanceGuard detecta violations
     - **Deliverable**: Performance preservation test

112. [ ] **Task 4.15**: Test graceful degradation scenarios *(18min)*
     - **AC**: Sistema funciona normalmente quando interactividade falha
     - **Test**: Database down, timeout, user abort scenarios
     - **Deliverable**: Graceful degradation test suite

113. [ ] **Task 4.16**: Create automated compatibility test runner *(25min)*
     - **AC**: Script que executa toda suite de compatibility tests
     - **Test**: Test runner executa todos os testes e reporta resultados
     - **Deliverable**: compatibility_test_runner.py

---

## 🎯 **EPIC 3.6: MISSING CACHES INTEGRATION** *(1 dia, 4 micro-tasks críticas)*

### **Sprint Goal**
> Integrar caches específicos omitidos na auditoria (_HEADERS, _EXISTING_IDS) e mapear write-back passo 13 conforme crítica linha 91 e 108-109.

### **Definition of Done**
- [ ] _HEADERS e _EXISTING_IDS incluídos na cache invalidation strategy
- [ ] Write-back passo 13 integration point mapeado e implementado
- [ ] Cache coordination expandida para todos os caches identificados
- [ ] Decision application timing antes do consolidated write-back definido

---

#### **Integração de Caches Omitidos (4 tasks × 15-20min)**

**US003.6**: *Como sistema, quero invalidação completa de todos os caches identificados na crítica*

114. [ ] **Task 3.21**: Map _HEADERS and _EXISTING_IDS caches *(18min)*
     - **AC**: Localizar e documentar usage de _HEADERS e _EXISTING_IDS no codebase
     - **Test**: Grep search para identificar onde esses caches são usados
     - **Deliverable**: Cache usage map completo incluindo todos os 4 caches

115. [ ] **Task 3.22**: Extend cache invalidation to _HEADERS and _EXISTING_IDS *(20min)*
     - **AC**: Invalidation strategy expandida para incluir todos os caches
     - **Test**: Cache invalidation chain funciona para os 4 caches
     - **Deliverable**: Complete cache invalidation interface

116. [ ] **Task 3.23**: Map pipeline step 13 write-back integration point *(15min)*
     - **AC**: Identificar exatamente onde é o "passo 13" mencionado na crítica
     - **Test**: Localizar step 13 no TreatPipeline.run() method
     - **Deliverable**: Step 13 integration coordinates (file:line:function)

117. [ ] **Task 3.24**: Design decision application before consolidated write-back *(17min)*
     - **AC**: Strategy para aplicar decisions ANTES do write-back consolidado
     - **Test**: Decision timing não interfere com consolidated write-back
     - **Deliverable**: Decision application timing specification

---

## 🎯 **EPIC 2.6: DATA MIGRATION & ISSUES INTEGRATION** *(1.5 dias, 6 micro-tasks importantes)*

### **Sprint Goal**
> Implementar migração municipios.csv, GitHub issues integration, e rules management UI específica conforme linhas 125 e 135 da crítica.

### **Definition of Done**
- [ ] municipios.csv migrado para SQLite geografia table
- [ ] GitHub issues integration para task tracking implementada
- [ ] Rules management UI específica funcional
- [ ] Data migration strategy documentada

---

#### **Migração e Issues Integration (6 tasks × 15-25min)**

**US002.6**: *Como desenvolvedor, quero ferramentas completas para tracking e data management*

118. [ ] **Task 2.19**: Implement municipios.csv to SQLite migration *(25min)*
     - **AC**: Script para migrar municipios.csv → geografia table
     - **Test**: Migration script executa sem erros e dados estão corretos
     - **Deliverable**: migrate_municipios.py functional

119. [ ] **Task 2.20**: Design GitHub issues integration for task tracking *(20min)*
     - **AC**: Strategy para associar tasks com GitHub issues
     - **Test**: Task ID pode ser linkado com issue number
     - **Deliverable**: Issues integration specification

120. [ ] **Task 2.21**: Implement GitHub issues API integration *(25min)*
     - **AC**: Script para criar/atualizar issues baseado em task progress
     - **Test**: Issue creation e update funcionam via GitHub API
     - **Deliverable**: github_issues_tracker.py

121. [ ] **Task 2.22**: Design rules management UI interface *(18min)*
     - **AC**: UI específica para view, edit, delete warning rules
     - **Test**: Interface design permite all CRUD operations on rules
     - **Deliverable**: Rules management UI specification

122. [ ] **Task 2.23**: Implement rules management CLI commands *(22min)*
     - **AC**: CLI commands para manage rules: list, create, edit, delete
     - **Test**: All rules management operations funcionam via CLI
     - **Deliverable**: rules_manager.py CLI tool

123. [ ] **Task 2.24**: Test complete data migration and tracking *(15min)*
     - **AC**: Migration + issues tracking working end-to-end
     - **Test**: Full workflow: migrate data → track progress → manage rules
     - **Deliverable**: Integrated data management system

---

## 🎯 **EPIC 0.6: MISSING MODULES IMPLEMENTATION** *(1 dia, 6 micro-tasks médias)*

### **Sprint Goal**
> Implementar módulos assumidos se ausentes e validar exemplos de código contra APIs reais conforme linhas 82 e 141 da crítica.

### **Definition of Done**
- [ ] early_exit_checker.py e schema_validator.py criados se ausentes
- [ ] Exemplos de código validados contra APIs reais
- [ ] Decision timing vs write-back sequence validada
- [ ] Module implementation strategy documentada

---

#### **Módulos Ausentes e Validação (6 tasks × 12-20min)**

**US000.6**: *Como desenvolvedor, quero todos os módulos assumidos implementados ou validados*

124. [ ] **Task 0.21**: Implement early_exit_checker.py if missing *(20min)*
     - **AC**: Módulo early_exit_checker criado se não existir no codebase
     - **Test**: early_exit_checker funciona como esperado nas tasks
     - **Deliverable**: early_exit_checker.py implementation ou validation

125. [ ] **Task 0.22**: Implement schema_validator.py if missing *(20min)*
     - **AC**: Módulo schema_validator criado se não existir no codebase  
     - **Test**: schema_validator funciona como esperado nas tasks
     - **Deliverable**: schema_validator.py implementation ou validation

126. [ ] **Task 0.23**: Validate code examples against real APIs *(18min)*
     - **AC**: Todos os exemplos em ARQUITETURA_WARNINGS validados contra código real
     - **Test**: Code examples executam sem erros com APIs atuais
     - **Deliverable**: Validated code examples in documentation

127. [ ] **Task 0.24**: Test decision application timing *(15min)*
     - **AC**: Confirmar que decisions são aplicadas antes do consolidated write-back
     - **Test**: Decision → data change → write-back sequence funciona
     - **Deliverable**: Decision timing validation

128. [ ] **Task 0.25**: Design module discovery and creation strategy *(12min)*
     - **AC**: Strategy para descobrir módulos ausentes e criá-los
     - **Test**: Module discovery detecta missing modules correctly
     - **Deliverable**: Module discovery strategy specification

129. [ ] **Task 0.26**: Document missing modules implementation approach *(10min)*
     - **AC**: Documentação de como handle missing assumed modules
     - **Test**: Documentation covers all missing module scenarios
     - **Deliverable**: Missing modules implementation guide

---

## 🎯 **EPIC 4.6: LOGGING INTEGRATION LAYER** *(30min, 2 micro-tasks baixa prioridade)*

### **Sprint Goal**
> Implementar integration layer completa com sistema de logging existente conforme linhas 15-16 da crítica.

### **Definition of Done**
- [ ] Integration layer com log.warning() existente funcional
- [ ] Fallback behavior documentado e testado
- [ ] Logging compatibility mantida 100%

---

#### **Sistema de Logging Integration (2 tasks × 15min)**

**US004.6**: *Como sistema, quero integration perfeita com logging existente*

130. [ ] **Task 4.17**: Design logging integration layer *(15min)*
     - **AC**: Layer que integra warning interceptor com log.warning() existente
     - **Test**: Logging funciona com/sem interceptor ativo
     - **Deliverable**: Logging integration specification

131. [ ] **Task 4.18**: Test complete logging compatibility *(15min)*
     - **AC**: Sistema de logging existente funciona 100% inalterado
     - **Test**: All existing log.warning() calls still work normally
     - **Deliverable**: Logging compatibility validation

---

## 📊 **Progress Tracking System**

### **Daily Progress Template**
```bash
📅 **Dia X - Epic Y**
🎯 Goal: [Sprint goal for the day]
📋 Tasks: [X/Y completed]
⏱️  Time: [Total time spent]
✅ Completed: [List of completed tasks]
🔄 In Progress: [Current task]
❌ Blocked: [Any blockers]
🎉 Wins: [Small celebrations]
📝 Notes: [Important observations]
```

### **Epic Progress Visual**
```bash
🎯 SISTEMA INTERATIVO DE WARNINGS      [████████░░] 80% (74/93 tasks)

├─ EPIC 1: Discovery & Compatibility   [██████████] 100% ✅ (30/30)
├─ EPIC 2: Working Prototype          [████████░░] 85% (30/35)
└─ EPIC 3: BI Integration             [██░░░░░░░░] 25% (7/28)

Current Sprint: Epic 2 - Working Prototype
Current Task: [Task 52/93] Pass DataFrame context to intercept() method
Estimated Completion: Tomorrow 15:00
```

### **Micro-Task Status Levels**
- ⏳ **Pending**: Not started
- 🔄 **In Progress**: Currently working  
- ✅ **Completed**: Done and tested
- 🚫 **Blocked**: Waiting on external dependency
- 🔍 **Review**: Needs validation
- 🎉 **Deployed**: Live and working

### **TDAH Support Metrics**
- **Focus Duration**: Track actual vs estimated time per task
- **Context Switches**: Minimize to < 1 per hour
- **Dopamine Hits**: ✅ checkboxes completed per day
- **Energy Levels**: Track energy at start/end of each task
- **Break Frequency**: Ensure adequate breaks between tasks

---

## 🎯 **Definition of Ready (DoR)**

Para cada task estar pronta para implementação:
- [ ] **Clear Acceptance Criteria**: O que constitui "done"
- [ ] **Test Strategy**: Como verificar que funciona
- [ ] **Rollback Plan**: Como desfazer se necessário
- [ ] **Time Box**: Estimativa realística (5-10 min)
- [ ] **Dependencies**: Pré-requisitos identificados
- [ ] **Context**: Informações necessárias disponíveis

## 🎯 **Definition of Done (DoD)**

Para cada task ser considerada completa:
- [ ] **Functionality Working**: Feature implementada e funcional
- [ ] **Test Passed**: Test case executa successfully
- [ ] **No Regressions**: Sistema existente ainda funciona
- [ ] **Documentation**: Mudança documentada se necessário
- [ ] **Deliverable Created**: Output específico da task produzido
- [ ] **Next Task Ready**: Contexto preparado para próxima task

---

**Documento criado**: 2025-01-09  
**Sprint Master**: Claude  
**Product Owner**: David  
**Total Estimated Time**: ~12-15 horas (93 tasks × 5-10min each)  
**Timeline**: 3 semanas (discovery + 2 sprints de desenvolvimento)

*📋 Related docs: `PRE_PROJETO_WARNINGS.md`, `ARQUITETURA_WARNINGS.md`, `TDAH_OPTIMIZATION.md`*