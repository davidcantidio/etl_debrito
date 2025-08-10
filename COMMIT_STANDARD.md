# 📋 ETL Debrito - Padrão de Commits Obrigatório

## 🎯 **Formato Rigoroso**

### **Estrutura Obrigatória:**
```
[EPIC-{id}] {type}: {description}

{optional body}

Task: {task_id} | Time: {minutes}min | Status: {red|green|refactor}
```

## ✅ **Types Permitidos**

| Type | Descrição | Quando Usar |
|------|-----------|-------------|
| `feat` | Nova funcionalidade | Implementar nova feature/capability |
| `fix` | Correção de bug | Resolver problema/erro identificado |
| `test` | Testes | Adicionar/modificar testes (TDD red phase) |
| `refactor` | Refatoração | Melhorar código sem mudar funcionalidade |
| `docs` | Documentação | Atualizar README, comentários, docs |
| `milestone` | Marco importante | Épico completo ou deliverable importante |
| `wip` | Work in progress | **Evitar** - usar apenas se necessário |

## 📝 **Exemplos Práticos**

### **Desenvolvimento Normal:**
```bash
[EPIC-3] feat: implement warning interceptor core logic

Added basic warning capture mechanism with filtering by module
and severity level. Supports both manual and automatic suppression.

Task: 3.1b.2 | Time: 25min | Status: green
```

### **Correção de Bug:**
```bash
[EPIC-0] fix: resolve production mode logging configuration

Fixed issue where production mode was not properly suppressing
debug messages, causing performance degradation in prod env.

Task: 0.2 | Time: 15min | Status: refactor
```

### **Fase de Testes (TDD Red):**
```bash
[EPIC-8] test: add task timer accuracy calculation tests

Created comprehensive test suite for accuracy metrics calculation
including edge cases for very short and very long tasks.

Task: 8.3b.1 | Time: 8min | Status: red
```

### **Milestone Importante:**
```bash
[EPIC-3] milestone: Interactive Warning System MVP complete

All core functionality implemented and tested:
- Warning capture and filtering ✅
- Interactive decision prompts ✅  
- SQLite persistence layer ✅
- Rules engine for auto-resolution ✅

Tasks: 3.1a, 3.1b.1, 3.1b.2, 3.1b.3, 3.2a, 3.2b.1, 3.2b.2 | Total: 68min
```

## 🎮 **Tracking & Analytics**

### **Como o Sistema Funciona:**

1. **Commit Pattern Detection**: Sistema busca `[EPIC-X]` nos commits
2. **Task Information Parsing**: Extrai tempo gasto e status TDD
3. **Progress Calculation**: Compara com baseline do `gantt_schedule.mmd`
4. **Visual Dashboard**: Gera Gantt rico com métricas de performance

### **Dados Coletados Automaticamente:**

- ⏱️ **Tempo real** gasto por task
- 🎯 **Accuracy** (tempo real vs estimado)
- 🧪 **TDD Cycles** (red → green → refactor)
- 📊 **Velocity** por épico
- 🏆 **Performance** metrics

## 🚫 **Anti-Patterns - NÃO FAZER**

### ❌ **Commits Inválidos:**
```bash
# Muito vago
git commit -m "fix stuff"

# Sem épico
git commit -m "feat: add new feature"

# Formato errado
git commit -m "EPIC-3: implemented warning system"

# Sem task info quando necessário
[EPIC-3] feat: major feature implementation
# (Missing: Task: X | Time: Ymin | Status: Z)
```

### ✅ **Versões Corretas:**
```bash
[EPIC-3] feat: implement core warning capture mechanism

Task: 3.1b.2 | Time: 25min | Status: green

[EPIC-3] refactor: optimize warning filtering performance

Task: 3.1b.3 | Time: 12min | Status: refactor
```

## 🛠️ **Ferramentas de Apoio**

### **1. Helper CLI:**
```bash
python commit_helper.py
# Guided interactive commit creation
```

### **2. Git Hook Validation:**
```bash
# Automatically installed in .git/hooks/commit-msg
# Rejects commits that don't follow standard
```

### **3. Progress Tracking:**
```bash
python gantt_tracker.py
# Generates rich dashboard from commits
```

## 📊 **Dashboard Generated**

O sistema gera automaticamente:

### **Visual Elements:**
- 📊 **Dual-bar Gantt**: Planned vs Actual progress
- 🎯 **Accuracy Metrics**: Time estimation performance
- 🧪 **TDD Cycle Tracking**: Red-Green-Refactor progress
- 📈 **Performance Analytics**: Velocity and efficiency

### **Metrics Calculated:**
- **Time Accuracy**: `actual_time / estimated_time` 
- **Completion Rate**: `completed_tasks / total_tasks`
- **TDD Health**: Balanced red/green/refactor cycles
- **Velocity**: Tasks per day/week trends

## 🎯 **Enforcement**

### **Mandatory for:**
- ✅ All development commits
- ✅ Bug fixes
- ✅ Feature implementations
- ✅ Milestone deliverables

### **Optional for:**
- 📝 Documentation-only changes (can omit task info)
- 🔧 Tool/config updates (can omit task info)
- 🚀 Deployment/release commits

### **Git Hook Enforcement:**
- Commits are **automatically rejected** if they don't follow the pattern
- No exceptions - standard must be followed rigorously
- Use `commit_helper.py` if unsure about format

## 🎉 **Benefits**

### **For Developers:**
- 🎮 **Gamified Progress**: See real metrics and achievements
- 📊 **Performance Insights**: Learn about estimation accuracy
- 🎯 **Clear Goals**: Explicit task tracking and progress

### **For Project Management:**
- 📈 **Real Metrics**: Actual time vs planned
- 🎯 **Accurate Planning**: Data-driven estimates
- 📊 **Progress Visibility**: Always-current dashboard

### **For Team:**
- 🤝 **Consistency**: Everyone follows same standard
- 📚 **Knowledge Transfer**: Clear commit history
- 🔍 **Easy Debugging**: Find exactly when/what changed

---

## 🚀 **Getting Started**

1. **Install commit helper:**
   ```bash
   python commit_helper.py  # First run sets up everything
   ```

2. **Make your first standardized commit:**
   ```bash
   [EPIC-X] feat: your amazing feature
   
   Task: X.Ya | Time: 15min | Status: green
   ```

3. **See the results:**
   ```bash
   python gantt_tracker.py  # Opens rich dashboard
   ```

4. **Keep improving:**
   - Monitor your accuracy metrics
   - Improve time estimation skills
   - Follow TDD cycles consistently
   - Celebrate milestones! 🎉

---

**Remember**: Consistency is key! 🔑  
Follow the standard rigorously for accurate tracking and beautiful analytics.