# 🌿 Branch Strategy - ETL Debrito Project

## 📋 Overview
Estratégia de branching otimizada para desenvolvimento TDD com épicos organizados e integração contínua.

## 🎯 Branch Structure

### **Main Branches**
- **`main`** - Branch de produção estável
- **`refactor`** - Branch principal de desenvolvimento (current)

### **Feature Branches**
Cada épico tem sua própria feature branch:

```
feature/epic-0    - Environment & Production Safety
feature/epic-0.5  - Integration Architecture Fixes  
feature/epic-2    - Discovery & Compatibility
feature/epic-3    - Interactive Warning Resolution System
feature/epic-4    - TDAH Tooling Implementation
feature/epic-5    - Cache Management Specifics
feature/epic-6    - Data Migration & Issues Integration
feature/epic-7    - Missing Caches Integration
feature/epic-8    - Task Time Monitoring & Analytics
```

## 🔄 Workflow TDD

### **Development Flow**
1. **Checkout** feature branch do épico correspondente
2. **RED Phase**: Criar tests que falham
3. **GREEN Phase**: Implementar código mínimo
4. **REFACTOR Phase**: Otimizar mantendo tests verdes
5. **PR** para `refactor` branch com review

### **Merge Strategy**
- **Squash commits** ao fazer merge para manter história limpa
- **Delete branch** após merge bem-sucedido
- **Automatic Gantt update** triggado por merges

## 📝 Naming Conventions

### **Commit Messages**
```bash
[EPIC-{id}] {type}: {description}

# Exemplos:
[EPIC-0] feat: Add environment configuration loader
[EPIC-4] test: Add timer component RED tests  
[EPIC-7] refactor: Optimize cache write-back logic
```

### **PR Titles**
```bash
[Epic {id}] {Epic Name} - {Phase}

# Exemplos:  
[Epic 0] Environment & Production Safety - Complete
[Epic 4] TDAH Tooling Implementation - Phase 1
```

## 🛡️ Branch Protection

### **Protected Branches**
- **`main`**: Require PR review + status checks
- **`refactor`**: Require status checks + up-to-date

### **Required Status Checks**  
- ✅ `pytest` - All tests passing
- ✅ `coverage` - Coverage ≥ 90%
- ✅ `gantt-update` - Diagrams updated
- ✅ `formatting` - Code formatting check

## 🚀 Release Strategy

### **Version Tagging**
- **Epic completion** → Minor version (v1.1.0, v1.2.0)
- **Hotfixes** → Patch version (v1.1.1, v1.1.2)
- **Major milestones** → Major version (v2.0.0)

### **Release Notes**
Auto-generated baseado nos épicos completados e issues fechadas.

## 📊 Integration with GitHub

### **Automatic Updates**
- **Issue close** → Epic progress updated
- **PR merge** → Gantt charts refreshed
- **Branch create** → Auto-link to Epic Issue
- **Milestone complete** → Release notes generated

## 🎯 Epic Development Guidelines

### **Before Starting Epic**
1. Checkout ou criar feature branch
2. Review Epic Issue e tasks
3. Setup desenvolvimento local
4. Verificar dependencies

### **During Development** 
1. Commit frequente com mensagens claras
2. Push regular para backup
3. Update Issue com progresso
4. Run tests localmente

### **Before PR**
1. Squash commits relacionados
2. Update documentação
3. Run full test suite
4. Update Epic Issue status

---

**🤖 Este documento é mantido automaticamente pelo sistema de automação GitHub.**