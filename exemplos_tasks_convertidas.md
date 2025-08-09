# Exemplos de Tasks Convertidas para TDD

## 📋 Comparação: Antes vs Depois

### Task Original (Não-TDD)
```json
{
  "id": "0.4",
  "title": "Implementar PerformanceGuard básico",
  "story_points": 20,
  "estimate_minutes": 60,
  "description": "Criar classe PerformanceGuard para limitar e auditar chamadas externas",
  "deliverables": [
    "transform/warnings/performance.py",
    "tests/test_performance_guard.py"
  ],
  "test_plan": [
    "Teste unitário com max_calls=2 e 3 chamadas",
    "Verificar trace das operações no call_log"
  ]
}
```

### Tasks Convertidas (TDD)
```json
[
  {
    "id": "0.4.1",
    "title": "TEST: should_raise_exception_when_max_calls_exceeded",
    "tdd_phase": "red",
    "estimate_minutes": 5,
    "description": "Escrever teste que verifica exceção ao exceder limite",
    "test_specs": ["should_raise_MaxCallsExceededException_when_limit_exceeded"],
    "deliverables": ["tests/test_performance_guard.py::test_should_raise_exception_when_max_calls_exceeded"]
  },
  {
    "id": "0.4.2", 
    "title": "IMPL: basic_call_counter_with_exception",
    "tdd_phase": "green",
    "estimate_minutes": 8,
    "description": "Implementar contador básico que lança exceção",
    "deliverables": [
      "tests/test_performance_guard.py::test_should_raise_exception_when_max_calls_exceeded",
      "transform/warnings/performance.py"
    ]
  },
  {
    "id": "0.4.3",
    "title": "TEST: should_increment_counter_on_each_call",
    "tdd_phase": "red", 
    "estimate_minutes": 5,
    "test_specs": ["should_increment_counter_when_before_api_call_invoked"]
  },
  {
    "id": "0.4.4",
    "title": "IMPL: call_counter_increment_logic",
    "tdd_phase": "green",
    "estimate_minutes": 8
  },
  {
    "id": "0.4.5",
    "title": "TEST: should_log_all_operations_in_call_trace",
    "tdd_phase": "red",
    "estimate_minutes": 5,
    "test_specs": ["should_record_operation_name_and_timestamp"]
  },
  {
    "id": "0.4.6",
    "title": "IMPL: operation_logging_functionality", 
    "tdd_phase": "green",
    "estimate_minutes": 8
  },
  {
    "id": "0.4.7",
    "title": "REFACTOR: extract_configuration_class",
    "tdd_phase": "refactor",
    "estimate_minutes": 10,
    "description": "Extrair configuração e melhorar estrutura"
  }
]
```

## 🔧 Implementação Prática - Exemplo Real

### RED: Primeiro Teste Falhando
```python
# tests/test_performance_guard.py
import pytest
from transform.warnings.performance import PerformanceGuard

def test_should_raise_exception_when_max_calls_exceeded():
    """
    GIVEN um PerformanceGuard com limite de 2 chamadas
    WHEN fizer 3 chamadas consecutivas
    THEN deve lançar MaxCallsExceededException
    """
    # Arrange
    guard = PerformanceGuard(max_calls=2)
    
    # Act & Assert
    guard.before_api_call("first_call")
    guard.before_api_call("second_call")
    
    with pytest.raises(MaxCallsExceededException) as exc_info:
        guard.before_api_call("third_call")
    
    assert "exceeded maximum of 2 calls" in str(exc_info.value)
```

**Resultado**: `ModuleNotFoundError: No module named 'transform.warnings.performance'`
✅ **Teste falha pelo motivo correto**

### GREEN: Implementação Mínima
```python
# transform/warnings/performance.py
class MaxCallsExceededException(Exception):
    """Exceção lançada quando limite de chamadas é excedido."""
    pass

class PerformanceGuard:
    def __init__(self, max_calls: int):
        self.max_calls = max_calls
        self.call_count = 0
    
    def before_api_call(self, operation: str):
        if self.call_count >= self.max_calls:
            raise MaxCallsExceededException(f"exceeded maximum of {self.max_calls} calls")
        self.call_count += 1
```

**Resultado**: Teste passa ✅

### Próximo Ciclo RED: Teste de Logging
```python
def test_should_log_all_operations_in_call_trace():
    """
    GIVEN um PerformanceGuard
    WHEN fizer chamadas com diferentes operações
    THEN deve registrar todas no call_log
    """
    # Arrange
    guard = PerformanceGuard(max_calls=3)
    
    # Act
    guard.before_api_call("batchGet")
    guard.before_api_call("batchUpdate")
    
    # Assert
    assert len(guard.call_log) == 2
    assert guard.call_log[0]["operation"] == "batchGet"
    assert guard.call_log[1]["operation"] == "batchUpdate"
    assert all("timestamp" in entry for entry in guard.call_log)
```

### GREEN: Implementação do Logging
```python
import time
from typing import List, Dict

class PerformanceGuard:
    def __init__(self, max_calls: int):
        self.max_calls = max_calls
        self.call_count = 0
        self.call_log: List[Dict] = []
    
    def before_api_call(self, operation: str):
        if self.call_count >= self.max_calls:
            raise MaxCallsExceededException(f"exceeded maximum of {self.max_calls} calls")
        
        # Log da operação
        self.call_log.append({
            "operation": operation,
            "timestamp": time.time()
        })
        
        self.call_count += 1
```

### REFACTOR: Melhorar Design
```python
from dataclasses import dataclass
from datetime import datetime
from typing import List

@dataclass
class ApiCallRecord:
    """Registro de uma chamada de API."""
    operation: str
    timestamp: datetime
    
class PerformanceGuard:
    """Guard para monitorar e limitar chamadas de API."""
    
    def __init__(self, max_calls: int):
        self._validate_max_calls(max_calls)
        self.max_calls = max_calls
        self.call_count = 0
        self.call_log: List[ApiCallRecord] = []
    
    def before_api_call(self, operation: str) -> None:
        """Registra chamada e verifica limite."""
        self._check_call_limit()
        self._record_call(operation)
        self.call_count += 1
    
    def _validate_max_calls(self, max_calls: int) -> None:
        if max_calls <= 0:
            raise ValueError("max_calls deve ser positivo")
    
    def _check_call_limit(self) -> None:
        if self.call_count >= self.max_calls:
            raise MaxCallsExceededException(
                f"exceeded maximum of {self.max_calls} calls"
            )
    
    def _record_call(self, operation: str) -> None:
        record = ApiCallRecord(
            operation=operation,
            timestamp=datetime.now()
        )
        self.call_log.append(record)
```

## 📊 Benefícios da Conversão

### Antes (1 task grande)
- **Duração**: 60 minutos
- **Risco**: Alto (muitos pontos de falha)  
- **Feedback**: Só no final
- **Debugging**: Difícil localizar problemas

### Depois (7 micro-tasks)
- **Duração**: 7 × 5-10 min = 35-70 min total
- **Risco**: Baixo (isolado por comportamento)
- **Feedback**: A cada 5-8 minutos
- **Debugging**: Fácil (teste específico falha)

## 🎯 Exemplo Complexo: Integração com SheetsFetcher

### Task Original
```json
{
  "title": "Integrar PerformanceGuard ao SheetsFetcher",
  "estimate_minutes": 45,
  "description": "Envolver chamadas batchGet/batchUpdate com PerformanceGuard"
}
```

### Conversão TDD Detalhada

#### Ciclo 1: Integration Setup
```python
# RED
def test_should_inject_performance_guard_into_sheets_fetcher():
    guard = PerformanceGuard(max_calls=2)
    fetcher = SheetsFetcher(credentials, performance_guard=guard)
    assert fetcher.performance_guard is guard

# GREEN  
class SheetsFetcher:
    def __init__(self, credentials, performance_guard=None):
        self.credentials = credentials
        self.performance_guard = performance_guard or PerformanceGuard(max_calls=float('inf'))
```

#### Ciclo 2: batchGet Integration
```python
# RED
def test_should_call_performance_guard_before_batchget():
    guard = Mock(spec=PerformanceGuard)
    fetcher = SheetsFetcher(mock_creds, performance_guard=guard)
    
    fetcher.batch_get_data(['sheet1', 'sheet2'])
    
    guard.before_api_call.assert_called_once_with("batchGet")

# GREEN
def batch_get_data(self, sheet_names):
    self.performance_guard.before_api_call("batchGet")
    # ... resto da implementação
```

#### Ciclo 3: batchUpdate Integration
```python
# RED
def test_should_call_performance_guard_before_batchupdate():
    guard = Mock(spec=PerformanceGuard)
    fetcher = SheetsFetcher(mock_creds, performance_guard=guard)
    
    fetcher.batch_update_data({'sheet1': {'range': 'A1', 'values': [[1]]}})
    
    guard.before_api_call.assert_called_once_with("batchUpdate")

# GREEN - Similar ao anterior
```

#### Ciclo 4: End-to-End Test
```python  
# RED
@pytest.mark.integration
def test_should_maintain_2_api_calls_with_performance_guard():
    """Teste de regressão: guard não deve aumentar número de chamadas."""
    guard = PerformanceGuard(max_calls=2)
    fetcher = SheetsFetcher(real_credentials, performance_guard=guard)
    
    # Executar ETL completo
    fetcher.extract_and_load_all_data()
    
    # Verificar que guard registrou exatamente 2 chamadas
    assert len(guard.call_log) == 2
    assert guard.call_log[0].operation == "batchGet"
    assert guard.call_log[1].operation == "batchUpdate"
```

## 🏆 Resultado Final: Comparação de Qualidade

### Métricas de Qualidade - Antes
- **Cobertura de testes**: ~30%
- **Feedback cycle**: 1 vez por hora
- **Bugs encontrados**: Após integração
- **Refatoração**: Arriscada

### Métricas de Qualidade - Depois  
- **Cobertura de testes**: 95%+
- **Feedback cycle**: A cada 5-10 minutos
- **Bugs encontrados**: Imediatamente
- **Refatoração**: Segura (testes como rede de proteção)

---

**Lição Principal**: TDD não é apenas sobre testes - é sobre design incremental guiado por comportamentos verificáveis, resultando em código mais confiável e maintível.