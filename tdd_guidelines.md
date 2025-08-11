# Guia de Boas Práticas TDD - ETL Debrito Project

## 🎯 Visão Geral

Este guia estabelece padrões e práticas para desenvolvimento orientado por testes (TDD) no projeto ETL Debrito, garantindo qualidade, manutenibilidade e confiabilidade do pipeline de dados.

## 🔄 Ciclo TDD Fundamental

### 🔴 FASE RED - Teste que Falha
```python
def test_should_extract_campaign_data_when_valid_sheet_id():
    # Arrange
    sheets_fetcher = SheetsFetcher(mock_credentials)
    valid_sheet_id = "1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE"
    
    # Act & Assert
    with pytest.raises(NotImplementedError):
        result = sheets_fetcher.extract_campaign_data(valid_sheet_id)
```

**✅ Regras da Fase RED:**
- Escreva o teste mais simples possível que falhe
- Teste deve expressar claramente o comportamento desejado
- Use nomes descritivos: `test_should_[behavior]_when_[condition]`
- Execute o teste para confirmar que falha pelo motivo correto

### 🟢 FASE GREEN - Código Mínimo
```python
class SheetsFetcher:
    def extract_campaign_data(self, sheet_id: str):
        # Implementação mínima para passar o teste
        if sheet_id:
            return {"campaigns": []}
        raise NotImplementedError()
```

**✅ Regras da Fase GREEN:**
- Escreva apenas o código necessário para passar o teste
- Não se preocupe com elegância - foque em funcionalidade
- Não implemente funcionalidades não testadas
- Execute TODOS os testes para garantir não-regressão

### 🔵 FASE REFACTOR - Melhoria de Design
```python
class SheetsFetcher:
    def extract_campaign_data(self, sheet_id: str) -> Dict[str, List]:
        """Extrai dados de campanhas de uma planilha Google Sheets."""
        self._validate_sheet_id(sheet_id)
        return self._fetch_campaign_data(sheet_id)
    
    def _validate_sheet_id(self, sheet_id: str) -> None:
        if not sheet_id or not isinstance(sheet_id, str):
            raise ValueError("Sheet ID deve ser uma string não vazia")
```

**✅ Regras da Fase REFACTOR:**
- Melhore design mantendo funcionalidade
- Elimine duplicação de código
- Torne código mais legível e maintível
- Execute testes constantemente durante refatoração

## 📋 Estrutura de Tasks TDD

### Micro-Tasks por Funcionalidade
Cada funcionalidade deve ser quebrada em micro-tasks de 5-15 minutos:

1. **RED**: `test_should_[behavior]_when_[condition]` (5 min)
2. **GREEN**: `implement_[minimal_feature]` (8 min)  
3. **REFACTOR**: `improve_[design_aspect]` (10 min)

### Exemplo Prático: PerformanceGuard
```json
{
  "id": "1.1",
  "title": "TEST: should_raise_exception_when_max_calls_exceeded",
  "tdd_phase": "red",
  "estimate_minutes": 5
},
{
  "id": "1.2", 
  "title": "IMPL: basic_call_counter_with_limit",
  "tdd_phase": "green",
  "estimate_minutes": 8
},
{
  "id": "1.3",
  "title": "REFACTOR: extract_configuration_class",
  "tdd_phase": "refactor", 
  "estimate_minutes": 10
}
```

## 🧪 Padrões de Teste

### Nomenclatura de Testes
```python
# ✅ Bom: Descreve comportamento esperado
def test_should_invalidate_cache_when_new_data_arrives():
def test_should_raise_ValueError_when_sheet_id_is_empty():
def test_should_return_cached_data_when_within_ttl():

# ❌ Ruim: Focado em implementação, não comportamento  
def test_get_sheets_data():
def test_cache_method():
def test_error_handling():
```

### Estrutura AAA (Arrange-Act-Assert)
```python
def test_should_consolidate_api_calls_when_multiple_sheets():
    # Arrange
    fetcher = SheetsFetcher(mock_credentials)
    sheet_ids = ["sheet1", "sheet2", "sheet3"]
    expected_call_count = 1  # Consolidado em batchGet
    
    # Act
    with patch('sheets_client.batchGet') as mock_batch:
        fetcher.fetch_multiple_sheets(sheet_ids)
    
    # Assert
    assert mock_batch.call_count == expected_call_count
```

### Mocks e Fixtures
```python
# fixtures/sheets_fixtures.py
@pytest.fixture
def mock_sheets_service():
    """Mock do Google Sheets API service."""
    with patch('googleapiclient.discovery.build') as mock_service:
        yield mock_service

@pytest.fixture  
def sample_campaign_data():
    """Dados de amostra para testes de campanhas."""
    return {
        "metaGeral": [
            {"campaign_id": "123", "impressions": 1000, "clicks": 50}
        ]
    }
```

## 🏗️ Arquitetura TDD para ETL

### Testando Extract Layer
```python
def test_should_extract_all_required_sheets_in_single_batchget():
    # Testa otimização de 2 API calls
    fetcher = SheetsFetcher(credentials)
    required_sheets = ["metaGeral", "linkedinGeral", "tiktokGeral"]
    
    with patch('sheets_service.spreadsheets.values.batchGet') as mock_batch:
        fetcher.extract_all_data(SPREADSHEET_ID)
        
    assert mock_batch.call_count == 1
    batch_request = mock_batch.call_args[1]['body']
    assert len(batch_request['ranges']) == len(required_sheets)
```

### Testando Transform Layer  
```python
def test_should_validate_required_columns_and_collect_warnings():
    # Testa pipeline de validação com captura de warnings
    transformer = TreatPipeline()
    invalid_data = pd.DataFrame({"wrong_column": [1, 2, 3]})
    
    with warnings.catch_warnings(record=True) as w:
        transformer.validate_columns(invalid_data, required=["campaign_id"])
        
    assert len(w) == 1
    assert "missing required columns" in str(w[0].message).lower()
```

### Testando Load Layer
```python
def test_should_write_all_changes_in_single_batchupdate():
    # Testa consolidação de escritas
    writer = DestWriter(credentials)  
    changes = {
        "sheet1": {"range": "A1:C10", "values": [[1, 2, 3]]},
        "sheet2": {"range": "A1:B5", "values": [[4, 5]]}
    }
    
    with patch('sheets_service.spreadsheets.values.batchUpdate') as mock_batch:
        writer.write_all_changes(SPREADSHEET_ID, changes)
        
    assert mock_batch.call_count == 1
```

## 📊 Métricas e Qualidade

### Coverage Requirements
```bash
# Configuração pytest-cov
pytest --cov=transform --cov-report=html --cov-fail-under=90
```

### Performance Testing
```python
def test_should_complete_etl_pipeline_within_time_limit():
    """Pipeline completo deve rodar em menos de 60 segundos."""
    start_time = time.time()
    
    pipeline = TreatPipeline()
    pipeline.run_complete_etl()
    
    execution_time = time.time() - start_time
    assert execution_time < 60, f"Pipeline took {execution_time}s, expected < 60s"
```

### Integration Tests
```python
@pytest.mark.integration
def test_should_maintain_2_api_calls_architecture():
    """Teste de regressão para arquitetura otimizada."""
    with patch('google.oauth2.service_account') as mock_auth:
        with patch('googleapiclient.discovery.build') as mock_service:
            # Setup mocks
            mock_service.return_value.spreadsheets.return_value.values.return_value.batchGet.return_value.execute.return_value = MOCK_BATCH_GET_RESPONSE
            mock_service.return_value.spreadsheets.return_value.values.return_value.batchUpdate.return_value.execute.return_value = {}
            
            # Execute pipeline
            pipeline = TreatPipeline()
            pipeline.run_complete_etl()
            
            # Verify API call count
            batch_get_calls = mock_service.return_value.spreadsheets.return_value.values.return_value.batchGet.call_count
            batch_update_calls = mock_service.return_value.spreadsheets.return_value.values.return_value.batchUpdate.call_count
            
            assert batch_get_calls == 1, "Should use exactly 1 batchGet call"
            assert batch_update_calls == 1, "Should use exactly 1 batchUpdate call"
```

## 🔧 Ferramentas e Setup

### pytest.ini
```ini
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    --strict-markers
    --strict-config
    --verbose
    --cov=transform
    --cov-branch
    --cov-report=term-missing
    --cov-report=html
markers =
    unit: Unit tests
    integration: Integration tests  
    slow: Tests that take more than 1 second
    api: Tests that make real API calls
```

### Pre-commit Hooks
```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: tests
        name: Run tests
        entry: pytest -x --ff
        language: system
        pass_filenames: false
        always_run: true
      - id: coverage
        name: Check coverage
        entry: pytest --cov=transform --cov-fail-under=90 -q
        language: system
        pass_filenames: false
        always_run: true
```

## 🚨 Anti-Patterns e Como Evitar

### ❌ Anti-Pattern: Testes que Testam Implementação
```python
# Ruim: Testa detalhes internos
def test_uses_pandas_dataframe():
    result = processor.transform_data(data)
    assert isinstance(result, pd.DataFrame)
```

### ✅ Pattern: Testes que Testam Comportamento  
```python
# Bom: Testa resultado/comportamento
def test_should_normalize_currency_values():
    data = [{"value": "R$ 1.000,50"}, {"value": "R$ 2.500,00"}]
    result = processor.normalize_currency(data)
    expected = [{"value": 1000.5}, {"value": 2500.0}]
    assert result == expected
```

### ❌ Anti-Pattern: Um Teste para Múltiplos Comportamentos
```python
# Ruim: Teste faz muitas coisas
def test_complete_etl_process():
    # Testa extração
    data = extractor.extract()
    assert data is not None
    
    # Testa transformação  
    clean_data = transformer.clean(data)
    assert len(clean_data) > 0
    
    # Testa carga
    result = loader.load(clean_data)
    assert result.success
```

### ✅ Pattern: Um Comportamento por Teste
```python
# Bom: Cada teste tem uma responsabilidade
def test_should_extract_data_successfully():
    data = extractor.extract()
    assert data is not None
    assert len(data) > 0

def test_should_clean_invalid_records():
    dirty_data = [{"valid": True}, {"valid": False}, {}]
    clean_data = transformer.clean(dirty_data) 
    assert all(record.get("valid") for record in clean_data)

def test_should_confirm_successful_load():
    sample_data = [{"id": 1, "value": "test"}]
    result = loader.load(sample_data)
    assert result.success
    assert result.records_loaded == len(sample_data)
```

## 📈 CI/CD Integration

### GitHub Actions Workflow
```yaml
# .github/workflows/tdd.yml
name: TDD Workflow
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install poetry
          poetry install
      - name: Run TDD tests
        run: |
          poetry run pytest --cov=transform --cov-fail-under=90
      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

## 🎓 Recursos e Referências

### Leitura Recomendada
- **"Test Driven Development: By Example"** - Kent Beck
- **"Growing Object-Oriented Software, Guided by Tests"** - Freeman & Pryce
- **"Clean Code"** - Robert C. Martin (Capítulos sobre testes)

### Ferramentas do Ecossistema
- **pytest**: Framework de teste principal
- **pytest-cov**: Coverage reporting
- **pytest-mock**: Mocking utilities  
- **factory_boy**: Test data factories
- **freezegun**: Time mocking for data pipelines
- **responses**: HTTP request mocking

---

**Última atualização**: 2024-01-09  
**Status**: Documento vivo - atualizar conforme evolução do projeto  
**Responsável**: Equipe de Desenvolvimento ETL Debrito