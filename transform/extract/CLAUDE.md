# CLAUDE.md - Extract Layer 📥

## Responsabilidade
**Extração ultra-otimizada de dados do Google Sheets com 1 única chamada API consolidada.**

## 🎯 Arquitetura de Extração

### Estratégia Principal: 1x batchGet Consolidado
```python
# Ao invés de múltiplas chamadas:
# sheets_service.get(sheet1)  # ❌
# sheets_service.get(sheet2)  # ❌  
# sheets_service.get(sheet3)  # ❌

# Fazemos UMA chamada consolidada:
all_data = fetcher.get([sheet1, sheet2, sheet3, ...])  # ✅
```

## 🔧 Componentes Principais

### SheetsFetcher (`sheets_fetcher.py`)
**Classe principal para extração otimizada do Google Sheets**

#### Funcionalidades:
- ✅ **batchGet consolidado**: Carrega todas as abas em uma chamada
- ✅ **Cache interno**: Evita chamadas desnecessárias
- ✅ **Error handling robusto**: Fallbacks para diferentes cenários
- ✅ **Thread-safe operations**: Safe connection pooling integrado

#### Uso Típico:
```python
from transform.extract.sheets_fetcher import SheetsFetcher

# Configuração
fetcher = SheetsFetcher(
    spreadsheet_id="1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE",
    creds_path="creds.json"
)

# Extração consolidada - UMA chamada API
sheet_names = ["metaGeral", "linkedinGeral", "GAGeral", ...]
all_data = fetcher.get(sheet_names)  # 📡 1x batchGet API call

# Dados disponíveis imediatamente
df_meta = all_data["metaGeral"] 
df_linkedin = all_data["linkedinGeral"]
```

## 🚀 Otimizações Implementadas

### 1. Ultra-Safe Connection Pooling
```python
from transform.transform.utils.safe_connection_pool import apply_ultra_safe_pooling
apply_ultra_safe_pooling()  # Elimina threading crashes
```

### 2. Worksheet Caching (5min TTL)
```python
def get_cached_worksheets(gc, spreadsheet_id: str):
    # Cache inteligente para worksheets()
    # Evita chamadas repetidas para metadados
    return cached_worksheets  # ⚡ Cache hit economiza 1 API call
```

### 3. Sheet Name Normalization
```python
from transform.transform.utils.sheet_name_normalizer import normalize_sheet_name
# Handles: "CONTEÚDO _MÍDIA" → Safe range names
```

### 4. Early Exit Strategies
```python
from transform.load.utils.early_exit_checker import should_skip_sheet
skip_sheet, reason = should_skip_sheet(df_raw, sheet_name)
if skip_sheet:
    return  # Skip processing if no new data
```

## 📊 Performance Metrics

### API Usage:
- **Before**: 1 call per sheet + metadata calls = ~20+ calls
- **After**: 1 consolidated batchGet = **1 call total** 
- **Savings**: 95%+ reduction in Extract phase

### Memory Efficiency:
- **Batch processing**: All data loaded once
- **Immediate availability**: Zero wait time between sheets
- **Cache utilization**: Metadata cached for 5 minutes

## 🛠️ Configuration & Setup

### Required Environment:
```bash
GOOGLE_CREDS_PATH="creds.json"         # Service account credentials
GOOGLE_SHEET_ID="1jP..."               # Target spreadsheet ID
```

### Service Account Setup:
1. **Share spreadsheet** with service account email
2. **Permissions**: Editor (for write-back) or Viewer (read-only)
3. **Scopes**: `https://www.googleapis.com/auth/spreadsheets`

## 🐛 Common Issues & Solutions

### 1. HTTP 403 Forbidden
**Cause**: Spreadsheet not shared with service account  
**Solution**: Share with email from `creds.json` → `client_email`

### 2. Threading Crashes
**Cause**: Connection pool iteration issues  
**Solution**: Already solved with `ultra_safe_pooling`

### 3. Rate Limits (HTTP 429)
**Cause**: Too many API calls  
**Solution**: Consolidated approach already prevents this

### 4. Invalid Range Names
**Cause**: Sheet names with special characters  
**Solution**: `sheet_name_normalizer` handles encoding

## 🔍 Debugging

### Enable Debug Logs:
```python
import logging
logging.getLogger('transform.extract').setLevel(logging.DEBUG)
```

### Key Log Messages:
- `📡 batchGet consolidado para N abas` - Extraction started
- `✅ Leitura consolidada concluída` - Extraction completed  
- `📥 Cache hit para worksheets()` - Metadata cached
- `🔗 Connection pool: active` - Thread-safe pooling active

## 📈 Future Optimizations

### Potential Improvements:
- **Delta extraction**: Only fetch changed ranges
- **Compression**: Reduce payload size
- **Parallel auth**: Background credential refresh
- **Smart prefetch**: Predictive loading based on usage patterns

---

**Key Success Metric**: 1 API call for entire Extract phase  
**Status**: Fully optimized and thread-safe  
**Last Updated**: 2025-01-09