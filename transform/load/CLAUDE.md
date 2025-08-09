# CLAUDE.md - Load Layer 📤

## Responsabilidade
**Carregamento ultra-otimizado de dados transformados em destinos finais com 1 única chamada API consolidada.**

## 🎯 Arquitetura de Load

### Estratégia Principal: 1x batchUpdate Consolidado
```python
# Ao invés de múltiplas escritas:
# sheets_service.update(dest1, data1)  # ❌
# sheets_service.update(dest2, data2)  # ❌  
# sheets_service.update(dest3, data3)  # ❌

# Fazemos UMA escrita consolidada:
consolidated_write_back(all_changes, creds_path, spreadsheet_id)  # ✅
```

**Resultado**: Todas as escritas executadas em uma única chamada API.

## 🏗️ Componentes Principais

### 1. Consolidated Write-Back (`testar_pipeline_real.ipynb`)
**Função principal que executa todas as escritas em uma única chamada**

#### Funcionalidades:
- ✅ **Change Collection**: Coleta todas as mudanças em memória
- ✅ **Payload Consolidation**: Agrupa múltiplos ranges em um body
- ✅ **Single API Call**: 1x batchUpdate para todos os destinos
- ✅ **Error Recovery**: Fallbacks robusto para falhas

#### Uso Típico:
```python
def consolidated_write_back(changes_list: list, creds_path: str, spreadsheet_id: str):
    # Prepara payloads de origem e destino
    all_data_payloads = []
    
    for change in changes_list:
        if change["type"] == "origin":
            payload = prepare_origin_payload(...)
        elif change["type"] == "dest": 
            payload = prepare_dest_payload(...)
        all_data_payloads.append(payload)
    
    # 🚀 UMA ÚNICA chamada batchUpdate
    body = {"valueInputOption": "USER_ENTERED", "data": all_data_payloads}
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()  # 📡 1x API call for ALL writes
```

### 2. Destination Writer (`dest_writer.py`)
**Escritor otimizado para abas de destino (modelo\*)**

#### Funcionalidades:
- ✅ **Smart Deduplication**: Evita registros duplicados  
- ✅ **ID Generation**: Gera IDs únicos para cada registro
- ✅ **Prefetch Optimization**: Carrega IDs existentes uma vez
- ✅ **Batch Preparation**: Prepara dados para batchUpdate

#### Key Functions:
```python
def prepare_dest_payload(df_model, sheet_name, creds_path, spreadsheet_id, dry_run=False):
    # Prepara payload para escrita em aba de destino
    # - Remove duplicatas baseado em ID
    # - Formata dados para batchUpdate
    # - Retorna payload pronto
```

```python  
def prefetch_meta(fetcher, spreadsheet_id):
    # Carrega metadados de TODAS as abas de destino
    # Executa UMA ÚNICA VEZ no início
    # Popula _EXISTING_IDS cache
```

### 3. Origin Writer (`origin_writer.py`) 
**Escritor para abas de origem (meta\*, tiktok\*, etc.)**

#### Funcionalidades:
- ✅ **Origin Update**: Atualiza dados processados na origem
- ✅ **Status Tracking**: Marca registros como processados
- ✅ **Batch Preparation**: Integração com consolidated write-back

#### Key Functions:
```python
def prepare_origin_payload(df_raw, df_ok, sheet_name, dry_run=False):
    # Prepara payload para write-back na aba origem
    # - Adiciona colunas de status/processamento  
    # - Mantém dados originais intactos
    # - Retorna payload para batchUpdate
```

## 🚀 Otimizações Críticas

### 1. Change Collection Pattern
```python
# Coleta TODAS as mudanças antes de escrever
all_changes = []

for sheet in sheets:
    # Process sheet
    out = run_etl_for_sheet(...)
    
    # Collect changes (não escreve ainda!)
    if out.get("origin_changes"):
        all_changes.append(out["origin_changes"])
    if out.get("dest_changes"):
        all_changes.append(out["dest_changes"])

# 🚀 Escreve TUDO de uma vez
consolidated_write_back(all_changes, ...)
```

### 2. Smart Deduplication
```python
# Prefetch de IDs existentes - UMA vez no início
total_existing_ids = sum(len(ids) for ids in _EXISTING_IDS.values())
logger.info(f"📊 IDs existentes: {total_existing_ids:,} registros")

# Durante processamento - dedup em memória
existing = _EXISTING_IDS[dest_sheet_name]
new_ids = [id for id in new_ids if id not in existing]
```

### 3. Early Exit Strategies
```python
from transform.load.utils.early_exit_checker import should_skip_sheet

skip_sheet, skip_reason = should_skip_sheet(df_raw, sheet_name)
if skip_sheet:
    logger.info(f"⏭️ {sheet_name}: {skip_reason}")
    return {"dest": pd.DataFrame(), ...}  # Skip processing
```

### 4. Payload Optimization
```python
# Otimização de payload size
def prepare_payload(df, range_name):
    # Remove linhas vazias
    df = df.dropna(how='all')
    
    # Converte para formato mais eficiente
    values = df.fillna('').astype(str).values.tolist()
    
    return {
        "range": range_name,
        "values": values,
        "majorDimension": "ROWS"
    }
```

## 📊 Performance Metrics

### Write Performance:
- **Before**: 1 write per destination = ~15-20 API calls
- **After**: 1 consolidated batchUpdate = **1 API call total**
- **Savings**: 95%+ reduction in Load phase

### Deduplication Stats:
```python
# Típico output de deduplicação:
# 📊 Estatísticas de deduplicação:
#   • Total de linhas processadas: 46,670
#   • Registros novos: 12,543  
#   • Registros duplicados (ignorados): 34,127 (73.1%)
```

### Memory Efficiency:
- **Batch collection**: Changes collected in memory
- **Efficient payloads**: Minimal data serialization
- **Smart cleanup**: Garbage collection after write

## 🛠️ Configuration

### Destination Sheets Map:
```python
DESTINATION_SHEETS = {
    "geral": "modeloGeral",
    "idade": "modeloIdade", 
    "genero": "modeloGenero",
    "regiao": "modeloRegiao",
    "alcance": "modeloAlcance"
}
```

### Write Flags:
```python
WRITE_BACK_ORIGIN = True   # Write to source sheets  
WRITE_BACK_DEST = True     # Write to destination sheets
DRY_RUN_DEST = False      # True = simulate writes only
```

## 🐛 Common Issues & Solutions

### 1. Duplicate Record Issues
**Cause**: ID generation inconsistency  
**Solution**: Ensure `gerar_id()` function is deterministic

### 2. Permission Errors (HTTP 403)
**Cause**: Service account lacks Editor permissions  
**Solution**: Share spreadsheet with service account as Editor

### 3. Payload Too Large
**Cause**: Too much data in single batchUpdate  
**Solution**: Chunking already implemented in payload preparation

### 4. Rate Limiting (HTTP 429) 
**Cause**: Too many requests too quickly  
**Solution**: Consolidated approach prevents this issue

## 🔍 Debugging

### Enable Load Debugging:
```python
import logging
logging.getLogger('transform.load').setLevel(logging.DEBUG)
```

### Key Log Messages:
- `📡 batchUpdate consolidado para N mudanças` - Write started
- `✅ batchUpdate consolidado concluído` - Write completed
- `📊 IDs existentes carregados` - Prefetch completed
- `⏭️ Sheet: Nenhum registro novo` - Early exit triggered

### Dry-Run Mode:
```python
# Set DRY_RUN_DEST = True to simulate writes
DRY_RUN_DEST = True
# Output: "🔍 [Dry-run] batchUpdate enviaria N ranges, X células"
```

## 🔄 Data Flow Examples

### Complete Load Process:
```python
# 1. Collect all changes
all_changes = [
    {"type": "origin", "sheet_name": "metaGeral", "df_raw": ..., "df_ok": ...},
    {"type": "dest", "sheet_name": "metaGeral", "df_model": ...},
    {"type": "origin", "sheet_name": "linkedinGeral", "df_raw": ..., "df_ok": ...},
    {"type": "dest", "sheet_name": "linkedinGeral", "df_model": ...},
]

# 2. Prepare consolidated payload  
all_payloads = []
for change in all_changes:
    if change["type"] == "origin":
        payload = prepare_origin_payload(...)
    elif change["type"] == "dest":
        payload = prepare_dest_payload(...)
    all_payloads.append(payload)

# 3. Single write operation
body = {"valueInputOption": "USER_ENTERED", "data": all_payloads}
service.spreadsheets().values().batchUpdate(spreadsheetId=id, body=body).execute()
```

## 📈 Future Optimizations

### Potential Improvements:
- **Parallel writes**: Multiple spreadsheets simultaneously
- **Delta writes**: Only changed cells
- **Compression**: Reduce payload size further  
- **Write validation**: Verify writes succeeded
- **Rollback capability**: Undo failed writes

---

**Key Success Metric**: 1 API call for entire Load phase  
**Status**: Fully optimized with smart deduplication  
**Last Updated**: 2025-01-09