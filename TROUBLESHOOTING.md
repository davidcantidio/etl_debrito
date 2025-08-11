# TROUBLESHOOTING.md - ETL Pipeline Debugging Guide

## 🚨 Emergency Commands

### Quick Pipeline Status Check:
```bash
# Check if pipeline can start
python -c "
from transform import SheetsFetcher, TreatPipeline
print('✅ Pipeline ready to run')
"

# Check logs for recent errors
tail -50 logs/pipeline_debug.log | grep -E "(ERROR|❌|CRITICAL)"
```

### Force-Fix Threading Issues:
```python
# If pipeline crashes with threading errors, run this:
from transform.transform.utils.safe_connection_pool import apply_ultra_safe_pooling, close_all_connections
close_all_connections()
apply_ultra_safe_pooling()
```

## 🔍 Common Error Patterns & Solutions

### 1. "ModuleNotFoundError: No module named 'transform.platforms'"
**Log Pattern**: `ModuleNotFoundError: No module named 'transform.platforms'`  
**Location**: Usually in platform dispatch  
**Root Cause**: Incorrect import path after ETL reorganization

**Solution**:
```bash
# Check the dispatch function:
grep -n "import_module.*transform\.platforms" transform/transform/platforms/__init__.py

# Should be:  
# import_module(f"transform.transform.platforms.{mod}")
# NOT:
# import_module(f"transform.platforms.{mod}")  
```

### 2. "Iteration over this class is unlikely to be threadsafe"
**Log Pattern**: `Iteration over this class is unlikely to be threadsafe`  
**Location**: Connection pool operations  
**Root Cause**: Unsafe iteration over connection pools

**Solution**:
```python
# IMMEDIATE FIX - Apply ultra-safe pooling:
from transform.transform.utils.safe_connection_pool import apply_ultra_safe_pooling
apply_ultra_safe_pooling()

# PREVENTION - Never iterate over pools:
# ❌ stats["pools"] += len(adapter.poolmanager.pools)  
# ✅ return {"pools": "disabled", "connections": "disabled"}
```

### 3. "HTTP Error 403: Forbidden" 
**Log Pattern**: `❌ Erro de permissão (403)`  
**Root Cause**: Spreadsheet not shared with service account

**Solution**:
```bash
# 1. Get service account email:
python -c "
import json
with open('creds.json') as f:
    creds = json.load(f)
print(f'📧 Service Account: {creds[\"client_email\"]}')
"

# 2. Share spreadsheet with this email as Editor
# 3. Verify access:
python -c "
import gspread, google.auth
creds, _ = google.auth.load_credentials_from_file('creds.json')
gc = gspread.authorize(creds)
ss = gc.open_by_key('1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE')
print(f'✅ Access OK: {ss.title}')
"
```

### 4. "No data processed" - Pipeline runs but processes nothing
**Log Pattern**: `⚠️ Nenhuma aba foi processada devido a erros`  
**Root Cause**: Individual sheet processing failing silently

**Solution**:
```bash
# Enable detailed debugging:
export ETL_DEBUG=1

# Check specific sheet processing:
python -c "
from transform.transform.platforms import dispatch
try:
    func = dispatch('metaGeral')
    print('✅ Platform dispatch working')
except Exception as e:
    print(f'❌ Dispatch error: {e}')
"

# Check sheet validation:
python -c "
import pandas as pd
from transform.load.utils.early_exit_checker import should_skip_sheet
df_test = pd.DataFrame({'test': [1,2,3]})
skip, reason = should_skip_sheet(df_test, 'metaGeral')
print(f'Skip: {skip}, Reason: {reason}')
"
```

## 📊 Performance Troubleshooting

### Too Many API Calls (Should be 2)
**Symptoms**: Pipeline slow, rate limiting errors  
**Debug**:
```bash
# Count API calls in logs:
grep -c "Making request:" logs/pipeline_debug.log

# Should see exactly 2 patterns:
# "📡 CHAMADA 1/2: batchGet consolidado"  
# "📡 CHAMADA 2/2: batchUpdate consolidado"

# If more than 2, check for:
grep -E "(get|update).*API" logs/pipeline_debug.log
```

### Memory Issues
**Symptoms**: Pipeline slow, high memory usage  
**Debug**:
```python
import gc, psutil, os
print(f"Memory usage: {psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024:.1f} MB")
gc.collect()  # Force garbage collection
```

### Rate Limiting Issues  
**Symptoms**: HTTP 429 errors
**Debug**:
```bash
# Check rate limiting configuration:
grep -A5 -B5 "rate_limiting" testar_pipeline_real.py

# Should be conditional:
# if API_OPTIMIZATIONS.get("rate_limiting", False):
```

## 🔧 Debug Mode Commands

### Enable Full Debug Logging:
```python
import logging
logging.getLogger('transform').setLevel(logging.DEBUG)
logging.getLogger('extract').setLevel(logging.DEBUG)
logging.getLogger('load').setLevel(logging.DEBUG)
```

### Test Each ETL Layer:
```python
# Test Extract:
from transform.extract.sheets_fetcher import SheetsFetcher
fetcher = SheetsFetcher("1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE", "creds.json")
data = fetcher.get(["metaGeral"])  
print(f"✅ Extract: {len(data)} sheets loaded")

# Test Transform:
from transform.transform.transform_pipeline import TreatPipeline
pipeline = TreatPipeline("creds.json", "1jP...", "metaGeral", {}, write_back=False)
# Need data to test fully

# Test Load:  
from transform.load.dest_writer import prepare_dest_payload
# payload = prepare_dest_payload(df_model, "metaGeral", "creds.json", "1jP...", dry_run=True)
```

## 📋 Maintenance Commands

### Clear All Caches:
```python
# Clear worksheet cache:
from testar_pipeline_real import _WORKSHEETS_CACHE
_WORKSHEETS_CACHE.clear()

# Clear BI lookup cache:
from transform.transform.bi_param_utils import BIParamLookup
BIParamLookup._df = None
BIParamLookup._last_load = 0.0

# Clear connection pool:
from transform.transform.utils.safe_connection_pool import close_all_connections
close_all_connections()
```

### Validate Pipeline Configuration:
```python
# Check critical settings:
from testar_pipeline_real import API_OPTIMIZATIONS
required_optimizations = {
    "ultra_mode": True,
    "consolidate_batch_get": True, 
    "consolidate_batch_update": True,
}

for key, expected in required_optimizations.items():
    actual = API_OPTIMIZATIONS.get(key, False)
    if actual != expected:
        print(f"❌ {key}: expected {expected}, got {actual}")
    else:
        print(f"✅ {key}: {actual}")
```

### Check Google API Quotas:
```bash
# Monitor API usage in logs:
grep -E "(batchGet|batchUpdate)" logs/pipeline_debug.log | tail -10

# Should see pattern like:
# "📡 CHAMADA 1/2: batchGet consolidado para 25 abas"
# "📡 CHAMADA 2/2: batchUpdate consolidado para 18 mudanças"
```

## 🎯 Performance Profiling

### Time Each ETL Phase:
```python
import time

# Profile Extract:
start = time.time()
all_data = fetcher.get(sheet_names)
extract_time = time.time() - start
print(f"📥 Extract: {extract_time:.2f}s")

# Profile Transform (per sheet):
start = time.time()
for sheet in sheets:
    # process sheet
    pass
transform_time = time.time() - start  
print(f"🔄 Transform: {transform_time:.2f}s")

# Profile Load:
start = time.time()
consolidated_write_back(all_changes, creds_path, spreadsheet_id)
load_time = time.time() - start
print(f"📤 Load: {load_time:.2f}s")
```

### Memory Profiling:
```python
import tracemalloc

tracemalloc.start()

# Run pipeline phase
# ...

current, peak = tracemalloc.get_traced_memory()
print(f"Current memory usage: {current / 1024 / 1024:.1f} MB")
print(f"Peak memory usage: {peak / 1024 / 1024:.1f} MB")
tracemalloc.stop()
```

## 🚨 Emergency Recovery

### Pipeline Completely Broken:
```bash
# 1. Reset to known good state:
git status
git stash  # If uncommitted changes
git reset --hard HEAD

# 2. Verify basic imports:
python -c "from transform import SheetsFetcher; print('✅ Import OK')"

# 3. Test connectivity:
python -c "
import gspread, google.auth
creds, _ = google.auth.load_credentials_from_file('creds.json')
gc = gspread.authorize(creds)
print('✅ Auth OK')
"

# 4. Run minimal test:
jupyter notebook --no-browser testar_pipeline_real.ipynb
```

### Data Corruption Issues:
```python
# Backup before fixes:
import pandas as pd
from transform.extract.sheets_fetcher import SheetsFetcher

fetcher = SheetsFetcher("1jP...", "creds.json")
backup_data = fetcher.get(["metaGeral", "linkedinGeral"])  

# Save backups:
for sheet_name, df in backup_data.items():
    df.to_csv(f"backup_{sheet_name}_{pd.Timestamp.now():%Y%m%d_%H%M}.csv")
```

### 12. Pinterest Execution Order Dependency
**Log Pattern**: `RuntimeError: ERRO: pinterestGeral ainda não foi executado`  
**Location**: Pinterest demographic merge operations  
**Root Cause**: pinterestIdade/Genero/Regiao depend on pinterestGeral being processed first

**Solution**:
```python
# ALWAYS process pinterestGeral BEFORE demographic sheets:
sheet_order = [
    "pinterestGeral",  # MUST BE FIRST
    "pinterestIdade", 
    "pinterestGenero",
    "pinterestRegiao"
]

# Or check if cache exists before processing:
if hasattr(builtins, '_pinterest_geral_tratado'):
    # Safe to process demographic sheets
    pass
else:
    # Process pinterestGeral first
    pass
```

### 13. Global State Concurrency Risks
**Log Pattern**: Unpredictable behavior in parallel executions  
**Location**: builtins.fetcher, _pinterest_geral_tratado, cache globals  
**Root Cause**: Shared global state without thread safety

**Solution**:
```python
# SHORT-TERM: Ensure single-threaded execution
# LONG-TERM: Refactor to use explicit context passing

# Example of safer pattern:
class PipelineContext:
    def __init__(self):
        self.fetcher = None
        self.pinterest_cache = {}
        self._lock = threading.Lock()
    
    def get_pinterest_cache(self, key):
        with self._lock:
            return self.pinterest_cache.get(key)
```

### 14. Cache TTL Expiration During Long Runs
**Log Pattern**: Unexpected API calls after 5 minutes  
**Location**: SheetsFetcher cache expiration  
**Root Cause**: Hardcoded TTL of 300 seconds

**Solution**:
```python
# Create sheets_config.yaml or .env:
SHEETS_CACHE_TTL=900  # 15 minutes for long pipelines

# Or dynamically set based on sheet count:
ttl = max(300, len(sheet_names) * 30)  # 30s per sheet minimum
fetcher = SheetsFetcher(cache_ttl=ttl)
```

---

## 🌐 GitHub Pages & Deployment Issues

### 15. GitHub Pages 404 Error (Site Not Loading)
**Symptom**: https://davidcantidio.github.io/etl_debrito/ returns 404  
**Location**: GitHub Pages deployment  
**Root Causes**: Jekyll build issues, stuck Pages build process, configuration problems

#### Emergency Diagnostic Commands:
```bash
# Check Pages configuration status
gh api repos/davidcantidio/etl_debrito/pages

# Check recent Pages builds
gh api repos/davidcantidio/etl_debrito/pages/builds | head -5

# Check recent workflow runs
gh run list --workflow="🎯 Update Project Gantt Charts" --limit=3

# Check gh-pages branch content
gh api repos/davidcantidio/etl_debrito/contents?ref=gh-pages

# Test if site content exists (should work if deployed)
curl -I https://davidcantidio.github.io/etl_debrito/
```

#### Common Root Causes & Solutions:

**A. Stuck Pages Build Process**
```json
{"status":"building","duration":25380117} // 7+ hours stuck
```
**Solution**: Switch to official GitHub Pages actions
```yaml
# Replace third-party action with official ones
- uses: actions/configure-pages@v4
- uses: actions/upload-pages-artifact@v3  
- uses: actions/deploy-pages@v4
```

**B. Jekyll Build Failure** 
```yaml
# Missing Gemfile in docs/ directory
```
**Solution**: Create `docs/Gemfile`:
```ruby
source "https://rubygems.org"
gem "github-pages", group: :jekyll_plugins
gem "jekyll", "~> 3.9.0"
gem "minima"
```

**C. Workflow Configuration Issues**
```yaml
# Missing Pages environment
```
**Solution**: Add proper environment to workflow:
```yaml
environment:
  name: github-pages
  url: ${{ steps.deployment.outputs.page_url }}
permissions:
  pages: write
  id-token: write
```

#### Proven Fix Sequence (GitHub Pages 404):
1. **Force fresh workflow**: `gh workflow run "🎯 Update Project Gantt Charts" --field force_deploy=true`
2. **Wait 2-3 minutes** for deployment
3. **Check build status**: `gh api repos/davidcantidio/etl_debrito/pages/builds | head -1`
4. **Verify site**: `curl -I https://davidcantidio.github.io/etl_debrito/`
5. **If still 404**: Switch to official GitHub Pages actions (see docs/github-pages-setup.md)

---

## 🔧 Workflow Debugging

### Quick Workflow Status:
```bash
# Check if workflow is running
gh run list --limit=3

# Watch current run
gh run watch $(gh run list --limit=1 --json databaseId --jq .[0].databaseId)

# View specific job logs
gh run view --log --job=JOB_ID
```

### Jekyll Build Debugging:
```bash
# Test Jekyll build locally (if Ruby installed)
cd docs
bundle install
bundle exec jekyll build --destination _site

# Check for Jekyll errors in workflow
gh run view --log | grep -A 10 -B 10 "Jekyll\|bundle"
```

---

**🆘 If all else fails**: Check `logs/pipeline_debug.log` for the exact error location and compare with working patterns in CLAUDE.md files.

**Last Updated**: 2025-08-11