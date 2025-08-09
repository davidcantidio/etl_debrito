"""
Transform Module - ETL Architecture (Extract/Transform/Load)

Este módulo implementa a arquitetura ETL completa:

📥 EXTRACT: Raw data extraction and ingestion
🔄 TRANSFORM: Clean, curated and transformed data  
📤 LOAD: Optimized data loading for consumption

Estrutura:
- extract/: Extração de dados brutos das fontes
- transform/: Transformação e limpeza dos dados
- load/: Carga otimizada para destinos finais
"""

from .extract.sheets_fetcher import SheetsFetcher
from .transform.transform_pipeline import TreatPipeline
from .load.dest_writer import prepare_dest_payload
from .load.origin_writer import prepare_origin_payload

__all__ = [
    'SheetsFetcher',
    'TreatPipeline', 
    'prepare_dest_payload',
    'prepare_origin_payload'
]