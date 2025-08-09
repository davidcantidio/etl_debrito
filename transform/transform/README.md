# Transform Layer 🔄

**Transformação e Limpeza de Dados (Data Transformation)**

Esta camada transforma dados brutos do Extract em dados limpos, validados e enriquecidos.

## Responsabilidades:

- **Limpeza**: Remover inconsistências e padronizar formatos
- **Validação**: Aplicar regras de negócio e validações de esquema
- **Enriquecimento**: Adicionar campos calculados e lookups
- **Normalização**: Padronizar nomenclaturas e geografias
- **Deduplicação**: Remover registros duplicados

## Estrutura:

### Core:
- `transform_pipeline.py`: Pipeline principal de transformação
- `transform_runner.py`: Executor do pipeline
- `settings.py`: Configurações do módulo transform

### Platforms:
- `platforms/`: Transformações específicas por plataforma
  - `meta.py`: Transformações Facebook/Instagram
  - `linkedin.py`: Transformações LinkedIn
  - `tiktok.py`: Transformações TikTok
  - `pinterest.py`: Transformações Pinterest
  - `ga.py`: Transformações Google Analytics

### Utils:
- `utils/`: Utilitários de transformação
  - Validações, renomeações, campos calculados
  - Normalização de dados geográficos
  - Mapeamento de campanhas e UTMs

## Princípios:

- ✅ Dados confiáveis e consistentes
- ✅ Aplicação de regras de negócio
- ✅ Enriquecimento contextual
- ✅ Padronização cross-plataforma
EOF < /dev/null
