#!/usr/bin/env bash

# Projeto: davidcantidio/etl_debrito
# Script para criar labels e issues GitHub, sem passos já realizados nem comandos inválidos.

OWNER="davidcantidio"
REPO="etl_debrito"

# ────────────────────────────────────────────────────────────────────────────
# Função: create_label
# Cria um label apenas se ainda não existir.
# ────────────────────────────────────────────────────────────────────────────
create_label() {
  local name=$1 color=$2 desc=$3

  if gh label list --repo "$OWNER/$REPO" --limit 1000 | grep -Fxq "$name"; then
    echo "⚠️ Label '$name' já existe, pulando."
  else
    echo "🔖 Criando label: $name"
    gh label create "$name" \
      --color "$color" \
      --description "$desc" \
      --repo "$OWNER/$REPO"
  fi
}

# ────────────────────────────────────────────────────────────────────────────
# 1) Labels a garantir (inclui 'ci', 'lint', 'docs' que faltavam)
# ────────────────────────────────────────────────────────────────────────────
declare -A labels=(
  [infra]="6a737d|Infraestrutura e organização do projeto"
  [feat]="2cbe4e|Funcionalidade nova"
  [bug]="d73a4a|Correção de erro"
  [refactor]="fbca04|Refatoração sem mudança funcional"
  [test]="1d76db|Cobertura de testes"
  [doc]="0075ca|Documentação geral"
  [ST]="5319e7|Story principal"
  [task]="e99695|Subtarefa granular de uma ST"
  [blocked]="ffcc00|Bloqueada por dependência"
  [ci]="0052cc|CI/CD pipelines"
  [lint]="d4c5f9|Estilo e linting"
  [docs]="0e8a16|Documentação interna"
)

for name in "${!labels[@]}"; do
  IFS="|" read -r color desc <<< "${labels[$name]}"
  create_label "$name" "$color" "$desc"
done


# ────────────────────────────────────────────────────────────────────────────
# Função: create_issue
# Cria issue apenas se ainda não existir (por título).
# ────────────────────────────────────────────────────────────────────────────
create_issue() {
  local title=$1
  local body=$2
  local labels_csv=$3

  if gh issue list --repo "$OWNER/$REPO" --label ST --limit 1000 | grep -Fqx "$title"; then
    echo "⚠️ Issue '$title' já existe, pulando."
  else
    echo "📝 Criando issue: $title"
    gh issue create \
      --repo "$OWNER/$REPO" \
      --title "$title" \
      --body "$body" \
      --label "$labels_csv"
  fi
}

# ────────────────────────────────────────────────────────────────────────────
# 2) Issues ST-0.1 a ST-0.10
# Ajuste: não refazer as que já foram criadas.
# ────────────────────────────────────────────────────────────────────────────

create_issue \
  "ST-0.1 · Criar pasta ponto_de_controle e mover código para estrutura isolada" \
  "Como desenvolvedor,
Quero isolar a lógica de ponto de controle em uma pasta própria
Para manter o projeto organizado e facilitar manutenção.

**Critérios de aceitação:**
- Criar pasta \`ponto_de_controle/\` na raiz do projeto
- Mover notebook de ponto de controle para lá
- Atualizar imports se necessário
- Confirmar que o código continua executando sem erro (DRY-RUN)" \
  "infra,task"

create_issue \
  "ST-0.2 · Criar __init__.py e organizar funções auxiliares" \
  "Como desenvolvedor,
Quero organizar as funções relacionadas ao ponto de controle
Para modularizar o código e facilitar testes e manutenção.

**Critérios de aceitação:**
- Criar \`__init__.py\` dentro da pasta \`ponto_de_controle\`
- Mover e nomear corretamente funções auxiliares (ex.: read_origin_df, transform_df, etc.)
- Confirmar que o código DRY-RUN continua funcionando
- Atualizar caminhos de importação nos notebooks e scripts" \
  "infra,task"

create_issue \
  "ST-0.3 · Consolidar constantes em ponto_de_controle/constants.py" \
  "Como desenvolvedor,
Quero centralizar DEST_COLUMNS, MIN_DATE e demais constantes reutilizadas
Para reduzir duplicação e tornar a configuração do pipeline mais transparente.

**Critérios de aceitação:**
- Criar módulo \`ponto_de_controle/constants.py\`
- Mover/definir nele: DEST_COLUMNS, MIN_DATE, HEAD_ROW_DEST, ORIGIN_TAB, DEST_TAB
- Atualizar todos os imports no notebook e scripts
- Executar notebook em DRY-RUN sem erros" \
  "refactor"

create_issue \
  "ST-0.4 · Criar entrypoint CLI ponto_de_controle (__main__.py)" \
  "Como analista de dados,
Quero executar o pipeline via linha de comando com \`python -m ponto_de_controle --dry-run\`
Para facilitar automações e agendamentos.

**Critérios de aceitação:**
- Adicionar \`ponto_de_controle/__main__.py\` usando argparse (suporta --dry-run)
- Reaproveitar funções existentes (read_origin_df, transform_df, …)
- Garantir compatibilidade com execução dentro do notebook
- Atualizar README com exemplo de uso" \
  "infra"

create_issue \
  "ST-0.5 · Adicionar testes unitários (pytest) para funções-chave" \
  "Como responsável por QA,
Quero testes automatizados para funções críticas do ponto de controle
Para detectar regressões precocemente.

**Critérios de aceitação:**
- Configurar pytest no projeto (se ainda não houver)
- Criar testes para:
  • read_origin_df (mockando read_df)
  • transform_df (verificar colunas e tipos)
- Cobertura mínima de 80% nas funções testadas
- Testes rodando em GitHub Actions (ver ST-0.6)" \
  "test"

create_issue \
  "ST-0.6 · Configurar GitHub Actions CI (lint + testes)" \
  "Como equipe,
Quero rodar lint e testes automaticamente em cada PR
Para manter a qualidade contínua do código.

**Critérios de aceitação:**
- Workflow .github/workflows/ci.yml que execute:
  • ruff check e ruff format --check
  • pytest
- CI deve falhar se lint ou testes falharem
- Badge de status adicionado ao README" \
  "ci"

create_issue \
  "ST-0.7 · Integrar linting (ruff) e formatação automática" \
  "Como desenvolvedor,
Quero padronizar estilo de código com ruff (lint + format)
Para reduzir review noise e garantir consistência.

**Critérios de aceitação:**
- Adicionar ruff às dependências
- Criar/atualizar pyproject.toml com regras básicas
- Rodar ruff format em todo o projeto
- Atualizar README com instruções de uso" \
  "lint"

create_issue \
  "ST-0.8 · Documentar módulo ponto_de_controle (README interno)" \
  "Como novo integrante do time,
Quero encontrar documentação clara do módulo ponto_de_controle
Para entender fluxos, dependências e como executar.

**Critérios de aceitação:**
- Criar ponto_de_controle/README.md com:
  • Visão geral do pipeline (origem → transformação → destino)
  • Instruções de execução (notebook e CLI)
  • Variáveis de ambiente necessárias
  • Como rodar testes e lint
- Linkar esse README no README principal" \
  "docs"

create_issue \
  "ST-0.9 · Destacar campanhas ativas vs. encerradas no Google Sheets" \
  "Como analista de marketing,
Quero que o pipeline pinte de verde as linhas de campanhas em execução e de cinza as encerradas
Para que o time visualize rapidamente o status direto na planilha.

**Critérios de aceitação:**
- Implementar apply_status_formatting(ws) em ponto_de_controle/sheets_utils.py
  • Recebe gspread.Worksheet
  • Usa start/end para definir cores (#D5E8D4 e #E6E6E6)
  • Idempotente (não duplica regras)
- Chamar após write_dataframe_to_sheet_final
- DRY-RUN apenas loga quantas linhas seriam formatadas" \
  "feature"

create_issue \
  "ST-0.10 · Converter notebook em script automático (via jupytext)" \
  "Como desenvolvedor,
Quero exportar o notebook para um módulo .py em cada push
Para evoluir rumo a produção sem dependência do Jupyter.

**Critérios de aceitação:**
- Adicionar jupytext às dev-dependencies
- Criar scripts/export_ponto_de_controle.sh que execute:
  • jupytext ponto_de_controle/ponto_de_controle_notebook.ipynb --to py:percent -o ponto_de_controle/pipeline.py
- Incluir passo no CI para rodar esse script
- Garantir pipeline.py passe em ruff + pytest
- Documentar no README interno" \
  "infra"

echo "✅ Todas as labels e issues foram processadas."

