#!/usr/bin/env bash
set -euo pipefail

#──────────────────────── Parâmetros principais ────────────────────────
REPO="davidcantidio/polato-diagnostico-digital"
MILESTONE="Sprint 4"
PROJECT_OWNER="davidcantidio"
PROJECT_NUMBER=8
ASSIGNEE="@me"
TITLE="T-117 – IG-06: Merge com dados orgânicos de desempenho (Instagram)"

#──────────────────────── 1. Verificar autenticação ────────────────────
if ! gh auth status >/dev/null 2>&1; then
  echo "Erro: GitHub CLI não autenticado. Execute 'gh auth login'."
  exit 1
fi

#──────────────────────── 2. Criar milestone se necessário ─────────────
if ! gh api "repos/$REPO/milestones" --jq '.[].title' | grep -qx "$MILESTONE"; then
  echo "🗓  Criando milestone $MILESTONE…"
  gh api -X POST "repos/$REPO/milestones" \
    -f title="$MILESTONE" \
    -f description="Sprint focada em benchmarking & automação de saídas (encerra 04 ago 2025)" \
    -f due_on="2025-08-04T23:59:59Z" >/dev/null
fi

#──────────────────────── 3. Corpo da issue ────────────────────────────
CHECKLIST=$(cat <<'CHECK'
- Criar script `scripts/etl/IG-06_merge_ig_posts_organic.py`
- Ler os arquivos `.parquet`:
  • `IG-00_DATA_posts.parquet`
  • `IG-00_DATA_organic.parquet`
- Garantir chave única comum (ex.: `post_id`)
- Realizar merge linha a linha entre posts e dados de desempenho
- Calcular métricas derivadas:
  • interações por post
  • ER por post
  • CTR visual (impressoes / alcance)
- Validar presença das colunas: `alcance`, `impressoes`, `likes`, `comments`, `ER`
- Salvar como `IG-06_DATA_merged_posts_organic.parquet`
- Atualizar `slide_map.yml` com entrada: IG-06
- Criar testes automatizados em `tests/test_IG06_merge_posts_organic.py`
CHECK
)

BODY=$(cat <<EOF
### Descrição
Unificar os dados de postagens (visuais e metadados) com os dados orgânicos de desempenho (alcance, impressões, interações) para análises completas de performance por conteúdo.

### Checklist
$(echo "$CHECKLIST" | sed 's/^- /\- [ ] /g')

### Critérios de Aceitação
- Posts fundidos corretamente com métricas orgânicas
- Métricas derivadas (ER, CTR visual) computadas com precisão
- Testes unitários garantem robustez e integridade do merge
- slide_map.yml contém entrada IG-06
- PR mergeado na branch \`main\`

### Definition of Done
PR na \`main\`, CI verde, issue fechada e item movido para **Done** no Project.

⏱️ Tempo estimado: 0,6 h
EOF
)

#──────────────────────── 4. Funções auxiliares ────────────────────────
close_duplicates() {
  local title="$1"
  mapfile -t IDS < <(gh issue list -R "$REPO" --state open \
    --search "$title in:title" --json number --jq '.[].number' 2>/dev/null || true)
  for id in "${IDS[@]:-}"; do
    echo "Fechando issue duplicada: #$id"
    gh issue close "$id" -R "$REPO" --comment "Substituído por versão granular 🔄" || true
  done
}

add_to_project() {
  local issue_url="$1"
  gh project item-add "$PROJECT_NUMBER" \
    --owner "$PROJECT_OWNER" \
    --url "$issue_url" >/dev/null 2>&1 || true
}

#──────────────────────── 5. Criação da issue ──────────────────────────
echo -e "\n📋 Criando issue Sprint 4…\n"

close_duplicates "$TITLE"

URL=$(gh issue create -R "$REPO" \
  --title "$TITLE" \
  --assignee "$ASSIGNEE" \
  --milestone "$MILESTONE" \
  --body "$BODY")

add_to_project "$URL"

echo -e "\n✅ Issue criada: $URL"
