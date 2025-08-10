Instruções para Claude Code — Mindmap + Flow + Gantt a partir dos épicos TDD
0) Entrada (contrato)
Diretório com 1..N arquivos epico_*.json no formato do template_epico_tdd.json.

Estrutura mínima por arquivo:

epic.id (string)

epic.name (string)

epic.tasks[] (array)

id (string, único por épico)

title (string)

tdd_phase ∈ {red, green, refactor} ou tdd_skip_reason (analysis/doc)

estimate_minutes (int: 5–15)

dependencies (array de strings; opcional)

deliverables (array; opcional)

Se algum campo estiver ausente, trate com defaults: tdd_phase = "analysis", estimate_minutes = 10, dependencies = [].

1) Tarefas do Claude
Ler todos os epico_*.json fornecidos (lista de caminhos passada pelo usuário).

Normalizar IDs globais: GID = epic.id + ":" + task.id.

Construir o grafo de dependências com base em dependencies (entre tasks do mesmo épico e, se referenciadas, cruzadas).

Calcular duração do épico = soma de estimate_minutes de suas tarefas.

Definir ordem macro entre épicos:

Se não houver info explícita, use heurística:

0 → 0.5 → 2 → 3 → (5,6 em paralelo) → 7 → 8.

Caso encontre dependencies entre épicos, respeite-as (ex.: task 5.1 dependendo de 3.x faz 5 depois de 3).

Produzir 3 blocos Mermaid:

Mindmap (hierarquia): épicos → tarefas.

Flowchart (DAG): épicos + tarefas-chave (opcional) com setas de dependência.

Gantt (cronograma): épicos com after/datas e, opcionalmente, sub-tarefas.

2) Regras de mapeamento
2.1 Mindmap (hierarquia)
Use apenas nós (sem setas).

Agrupe por épico.

Para tarefas, inclua emoji por tdd_phase:

red: 🟥 | green: 🟩 | refactor: 🟨 | analysis/doc: 🟪

Label da task: 🟥 <id> — <title> (Xmin).

2.2 Flowchart (dependências)
flowchart LR.

Nós de épicos (retângulos).

Nós de tarefas-chave (opcional) para ilustrar cadeia crítica:

Critério simples: pegue as 2–4 tasks com maior estimate_minutes de cada épico ou com mais dependências.

Desenhe setas de épico → épico com base na ordem macro (ou dependências reais).

Desenhe setas task → task quando task depende de outra task do mesmo épico (para exemplo).

2.3 Gantt (cronograma)
gantt + dateFormat YYYY-MM-DD.

title ETL Debrito — Cronograma.

Cada épico recebe um bloco com id curto (e<epic.id>), duração aproximada:

Duração em dias = ceil( (Σ estimate_minutes) / (6h * 60) ).

Assuma 6h efetivas/dia (buffers, reuniões, revisões).

Dependências:

after eX quando épico Y depende de X.

Opcional: aninhar algumas tarefas-marco (as maiores) dentro do épico, em horas:

Horas = ceil(estimate_minutes / 60) (mín. 1h).

3) Estilo Mermaid
Mindmap — exemplo de cabeçalho
mermaid
Copiar
Editar
mindmap
  root((🧠 ETL Debrito<br/>Sistema Interativo<br/>de Warnings))
Flowchart — exemplo de cabeçalho
mermaid
Copiar
Editar
flowchart LR
  classDef epic fill:#eef,stroke:#55f,stroke-width:1px;
  classDef task fill:#efe,stroke:#393,stroke-width:1px;
Gantt — exemplo de cabeçalho
mermaid
Copiar
Editar
gantt
  title ETL Debrito — Cronograma (épicos & marcos)
  dateFormat  YYYY-MM-DD
  axisFormat  %d/%m
4) Saídas esperadas (3 blocos)
4.1 Mindmap (gerado)
Estrutura:

root

<Epic N: Nome> - 🟥 <id> — <title> (Xmin) - 🟩 <id> — <title> (Xmin) - 🟨 <id> — <title> (Xmin) - 🟪 <id> — <title> (Xmin)
Exemplo sintético (use os seus dados reais):

mermaid
Copiar
Editar
mindmap
  root((🧠 ETL Debrito<br/>Sistema Interativo<br/>de Warnings))
    📌 Épicos TDD
      🧯 Epic 0: Environment & Safety
        🟪 0.1a — Analisar variáveis de ambiente (10min)
        🟥 0.1b.1 — TEST: should_block_prod_writes (10min)
        🟩 0.1b.2 — IMPL: guardião de produção (10min)
        🟨 0.1b.3 — REFACTOR: centralizar check (10min)
      🖲️ Epic 3: Interactive Warning System
        🟥 3.1b.1 — TEST: capture <10ms (5min)
        🟩 3.1b.2 — IMPL: interceptor mínimo (8min)
        🟨 3.1b.3 — REFACTOR (10min)
4.2 Flowchart (gerado)
Nós de épicos, setas na ordem correta, e tarefas-chave opcionais.

mermaid
Copiar
Editar
flowchart LR
  classDef epic fill:#eef,stroke:#55f,stroke-width:1px;
  classDef task fill:#efe,stroke:#393,stroke-width:1px;

  E0[Epic 0]:::epic --> E05[Epic 0.5]:::epic --> E2[Epic 2]:::epic --> E3[Epic 3]:::epic
  E3 --> E5[Epic 5]:::epic --> E7[Epic 7]:::epic
  E2 --> E6[Epic 6]:::epic
  E5 --> E8[Epic 8]:::epic
  E3 --> E4[Epic 4]:::epic

  T3a[Teste: capture <10ms]:::task --> T3b[Impl interceptor]:::task --> T3c[Refactor]:::task
  E3 --> T3a
4.3 Gantt (gerado)
Duração por épico calculada de estimate_minutes.

Dependências com after.

mermaid
Copiar
Editar
gantt
  title ETL Debrito — Cronograma (épicos & marcos)
  dateFormat  YYYY-MM-DD
  axisFormat  %d/%m

  section Fundações
  Epic 0: Environment & Safety          :done,   e0,  2025-08-11, 2d
  Epic 0.5: Architecture Fixes          :active, e05, after e0,   2d

  section Núcleo
  Epic 2: Discovery & Compatibility     :        e2,  after e05, 3d
  Epic 3: Interactive Warning System    :        e3,  after e2,  4d
  Epic 5: Cache Management              :        e5,  after e3,  3d
  Epic 7: Missing Caches Integration    :        e7,  after e5,  2d

  section Dados & Produtividade
  Epic 6: Data Migration & Issues       :        e6,  after e2,  3d
  Epic 4: TDAH Tooling                  :        e4,  after e3,  4d

  section Observabilidade
  Epic 8: Time Monitoring & Analytics   :        e8,  after e5,  3d
Ajuste datas de início conforme sua sprint. Se quiser granularidade por tarefas, aninhe marcos principais com horas (1–3h) usando estimate_minutes/60.

5) Algoritmo (pseudo)
Carregar JSONs → epics[].

Para cada épico:

epic_duration_min = sum(task.estimate_minutes or 10)

epic_duration_days = ceil(epic_duration_min / (6*60))

phase_emoji = {"red":"🟥","green":"🟩","refactor":"🟨"}.get(task.tdd_phase,"🟪")

Construir mindmap:

Para cada épico → nó pai, listar tasks com emoji id — title (Xmin).

Construir flowchart:

Nós: E{id} para épicos.

Setas: ordem macro (ou inferida por dependências).

Tarefas-chave: top-k por estimate_minutes (ex.: k=3) → cadeia simples red→green→refactor quando existir.

Construir gantt:

Blocos: Epic <id> com id: e<id>, duração em dias.

Dependências: after eX conforme grafo macro.

(Opcional) Subtarefas-marco com horas.

6) Saída
Responder com 3 blocos Mermaid prontos (mindmap, flowchart, gantt).

Opcional: salvar também em arquivos:

docs/mindmap.mmd

docs/flow_dependencies.mmd

docs/gantt_schedule.mmd

7) Parâmetros de execução (Claude)
Entrada: lista de caminhos dos JSONs (ex.: ["epico_3.json","epico_4.json","epico_5.json","epico_6.json","epico_7.json"]).

Saída: três blocos Mermaid + sumário:

total de tasks, soma de minutos, duração por épico.