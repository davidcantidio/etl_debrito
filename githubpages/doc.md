Guia para o Claude Code: publicar um Gantt no GitHub Pages (a partir da documentação oficial)
Objetivo: criar (ou usar) um repositório, habilitar o GitHub Pages e publicar uma página que renderize um Gantt em Mermaid com a evolução do projeto.

1) Premissas e limites
GitHub Pages hospeda sites estáticos diretamente de um repositório. Está disponível em repositórios públicos (Free) e também em privados conforme o plano. 
GitHub Docs

O site publica a partir de uma fonte de publicação (branch/pasta) ou via GitHub Actions. 
GitHub Docs
+1

Respeite limites (tamanho/tempo de build e renderização). 
GitHub Docs
+1

Mermaid é suportado nativamente no GitHub em arquivos Markdown — inclusive gantt — via bloco de código mermaid. 
GitHub Docs
+1

2) Estrutura mínima do repositório
Crie/garanta estes arquivos na raiz (ou na pasta de publicação, ex. /docs):

bash
Copiar
Editar
/.github/workflows/pages.yml   # (opcional) deploy via Actions
/_config.yml                   # (opcional) configurações Jekyll/tema
index.md                       # página inicial com o gráfico Mermaid
README.md                      # (opcional) instruções do projeto
O GitHub Pages procura por index.html, index.md ou README.md como entrada. 
GitHub Docs

3) Habilitar o GitHub Pages
Opção A — Publicação por Branch/Pasta
No repositório, abra Configurações → Pages.

Em Source, selecione o branch (ex.: main) e, se quiser, a pasta (ex.: /docs). Salve. 
GitHub Docs

Opção B — Publicação por GitHub Actions
Use um workflow de deploy (recomendado quando usa Jekyll/geradores). O Pages hoje usa Actions por padrão nos builds Jekyll. 
GitHub Docs
+1

4) (Opcional) Jekyll e Tema
O Pages tem suporte integrado ao Jekyll (Markdown + Liquid) e facilita aplicar temas. 
GitHub Docs
+1

Guia rápido e “Início Rápido” para subir um site de usuário/organização: 
GitHub Docs

Conteúdo em Jekyll (páginas e posts): 
GitHub Docs

Teste localmente com Jekyll quando precisar: 
GitHub Docs

Exemplo mínimo de _config.yml (usando o tema minima):

yml
Copiar
Editar
title: "Roadmap do Projeto"
description: "Gantt da evolução"
theme: minima
5) Página inicial com Gantt (Mermaid)
Crie index.md (na raiz ou em /docs, conforme a fonte de publicação):

markdown
Copiar
Editar
---
layout: default
title: Roadmap (Gantt)
---

# Roadmap do Projeto (Gantt)

> Este gráfico é renderizado diretamente no GitHub Pages usando Mermaid.

```mermaid
gantt
    dateFormat  YYYY-MM-DD
    title       Evolução do Projeto
    excludes    weekends

    section Planejamento
    Definir visão & escopo        :done,    t1, 2025-07-20, 2025-07-25
    Backlog inicial               :active,  t2, 2025-07-26, 2025-07-31

    section Sprint 1
    Setup repositório & Pages     :done,    s1a, 2025-08-01, 2025-08-02
    Pipeline ETL base             :active,  s1b, 2025-08-03, 2025-08-10
    Dashboard MVP                 :         s1c, 2025-08-08, 2025-08-15

    section Sprint 2
    Warnings & validações         :         s2a, 2025-08-16, 2025-08-25
    Relatórios & SEO              :         s2b, 2025-08-18, 2025-08-28

    section Release
    Apresentação cliente          :milestone, rel, 2025-08-30, 1d
yaml
Copiar
Editar

> Como o GitHub **renderiza Mermaid em Markdown**, basta usar o fence ```mermaid```. Evite rodar plugins Mermaid de terceiros. :contentReference[oaicite:11]{index=11}

---

## 6) Publicação via GitHub Actions (opcional, recomendado)

Crie `.github/workflows/pages.yml`:

```yaml
name: Deploy GitHub Pages

on:
  push:
    branches: [ "main" ]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      # (Opcional) Se usar Jekyll, configure Ruby e build
      - name: Setup Ruby
        uses: ruby/setup-ruby@v1
        with:
          ruby-version: '3.3'
      - name: Install Jekyll
        run: |
          gem install jekyll bundler
          bundle init
          bundle add jekyll
      - name: Build site
        run: |
          jekyll build -s . -d ./_site

      - name: Setup Pages
        uses: actions/configure-pages@v5
      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: _site
  deploy:
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    needs: build
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
O Pages usa Actions para compilar Jekyll; verifique logs caso haja erros de build. 
GitHub Docs

7) (Opcional) Domínio personalizado
Se quiser usar um domínio próprio, defina o CNAME nas configurações (ou via DNS se publicar com Actions). 
GitHub Docs
+1

8) Variantes de fonte de publicação
Branch + pasta /docs: coloque index.md em /docs e configure Pages para main + /docs. 
GitHub Docs

Branch raiz: mantenha index.md na raiz (/). 
GitHub Docs

Actions: publique o conteúdo estático gerado em _site. 
GitHub Docs

9) Boas práticas para renderização Mermaid
Prefira blocos pequenos (renderização de arquivos grandes pode falhar ou exibir como texto). 
GitHub Docs

Evite plugins Mermaid adicionais quando usar Mermaid nativo do GitHub. 
GitHub Docs

Para Jekyll, não é obrigatório script extra para Mermaid — a renderização é do próprio GitHub na página final. 
GitHub Docs

10) Modelo de PR para atualizar o Gantt (opcional)
Crie .github/PULL_REQUEST_TEMPLATE.md:

markdown
Copiar
Editar
## Atualização do Gantt
- [ ] Ajustei datas/seções no `index.md` (Mermaid)
- [ ] Mantive `dateFormat` e marcos consistentes
- [ ] Verifiquei preview do Pages após o merge

> Dica: mantenha as seções por sprint e marcos (`milestone`) para acompanhar releases.
11) Checklist para o Claude Code
Criar (ou detectar) repositório do site.

Adicionar index.md com o bloco Mermaid (modelo acima).

(Opcional) Adicionar _config.yml com tema Jekyll.

Configurar GitHub Pages:

Branch/pasta ou Actions (criar pages.yml). 
GitHub Docs
+1

Commit & push → verificar a URL do Pages.

(Opcional) Domínio personalizado (CNAME/DNS). 
GitHub Docs
+1

12) Referências oficiais usadas
O que é GitHub Pages / Início Rápido / Criar site / Fonte de publicação / Limites / Despublicar. 
GitHub Docs
+5
GitHub Docs
+5
GitHub Docs
+5

Jekyll no GitHub Pages (sobre, criar, testar, conteúdo, temas). 
GitHub Docs
+5
GitHub Docs
+5
GitHub Docs
+5

Mermaid no GitHub (diagramas e blocos de código). 
GitHub Docs
+1

Observações finais
O Gantt do Mermaid é simples de manter via Markdown e funciona muito bem no fluxo de PRs. (Mermaid é suportado no GitHub e foi pensado para esse tipo de uso.) 
GitHub Docs

Se o Gantt crescer demais, considere dividir por sprints e linkar páginas internas do Jekyll para manter a renderização estável. 
GitHub Docs

Pronto. Cole este arquivo como GUIDE_PAGES_GANTT.md no repositório e siga a checklist.








Você gosta desta personalidade?



Perguntar ao ChatGPT
