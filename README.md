# 📊 Pipeline de ETL para Campanhas Digitais

Este projeto automatiza o processo de extração, transformação e carga (ETL) de dados de campanhas digitais veiculadas em diferentes plataformas como **TikTok**, **Meta (Facebook/Instagram)** e **LinkedIn**. As informações são centralizadas em uma planilha no Google Sheets para facilitar análises e tomadas de decisão.

---

## 🧠 O que o projeto faz

O sistema lê os dados brutos exportados pelas plataformas, que muitas vezes têm formatos e nomes de campos diferentes, e transforma tudo em um padrão único. Ao final, os dados ficam prontos para análise, sem necessidade de ajuste manual.

### Em resumo:
- 📥 **Extrai** dados de diferentes abas no Google Sheets.
- 🛠️ **Transforma** os dados para um formato padronizado:
  - Corrige nomes de colunas,
  - Normaliza nomes de campanhas e regiões,
  - Traduz objetivos de campanha para o português (ex: `TRAFFIC` → `Tráfego`),
  - Gera identificadores únicos para cada linha,
  - Cruza dados de perfomance da campanha com dados do plano
- 📤 **Carrega** apenas os registros novos em abas organizadas por tipo (idade, gênero, região, etc.).

---

## 🧩 Estrutura do projeto

```plaintext
ETL/
├── append_only_new_*.py         # Scripts que processam e atualizam os dados
├── scripts/
│   ├── ELET_etl_geral_meta.py
│   ├── ELET_etl_geral_tiktok.py
│   ├── ELET_etl_genero_tiktok.py
│   ├── ELET_etl_regiao_tiktok.py
│   └── ...                      # ETLs separados por plataforma e dimensão
├── utils/
│   ├── geolocalizacao.py        # Trata nomes de cidades e estados
│   ├── campanha_mapper.py       # Mapeia campanhas e siglas
│   ├── objetivos.py             # Padroniza objetivos de campanha
│   ├── get_campaign_parameterization.py
│   ├── google_sheets.py         # Integra com Google Sheets
│   ├── get_missing_records.py
│   └── ...
├── creds.json                   # (ignorado no Git) Credenciais de acesso
├── requirements.txt             # Dependências do projeto




---

## 🔄 Exemplo de fluxo para TikTok (Região)

1. O script `append_only_new_regiao_tiktok.py` é iniciado.
2. Ele busca os dados brutos da aba `Tiktok Regiao` na planilha.
3. Um algoritmo identifica e corrige o nome da **região**, mesmo que esteja com variações como:
   - `Greater São Paulo Area` → `São Paulo`
   - `Brazil: Maranhão` → `Maranhão`
   - `Unknown`, `-1` → `Não identificado`
4. O **objetivo da campanha** é traduzido para facilitar leitura:
   - `REACH` → `Alcance`
   - `VIDEO_VIEWS` → `Visualização`
5. O script identifica quais registros são novos e os insere na aba `modeloRegiao`.

---

## 💡 Por que isso é útil?

Antes, cada time precisava revisar manualmente os relatórios exportados de cada plataforma. Agora:

- Os dados estão organizados,
- Os nomes são padronizados (ideal para gráficos),
- E tudo é atualizado automaticamente com um só comando.

---

## 🚀 Como executar

Recomenda-se usar um ambiente virtual:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


