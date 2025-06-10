ETL de Dados Pinterest

Visão Geral do Projeto

Este repositório contém um pipeline de ETL implementado em um Jupyter Notebook (testar_pipeline_real.ipynb) para processar métricas de campanhas do Pinterest, distribuindo-as ao nível de cada pin_id com base em dimensões (idade, gênero e região) e exportando os resultados para abas de modelo em uma mesma planilha.

Objetivos

Extrair dados brutos de abas específicas de uma planilha Google Sheets.

Pré-processar e normalizar colunas (conversão de tipos, limpeza de valores faltantes, padronização de datas).

Distribuir métricas de dimensão (impressões, cliques e custo, além de vídeo assistido 100%) ao nível de cada pin_id, agrupando por campaign_id e date e ponderando por impressões ou custo.

Gravar os resultados consolidados de cada dimensão de volta em abas de modelo na própria planilha (não gera CSV localmente).

Etapas Detalhadas do Pipeline

✅ Leitura e Pré-processamento

[...] (mesmo conteúdo anterior)

📦 Etapa 3 – Enriquecimento, redistribuição e escrita dos dados demográficos

Preparação dos dados demográficos

Aplica prepare_dimension(df_raw, dim, metrics) para limpar colunas (campaign_id, date, dim) e converter métricas (impressions, link_clicks, video_watched_100, cost) para tipo numérico.

Redistribuição das métricas

Gera pesos via build_weights(df_general_ok), usando impressões ou custo por (campaign_id, date).

Distribui proporcionalmente cost com distribute_float e métricas inteiras com distribute_int para cada pin_id.

Constrói df_result, incluindo colunas de contexto (account_name, Campanha, ID_Campanha, URL_do_Anuncio), além de Engajamento_Total, ID e a dimensão (age/gender/region).

Escrita nas abas de destino

Utiliza write_back_destiny(df_result, spreadsheet_id, sheet_name, write_back_flag, dry_run_flag), que:

Cria ou atualiza a aba de modelo correspondente (modelo{Dim}) respeitando a ordem de colunas definida em _FINAL_ORDER[dim].

Quando dry_run_flag=True, valida apenas a estrutura (colunas e tipos) sem sobrescrever dados existentes.

Quando write_back_flag=True, grava os dados transformados in-place na planilha, preservando fórmulas e formatações definidas pelo usuário.

Gera logs detalhados informando quantidade de linhas e colunas atualizadas, e emite warnings caso haja discrepâncias de ordenação ou colunas inesperadas.

[...] (mesmo conteúdo anterior)

🔄 Etapa 4 – Mecanismo de Controle, Logs, Execução Modular e Flags

Iteração Principal

Percorre cada folha em all_raw via:

for sheet, df_raw in all_raw.items():
    out = run_etl_for_sheet(
        sheet=sheet,
        wb_origin_flag=WRITE_BACK_ORIGIN,
        wb_dest_flag=WRITE_BACK_DEST,
        dry_run_dest=DRY_RUN_DEST,
        preloaded_raw=df_raw
    )
    results[sheet] = {"dest": out["dest"], "taxo": out.get("taxo")}

O loop garante execução modular para cada aba, respeitando configurações específicas.

Controle de Flags

WRITE_BACK_ORIGIN: habilita/desabilita gravação in-place das correções na aba de origem (via write_back_origin).

WRITE_BACK_DEST: habilita/desabilita gravação dos resultados nas abas de modelo de destino (via write_back_destiny).

Gravação em lote usando `batchUpdate` garante apenas uma chamada POST mesmo para várias abas de modelo.

DRY_RUN_DEST: quando True, valida apenas estrutura e ordenação de colunas sem sobrescrever dados existentes.

Geração de Logs

Nível DEBUG: registra início e fim de cada sheet, parâmetros utilizados e contagem de linhas e colunas antes e depois.

Nível INFO: highlights de sucesso na execução e escrita de cada aba destino, ex.: "Aba 'modeloIdade' atualizada com 1.200 registros".

Nível WARNING: dispara quando a ordem de colunas difere de _FINAL_ORDER, ou quando colunas inesperadas são encontradas (mas não falha a execução para destino).

Tratamento de Erros e Exceções

Bloqueia falha total se uma sheet específica falhar; coleta exceções e continua a próxima sheet.

As exceções e stacks são logadas para auditoria posterior.

Retorno da Função

Ao fim do loop, execute_pipeline() retorna o dicionário results com detalhes de destino (dest) e eventuais dados de taxonomia (taxo).

Esta arquitetura garante separação clara entre a lógica de ETL para cada aba e o controle central de execução, permitindo ajustes via flags sem alterar o fluxo principal.

[...] (mesmo conteúdo anterior)

✅ Considerações Finais

O pipeline está preparado para lidar com dados brutos e realizar validações estruturais e métricas.

É flexível para lidar com novas colunas e alterações no schema desde que respeitado o padrão de chaves campaign_id + date.

A execução modular e os logs ricos permitem depuração e auditoria precisas, mesmo em execuções parciais.

O uso do Google Sheets como origem e destino centraliza a manipulação dos dados em uma interface familiar para usuários finais, com ganho operacional.

🧠 Apêndice Técnico para Codex

Evolução das colunas por estágio:

Estágio

Colunas

Observações

Origem: pinterestGeral

pin_id, campaign_id, campaign_name, ad_group_name, ad_name, date, account_name, impressions, link_clicks, video_watched_100, cost, utm_content (opcional), placement (opcional)

Aba bruta na planilha; colunas podem variar se UTM content ou placement não estiverem presentes.

df_raw (memória)

Idêntico à aba Origem

Não há modificações, somente leitura inicial.

df_ok (pós-prepare_general)

Todas as de df_raw + Veiculo, ID_Veiculo, Campanha, ID_Campanha, objective, Engajamento_Total, ID, URL_do_Anuncio

Novas colunas de BI; essas são extras em relação à aba de origem e devem ser consideradas somente para destino, não para write-back de origem.

Origem: pinterestIdade/Gê nero/Região

campaign_id, date, <dim> (age/gender/region), impressions, link_clicks, video_watched_100, cost

Aba bruta secundária para cada dimensão; nomes de colunas fixos.

df_dim_prep (pós-prepare_dimension)

Idêntico a cada aba de origem demográfica, com conversão numérica e limpeza de nulos.

Preparação isolada antes de merge.

df_result (após merge_dimension)

Combinação de df_dim_prep com contexto de df_ok: todas as colunas de dimensão, métricas redistribuídas por pin_id, e colunas BI de contexto (account_name, Campanha, ID_Campanha, URL_do_Anuncio, Engajamento_Total, ID)

Contém colunas extras herdadas de df_ok; ordem ainda não aplicada.

Destino: modelo

Mesmas colunas de df_result, reordenadas conforme _FINAL_ORDER[dim].

Remove colunas não listadas em _FINAL_ORDER e ajusta ordem final; grava em abas modeloIdade, modeloGenero, modeloRegiao.

Colunas descartadas em cada fase:

Do df_ok para df_result: nenhuma, pois df_result herda todo o contexto.

Do df_result para modelo<Dim>: quaisquer colunas extras que não apareçam em _FINAL_ORDER[dim] são lançadas (e.g., colunas de debug ou temporárias).

Ponto Crítico (Erro Corrente)

A função write_back_origin(df_raw, df_ok) lança erro ao detectar colunas adicionais.

Essas colunas (ID_Campanha, Veiculo, etc.) são introduzidas intencionalmente após o enriquecimento com dados de BI.

Proposta de solução:

Alterar write_back_origin para comparar apenas colunas em comum (interseção).

Ou registrar colunas extras via logging.warning(...) sem falhar a execução.

Possíveis Melhorias Futuras

Automatizar validações de somatório das métricas com tolerância parametrizável.

Consolidar o dicionário _FINAL_ORDER em um módulo separado para reuso por múltiplos pipelines.

Migrar o controle de execuções para scripts CLI ou agendamentos automatizados (ex: cron ou Airflow).