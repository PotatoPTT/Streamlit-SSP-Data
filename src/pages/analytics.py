import streamlit as st
from utils.database.connection import DatabaseConnection
import pandas as pd
import joblib
import os
from pathlib import Path
import plotly.graph_objects as go
import plotly.express as px
from tslearn.preprocessing import TimeSeriesScalerMeanVariance


def fetch_data_for_model(db_conn, params):
    """Busca e prepara os dados de séries temporais para o modelo."""
    query = """
        SELECT
            m.nome AS municipio,
            TO_CHAR((o.ano || '-' || LPAD(o.mes::text, 2, '0') || '-01')::date, 'YYYY-MM') AS ano_mes,
            SUM(o.quantidade) AS quantidade
        FROM ocorrencias o
        JOIN municipios m ON o.municipio_id = m.id
        JOIN regioes r ON m.regiao_id = r.id
        JOIN crimes c ON o.crime_id = c.id
        WHERE (o.ano || '-' || LPAD(o.mes::text, 2, '0') || '-01')::date BETWEEN TO_DATE(%s, 'YYYY-MM') AND TO_DATE(%s, 'YYYY-MM')
        AND c.natureza = %s
    """
    sql_params = [params['data_inicio'], params['data_fim'], params['crime']]

    if params['regiao'] != 'Todas':
        query += " AND r.nome = %s"
        sql_params.append(params['regiao'])

    query += " GROUP BY m.nome, ano_mes ORDER BY m.nome, ano_mes;"

    df = pd.DataFrame(db_conn.fetch_all(query, tuple(sql_params)), columns=[
                      'municipio', 'ano_mes', 'quantidade'])

    if df.empty:
        return pd.DataFrame()

    time_series_df = df.pivot_table(
        index='municipio', columns='ano_mes', values='quantidade').fillna(0)
    return time_series_df


def plot_time_series_by_cluster(time_series_df, labels):
    """Plota as séries temporais agrupadas por cluster (normalizadas para visualização)."""
    # Normaliza as séries por série (z-score) para facilitar a comparação visual
    try:
        X = time_series_df.values
        n_ts, n_t = X.shape
        scaler_local = TimeSeriesScalerMeanVariance(mu=0., std=1.)
        X_scaled = scaler_local.fit_transform(X.reshape(n_ts, n_t, 1)).reshape(n_ts, n_t)
        norm_df = pd.DataFrame(X_scaled, index=time_series_df.index, columns=time_series_df.columns)
    except Exception:
        # Se algo falhar, cai para os dados brutos
        norm_df = time_series_df.copy()

    norm_df['cluster'] = labels
    clusters = sorted(norm_df['cluster'].unique())

    for cluster in clusters:
        st.markdown(f"#### Cluster {cluster}")
        fig = go.Figure()
        cluster_data = norm_df[norm_df['cluster'] == cluster].drop('cluster', axis=1)

        for index, row in cluster_data.iterrows():
            fig.add_trace(go.Scatter(
                x=cluster_data.columns, y=row, mode='lines', name=index))

        fig.update_layout(
            title=f'Séries Temporais (normalizadas) - Cluster {cluster}',
            xaxis_title='Mês/Ano',
            yaxis_title='Valor normalizado (z-score)',
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)


def plot_map_by_cluster(db, time_series_df_with_labels):
    """Plota um mapa dos municípios coloridos por cluster."""
    municipios_clusters = time_series_df_with_labels[
        ['cluster']].reset_index()
    # A coluna de índice do time_series_df era 'municipio' — garante nome consistente
    idx_name = municipios_clusters.columns[0]
    if idx_name != 'municipio' and 'municipio' in municipios_clusters.columns:
        municipios_clusters.rename(columns={'municipio': 'nome'}, inplace=True)
    else:
        # Se o reset_index criou outra coluna de nome, tenta padronizar para 'nome'
        municipios_clusters.rename(columns={idx_name: 'nome'}, inplace=True)

    # Busca coordenadas usando query parametrizada para evitar problemas com nomes que contenham apóstrofos
    municipios_nomes = municipios_clusters['nome'].unique().tolist()
    if not municipios_nomes:
        st.warning("Nenhum município disponível para mapear.")
        return

    query = 'SELECT nome, latitude, longitude FROM municipios WHERE nome = ANY(%s);'
    coords = db.fetch_all(query, (municipios_nomes,))
    coords_df = pd.DataFrame(coords, columns=['nome', 'latitude', 'longitude'])
    coords_df.dropna(subset=['latitude', 'longitude'], inplace=True)

    # Merge com os dados de cluster
    map_df = pd.merge(municipios_clusters, coords_df, left_on='nome', right_on='nome')

    if map_df.empty:
        st.warning("Não foi possível gerar o mapa (coordenadas não encontradas).")
        return

    # Garantir que cluster seja categórico para usar cores discretas
    # e definir ordem numérica para a legenda
    # preserva valores originais como strings para exibição, mas ordena numericamente
    map_df['cluster'] = map_df['cluster'].astype(str)
    try:
        ordered_clusters = sorted(map_df['cluster'].unique(), key=lambda x: int(x))
    except Exception:
        # fallback lexicográfico se não for possível converter para int
        ordered_clusters = sorted(map_df['cluster'].unique())
    map_df['cluster'] = pd.Categorical(map_df['cluster'], categories=ordered_clusters, ordered=True)

    # Escolha de paleta com múltiplas cores distintas
    try:
        color_seq = px.colors.qualitative.Dark24
    except Exception:
        # Fallback simples caso não exista
        color_seq = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880']

    # Plotar o mapa com maior altura e cores discretas por cluster
    st.markdown("#### Mapa de Clusters por Município")
    fig = px.scatter_mapbox(
        map_df,
        lat="latitude",
        lon="longitude",
        color="cluster",
        hover_name="nome",
        hover_data=["cluster"],
        mapbox_style="carto-positron",
        zoom=6,
        title="Distribuição Geográfica dos Clusters",
        height=700,
        color_discrete_sequence=color_seq,
        category_orders={'cluster': ordered_clusters}
    )

    # Aumentar o tamanho dos marcadores para melhor visibilidade
    fig.update_traces(marker=dict(size=10))
    fig.update_layout(legend_title_text='Cluster', margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)


def show_analytics(df_anos, df_regioes, df_meses_por_ano):
    """Analytics page"""
    st.markdown("# 📊 Análises Preditivas")
    st.info(
        "Selecione os parâmetros abaixo para gerar ou consultar uma análise preditiva.")

    db = DatabaseConnection()  # Mover a conexão para o início

    # --- Mapeamento de Meses ---
    meses_map = {
        "Janeiro": 1, "Fevereiro": 2, "Março": 3, "Abril": 4, "Maio": 5, "Junho": 6,
        "Julho": 7, "Agosto": 8, "Setembro": 9, "Outubro": 10, "Novembro": 11, "Dezembro": 12
    }
    meses_map_inv = {v: k for k, v in meses_map.items()}
    anos_list = df_anos["ano"].sort_values(ascending=False).tolist()

    # --- Filtros ---
    st.markdown("### Filtros para o Modelo")

    # --- Filtros de Data ---
    st.markdown("##### Período da Análise")
    col_start, col_end = st.columns(2)

    with col_start:
        ano_inicio = st.selectbox("Ano de Início", anos_list, index=0)
        meses_disponiveis_inicio = sorted(df_meses_por_ano[df_meses_por_ano["ano"]
                                                         == ano_inicio]["mes"].unique())
        meses_nomes_inicio = [meses_map_inv[m]
                              for m in meses_disponiveis_inicio]
        mes_inicio = st.selectbox(
            "Mês de Início", meses_nomes_inicio, index=0)

    with col_end:
        anos_fim_disponiveis = [
            ano for ano in anos_list if ano >= ano_inicio]
        ano_fim = st.selectbox(
            "Ano de Fim", anos_fim_disponiveis, index=0)

        meses_disponiveis_fim = sorted(df_meses_por_ano[df_meses_por_ano["ano"]
                                                      == ano_fim]["mes"].unique())

        if ano_fim == ano_inicio:
            mes_inicio_num = meses_map[mes_inicio]
            meses_fim_filtrados = [
                m for m in meses_disponiveis_fim if m >= mes_inicio_num]
        else:
            meses_fim_filtrados = meses_disponiveis_fim

        meses_nomes_fim = [meses_map_inv[m] for m in meses_fim_filtrados]
        mes_fim_index = len(
            meses_nomes_fim) - 1 if meses_nomes_fim else 0
        mes_fim = st.selectbox(
            "Mês de Fim", meses_nomes_fim, index=mes_fim_index)

    # --- Filtro de Região ---
    st.markdown("##### Localização")
    regioes_list = ["Todas"] + df_regioes["nome"].tolist()
    regiao_selecionada = st.selectbox("Região", regioes_list)

    # --- Filtro de Crime ---
    st.markdown("##### Tipo de Crime")
    # Busca a lista de crimes do banco de dados
    crimes_list = db.fetch_all("SELECT natureza FROM crimes ORDER BY natureza;")
    # Transforma a lista de tuplas em uma lista de strings
    crimes_list = [crime[0] for crime in crimes_list]
    crime_selecionado = st.selectbox("Natureza do Crime", crimes_list)

    # --- Lógica de Parâmetros ---
    if not mes_fim:
        st.error("Não há meses disponíveis para o ano final selecionado.")
        return

    mes_inicio_num = meses_map[mes_inicio]
    mes_fim_num = meses_map[mes_fim]

    params = {
        "data_inicio": f"{ano_inicio}-{mes_inicio_num:02d}",
        "data_fim": f"{ano_fim}-{mes_fim_num:02d}",
        "regiao": regiao_selecionada,
        "crime": crime_selecionado,
        "tipo_modelo": "predicao_ocorrencias"
    }

    # --- Verificação de Solicitações Existentes para os dois métodos (kmeans e kdba) ---
    params_k = params.copy()
    params_k['metodo'] = 'kmeans'
    params_d = params.copy()
    params_d['metodo'] = 'kdba'

    solicit_k = db.get_solicitacao_by_params(params_k)
    solicit_d = db.get_solicitacao_by_params(params_d)

    # Helper to display status label
    def label_for(s, name):
        if not s:
            return f"{name}: (nenhuma)"
        return f"{name}: {s['status']}"

    # Se alguma solicitação existe, mostrar seletor de método com status
    if solicit_k or solicit_d:
        options = [
            ('kmeans', label_for(solicit_k, 'K-Means')),
            ('kdba', label_for(solicit_d, 'K-DBA'))
        ]
        # Escolhe por padrão o K-Means se estiver concluído, senão o primeiro disponível
        default = 0
        if solicit_k and solicit_k['status'] == 'CONCLUIDO':
            default = 0
        elif solicit_d and solicit_d['status'] == 'CONCLUIDO':
            default = 1

        choice = st.selectbox('Modelo disponível', [o[1] for o in options], index=default)
        metodo_map = {'K-Means': 'kmeans', 'K-DBA': 'kdba'}
        # Map selection back to method key
        selected_method = ['kmeans', 'kdba'][[o[1] for o in options].index(choice)]
        selected_solicit = solicit_k if selected_method == 'kmeans' else solicit_d

        status = selected_solicit['status'] if selected_solicit else None
        if status == 'CONCLUIDO':
            st.success("Modelo concluído — exibindo resultados.")
            model_filename = selected_solicit.get('caminho_artefato')
            if not model_filename:
                st.error("Artefato do modelo não encontrado (valor vazio). Considere gerar o modelo novamente.")
                db.close()
                return

            # Resolve full absolute path: if DB stored absolute path use it,
            # otherwise assume file lives under project_root/output/models
            project_root = Path(__file__).resolve().parents[2]
            if os.path.isabs(model_filename):
                model_full_path = model_filename
            else:
                model_full_path = str(project_root / 'output' / 'models' / model_filename)

            if not os.path.exists(model_full_path):
                st.error(f"Artefato do modelo não encontrado no caminho: {model_full_path}. Considere gerar o modelo novamente.")
                db.close()
                return

            model_data = joblib.load(model_full_path)
            model = model_data['model']
            scaler = model_data['scaler']
            k = model_data.get('k')
            silhouette = model_data.get('silhouette')

            if silhouette is not None:
                st.metric("Score de Silhueta", f"{silhouette:.4f}")
            if k is not None:
                st.metric("Número Ideal de Clusters (K)", k)

            # Buscar dados e gerar visualizações
            time_series_df = fetch_data_for_model(db, params)

            if not time_series_df.empty:
                labels = model.predict(scaler.transform(time_series_df.values))
                time_series_df_with_labels = time_series_df.copy()
                time_series_df_with_labels['cluster'] = labels

                plot_time_series_by_cluster(time_series_df.copy(), labels)
                plot_map_by_cluster(db, time_series_df_with_labels)

                # Tabela com região
                st.markdown("#### Tabela de Municípios por Cluster")
                table_df = time_series_df_with_labels[['cluster']].reset_index()
                table_df.rename(columns={table_df.columns[0]: 'municipio'}, inplace=True)
                municipios_list = table_df['municipio'].unique().tolist()
                if municipios_list:
                    q = 'SELECT m.nome, r.nome FROM municipios m JOIN regioes r ON m.regiao_id = r.id WHERE m.nome = ANY(%s);'
                    rows = db.fetch_all(q, (municipios_list,))
                    regions_df = pd.DataFrame(rows, columns=['municipio', 'regiao'])
                    table_df = table_df.merge(regions_df, on='municipio', how='left')
                else:
                    table_df['regiao'] = None
                display_df = table_df[['municipio', 'regiao', 'cluster']].sort_values('cluster')
                st.dataframe(display_df)

        elif status in ['PENDENTE', 'PROCESSANDO']:
            import time
            st.info(f"Uma solicitação para este modelo já existe e está com o status: **{status}**.")
            st.write("A página verificará o status a cada 10 segundos. Por favor, aguarde.")
            with st.spinner('Aguardando atualização de status...'):
                time.sleep(10)
            st.rerun()

        elif status == 'FALHOU':
            err = selected_solicit.get('mensagem_erro')
            st.error(f"A última tentativa de gerar este modelo falhou: {err}")
            # If failure message indicates missing artifact, offer regeneration
            if err and ('não encontrado' in err or 'ausente' in err):
                st.warning("O artefato associado a esta solicitação está ausente. Deseja regenerar?")
                if st.button("Regenerar modelo"):
                    nova_id = db.create_solicitacao(params_k if selected_method == 'kmeans' else params_d)
                    if nova_id:
                        st.success(f"Solicitação de regeneração criada (ID: {nova_id}).")
                        st.rerun()
                    else:
                        st.error("Não foi possível criar a solicitação de regeneração.")
            else:
                if st.button("Tentar novamente"):
                    nova_id = db.create_solicitacao(params_k if selected_method == 'kmeans' else params_d)
                    if nova_id:
                        st.success(f"Nova solicitação criada com sucesso (ID: {nova_id}). O modelo será treinado em segundo plano.")
                        st.rerun()
                    else:
                        st.error("Não foi possível criar uma nova solicitação. Verifique se os parâmetros já não estão pendentes.")

    else:
        # Nenhuma solicitação encontrada: criar ambas (kmeans e kdba)
        st.warning("Nenhum modelo encontrado com os parâmetros selecionados. Será criada uma solicitação para ambos os métodos: K-Means e K-DBA.")
        with st.form("form_confirmacao"):
            st.write("Deseja solicitar a criação de novos modelos (K-Means e K-DBA) com estes parâmetros? O processo pode levar alguns minutos.")
            submitted = st.form_submit_button("Sim, criar solicitações")

            if submitted:
                id_k = db.create_solicitacao(params_k)
                id_d = db.create_solicitacao(params_d)
                msgs = []
                if id_k:
                    msgs.append(f"K-Means criada (ID: {id_k})")
                else:
                    msgs.append("K-Means não criada (pode já existir)")
                if id_d:
                    msgs.append(f"K-DBA criada (ID: {id_d})")
                else:
                    msgs.append("K-DBA não criada (pode já existir)")
                st.success("; ".join(msgs))
                st.rerun()

    # Fechar a conexão no final da execução da página
    db.close()

