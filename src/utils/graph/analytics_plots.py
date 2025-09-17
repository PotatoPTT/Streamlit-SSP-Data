"""
Módulo para visualizações específicas de análise preditiva e clustering.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from tslearn.preprocessing import TimeSeriesScalerMeanVariance


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


def display_model_metrics(silhouette, k):
    """Exibe as métricas do modelo."""
    if silhouette is not None:
        st.metric("Score de Silhueta", f"{silhouette:.4f}")
    if k is not None:
        st.metric("Número Ideal de Clusters (K)", k)