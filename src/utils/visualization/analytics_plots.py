"""
Módulo para visualizações específicas de análise preditiva e clustering.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from tslearn.preprocessing import TimeSeriesScalerMeanVariance
from utils.config.constants import MESES, MAP_MIN_MARKER_RADIUS, MAP_MAX_MARKER_RADIUS, MAP_CENTER_SP, MAP_INITIAL_ZOOM_SP


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
        st.plotly_chart(fig, width='stretch')


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
    st.plotly_chart(fig, width='stretch')


def display_model_metrics(silhouette, k):
    """Exibe as métricas do modelo."""
    if silhouette is not None:
        st.metric("Score de Silhueta", f"{silhouette:.4f}")
    if k is not None:
        st.metric("Número Ideal de Clusters (K)", k)


def plot_maps_crime_counts_plotly(df_map_data, year=None, crimes=None, max_height=650):
    """
    Plota mapas por crime/ano usando Plotly (inline no Streamlit).

    df_map_data: DataFrame com colunas mínimas ['Ano','Natureza','mes','quantidade','latitude','longitude','Nome_Municipio']
    year: filtro opcional de ano
    crimes: lista opcional de crimes a plotar (por default usa todos presentes)
    max_height: altura padrão do gráfico
    """
    import plotly.graph_objects as go
    import plotly.express as px

    if year is not None:
        df_map_data = df_map_data[df_map_data['Ano'] == year]

    # Agregar por município/lat/lon/Ano/Natureza/mes para reduzir pontos
    # (melhora performance quando há muitos registros por município)
    try:
        df_map_data = df_map_data.copy()
        # garantir tipos
        df_map_data['quantidade'] = pd.to_numeric(df_map_data['quantidade'], errors='coerce').fillna(0).astype(int)
        df_map_data['latitude'] = pd.to_numeric(df_map_data['latitude'], errors='coerce')
        df_map_data['longitude'] = pd.to_numeric(df_map_data['longitude'], errors='coerce')

        df_map_data = (
            df_map_data.groupby(['Nome_Municipio', 'latitude', 'longitude', 'Ano', 'Natureza', 'mes'], dropna=False, as_index=False)
            ['quantidade'].sum()
        )
    except Exception:
        # se algo der errado, prosseguir com os dados brutos
        pass

    if df_map_data.empty:
        st.warning("Nenhum dado de mapa para o filtro selecionado.")
        return

    if crimes is None:
        crimes = df_map_data['Natureza'].unique()

    # paleta para meses: preferir paleta escura/contrastante
    try:
        month_colors = px.colors.qualitative.Dark24[:12]
    except Exception:
        # paleta fallback escura
        month_colors = [
            '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728',
            '#9467bd', '#8c564b', '#e377c2', '#7f7f7f',
            '#bcbd22', '#17becf', '#393b79', '#637939'
        ]

    def _norm_radius_array(vals):
        """
        Normaliza tamanhos usando transformação sqrt para reduzir a influência de outliers
        (como municípios com valores muito altos — ex: São Paulo). Retorna lista de pixels.
        """
        vals = pd.to_numeric(vals, errors='coerce').fillna(0).astype(float)
        # usar transformação sqrt para comprimir a escala
        vals_sqrt = vals.copy()
        vals_sqrt[vals_sqrt > 0] = vals_sqrt[vals_sqrt > 0].pow(0.5)

        pos = vals_sqrt[vals_sqrt > 0]
        min_val = pos.min() if (pos > 0).any() else 0.0
        max_val = pos.max() if not pos.empty else 1.0

        min_r, max_r = MAP_MIN_MARKER_RADIUS, MAP_MAX_MARKER_RADIUS
        if max_val == min_val:
            return [min_r if v <= 0 else (min_r + (max_r - min_r) / 2) for v in vals_sqrt]

        sizes = []
        for original, v in zip(vals, vals_sqrt):
            if original <= 0:
                sizes.append(min_r)
            else:
                sizes.append(min_r + (max_r - min_r) * (v - min_val) / (max_val - min_val))
        return sizes

    st.markdown("#### Mapas - Distribuição por Município")
    for crime in crimes:
        crime_df = df_map_data[df_map_data['Natureza'] == crime]
        if crime_df.empty:
            continue

        fig = go.Figure()

        # para cada mês adiciona um trace separado (equivalente às layers do folium)
        for idx, month in enumerate(MESES, start=1):
            month_df = crime_df[crime_df['mes'] == idx]
            if month_df.empty:
                continue

            lat = month_df['latitude']
            lon = month_df['longitude']
            nomes = month_df['Nome_Municipio'].astype(str)
            qty = month_df['quantidade']
            sizes = _norm_radius_array(qty)

            hover = [
                f"{n}<br>{crime}: {q} ({month})"
                for n, q in zip(nomes, qty)
            ]

            # definir visibilidade: só o último mês presente deve iniciar visível
            try:
                last_month_present = int(crime_df['mes'].max())
            except Exception:
                last_month_present = None

            visible_state = True if (last_month_present is not None and idx == last_month_present) else 'legendonly'

            fig.add_trace(go.Scattermapbox(
                lat=lat,
                lon=lon,
                mode='markers',
                marker=dict(
                    size=sizes,
                    color=month_colors[(idx-1) % len(month_colors)],
                    opacity=0.9,
                    sizemode='area'
                ),
                name=month,
                hoverinfo='text',
                hovertext=hover,
                visible=visible_state
            ))

        fig.update_layout(
            mapbox_style="carto-positron",
            title=f"{crime} — {year if year is not None else 'Todos anos'}",
            margin=dict(l=0, r=0, t=40, b=0),
            height=max_height,
            mapbox=dict(center=MAP_CENTER_SP, zoom=MAP_INITIAL_ZOOM_SP)
        )
        # tamanho dos marcadores padrão
        try:
            fig.update_traces(marker=dict(sizemode='area'))
        except Exception:
            pass

        st.plotly_chart(fig, use_container_width=True)