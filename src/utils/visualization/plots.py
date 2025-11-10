"""
Módulo para visualizações específicas de análise preditiva e clustering.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from sklearn.preprocessing import RobustScaler
from utils.config.constants import MESES, MAP_MIN_MARKER_RADIUS, MAP_MAX_MARKER_RADIUS, MAP_CENTER_SP, MAP_INITIAL_ZOOM_SP


def plot_silhouette_by_cluster(features, labels):
    """Exibe a silhueta média por cluster em formato de gráfico e tabela."""
    try:
        from sklearn.metrics import silhouette_samples
    except ImportError:  # pragma: no cover - fallback para ambientes sem sklearn
        st.warning("Biblioteca scikit-learn não disponível para calcular a silhueta por cluster.")
        return

    if features is None or labels is None:
        return

    if isinstance(labels, (list, tuple)):
        labels_array = np.asarray(labels)
    elif isinstance(labels, pd.Series):
        labels_array = labels.values
    else:
        labels_array = np.asarray(labels)

    unique_clusters = pd.unique(labels_array)
    if len(unique_clusters) < 2:
        return

    if isinstance(features, pd.DataFrame):
        features_array = features.values
    else:
        features_array = np.asarray(features)

    if features_array.ndim == 3 and features_array.shape[-1] == 1:
        features_array = features_array.reshape(features_array.shape[0], -1)
    elif features_array.ndim > 2:
        features_array = features_array.reshape(features_array.shape[0], -1)

    if features_array.ndim != 2:
        st.warning("Não foi possível converter os dados dos clusters para formato compatível com a silhueta.")
        return

    try:
        sample_scores = silhouette_samples(features_array, labels_array)
    except Exception as err:
        st.warning(f"Não foi possível calcular a silhueta por cluster: {err}")
        return

    scores_df = pd.DataFrame({"cluster": labels_array, "silhueta": sample_scores})
    summary_df = (
        scores_df
        .groupby("cluster", as_index=False)
        .agg(
            silhueta_media=("silhueta", "mean"),
            silhueta_mediana=("silhueta", "median"),
            silhueta_minima=("silhueta", "min"),
            silhueta_maxima=("silhueta", "max"),
            quantidade=("silhueta", "count")
        )
        .sort_values("cluster")
    )

    st.markdown("#### valores brutos silhueta")
    fig = px.bar(
        summary_df,
        x="cluster",
        y="silhueta_media",
        color="cluster",
        text="silhueta_media",
        labels={
            "cluster": "Cluster",
            "silhueta_media": "Silhueta média"
        },
        title="Silhueta Média"
    )
    fig.update_traces(texttemplate="%{text:.3f}", textposition="outside")
    fig.update_layout(showlegend=False, yaxis_tickformat=".3f", margin=dict(t=60, b=0))
    st.plotly_chart(fig)

    st.dataframe(
        summary_df.rename(columns={
            "cluster": "Cluster",
            "silhueta_media": "media",
            "silhueta_mediana": "mediana",
            "silhueta_minima": "minima",
            "silhueta_maxima": "maxima",
            "quantidade": "municipios"
        }),
        width='stretch'
    )


def plot_time_series_by_cluster(time_series_df, labels, model=None):
    """Plota as séries temporais agrupadas por cluster (normalizadas para visualização)."""
    from utils.config.logging import get_logger
    logger = get_logger("PLOTS")
    
    # Normaliza as séries usando RobustScaler para facilitar a comparação visual
    try:
        X = time_series_df.values
        # logger.info(f"📊 Normalizando dados para plotagem...")
        # logger.info(f"   Dados originais - min: {X.min():.2f}, max: {X.max():.2f}, shape: {X.shape}")
        # logger.info(f"   Exemplo São Paulo (se existir): {time_series_df.loc['São Paulo'].values if 'São Paulo' in time_series_df.index else 'N/A'}")
        
        # IMPORTANTE: RobustScaler normaliza por COLUNA (features), mas queremos normalizar por LINHA (cada série temporal)
        # Precisamos normalizar cada município individualmente (como no treinamento)
        scaler_local = RobustScaler()
        
        # Normalizar cada série temporal individualmente (linha por linha)
        X_scaled = np.array([
            RobustScaler().fit_transform(X[i, :].reshape(-1, 1)).flatten() 
            for i in range(X.shape[0])
        ])
        
        # logger.info(f"   Dados normalizados - min: {X_scaled.min():.2f}, max: {X_scaled.max():.2f}")
        
        norm_df = pd.DataFrame(X_scaled, index=time_series_df.index, columns=time_series_df.columns)
        
       
    except Exception as e:
        # Se algo falhar, cai para os dados brutos
        logger.error(f"✗ ERRO ao normalizar dados: {e}. Usando dados brutos!")
        import traceback
        logger.error(traceback.format_exc())
        norm_df = time_series_df.copy()

    norm_df['cluster'] = labels
    clusters = sorted(norm_df['cluster'].unique())
    
    # Calcular centróides normalizados para cada cluster
    centroids_normalized = {}
    if model is not None and hasattr(model, 'cluster_centers_'):
        try:
            # Normalizar os centróides do modelo da mesma forma que os dados (individualmente)
            centroids = model.cluster_centers_.squeeze()
            if centroids.ndim == 1:
                centroids = centroids.reshape(1, -1)
            
            # Normalizar cada centróide individualmente (como fizemos com as séries)
            centroids_norm = np.array([
                RobustScaler().fit_transform(centroids[i, :].reshape(-1, 1)).flatten() 
                for i in range(centroids.shape[0])
            ])
            
            for i, cluster_id in enumerate(clusters):
                if i < len(centroids_norm):
                    centroids_normalized[cluster_id] = centroids_norm[i]
        except Exception as e:
            # Se falhar, não adiciona centróides
            logger.warning(f"Erro ao normalizar centróides: {e}")
            pass

    for cluster in clusters:
        # Contar número de cidades no cluster
        cluster_data = norm_df[norm_df['cluster'] == cluster].drop('cluster', axis=1)
        num_cities = len(cluster_data)
        
        
        
        st.markdown(f"#### Cluster {cluster} - {num_cities} {'cidade' if num_cities == 1 else 'cidades'}")
        fig = go.Figure()

        # Plotar as séries temporais de cada município
        for index, row in cluster_data.iterrows():
            # DEBUG: Log para São Paulo especificamente
            
            
            # Criar texto de hover customizado para confirmar normalização
            hover_text = [
                f"{index}<br>Período: {col}<br>Valor normalizado: {val:.3f}"
                for col, val in zip(cluster_data.columns, row.values)
            ]
            
            fig.add_trace(go.Scatter(
                x=cluster_data.columns, 
                y=row.values,  # Usar .values para garantir que são os valores normalizados
                mode='lines', 
                name=index,
                line=dict(width=1),
                opacity=0.5,
                hovertext=hover_text,
                hoverinfo='text'))

        # Adicionar centróide ao gráfico (se disponível)
        if cluster in centroids_normalized:
            fig.add_trace(go.Scatter(
                x=cluster_data.columns,
                y=centroids_normalized[cluster],
                mode='lines+markers',
                name=f'Centróide Cluster {cluster}',
                line=dict(color='red', width=3, dash='solid'),
                marker=dict(size=8, color='red', symbol='diamond'),
                opacity=1.0
            ))

        fig.update_layout(
            title=f'Séries Temporais (normalizadas) - Cluster {cluster} ({num_cities} cidades)',
            xaxis_title='Mês/Ano',
            yaxis_title='Valor normalizado (RobustScaler)',
            showlegend=True,
            hovermode='closest'
        )
        st.plotly_chart(fig)


def plot_centroids_comparison(time_series_df, labels, model):
    """Plota um gráfico comparativo mostrando apenas os centróides de cada cluster."""
    if model is None or not hasattr(model, 'cluster_centers_'):
        st.warning("Centróides não disponíveis para este modelo.")
        return
    
    try:
        # Normaliza os dados usando RobustScaler (individualmente por série)
        X = time_series_df.values
        
        # Normalizar cada série temporal individualmente
        X_scaled = np.array([
            RobustScaler().fit_transform(X[i, :].reshape(-1, 1)).flatten() 
            for i in range(X.shape[0])
        ])
        
        # Normalizar os centróides do modelo (individualmente)
        centroids = model.cluster_centers_.squeeze()
        if centroids.ndim == 1:
            centroids = centroids.reshape(1, -1)
        
        # Normalizar cada centróide individualmente
        centroids_norm = np.array([
            RobustScaler().fit_transform(centroids[i, :].reshape(-1, 1)).flatten() 
            for i in range(centroids.shape[0])
        ])
        
        # Criar DataFrame com os centróides normalizados
        norm_df = pd.DataFrame(X_scaled, index=time_series_df.index, columns=time_series_df.columns)
        norm_df['cluster'] = labels
        clusters = sorted(norm_df['cluster'].unique())
        
        # Contar cidades por cluster
        cluster_counts = norm_df['cluster'].value_counts().sort_index()
        
        # Criar gráfico com cores distintas
        st.markdown("#### Comparação de Centróides entre Clusters")
        fig = go.Figure()
        
        # Paleta de cores para diferenciar clusters
        colors = px.colors.qualitative.Plotly
        
        for i, cluster_id in enumerate(clusters):
            if i < len(centroids_norm):
                num_cities = cluster_counts.get(cluster_id, 0)
                fig.add_trace(go.Scatter(
                    x=time_series_df.columns,
                    y=centroids_norm[i],
                    mode='lines+markers',
                    name=f'Cluster {cluster_id} ({num_cities} cidades)',
                    line=dict(width=3, color=colors[i % len(colors)]),
                    marker=dict(size=8, symbol='diamond'),
                    opacity=1.0
                ))
        
        fig.update_layout(
            title='Centróides dos Clusters',
            xaxis_title='Mês/Ano',
            yaxis_title='Valor normalizado (RobustScaler)',
            showlegend=True,
            hovermode='x unified',
            height=600,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            )
        )
        st.plotly_chart(fig)
        
    except Exception as e:
        st.error(f"Erro ao plotar centróides: {e}")


def plot_map_by_cluster(db, time_series_df_with_labels):
    """Plota um mapa dos municípios coloridos por cluster."""
    municipios_clusters = time_series_df_with_labels[
        ['cluster']].reset_index()
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
    coords_df = db.fetch_df(query, (municipios_nomes,), columns=['nome', 'latitude', 'longitude'])
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

    # Contar número de cidades por cluster
    cluster_counts = map_df['cluster'].value_counts().to_dict()
    
    # Adicionar coluna com a contagem de cidades no cluster
    map_df['num_cidades_cluster'] = map_df['cluster'].map(cluster_counts)
    
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
        hover_data={
            "cluster": True,
            "num_cidades_cluster": ":,.0f",
            "latitude": False,
            "longitude": False
        },
        mapbox_style="carto-positron",
        zoom=6,
        title="Distribuição Geográfica dos Clusters",
        height=700,
        color_discrete_sequence=color_seq,
        category_orders={'cluster': ordered_clusters},
        labels={
            "num_cidades_cluster": "Cidades no cluster",
            "cluster": "Cluster"
        }
    )

    # Aumentar o tamanho dos marcadores para melhor visibilidade
    fig.update_traces(marker=dict(size=10))
    
    # Atualizar legenda para incluir contagem de cidades
    for cluster in ordered_clusters:
        count = cluster_counts.get(cluster, 0)
        fig.for_each_trace(
            lambda trace: trace.update(name=f"Cluster {trace.name} ({count} cidades)")
            if trace.name == cluster else ()
        )
    
    fig.update_layout(legend_title_text='Cluster', margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig)


def get_silhouette_group(silhouette_score):
    """
    Classifica o modelo em grupos baseado no Silhouette Score.
    
    Args:
        silhouette_score: Score de silhueta do modelo
        
    Returns:
        tuple: (grupo, cor) onde grupo é a letra (A, B, C, D) e cor é a cor do grupo
    """
    if silhouette_score is None:
        return "N/A", "gray"
    
    if 0.71 <= silhouette_score <= 1.0:
        return "A", "green"
    elif 0.51 <= silhouette_score <= 0.70:
        return "B", "blue"
    elif 0.26 <= silhouette_score <= 0.50:
        return "C", "orange"
    else:  # SC <= 0.25
        return "D", "red"


def display_model_metrics(silhouette, k):
    """
    Exibe as métricas do modelo.
    Integrado com Clustering Project: testa K de 2 a 15 e escolhe melhor via silhouette.
    """
    st.markdown("#### 🎯 Métricas do Modelo")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if k is not None:
            st.metric("Número de Clusters (K)", k, help="Melhor K encontrado após testar de 2 a 15")
        else:
            st.metric("Número de Clusters (K)", "N/A")
    
    with col2:
        if silhouette is not None:
            st.metric("Score de Silhueta", f"{silhouette:.4f}", 
                     help="Maior score de silhueta entre todos os K testados (2 a 15)")
        else:
            st.metric("Score de Silhueta", "N/A", help="Score não disponível para este modelo")
    
    with col3:
        # Classificação do modelo baseado no silhouette score
        grupo, cor = get_silhouette_group(silhouette)
        
        if grupo != "N/A":
            # Definir descrição do grupo
            grupo_descricoes = {
                "A": "Excelente (0.71-1.0)",
                "B": "Bom (0.51-0.70)",
                "C": "Moderado (0.26-0.50)",
                "D": "Fraco (≤0.25)"
            }
            
            # Criar HTML customizado para o grupo
            grupo_html = f"""
            <div style="
                padding: 10px;
                border-radius: 8px;
                background-color: {cor}33;
                border: 2px solid {cor};
                text-align: center;
                margin-top: 8px;
            ">
                <div style="font-size: 14px; color: #666; margin-bottom: 4px;">
                    Classificação
                </div>
                <div style="font-size: 32px; font-weight: bold; color: {cor};">
                    GRUPO {grupo}
                </div>
                <div style="font-size: 12px; color: #666; margin-top: 4px;">
                    {grupo_descricoes[grupo]}
                </div>
            </div>
            """
            st.markdown(grupo_html, unsafe_allow_html=True)
        else:
            st.metric("Classificação", "N/A", help="Score de silhueta não disponível")
    
    st.markdown("---")


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
        (como municípios com valores muito altos ex: São Paulo). Retorna lista de pixels.
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
            title=f"{crime} - {year if year is not None else 'Todos anos'}",
            margin=dict(l=0, r=0, t=40, b=0),
            height=max_height,
            mapbox=dict(center=MAP_CENTER_SP, zoom=MAP_INITIAL_ZOOM_SP)
        )
        # tamanho dos marcadores padrão
        try:
            fig.update_traces(marker=dict(sizemode='area'))
        except Exception:
            pass

        st.plotly_chart(fig)