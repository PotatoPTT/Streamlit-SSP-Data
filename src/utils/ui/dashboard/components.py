"""
Funções de renderização da interface do dashboard.
"""

import streamlit as st
import plotly.graph_objects as go
import streamlit.components.v1 as components
import unicodedata
from utils.ui.dashboard.utils import (
    processar_tabela_detalhada, verificar_mapas_disponiveis,
    carregar_conteudo_mapa
)
from utils.core.pipeline_manager import render_pipeline_control
from utils.visualization.pipeline import GraphPipeline
from utils.config.logging import get_logger
from utils.config.constants import MESES_MAP_INV

logger = get_logger("DASHBOARD_UI")


def render_filters_section(df_anos, df_regioes, df_municipios, pipeline_placeholder):
    """Renderiza a seção de filtros e controle de pipeline."""
    st.markdown("### Filtros")
    col1, col2, col3, col4 = st.columns([1, 2, 2, 1])

    with col1:
        year_filter = st.selectbox(
            "Ano",
            df_anos["ano"].sort_values(ascending=False).tolist(),
            key="year_filter"
        )

    with col2:
        region_filter = st.selectbox(
            "Região",
            ["Todas"] + df_regioes["nome"].tolist(),
            key="region_filter"
        )

    with col3:
        # Filtrar municípios baseado na região selecionada
        if region_filter != "Todas":
            mun_opts = df_municipios[df_municipios["regiao"] == region_filter]
        else:
            mun_opts = df_municipios

        # Ordenar municípios ignorando acentos
        mun_opts = mun_opts.assign(
            _nome_sem_acentos=mun_opts["nome"].apply(
                lambda x: unicodedata.normalize('NFKD', x).encode(
                    'ASCII', 'ignore').decode('ASCII')
            )
        ).sort_values("_nome_sem_acentos").drop(columns=["_nome_sem_acentos"])

        municipality_filter = st.selectbox(
            "Município",
            ["Todos"] + mun_opts["nome"].tolist(),
            key="municipality_filter"
        )

    with col4:
        st.write("")
        st.write("")
        render_pipeline_control(pipeline_placeholder)


def render_kpi_section(dados, year_filter):
    """Renderiza a seção de indicadores principais."""
    st.markdown("### Indicadores Principais")
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    with kpi_col1:
        st.metric(
            label="Total de Ocorrências",
            value=f"{dados['total_ocorrencias']:,}".replace(",", "."),
            help="Total de ocorrências registradas no período"
        )

    with kpi_col2:
        st.metric(
            label="Comparação Anual",
            value=f"{dados['variacao']:.2f}%",
            help=f"Variação em relação a {year_filter - 1}"
        )

    with kpi_col3:
        st.metric(
            label="Mês com mais ocorrências",
            value=dados['mes_top_nome'],
            help="Mês com mais ocorrências"
        )

    with kpi_col4:
        st.metric(
            label="Média Mensal de Ocorrências",
            value=f"{dados['media_mensal']:,.0f}".replace(",", "."),
            help="Média mensal de ocorrências"
        )


def render_charts_section(dados):
    """Renderiza a seção de gráficos."""
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.markdown("#### Evolução Mensal")
        fig1 = go.Figure(go.Scatter(
            x=dados['df_mes']["mes"].map(MESES_MAP_INV),
            y=dados['df_mes']["total"],
            mode='lines+markers',
            line=dict(color='royalblue')
        ))
        fig1.update_layout(
            title="Ocorrências por Mês",
            xaxis_title="Mês",
            yaxis_title="Total"
        )
        st.plotly_chart(fig1, width='stretch')

    with chart_col2:
        st.markdown("#### Tipos de Ocorrências")
        fig2 = go.Figure(go.Bar(
            x=dados['df_tipo']["total"],
            y=dados['df_tipo']["natureza"],
            orientation='h',
            marker_color='indianred'
        ))
        fig2.update_layout(
            title="Tipos de Crimes",
            xaxis_title="Total",
            yaxis_title="Crime"
        )
        st.plotly_chart(fig2, width='stretch')


def render_data_table_section(dados):
    """Renderiza a seção da tabela de dados detalhados."""
    st.markdown("#### Dados Detalhados")
    tabela_completa = processar_tabela_detalhada(
        dados['df_dados'], dados['df_anterior'])
    st.dataframe(tabela_completa, width='stretch')


def render_maps_section(year_filter):
    """Renderiza a seção de mapas interativos."""
    st.markdown("#### Mapa Interativo")

    # Verificar se existem mapas para o ano
    mapas_existem, crimes_mapas, mapas_ano_path = verificar_mapas_disponiveis(
        year_filter)

    if not mapas_existem:
        # Gerar mapas sob demanda
        st.info(
            f"🗺️ Mapas não encontrados para {year_filter}. Iniciando geração...")

        log_msg = f"[MAPS] Iniciando geração de mapas para o ano {year_filter}"
        logger.info(log_msg)

        with st.spinner(f"Gerando mapas para o ano {year_filter}..."):
            pipeline = GraphPipeline()
            try:
                pipeline.run(year_filter=int(year_filter))

                success_msg = f"[MAPS] Mapas para {year_filter} gerados com sucesso"
                logger.info(success_msg)

                st.success(f"✅ Mapas para {year_filter} gerados com sucesso!")
            except Exception as e:
                logger.error(
                    f"[MAPS] Erro ao gerar mapas para {year_filter}: {e}")

                st.error(f"❌ Erro ao gerar mapas: {e}")
                return

        # Verificar novamente após geração
        mapas_existem, crimes_mapas, mapas_ano_path = verificar_mapas_disponiveis(
            year_filter)

    if not mapas_existem:
        st.info(f"Nenhum mapa interativo disponível para o ano {year_filter}.")
        return

    # Seletor de crime para o mapa
    crime_mapa = st.selectbox(
        "Tipo de Crime (Mapa)",
        crimes_mapas,
        key="mapa_crime"
    )

    # Encontrar o arquivo correspondente
    crime_file = None
    for f in mapas_ano_path.glob("*.html"):
        if f.stem.replace("_", " ").replace("-", "-") == crime_mapa:
            crime_file = f
            break

    if crime_file and crime_file.exists():
        html_content = carregar_conteudo_mapa(str(crime_file))
        if html_content:
            components.html(html_content, height=600, scrolling=True)
        else:
            st.error("Erro ao carregar o conteúdo do mapa.")
    else:
        st.info("Mapa não encontrado para o filtro selecionado.")
