import streamlit as st
from utils.ui.dashboard.utils import processar_dados_dashboard
from utils.ui.dashboard.components import (
    render_filters_section, render_kpi_section, render_charts_section,
    render_data_table_section, render_maps_section
)
from utils.config.logging import get_logger

logger = get_logger("DASHBOARD")


def show_dashboard(df_anos, df_regioes, df_municipios, buscar_ocorrencias):
    """Dashboard principal com cache otimizado."""
    
    # Inicializar estado do pipeline
    if 'pipeline_output' not in st.session_state:
        st.session_state['pipeline_output'] = []

    layout_cols = st.columns([1, 14, 1])
    with layout_cols[1]:
        with st.container(border=True):
            pipeline_placeholder = st.empty()

            # === SEÇÃO DE FILTROS ===
            render_filters_section(df_anos, df_regioes, df_municipios, pipeline_placeholder)

            # Obter valores dos filtros
            year_filter = st.session_state.get('year_filter', df_anos["ano"].max())
            region_filter = st.session_state.get('region_filter', "Todas")
            municipality_filter = st.session_state.get('municipality_filter', "Todos")

        with st.container(border=True):
            # === PROCESSAR DADOS COM CACHE (DB calls cached internamente) ===
            df_dados = buscar_ocorrencias(year_filter, region_filter, municipality_filter)
            df_anterior = buscar_ocorrencias(year_filter - 1, region_filter, municipality_filter)
            dados = processar_dados_dashboard(df_dados, df_anterior)

            # === SEÇÃO DE KPIs ===
            render_kpi_section(dados, year_filter)

            # === SEÇÃO DE GRÁFICOS ===
            render_charts_section(dados)

            # === TABELA DETALHADA ===
            render_data_table_section(dados)

            # === SEÇÃO DE MAPAS ===
            render_maps_section(year_filter)
