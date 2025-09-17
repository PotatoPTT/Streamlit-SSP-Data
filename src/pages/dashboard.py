import streamlit as st
from utils.dashboard_utils import processar_dados_dashboard
from utils.dashboard_ui import (
    render_filters_section, render_kpi_section, render_charts_section,
    render_data_table_section, render_maps_section
)
from utils.api.config import get_logger

logger = get_logger("DASHBOARD")


def show_dashboard(df_anos, df_regioes, df_municipios, buscar_ocorrencias):
    """Dashboard principal com cache otimizado."""
    
    # Inicializar estado do pipeline
    if 'pipeline_output' not in st.session_state:
        st.session_state['pipeline_output'] = []
    
    pipeline_placeholder = st.empty()
    
    # === SEÇÃO DE FILTROS ===
    render_filters_section(df_anos, df_regioes, df_municipios, pipeline_placeholder)
    
    # Obter valores dos filtros
    year_filter = st.session_state.get('year_filter', df_anos["ano"].max())
    region_filter = st.session_state.get('region_filter', "Todas")
    municipality_filter = st.session_state.get('municipality_filter', "Todos")
    
    st.divider()
    
    # === PROCESSAR DADOS COM CACHE ===
    dados = processar_dados_dashboard(year_filter, region_filter, municipality_filter, buscar_ocorrencias)
    
    # === SEÇÃO DE KPIs ===
    render_kpi_section(dados, year_filter)
    
    # === SEÇÃO DE GRÁFICOS ===
    render_charts_section(dados)
    
    # === TABELA DETALHADA ===
    render_data_table_section(dados)
    
    # === SEÇÃO DE MAPAS ===
    render_maps_section(year_filter)
