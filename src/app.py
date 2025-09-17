from pages.about import show_about
from pages.reports import show_reports
from pages.analytics import show_analytics
from pages.dashboard import show_dashboard
from utils.database.connection import DatabaseConnection
import streamlit as st
import pandas as pd
import subprocess
import importlib.util
import sys
from pathlib import Path
import threading
import os
from datetime import datetime, timezone
# no in-app log buffer: logs will be written to the Streamlit console only


# Page configuration
st.set_page_config(page_title="SSP Data",
                   page_icon="🛡️",
                   layout="wide",
                   initial_sidebar_state="collapsed")

# Initialize session state
if 'current_page' not in st.session_state:
    st.session_state.current_page = "dashboard"


# Chart theme helper function
def get_chart_theme():
    """Always return dark mode chart theme configuration"""
    return {
        'primary_color': '#1f77b4',
        'text_color': '#fafafa',
        'grid_color': 'rgba(255,255,255,0.1)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'paper_bgcolor': 'rgba(0,0,0,0)'
    }


@st.cache_data(ttl=300)
def carregar_filtros():
    db = DatabaseConnection()
    df_anos = pd.DataFrame(db.fetch_all(
        "SELECT DISTINCT ano FROM ocorrencias ORDER BY ano DESC"), columns=["ano"])
    df_regioes = pd.DataFrame(db.fetch_all(
        "SELECT id, nome FROM regioes ORDER BY nome"), columns=["id", "nome"])
    df_municipios = pd.DataFrame(db.fetch_all("""
        SELECT m.id, m.nome, r.nome AS regiao
        FROM municipios m
        JOIN regioes r ON m.regiao_id = r.id
        ORDER BY m.nome
    """), columns=["id", "nome", "regiao"])
    df_meses_por_ano = pd.DataFrame(db.fetch_all(
        "SELECT DISTINCT ano, mes FROM ocorrencias ORDER BY ano, mes"), columns=["ano", "mes"])
    db.close()
    return df_anos, df_regioes, df_municipios, df_meses_por_ano


df_anos, df_regioes, df_municipios, df_meses_por_ano = carregar_filtros()


@st.cache_data(ttl=300)
def buscar_ocorrencias(ano, regiao, municipio):
    db = DatabaseConnection()
    sql = """
        SELECT o.mes, c.natureza, SUM(o.quantidade) AS total
        FROM ocorrencias o
        JOIN municipios m ON o.municipio_id = m.id
        JOIN regioes r ON m.regiao_id = r.id
        JOIN crimes c ON o.crime_id = c.id
        WHERE o.ano = %s
    """
    params = [ano]
    if regiao != "Todas":
        sql += " AND r.nome = %s"
        params.append(regiao)
    if municipio != "Todos":
        sql += " AND m.nome = %s"
        params.append(municipio)
    sql += " GROUP BY o.mes, c.natureza ORDER BY o.mes"
    df = pd.DataFrame(db.fetch_all(sql, params), columns=[
                      "mes", "natureza", "total"])
    db.close()
    return df


# Chart creation functions import


# Theme functions
def apply_theme_styles():
    """Apply dark mode CSS styles only"""
    st.markdown("""
    <style>
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    
    .stMetric {
        background-color: #262730 !important;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #444;
    }
    
    .stSelectbox > div > div {
        background-color: #262730;
        color: #fafafa;
    }
    
    .stDataFrame {
        background-color: #262730;
    }
    
    .stButton > button {
        background-color: #262730;
        color: #fafafa;
        border: 1px solid #444;
    }
    
    .stButton > button:hover {
        background-color: #444;
        border-color: #666;
    }
    
    .nav-button-active {
        background-color: #1f77b4 !important;
        color: white !important;
    }
    /* Esconde completamente a sidebar e o botão de expandir */
    [data-testid="stSidebar"], section[data-testid="stSidebar"] {
        display: none !important;
    }
    [data-testid="collapsedControl"] {
        display: none !important;
    }
    </style>
    """,
                unsafe_allow_html=True)


# Navigation function
def show_navigation():
    """Display top navigation bar without theme toggle"""
    col1, col2, col3, col4, col5, col6 = st.columns([2, 1, 1, 1, 1, 0.5])

    with col1:
        st.markdown("## 🛡️ SSP Data")

    current_page = st.session_state.get('current_page', 'dashboard')

    with col2:
        if st.button("Dashboard",
                     key="nav_dashboard",
                     help="Dashboard Principal",
                     type="primary"
                     if current_page == "dashboard" else "secondary"):
            st.session_state.current_page = "dashboard"
            st.rerun()

    with col3:
        if st.button("Análises",
                     key="nav_analytics",
                     help="Análises Detalhadas",
                     type="primary"
                     if current_page == "analytics" else "secondary"):
            st.session_state.current_page = "analytics"
            st.rerun()

    with col4:
        if st.button(
                "Relatórios",
                key="nav_reports",
                help="Relatórios",
                type="primary" if current_page == "reports" else "secondary"):
            st.session_state.current_page = "reports"
            st.rerun()

    with col5:
        if st.button(
                "Sobre",
                key="nav_about",
                help="Sobre o Sistema",
                type="primary" if current_page == "about" else "secondary"):
            st.session_state.current_page = "about"
            st.rerun()

    st.divider()


# Apply theme styles and show navigation
apply_theme_styles()
show_navigation()

# Code flag: if True the API will be started automatically on app load.
# Set to True in the source when you want the API to run together with Streamlit.
RUN_API_IN_BACKGROUND = False

if 'api_background' not in st.session_state:
    st.session_state['api_background'] = False
if 'api_process_pid' not in st.session_state:
    st.session_state['api_process_pid'] = None


if RUN_API_IN_BACKGROUND:
    # File-based guard to ensure API is started only once across reloads/sessions
    start_guard = Path('.api.started')
    if not start_guard.exists():
        # Start api.py in-process on a background thread. This is simpler and ensures
        # the API uses the same Python environment as Streamlit.
        api_script = Path('api') / 'api.py'
        try:
            spec = importlib.util.spec_from_file_location('ssp_api_module', str(api_script))
            if not spec or not spec.loader:
                raise RuntimeError('Não foi possível carregar o módulo api')
            api_mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(api_mod)

            if not hasattr(api_mod, 'main'):
                raise RuntimeError('api.py não define a função main()')

            t = threading.Thread(target=getattr(api_mod, 'main'), daemon=True, name='ssp_api_thread')
            t.start()
            # write guard file with timestamp
            try:
                start_guard.write_text(datetime.now(timezone.utc).isoformat())
            except Exception:
                pass
            st.session_state['api_background'] = True
            st.session_state['api_process_pid'] = None
            st.info('API iniciada em-thread (in-process)')
        except Exception as e:
            st.session_state['api_background'] = False
            st.session_state['api_process_pid'] = None
            st.error(f'Falha ao iniciar API em-thread: {e}')
    else:
        print('Arquivo de controle .api.started presente — assumindo que API já foi iniciada anteriormente')

# no in-app log UI: logs go to the console where Streamlit was started


# Import das páginas

# Page routing
if st.session_state.current_page == "dashboard":
    show_dashboard(df_anos, df_regioes, df_municipios, buscar_ocorrencias)
elif st.session_state.current_page == "analytics":
    show_analytics(df_anos, df_regioes, df_meses_por_ano)
elif st.session_state.current_page == "reports":
    show_reports()
elif st.session_state.current_page == "about":
    show_about()

# Footer
st.markdown("---")
st.markdown("SSP-Data")
