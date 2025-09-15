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
RUN_API_IN_BACKGROUND = True

if 'api_background' not in st.session_state:
    st.session_state['api_background'] = False
if 'api_process_pid' not in st.session_state:
    st.session_state['api_process_pid'] = None


if RUN_API_IN_BACKGROUND and not st.session_state.get('api_started_once', False):
    # Very simple: start the API subprocess once when the app is first loaded.
    api_script = Path('api') / 'api.py'
    python_exec = sys.executable or None
    if not python_exec and sys.platform.startswith('win'):
        venv_python = Path('.') / '.venv' / 'Scripts' / 'python.exe'
        if venv_python.exists():
            python_exec = str(venv_python)

        st.session_state['api_started_once'] = True
        if python_exec:
            try:
                # simple PID file guard to avoid repeated starts across reruns/sessions
                pid_file = Path('.api.pid')
                if pid_file.exists():
                    try:
                        existing_pid = int(pid_file.read_text().strip())
                        # check if process exists
                        try:
                            os.kill(existing_pid, 0)
                            st.session_state['api_process_pid'] = existing_pid
                            st.session_state['api_background'] = True
                            print(f'API appears to be already running (pid={existing_pid}); skipping start')
                        except Exception:
                            # not alive; start a new process and overwrite pid file
                            p = subprocess.Popen([python_exec, str(api_script)])
                            pid_file.write_text(str(p.pid))
                            st.session_state['api_process_pid'] = p.pid
                            st.session_state['api_background'] = True
                    except Exception:
                        # malformed pid file — remove and start
                        try:
                            pid_file.unlink()
                        except Exception:
                            pass
                        p = subprocess.Popen([python_exec, str(api_script)])
                        pid_file.write_text(str(p.pid))
                        st.session_state['api_process_pid'] = p.pid
                        st.session_state['api_background'] = True
                else:
                    p = subprocess.Popen([python_exec, str(api_script)])
                    try:
                        pid_file.write_text(str(p.pid))
                    except Exception:
                        pass
                    st.session_state['api_process_pid'] = p.pid
                    st.session_state['api_background'] = True
            except Exception as e:
                st.error(f'Falha ao iniciar API: {e}')
                st.session_state['api_process_pid'] = None
                st.session_state['api_background'] = False
    else:
        st.warning('API não iniciada: python executável não encontrado no ambiente atual.')

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
