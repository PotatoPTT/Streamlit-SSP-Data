from pages.about import show_about
from pages.reports import show_reports
from pages.analytics import show_analytics
from pages.dashboard import show_dashboard
from utils.database.connection import DatabaseConnection
import streamlit as st
import pandas as pd
import subprocess
import sys
from pathlib import Path
import threading
from collections import deque


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
if 'api_logs' not in st.session_state:
    # keep last 500 log lines
    st.session_state['api_logs'] = deque(maxlen=500)

if RUN_API_IN_BACKGROUND and not st.session_state['api_background']:
    # start background process automatically (no user checkbox)
    api_script = Path('api') / 'api.py'
    python_exec = None
    if sys.platform.startswith('win'):
        venv_python = Path('.') / '.venv' / 'Scripts' / 'python.exe'
        if venv_python.exists():
            python_exec = str(venv_python)
        else:
            # do not auto-fallback to system python on Windows unless explicitly desired
            python_exec = None
    else:
        python_exec = 'python'

    if python_exec:
        # start subprocess with text streams and line buffering
        p = subprocess.Popen([python_exec, str(api_script)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        st.session_state['api_background'] = True
        st.session_state['api_process_pid'] = p.pid
        st.info(f'API started in background (pid={p.pid})')

        # helper to stream subprocess output into Streamlit's console
        def _stream_pipe(pipe, prefix='[api]'):
            try:
                for line in iter(pipe.readline, ''):
                    if not line:
                        break
                    text = f"{prefix} {line.rstrip()}"
                    # write to stdout so it appears in the Streamlit process console
                    print(text)
                    # append to session_state deque for in-app display
                    try:
                        st.session_state['api_logs'].append(text)
                    except Exception:
                        # session_state may not be thread-safe in all Streamlit versions; ignore errors
                        pass
            except Exception:
                pass
            finally:
                try:
                    pipe.close()
                except Exception:
                    pass

        # spawn reader threads for stdout and stderr
        t_out = threading.Thread(target=_stream_pipe, args=(p.stdout, '[api][out]'), daemon=True)
        t_err = threading.Thread(target=_stream_pipe, args=(p.stderr, '[api][err]'), daemon=True)
        t_out.start()
        t_err.start()
    else:
        st.warning('API not started: python executable for auto-start not found. Toggle RUN_API_IN_BACKGROUND in the source to change this behavior.')

# Show recent API logs in an expander
with st.expander('API logs (últimas linhas)'):
    cols = st.columns([9, 1])
    with cols[1]:
        if st.button('Clear logs'):
            st.session_state['api_logs'].clear()
            st.experimental_rerun()

    logs = list(st.session_state.get('api_logs', []))
    if logs:
        # show last 200 lines only to avoid huge renders
        for ln in logs[-200:]:
            st.text(ln)
    else:
        st.text('Nenhuma linha de log disponível ainda.')


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
