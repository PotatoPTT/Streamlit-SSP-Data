from pages.about import show_about
from pages.reports import show_reports
from pages.analytics import show_analytics
from pages.dashboard import show_dashboard
from utils.data.connection import DatabaseConnection
from utils.core.api_manager import is_api_running, start_api, stop_api
from utils.config.logging import get_logger
from utils.constants import CHART_THEME
import streamlit as st
import pandas as pd
import atexit

logger = get_logger("STREAMLIT")


def cleanup_on_exit():
    """Para a API quando o Streamlit é encerrado."""
    try:
        stop_api()
    except:
        pass


# Registra a função de limpeza
atexit.register(cleanup_on_exit)

# Configuração de inicialização da API
# Define como True para iniciar a API automaticamente
RUN_API_IN_BACKGROUND = True

# Page configuration
st.set_page_config(page_title="SSP Data",
                   page_icon="🛡️",
                   layout="wide",
                   initial_sidebar_state="collapsed" if not RUN_API_IN_BACKGROUND else "expanded")

# Initialize session state
if 'current_page' not in st.session_state:
    st.session_state.current_page = "dashboard"


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
    sidebar_css = ""
    if not RUN_API_IN_BACKGROUND:
        sidebar_css = """
        /* Esconde completamente a sidebar e o botão de expandir */
        [data-testid="stSidebar"], section[data-testid="stSidebar"] {
            display: none !important;
        }
        [data-testid="collapsedControl"] {
            display: none !important;
        }
        """
    
    st.markdown(f"""
    <style>
    .stApp {{
        background-color: #0e1117;
        color: #fafafa;
    }}
    
    .stMetric {{
        background-color: #262730 !important;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #444;
    }}
    
    .stSelectbox > div > div {{
        background-color: #262730;
        color: #fafafa;
    }}
    
    .stDataFrame {{
        background-color: #262730;
    }}
    
    .stButton > button {{
        background-color: #262730;
        color: #fafafa;
        border: 1px solid #444;
    }}
    
    .stButton > button:hover {{
        background-color: #444;
        border-color: #666;
    }}
    
    .nav-button-active {{
        background-color: #1f77b4 !important;
        color: white !important;
    }}
    
    {sidebar_css}
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

# Gerenciamento automático da API
if RUN_API_IN_BACKGROUND:
    if not is_api_running():
        success, message = start_api()
        # Log apenas em caso de erro
        if not success:
            logger.error(f"Erro ao iniciar API: {message}")


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
