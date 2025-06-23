import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import psycopg2


# Page configuration
st.set_page_config(page_title="SP Segurança - Dashboard",
                   page_icon="🛡️",
                   layout="wide",
                   initial_sidebar_state="collapsed")

# Initialize session state
if 'dark_mode' not in st.session_state:
    st.session_state.dark_mode = False

if 'current_page' not in st.session_state:
    st.session_state.current_page = "dashboard"


# Chart theme helper function
def get_chart_theme():
    """Get current chart theme configuration"""
    if st.session_state.dark_mode:
        return {
            'primary_color': '#1f77b4',
            'text_color': '#fafafa',
            'grid_color': 'rgba(255,255,255,0.1)',
            'plot_bgcolor': 'rgba(0,0,0,0)',
            'paper_bgcolor': 'rgba(0,0,0,0)'
        }
    else:
        return {
            'primary_color': '#1f77b4',
            'text_color': '#262730',
            'grid_color': 'rgba(128,128,128,0.2)',
            'plot_bgcolor': 'rgba(0,0,0,0)',
            'paper_bgcolor': 'rgba(0,0,0,0)'
        }


@st.cache_data(ttl=300)
def carregar_filtros():
    conn = psycopg2.connect(
        host=st.secrets["POSTGRES_HOST"],
        database=st.secrets["POSTGRES_DB"],
        user=st.secrets["POSTGRES_USER"],
        password=st.secrets["POSTGRES_PASSWORD"],
        port=st.secrets.get("POSTGRES_PORT", 5432),
        sslmode="require"
    )
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ano FROM ocorrencias ORDER BY ano DESC")
    df_anos = pd.DataFrame(cur.fetchall(), columns=["ano"])
    cur.execute("SELECT id, nome FROM regioes ORDER BY nome")
    df_regioes = pd.DataFrame(cur.fetchall(), columns=["id", "nome"])
    cur.execute("""
        SELECT m.id, m.nome, r.nome AS regiao
        FROM municipios m
        JOIN regioes r ON m.regiao_id = r.id
        ORDER BY m.nome
    """)
    df_municipios = pd.DataFrame(cur.fetchall(), columns=["id", "nome", "regiao"])
    cur.close()
    conn.close()
    return df_anos, df_regioes, df_municipios


df_anos, df_regioes, df_municipios = carregar_filtros()


@st.cache_data(ttl=300)
def buscar_ocorrencias(ano, regiao, municipio):
    conn = psycopg2.connect(
        host=st.secrets["POSTGRES_HOST"],
        database=st.secrets["POSTGRES_DB"],
        user=st.secrets["POSTGRES_USER"],
        password=st.secrets["POSTGRES_PASSWORD"],
        port=st.secrets.get("POSTGRES_PORT", 5432),
        sslmode="require"
    )
    cur = conn.cursor()
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

    cur.execute(sql, params)
    df = pd.DataFrame(cur.fetchall(), columns=["mes", "natureza", "total"])
    cur.close()
    conn.close()
    return df

# Chart creation functions


def create_monthly_evolution_chart():
    """Create themed monthly evolution line chart"""
    theme = get_chart_theme()

    fig = go.Figure()

    # Show placeholder message
    fig.add_annotation(
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        text="Aguardando dados da API<br>📈 Gráfico será carregado quando conectado",
        showarrow=False,
        font=dict(size=16, color=theme['text_color']),
        align="center")

    fig.update_layout(title=dict(text="Evolução Mensal",
                                 font=dict(size=18, color=theme['text_color']),
                                 x=0.02),
                      xaxis=dict(title="Mês",
                                 showgrid=True,
                                 gridcolor=theme['grid_color'],
                                 color=theme['text_color'],
                                 showticklabels=False),
                      yaxis=dict(title="Número de Ocorrências",
                                 showgrid=True,
                                 gridcolor=theme['grid_color'],
                                 color=theme['text_color'],
                                 showticklabels=False),
                      plot_bgcolor=theme['plot_bgcolor'],
                      paper_bgcolor=theme['paper_bgcolor'],
                      height=400,
                      showlegend=False,
                      font=dict(color=theme['text_color']))

    return fig


def create_occurrence_types_chart():
    """Create themed occurrence types bar chart"""
    theme = get_chart_theme()

    fig = go.Figure()

    # Show placeholder message
    fig.add_annotation(
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        text="Aguardando dados da API<br>📊 Gráfico será carregado quando conectado",
        showarrow=False,
        font=dict(size=16, color=theme['text_color']),
        align="center")

    fig.update_layout(title=dict(text="Tipos de Ocorrências",
                                 font=dict(size=18, color=theme['text_color']),
                                 x=0.02),
                      xaxis=dict(title="Quantidade",
                                 showgrid=True,
                                 gridcolor=theme['grid_color'],
                                 color=theme['text_color'],
                                 showticklabels=False),
                      yaxis=dict(title="Tipo de Ocorrência",
                                 color=theme['text_color'],
                                 showticklabels=False),
                      plot_bgcolor=theme['plot_bgcolor'],
                      paper_bgcolor=theme['paper_bgcolor'],
                      height=400,
                      showlegend=False,
                      font=dict(color=theme['text_color']))

    return fig


# Theme functions
def apply_theme_styles():
    """Apply theme-specific CSS styles"""
    if st.session_state.dark_mode:
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
        </style>
        """,
                    unsafe_allow_html=True)
    else:
        st.markdown("""
        <style>
        .stApp {
            background-color: #ffffff;
            color: #262730;
        }
        
        .stMetric {
            background-color: #f0f2f6 !important;
            padding: 1rem;
            border-radius: 0.5rem;
            border: 1px solid #e0e0e0;
        }
        
        .nav-button-active {
            background-color: #1f77b4 !important;
            color: white !important;
        }
        </style>
        """,
                    unsafe_allow_html=True)


# Navigation function
def show_navigation():
    """Display top navigation bar with theme toggle"""
    col1, col2, col3, col4, col5, col6 = st.columns([2, 1, 1, 1, 1, 0.5])

    with col1:
        st.markdown("## 🛡️ SP Segurança")

    # Navigation buttons with active state styling
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

    with col6:
        if st.button("🌙" if not st.session_state.dark_mode else "☀️",
                     key="theme_toggle",
                     help="Toggle dark/light mode"):
            st.session_state.dark_mode = not st.session_state.dark_mode
            st.rerun()

    st.divider()


# Apply theme styles and show navigation
apply_theme_styles()
show_navigation()


# Dashboard page
def show_dashboard():
    """Main dashboard page"""

    # Output do pipeline (placeholder global)
    if 'pipeline_output' not in st.session_state:
        st.session_state['pipeline_output'] = []
    pipeline_placeholder = st.empty()

    # Filters section
    st.markdown("### Filtros")
    col1, col2, col3, col4 = st.columns([1, 2, 2, 1])

    with col1:
        year_filter = st.selectbox("Ano", df_anos["ano"])

    with col2:
        region_filter = st.selectbox(
            "Região", ["Todas"] + df_regioes["nome"].tolist())

    with col3:
        mun_opts = df_municipios[df_municipios["regiao"] ==
                                 region_filter] if region_filter != "Todas" else df_municipios
        municipality_filter = st.selectbox(
            "Município", ["Todos"] + mun_opts["nome"].tolist())

    with col4:
        # Botão para rodar o pipeline
        if st.button("🔄 Atualizar Dados", help="Executa o pipeline completo de atualização de dados"):
            import subprocess
            import sys
            import os
            root_dir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), '..'))
            cmd = [sys.executable, "-m", "src.utils.pipeline_runner"]
            st.session_state['pipeline_output'] = []

            def run_and_stream():
                process = subprocess.Popen(
                    cmd, cwd=root_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
                for line in process.stdout:
                    st.session_state['pipeline_output'].append(line)
                    # Limita para as últimas 40 linhas
                    output = ''.join(st.session_state['pipeline_output'][-20:])
                    pipeline_placeholder.code(output, language="bash")
                process.wait()
                if process.returncode == 0:
                    pipeline_placeholder.success(
                        "Pipeline executado com sucesso!")
            run_and_stream()
        # Exibe o output anterior, se houver
        elif st.session_state['pipeline_output']:
            output = ''.join(st.session_state['pipeline_output'][-40:])
            pipeline_placeholder.code(output, language="bash")

    st.divider()

    df_dados = buscar_ocorrencias(
        year_filter, region_filter, municipality_filter)
    df_anterior = buscar_ocorrencias(
        year_filter - 1, region_filter, municipality_filter)
    df_mes = df_dados.groupby("mes")["total"].sum().reset_index()
    df_tipo = df_dados.groupby("natureza")["total"].sum(
    ).reset_index().sort_values("total", ascending=True)
    total_ocorrencias = df_dados["total"].sum()
    total_anterior = df_anterior["total"].sum()
    mes_top = df_dados.groupby("mes")["total"].sum().idxmax()
    media_mensal = df_dados["total"].sum() / 12

    meses = {
        1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
        5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
        9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
    }
    df_dados["mes_nome"] = df_dados["mes"].map(meses)
    df_anterior["mes_nome"] = df_anterior["mes"].map(meses)
    tabela_atual = df_dados.pivot_table(
        index="natureza", columns="mes_nome", values="total", aggfunc="sum").fillna(0)
    ordem_colunas = [m for m in meses.values() if m in tabela_atual.columns]
    tabela_atual = tabela_atual[ordem_colunas]
    tabela_atual["Total"] = tabela_atual.sum(axis=1)
    tabela_anterior = df_anterior.groupby(
        "natureza")["total"].sum().rename("total_anterior")
    tabela_completa = tabela_atual.join(
        tabela_anterior, on="natureza").fillna(0)
    tabela_completa["Variação"] = (
        (tabela_completa["Total"] - tabela_completa["total_anterior"]) / tabela_completa["total_anterior"]) * 100
    tabela_completa.drop(columns=["total_anterior"], inplace=True)
    for col in meses.values():
        if col in tabela_completa.columns:
            tabela_completa[col] = tabela_completa[col].astype(int)
    tabela_completa["Total"] = tabela_completa["Total"].astype(int)
    tabela_completa["Variação"] = tabela_completa["Variação"].round(1)

    if total_anterior > 0:
        variacao = ((total_ocorrencias - total_anterior) /
                    total_anterior) * 100
    else:
        variacao = 0

    # KPI Cards
    st.markdown("### Indicadores Principais")
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    with kpi_col1:
        st.metric(label="Total de Ocorrências",
                  value=f"{total_ocorrencias:,}".replace(",", "."),
                  help="Total de ocorrências registradas no período")

    with kpi_col2:
        st.metric(label="Comparação Anual",
                  value=f"{variacao:.2f}%",
                  help=f"Variação em relação a {year_filter - 1}")

    with kpi_col3:
        st.metric(label="Mês com mais ocorrências",
                  value=f"{mes_top:,}",
                  help="Número do mês com mais ocorrências")

    with kpi_col4:
        st.metric(label="Média Mensal de Ocorrências",
                  value=f"{media_mensal:,.0f}".replace(",", "."),
                  help="Variação percentual do último mês")

    # Charts section
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.markdown("#### Evolução Mensal")
        fig1 = go.Figure(go.Scatter(
            x=df_mes["mes"], y=df_mes["total"], mode='lines+markers', line=dict(color='royalblue')))
        fig1.update_layout(title="Ocorrências por Mês",
                           xaxis_title="Mês", yaxis_title="Total")
        st.plotly_chart(fig1, use_container_width=True)

    with chart_col2:
        st.markdown("#### Tipos de Ocorrências")
        fig2 = go.Figure(go.Bar(
            x=df_tipo["total"], y=df_tipo["natureza"], orientation='h', marker_color='indianred'))
        fig2.update_layout(title="Tipos de Crimes",
                           xaxis_title="Total", yaxis_title="Crime")
        st.plotly_chart(fig2, use_container_width=True)

    # Reserved space for future map
    st.markdown("#### Mapa Interativo")
    import os
    from pathlib import Path
    import streamlit.components.v1 as components

    mapas_base = Path("output/maps")
    ano_mapa = str(year_filter)
    mapas_ano_path = mapas_base / ano_mapa
    if not mapas_ano_path.exists() or not any(mapas_ano_path.glob("*.html")):
        st.info(f"Nenhum mapa interativo disponível para o ano {ano_mapa}.")
    else:
        crimes_mapas = [f.stem.replace("_", " ").replace(
            "-", "-") for f in mapas_ano_path.glob("*.html")]
        crime_mapa = st.selectbox(
            "Tipo de Crime (Mapa)", crimes_mapas, key="mapa_crime")
        crime_file = None
        for f in mapas_ano_path.glob("*.html"):
            if f.stem.replace("_", " ").replace("-", "-") == crime_mapa:
                crime_file = f
                break
        if crime_file and crime_file.exists():
            with open(crime_file, "r", encoding="utf-8") as f:
                html_content = f.read()
            components.html(html_content, height=600, scrolling=True)
        else:
            st.info("Mapa não encontrado para o filtro selecionado.")

    # Data table section
    st.markdown("#### Dados Detalhados")
    # Empty dataframe placeholder

    st.dataframe(tabela_completa, use_container_width=True)


# Analytics page
def show_analytics():
    """Analytics page"""
    st.markdown("# 📊 Análises Detalhadas")
    st.info("Página de análises será implementada com dados da API")

    # Placeholder sections
    st.markdown("### Análises Avançadas")
    st.write(
        "Esta seção conterá análises estatísticas avançadas dos dados de segurança pública."
    )

    st.markdown("### Correlações")
    st.write(
        "Análises de correlação entre diferentes tipos de ocorrências e fatores externos."
    )

    st.markdown("### Previsões")
    st.write("Modelos preditivos para tendências de segurança pública.")


# Reports page
def show_reports():
    """Reports page"""
    st.markdown("# 📄 Relatórios")
    st.info("Página de relatórios será implementada com dados da API")

    # Placeholder sections
    st.markdown("### Relatórios Mensais")
    st.write("Relatórios consolidados mensais com análises detalhadas.")

    st.markdown("### Relatórios Anuais")
    st.write("Relatórios anuais com comparativos e tendências.")

    st.markdown("### Relatórios Customizados")
    st.write("Geração de relatórios personalizados por período e região.")


# About page
def show_about():
    """About page"""
    st.markdown("# ℹ️ Sobre o Sistema")

    st.markdown("""
    ## SP Segurança Dashboard
    
    Sistema profissional de análise e visualização de dados de segurança pública do Estado de São Paulo.
    
    ### Características
    - **Interface Profissional**: Design inspirado em ferramentas como Power BI e Tableau
    - **Dados em Tempo Real**: Integração com APIs oficiais para dados atualizados
    - **Análises Avançadas**: Ferramentas estatísticas e de machine learning
    - **Relatórios Customizados**: Geração de relatórios personalizados
    - **Visualizações Interativas**: Gráficos e mapas interativos
    - **Modo Escuro**: Interface adaptável com tema claro e escuro
    
    ### Recursos Futuros
    - Mapa interativo com dados georreferenciados
    - Alertas automáticos para anomalias
    - Integração com sistemas de emergência
    - API pública para desenvolvedores
    
    ### Suporte Técnico
    Para suporte técnico e mais informações, entre em contato através dos canais oficiais.
    """)

    # Footer links
    st.markdown("---")
    footer_col1, footer_col2, footer_col3, footer_col4 = st.columns(4)

    with footer_col1:
        st.markdown("**Sobre**")
        st.write("• Institucional")
        st.write("• Contato")
        st.write("• Imprensa")

    with footer_col2:
        st.markdown("**Recursos**")
        st.write("• Documentação")
        st.write("• API")
        st.write("• Dados Abertos")

    with footer_col3:
        st.markdown("**Legal**")
        st.write("• Privacidade")
        st.write("• Termos de Uso")

    with footer_col4:
        st.markdown("**Redes Sociais**")
        st.write("• Twitter")
        st.write("• LinkedIn")


# Page routing
if st.session_state.current_page == "dashboard":
    show_dashboard()
elif st.session_state.current_page == "analytics":
    show_analytics()
elif st.session_state.current_page == "reports":
    show_reports()
elif st.session_state.current_page == "about":
    show_about()

# Footer
st.markdown("---")
st.markdown("© 2024 SP Segurança. Todos os direitos reservados.")
