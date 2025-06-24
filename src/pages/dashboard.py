import streamlit as st
import plotly.graph_objects as go
import os
from pathlib import Path
import streamlit.components.v1 as components
import unicodedata
from utils.graph.graph_pipeline import GraphPipeline  # type: ignore
import time
import sys
import subprocess

# Caminho absoluto para o lockfile na pasta configs do root do projeto
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
LOCK_FILE = os.path.join(ROOT_DIR, 'configs', 'update.lock')
COOLDOWN_SECONDS = 60 * 60  # 60 minutos

def is_pipeline_locked():
    if not os.path.exists(LOCK_FILE):
        return False, None
    try:
        with open(LOCK_FILE, 'r') as f:
            ts = float(f.read().strip())
        elapsed = time.time() - ts
        if elapsed < COOLDOWN_SECONDS:
            return True, int(COOLDOWN_SECONDS - elapsed)
        else:
            return False, None
    except Exception:
        return False, None

def set_pipeline_lock():
    os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
    with open(LOCK_FILE, 'w') as f:
        f.write(str(time.time()))

def show_dashboard(df_anos, df_regioes, df_municipios, buscar_ocorrencias):
    """Main dashboard page"""
    # Output do pipeline (placeholder global)
    if 'pipeline_output' not in st.session_state:
        st.session_state['pipeline_output'] = []
    pipeline_placeholder = st.empty()

    # Filters section
    st.markdown("### Filtros")
    col1, col2, col3, col4 = st.columns([1, 2, 2, 1])

    with col1:
        year_filter = st.selectbox(
            "Ano", df_anos["ano"].sort_values(ascending=False).tolist())

    with col2:
        region_filter = st.selectbox(
            "Região", ["Todas"] + df_regioes["nome"].tolist())

    with col3:
        mun_opts = df_municipios[df_municipios["regiao"] ==
                                 region_filter] if region_filter != "Todas" else df_municipios
        # Sort desconsiderando acentos
        mun_opts = mun_opts.assign(_nome_sem_acentos=mun_opts["nome"].apply(
            lambda x: unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').decode('ASCII')))
        mun_opts = mun_opts.sort_values("_nome_sem_acentos").drop(
            columns=["_nome_sem_acentos"])
        municipality_filter = st.selectbox(
            "Município", ["Todos"] + mun_opts["nome"].tolist())

    with col4:
        locked, cooldown_left = is_pipeline_locked()
        if locked:
            cooldown_msg = f"Aguarde {cooldown_left//60} min para nova atualização." if cooldown_left is not None else "Aguarde o cooldown."
            st.button("🔄 Atualizar Dados", disabled=True, help=cooldown_msg)
            info_msg = f"A atualização já foi executada recentemente. Tente novamente em {cooldown_left//60} minutos." if cooldown_left is not None else "O pipeline está em cooldown."
            st.info(info_msg)
        else:
            if st.button("🔄 Atualizar Dados", help="Executa o pipeline completo de atualização de dados"):
                src_dir = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), '..'))
                cmd = [sys.executable, "-m", "utils.pipeline_runner"]
                st.session_state['pipeline_output'] = []
                set_pipeline_lock()
                def run_and_stream():
                    process = subprocess.Popen(
                        cmd, cwd=src_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
                    if process.stdout is not None:
                        for line in process.stdout:
                            st.session_state['pipeline_output'].append(line)
                            output = ''.join(
                                st.session_state['pipeline_output'][-20:])
                            pipeline_placeholder.code(output, language="bash")
                    process.wait()
                    if process.returncode == 0:
                        pipeline_placeholder.success(
                            "Pipeline executado com sucesso!")
                        set_pipeline_lock()  # Atualiza timestamp lock ao finalizar
                run_and_stream()
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
        # Usa nomes abreviados dos meses no eixo x
        fig1 = go.Figure(go.Scatter(
            x=df_mes["mes"].map(meses), y=df_mes["total"], mode='lines+markers', line=dict(color='royalblue')))
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

    # Data table section
    st.markdown("#### Dados Detalhados")
    st.dataframe(tabela_completa, use_container_width=True)

    # Map section
    st.markdown("#### Mapa Interativo")
    mapas_base = Path("output/maps")
    ano_mapa = str(year_filter)
    mapas_ano_path = mapas_base / ano_mapa
    # Geração sob demanda dos mapas
    if not mapas_ano_path.exists() or not any(mapas_ano_path.glob("*.html")):
        with st.spinner(f"Gerando mapas para o ano {ano_mapa}..."):
            pipeline = GraphPipeline()
            pipeline.run(year_filter=int(ano_mapa))
        # Atualiza o path após geração
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
