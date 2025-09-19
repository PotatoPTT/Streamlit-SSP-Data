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

        log_msg = f"[MAPS] Iniciando geração de mapas para o ano {year_filter}"
        logger.info(log_msg)

        with st.spinner(f"Gerando mapas para o ano {year_filter}..."):
            pipeline = GraphPipeline()
            try:
                pipeline.run(year_filter=int(year_filter))

                success_msg = f"[MAPS] Mapas para {year_filter} gerados com sucesso"
                logger.info(success_msg)

                st.success(f"Mapas para {year_filter} gerados com sucesso!")
                
                # Limpar cache para garantir que os mapas sejam detectados
                logger.info("Limpando cache de mapas após geração")
                verificar_mapas_disponiveis.clear()
                
                # Pequena pausa para garantir que os arquivos estejam disponíveis
                import time
                time.sleep(0.5)
                
                # Verificar novamente após geração (agora com cache limpo)
                mapas_existem, crimes_mapas, mapas_ano_path = verificar_mapas_disponiveis(
                    year_filter)
                
                if not mapas_existem:
                    st.warning("Mapas gerados, mas não foram detectados. Tente atualizar a página.")
                    return
                    
            except Exception as e:
                logger.error(
                    f"[MAPS] Erro ao gerar mapas para {year_filter}: {e}")

                st.error(f"Erro ao gerar mapas: {e}")
                return

    if not mapas_existem:
        st.info(f"Nenhum mapa interativo disponível para o ano {year_filter}.")
        return
        return

    # Seletor de crime para o mapa
    crime_mapa = st.selectbox(
        "Tipo de Crime (Mapa)",
        crimes_mapas,
        key="mapa_crime"
    )
    # Tentar renderizar inline via Plotly usando dados do pipeline/DB como preferência
    try:
        from utils.visualization.analytics_plots import plot_maps_crime_counts_plotly
        from utils.data.connection import DatabaseConnection

        # Tentar buscar dados brutos de mapa pelo mesmo caminho que o pipeline usaria
        db = DatabaseConnection()
        df_map = db.get_map_data(year=int(year_filter))
        if df_map is not None and not df_map.empty:
            # Usar Plotly para gerar mapas inline (mantém controle de crime via selectbox)
            # Filtramos para o crime selecionado
            crime_name = crime_mapa
            # Mapear o nome do selectbox para o valor presente no df (arquivo usa underscores)
            # Procuramos correspondência ignorando acentos e case
            def _normalize(s):
                import unicodedata
                return unicodedata.normalize('NFKD', str(s)).encode('ASCII', 'ignore').decode('ASCII').lower()

            norm_choice = _normalize(crime_name)
            # Encontrar correspondência aproximada entre Natureza e escolha
            matches = [c for c in df_map['Natureza'].unique() if _normalize(c) == norm_choice]
            if matches:
                selected_natureza = matches[0]
            else:
                # fallback: usa o texto do select como aparece (pode ser igual)
                selected_natureza = crime_name

            plot_maps_crime_counts_plotly(df_map, year=int(year_filter), crimes=[selected_natureza])
            db.close()
            return
        db.close()
    except Exception as e:
        # Se ocorrer qualquer erro, cai no fluxo de arquivos HTML já existente
        logger.debug(f"Não foi possível renderizar mapas inline via Plotly: {e}")

    # Encontrar o arquivo correspondente (fallback para HTML gerado)
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
