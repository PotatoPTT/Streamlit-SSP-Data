"""
Utilitários para o dashboard com funções cacheadas e processamento de dados.
"""

import streamlit as st
from pathlib import Path
from utils.config.logging import get_logger
from utils.config.constants import MESES_MAP_INV
from utils.data.connection import DatabaseConnection

logger = get_logger("DASHBOARD_UTILS")


def processar_dados_dashboard(df_dados, df_anterior):
    """Processa todos os dados necessários para o dashboard.

    Recebe os DataFrames já obtidos (ex.: via função cacheada buscar_ocorrencias)
    para evitar passar objetos não-hashable para o cache do Streamlit.
    """
    logger.info("Processando dados para dashboard (DFs fornecidos)")

    # Calcular métricas agregadas
    df_mes = df_dados.groupby("mes")["total"].sum().reset_index()
    df_tipo = df_dados.groupby("natureza")["total"].sum(
    ).reset_index().sort_values("total", ascending=True)

    total_ocorrencias = df_dados["total"].sum()
    total_anterior = df_anterior["total"].sum()

    # Métricas de KPIs
    try:
        mes_top = df_dados.groupby("mes")["total"].sum().idxmax()
        mes_top_nome = MESES_MAP_INV.get(mes_top, str(mes_top))
    except ValueError:
        mes_top_nome = "N/A"

    media_mensal = df_dados["total"].sum() / 12 if len(df_dados) > 0 else 0

    # Calcular variação anual
    if total_anterior > 0:
        variacao = ((total_ocorrencias - total_anterior) /
                    total_anterior) * 100
    else:
        variacao = 0

    return {
        'df_dados': df_dados,
        'df_anterior': df_anterior,
        'df_mes': df_mes,
        'df_tipo': df_tipo,
        'total_ocorrencias': total_ocorrencias,
        'total_anterior': total_anterior,
        'mes_top_nome': mes_top_nome,
        'media_mensal': media_mensal,
        'variacao': variacao
    }


@st.cache_data(ttl=300)  # Cache por 5 minutos
def processar_tabela_detalhada(df_dados, df_anterior):
    """Processa a tabela detalhada com dados mensais e variações."""
    # Mapear nomes dos meses
    df_dados_copy = df_dados.copy()
    df_anterior_copy = df_anterior.copy()

    df_dados_copy["mes_nome"] = df_dados_copy["mes"].map(MESES_MAP_INV)
    df_anterior_copy["mes_nome"] = df_anterior_copy["mes"].map(MESES_MAP_INV)

    # Criar tabela pivô
    tabela_atual = df_dados_copy.pivot_table(
        index="natureza", columns="mes_nome", values="total", aggfunc="sum"
    ).fillna(0)

    # Ordenar colunas por mês
    ordem_colunas = [m for m in MESES_MAP_INV.values()
                     if m in tabela_atual.columns]
    tabela_atual = tabela_atual[ordem_colunas]
    tabela_atual["Total"] = tabela_atual.sum(axis=1)

    # Calcular variação anual
    tabela_anterior = df_anterior_copy.groupby(
        "natureza")["total"].sum().rename("total_anterior")
    tabela_completa = tabela_atual.join(
        tabela_anterior, on="natureza").fillna(0)

    # Calcular percentual de variação
    tabela_completa["Variação"] = (
        (tabela_completa["Total"] - tabela_completa["total_anterior"]) /
        tabela_completa["total_anterior"]
    ) * 100

    # Limpar e formatar dados
    tabela_completa.drop(columns=["total_anterior"], inplace=True)

    # Converter colunas numéricas
    for col in MESES_MAP_INV.values():
        if col in tabela_completa.columns:
            tabela_completa[col] = tabela_completa[col].astype(int)

    tabela_completa["Total"] = tabela_completa["Total"].astype(int)
    tabela_completa["Variação"] = tabela_completa["Variação"].round(1)

    return tabela_completa



def limpar_cache_dashboard():
    """Limpa todo o cache relacionado ao dashboard."""
    try:
        processar_dados_dashboard.clear()
        processar_tabela_detalhada.clear()
        # get_map_data_cached is defined below; clear if available
        try:
            get_map_data_cached.clear()
        except Exception:
            pass
        logger.info("Cache do dashboard limpo com sucesso")
    except Exception as e:
        logger.error(f"Erro ao limpar cache do dashboard: {e}")


@st.cache_data(ttl=1800)
def get_map_data_cached(year):
    """Cacheia o resultado de DatabaseConnection.get_map_data(year).

    Usamos serialização/parametrização simples (um único inteiro "year") para
    que o Streamlit possa criar uma chave estável de cache. TTL é 30 minutos.
    """
    with DatabaseConnection() as db:
        logger.info(f"Buscando dados de mapa para o ano {year}")
        df = db.get_map_data(year=year)
        return df
