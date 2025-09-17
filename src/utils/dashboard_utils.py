"""
Utilitários para o dashboard com funções cacheadas e processamento de dados.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from utils.api.config import get_logger

logger = get_logger("DASHBOARD_UTILS")

# Mapeamento de meses
MESES = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
    5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
}


@st.cache_data(ttl=300)  # Cache por 5 minutos
def processar_dados_dashboard(year_filter, region_filter, municipality_filter, _buscar_ocorrencias):
    """Processa todos os dados necessários para o dashboard com cache."""
    logger.info(f"Processando dados para: Ano={year_filter}, Região={region_filter}, Município={municipality_filter}")
    
    # Buscar dados atuais e do ano anterior
    df_dados = _buscar_ocorrencias(year_filter, region_filter, municipality_filter)
    df_anterior = _buscar_ocorrencias(year_filter - 1, region_filter, municipality_filter)
    
    # Calcular métricas agregadas
    df_mes = df_dados.groupby("mes")["total"].sum().reset_index()
    df_tipo = df_dados.groupby("natureza")["total"].sum().reset_index().sort_values("total", ascending=True)
    
    total_ocorrencias = df_dados["total"].sum()
    total_anterior = df_anterior["total"].sum()
    
    # Métricas de KPIs
    try:
        mes_top = df_dados.groupby("mes")["total"].sum().idxmax()
        mes_top_nome = MESES.get(mes_top, str(mes_top))
    except ValueError:
        mes_top_nome = "N/A"
    
    media_mensal = df_dados["total"].sum() / 12 if len(df_dados) > 0 else 0
    
    # Calcular variação anual
    if total_anterior > 0:
        variacao = ((total_ocorrencias - total_anterior) / total_anterior) * 100
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
    
    df_dados_copy["mes_nome"] = df_dados_copy["mes"].map(MESES)
    df_anterior_copy["mes_nome"] = df_anterior_copy["mes"].map(MESES)
    
    # Criar tabela pivô
    tabela_atual = df_dados_copy.pivot_table(
        index="natureza", columns="mes_nome", values="total", aggfunc="sum"
    ).fillna(0)
    
    # Ordenar colunas por mês
    ordem_colunas = [m for m in MESES.values() if m in tabela_atual.columns]
    tabela_atual = tabela_atual[ordem_colunas]
    tabela_atual["Total"] = tabela_atual.sum(axis=1)
    
    # Calcular variação anual
    tabela_anterior = df_anterior_copy.groupby("natureza")["total"].sum().rename("total_anterior")
    tabela_completa = tabela_atual.join(tabela_anterior, on="natureza").fillna(0)
    
    # Calcular percentual de variação
    tabela_completa["Variação"] = (
        (tabela_completa["Total"] - tabela_completa["total_anterior"]) / 
        tabela_completa["total_anterior"]
    ) * 100
    
    # Limpar e formatar dados
    tabela_completa.drop(columns=["total_anterior"], inplace=True)
    
    # Converter colunas numéricas
    for col in MESES.values():
        if col in tabela_completa.columns:
            tabela_completa[col] = tabela_completa[col].astype(int)
    
    tabela_completa["Total"] = tabela_completa["Total"].astype(int)
    tabela_completa["Variação"] = tabela_completa["Variação"].round(1)
    
    return tabela_completa

@st.cache_data(ttl=60)  # Cache por 1 minuto
def verificar_mapas_disponiveis(year_filter):
    """Verifica se existem mapas para o ano especificado."""
    
    mapas_base = Path("output/maps")
    ano_mapa = str(year_filter)
    mapas_ano_path = mapas_base / ano_mapa
    
    existe = mapas_ano_path.exists() and any(mapas_ano_path.glob("*.html"))
    
    if existe:
        crimes_mapas = [
            f.stem.replace("_", " ").replace("-", "-") 
            for f in mapas_ano_path.glob("*.html")
        ]
        return True, crimes_mapas, mapas_ano_path
    
    return False, [], mapas_ano_path


@st.cache_data(ttl=1800)  # Cache por 30 minutos
def carregar_conteudo_mapa(crime_file_path):
    """Carrega o conteúdo HTML do mapa."""
    try:
        with open(crime_file_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logger.error(f"Erro ao carregar mapa: {e}")
        return None


def limpar_cache_dashboard():
    """Limpa todo o cache relacionado ao dashboard."""
    try:
        processar_dados_dashboard.clear()
        processar_tabela_detalhada.clear()
        verificar_mapas_disponiveis.clear()
        logger.info("Cache do dashboard limpo com sucesso")
    except Exception as e:
        logger.error(f"Erro ao limpar cache do dashboard: {e}")