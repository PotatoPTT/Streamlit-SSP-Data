"""
Processamento de dados e queries para a API.
"""

import pandas as pd
from utils.config.logging import get_logger

logger = get_logger("DATA")


def fetch_data_for_job(db_conn, params):
    """
    Busca os dados de ocorrências com base nos parâmetros da solicitação.
    Utiliza o método fetch_time_series_data da classe DatabaseConnection.
    """
    return db_conn.fetch_time_series_data(params)


def validate_time_series_data(time_series_df):
    """Valida se os dados de série temporal são adequados para análise."""
    if time_series_df.empty:
        raise ValueError("DataFrame de séries temporais está vazio.")
    
    if time_series_df.shape[1] < 2:
        raise ValueError("Série temporal muito curta para análise (menos de 2 pontos de dados).")
    
    if time_series_df.shape[0] < 2:
        raise ValueError("Número insuficiente de municípios para análise de clusters (menos de 2).")
    
    return True