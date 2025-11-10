"""
Processamento de dados e queries para a API.
Integrado com lógica de limpeza robusta do Clustering Project.
"""

import pandas as pd
import numpy as np
from utils.config.logging import get_logger

logger = get_logger("DATA")


def fetch_data_for_job(db_conn, params):
    """
    Busca os dados de ocorrências com base nos parâmetros da solicitação.
    Utiliza o método fetch_time_series_data da classe DatabaseConnection.
    """
    return db_conn.fetch_time_series_data(params)


def clean_invalid_data(time_series_data, city_names, crime_name):
    """
    Limpa dados inválidos (NaN, Inf) substituindo por 0.0.
    Implementação baseada no Clustering Project.

    Args:
        time_series_data (np.array): Dados das séries temporais.
        city_names (list): Lista de nomes das cidades.
        crime_name (str): Nome do crime para mensagens de log.

    Returns:
        tuple: (cleaned_data, cleaned_city_names, stats_dict) ou (None, None, None) se inválidos.
    """
    stats = {
        'original_series': len(city_names),
        'removed_nan_cols': 0,
        'removed_nan_rows': 0,
        'had_invalid_values': False
    }
    
    try:
        # Remover colunas (timesteps) completamente NaN
        if time_series_data.shape[0] > 0:
            mask_valid_cols = ~np.all(np.isnan(time_series_data), axis=0)
            cols_removed = np.sum(~mask_valid_cols)
            if cols_removed > 0:
                logger.info(f"Removendo {cols_removed} colunas completamente NaN para '{crime_name}'")
                stats['removed_nan_cols'] = int(cols_removed)
            
            if not np.any(mask_valid_cols):
                logger.warning(f"Todas as colunas são NaN para o crime '{crime_name}'.")
                return None, None, None
            time_series_data = time_series_data[:, mask_valid_cols]

        # Remover linhas (séries) completamente NaN
        if time_series_data.shape[1] > 0:
            mask_valid_rows = ~np.all(np.isnan(time_series_data), axis=1)
            rows_removed = np.sum(~mask_valid_rows)
            if rows_removed > 0:
                logger.info(f"Removendo {rows_removed} séries completamente NaN para '{crime_name}'")
                stats['removed_nan_rows'] = int(rows_removed)
            
            if not np.any(mask_valid_rows):
                logger.warning(f"Todas as linhas são NaN para o crime '{crime_name}'.")
                return None, None, None
            
            time_series_data = time_series_data[mask_valid_rows, :]
            city_names = [city_names[i]
                          for i in range(len(city_names)) if mask_valid_rows[i]]
        else:
            logger.warning(f"Nenhuma coluna válida para o crime '{crime_name}'.")
            return None, None, None

        # Substituir valores inválidos remanescentes por 0.0
        if np.any(np.isnan(time_series_data)) or np.any(np.isinf(time_series_data)):
            logger.info(f"Limpando dados inválidos para '{crime_name}' (substituindo por 0.0)...")
            stats['had_invalid_values'] = True
            time_series_data = np.nan_to_num(
                time_series_data, nan=0.0, posinf=0.0, neginf=0.0)

        stats['final_series'] = len(city_names)
        return time_series_data, city_names, stats
        
    except Exception as e:
        logger.exception(f"ERRO ao limpar dados para '{crime_name}': {e}")
        return None, None, None


def remove_null_series(time_series_data, city_names, crime_name, threshold_percent=100.0):
    """
    Remove séries temporais que são completamente ou majoritariamente zeradas.
    Implementação baseada no Clustering Project.
    
    Args:
        time_series_data (np.array): Dados das séries temporais.
        city_names (list): Lista de nomes das cidades.
        crime_name (str): Nome do crime para mensagens de log.
        threshold_percent (float): Percentual de zeros para considerar série nula (100.0 = apenas totalmente zeradas).
    
    Returns:
        tuple: (cleaned_data, cleaned_city_names, removed_count, removed_cities)
    """
    if time_series_data.shape[0] == 0:
        return time_series_data, city_names, 0, []
    
    original_count = time_series_data.shape[0]
    
    # Calcular percentual de zeros em cada série
    zero_percentages = np.mean(time_series_data == 0, axis=1) * 100
    
    # Manter apenas séries abaixo do threshold
    mask_valid = zero_percentages < threshold_percent
    removed_count = np.sum(~mask_valid)
    
    # Armazenar nomes dos municípios removidos
    removed_cities = [city_names[i] for i in range(len(city_names)) if not mask_valid[i]]
    
    if removed_count > 0:
        logger.info(f"Removendo {removed_count} séries com >={threshold_percent}% de zeros para '{crime_name}'")
        time_series_data = time_series_data[mask_valid, :]
        city_names = [city_names[i] for i in range(len(city_names)) if mask_valid[i]]
    
    return time_series_data, city_names, int(removed_count), removed_cities


def prepare_and_clean_data(time_series_df, crime_name):
    """
    Pipeline completo de preparação e limpeza de dados.
    
    Args:
        time_series_df (pd.DataFrame): DataFrame com séries temporais (municípios × datas).
        crime_name (str): Nome do crime para logging.
    
    Returns:
        tuple: (time_series_array, city_names, cleaning_stats) ou (None, None, None) se falhar.
    """
    from utils.ml.config import THRESHOLD_SERIE_ZERO
    
    logger.info(f"Iniciando preparação de dados para '{crime_name}'...")
    logger.info(f"  Municípios originais: {time_series_df.shape[0]}")
    logger.info(f"  Períodos temporais: {time_series_df.shape[1]}")
    
    cleaning_stats = {
        'original_municipalities': time_series_df.shape[0],
        'original_periods': time_series_df.shape[1],
        'removed_nan_series': 0,
        'removed_null_series': 0,
        'final_municipalities': 0,
        'final_periods': 0,
        'had_invalid_values': False,
        'removed_cities': []  # Lista de municípios removidos
    }
    
    # Converter para numpy array
    city_names = time_series_df.index.tolist()
    time_series_data = time_series_df.values.astype(np.float64)
    
    # Etapa 1: Limpar dados inválidos (NaN, Inf)
    time_series_data, city_names, clean_stats = clean_invalid_data(
        time_series_data, city_names, crime_name)
    
    if time_series_data is None:
        logger.error(f"Falha na limpeza de dados inválidos para '{crime_name}'")
        return None, None, None
    
    cleaning_stats['removed_nan_series'] = clean_stats.get('removed_nan_rows', 0)
    cleaning_stats['had_invalid_values'] = clean_stats.get('had_invalid_values', False)
    
    # Etapa 2: Remover séries nulas/zeradas
    time_series_data, city_names, removed_null, removed_cities = remove_null_series(
        time_series_data, city_names, crime_name, threshold_percent=THRESHOLD_SERIE_ZERO)
    
    cleaning_stats['removed_null_series'] = removed_null
    cleaning_stats['removed_cities'] = removed_cities  # Armazenar lista de municípios removidos
    cleaning_stats['final_municipalities'] = len(city_names)
    cleaning_stats['final_periods'] = time_series_data.shape[1] if len(city_names) > 0 else 0
    
    # Validação final
    if len(city_names) < 2:
        logger.error(f"Número insuficiente de municípios após limpeza: {len(city_names)}")
        return None, None, None
    
    logger.info(f"Preparação concluída para '{crime_name}':")
    logger.info(f"  Municípios finais: {cleaning_stats['final_municipalities']}")
    logger.info(f"  Séries removidas (NaN): {cleaning_stats['removed_nan_series']}")
    logger.info(f"  Séries removidas (nulas): {cleaning_stats['removed_null_series']}")
    logger.info(f"  Total removido: {cleaning_stats['removed_nan_series'] + cleaning_stats['removed_null_series']}")
    
    return time_series_data, city_names, cleaning_stats


def validate_time_series_data(time_series_df):
    """Valida se os dados de série temporal são adequados para análise."""
    if time_series_df.empty:
        raise ValueError("DataFrame de séries temporais está vazio.")
    
    if time_series_df.shape[1] < 2:
        raise ValueError(
            "Série temporal muito curta para análise (menos de 2 pontos de dados).")
    
    if time_series_df.shape[0] < 2:
        raise ValueError(
            "Número insuficiente de municípios para análise de clusters (menos de 2).")
    
    return True