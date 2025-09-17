"""
Processamento de dados e queries para a API.
"""

import pandas as pd
from utils.api.config import get_logger

logger = get_logger("DATA")


def fetch_data_for_job(db_conn, params):
    """Busca os dados de ocorrências com base nos parâmetros da solicitação."""
    logger.info(f"Buscando dados para o período de {params['data_inicio']} a {params['data_fim']} na região '{params['regiao']}' para o crime '{params['crime']}'...")

    query = """
        SELECT
            m.nome AS municipio,
            TO_CHAR((o.ano || '-' || LPAD(o.mes::text, 2, '0') || '-01')::date, 'YYYY-MM') AS ano_mes,
            SUM(o.quantidade) AS quantidade
        FROM ocorrencias o
        JOIN municipios m ON o.municipio_id = m.id
        JOIN regioes r ON m.regiao_id = r.id
        JOIN crimes c ON o.crime_id = c.id
        WHERE (o.ano || '-' || LPAD(o.mes::text, 2, '0') || '-01')::date BETWEEN TO_DATE(%s, 'YYYY-MM') AND TO_DATE(%s, 'YYYY-MM')
        AND c.natureza = %s
    """
    sql_params = [params['data_inicio'], params['data_fim'], params['crime']]

    if params['regiao'] != 'Todas':
        query += " AND r.nome = %s"
        sql_params.append(params['regiao'])

    query += """
        GROUP BY m.nome, ano_mes
        ORDER BY m.nome, ano_mes;
    """

    df = pd.DataFrame(db_conn.fetch_all(query, tuple(sql_params)), columns=[
                      'municipio', 'ano_mes', 'quantidade'])

    if df.empty:
        raise ValueError("A consulta de dados não retornou resultados.")

    # Pivotar para formato de série temporal
    time_series_df = df.pivot_table(
        index='municipio', columns='ano_mes', values='quantidade').fillna(0)

    logger.info(f"Dados transformados: {time_series_df.shape[0]} municípios e {time_series_df.shape[1]} meses.")
    return time_series_df


def validate_time_series_data(time_series_df):
    """Valida se os dados de série temporal são adequados para análise."""
    if time_series_df.empty:
        raise ValueError("DataFrame de séries temporais está vazio.")
    
    if time_series_df.shape[1] < 2:
        raise ValueError("Série temporal muito curta para análise (menos de 2 pontos de dados).")
    
    if time_series_df.shape[0] < 2:
        raise ValueError("Número insuficiente de municípios para análise de clusters (menos de 2).")
    
    return True