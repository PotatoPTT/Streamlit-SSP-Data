"""
Utilitários para análises preditivas e processamento de dados de séries temporais.
"""

import pandas as pd
import joblib
import os
import json
from pathlib import Path
import streamlit as st
from utils.data.connection import DatabaseConnection
from utils.config.logging import get_logger

logger = get_logger("ANALYTICS_UTILS")


def fetch_data_for_model(db_conn, params):
    """Busca e prepara os dados de séries temporais para o modelo."""
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

    query += " GROUP BY m.nome, ano_mes ORDER BY m.nome, ano_mes;"

    df = pd.DataFrame(db_conn.fetch_all(query, tuple(sql_params)), columns=[
                      'municipio', 'ano_mes', 'quantidade'])

    if df.empty:
        return pd.DataFrame()

    time_series_df = df.pivot_table(
        index='municipio', columns='ano_mes', values='quantidade').fillna(0)
    return time_series_df


def get_meses_mapping():
    """Retorna o mapeamento entre nomes de meses e números."""
    meses_map = {
        "Janeiro": 1, "Fevereiro": 2, "Março": 3, "Abril": 4, "Maio": 5, "Junho": 6,
        "Julho": 7, "Agosto": 8, "Setembro": 9, "Outubro": 10, "Novembro": 11, "Dezembro": 12
    }
    meses_map_inv = {v: k for k, v in meses_map.items()}
    return meses_map, meses_map_inv


def build_model_params(ano_inicio, mes_inicio, ano_fim, mes_fim, regiao_selecionada, crime_selecionado):
    """Constrói os parâmetros do modelo a partir dos inputs da interface."""
    meses_map, _ = get_meses_mapping()
    
    mes_inicio_num = meses_map[mes_inicio]
    mes_fim_num = meses_map[mes_fim]

    return {
        "data_inicio": f"{ano_inicio}-{mes_inicio_num:02d}",
        "data_fim": f"{ano_fim}-{mes_fim_num:02d}",
        "regiao": regiao_selecionada,
        "crime": crime_selecionado,
        "tipo_modelo": "predicao_ocorrencias"
    }


def get_status_label(solicitacao, name):
    """Retorna o rótulo de status formatado para exibição."""
    if not solicitacao:
        return f"{name}: (nenhuma)"
    return f"{name}: {solicitacao['status']}"


def load_model_from_file_or_db(model_filename, selected_solicit, db):
    """Carrega o modelo do arquivo local ou do banco de dados."""
    project_root = Path(__file__).resolve().parents[4]
    
    if os.path.isabs(model_filename):
        model_full_path = model_filename
    else:
        model_full_path = str(project_root / 'output' / 'models' / model_filename)

    if not os.path.exists(model_full_path):
        # Tenta buscar artefato do DB
        try:
            solicit_id = selected_solicit.get('id')
            if solicit_id:
                blob = db.fetch_model_blob_by_solicitacao(solicit_id)
                if blob:
                    out_dir = Path(project_root) / 'output' / 'models'
                    out_dir.mkdir(parents=True, exist_ok=True)
                    with open(model_full_path, 'wb') as f:
                        f.write(blob)
                    st.info('Artefato baixado do banco (por solicitacao) e salvo localmente.')
                else:
                    raise FileNotFoundError(f"Artefato não encontrado no banco para solicitação {solicit_id}")
            else:
                raise FileNotFoundError(f"ID da solicitação não encontrado")
        except Exception as e:
            raise FileNotFoundError(f"Erro ao recuperar artefato: {e}")

    return joblib.load(model_full_path)


def get_model_filename(method, params):
    """Gera o nome do arquivo do modelo baseado nos parâmetros."""
    return f"model_{method}_{params['data_inicio']}_{params['data_fim']}_{params['regiao']}_{params['crime']}.joblib"


def prepare_municipalities_table(time_series_df_with_labels, db):
    """Prepara a tabela de municípios com informações de cluster e região."""
    table_df = time_series_df_with_labels[['cluster']].reset_index()
    table_df.rename(columns={table_df.columns[0]: 'municipio'}, inplace=True)
    
    municipios_list = table_df['municipio'].unique().tolist()
    if municipios_list:
        q = 'SELECT m.nome, r.nome FROM municipios m JOIN regioes r ON m.regiao_id = r.id WHERE m.nome = ANY(%s);'
        rows = db.fetch_all(q, (municipios_list,))
        regions_df = pd.DataFrame(rows, columns=['municipio', 'regiao'])
        table_df = table_df.merge(regions_df, on='municipio', how='left')
    else:
        table_df['regiao'] = None
    
    return table_df[['municipio', 'regiao', 'cluster']].sort_values('cluster')


def get_available_months_for_year(df_meses_por_ano, year):
    """Retorna os meses disponíveis para um ano específico."""
    return sorted(df_meses_por_ano[df_meses_por_ano["ano"] == year]["mes"].unique())


def filter_end_months(meses_disponiveis_fim, ano_fim, ano_inicio, mes_inicio_num):
    """Filtra os meses disponíveis para o fim do período baseado no início."""
    if ano_fim == ano_inicio:
        return [m for m in meses_disponiveis_fim if m >= mes_inicio_num]
    else:
        return meses_disponiveis_fim


# ==================== FUNÇÕES DE CACHE ====================

@st.cache_data(ttl=300)  # Cache por 5 minutos
def get_crimes_list():
    """Busca a lista de crimes disponíveis no banco."""
    db = DatabaseConnection()
    try:
        crimes = db.fetch_all("SELECT DISTINCT natureza FROM crimes ORDER BY natureza")
        return [crime[0] for crime in crimes]
    finally:
        db.close()


@st.cache_data(ttl=60)  # Cache por 1 minuto para verificações de status
def get_solicitacao_by_params_cached(params_str):
    """Versão cacheada da busca de solicitação por parâmetros."""
    params = json.loads(params_str)
    
    db = DatabaseConnection()
    try:
        return db.get_solicitacao_by_params(params)
    finally:
        db.close()


@st.cache_data(ttl=300)  # Cache por 5 minutos
def fetch_data_for_model_cached(params_str):
    """Versão cacheada da busca de dados para o modelo."""
    params = json.loads(params_str)
    db = DatabaseConnection()
    try:
        return fetch_data_for_model(db, params)
    finally:
        db.close()


@st.cache_data(ttl=30)  # Cache por 30 segundos para operações de escrita
def update_solicitacao_status_cached(solicitacao_id, novo_status):
    """Versão cacheada para atualizar status da solicitação."""
    db = DatabaseConnection()
    try:
        result = db.update_solicitacao_status(solicitacao_id, novo_status)
        # Limpar cache relacionado após update
        get_solicitacao_by_params_cached.clear()
        return result
    finally:
        db.close()


@st.cache_data(ttl=60)  # Cache por 1 minuto
def create_or_reactivate_solicitacao_cached(params_str):
    """Versão cacheada para criar ou reativar solicitação."""
    params = json.loads(params_str)
    
    db = DatabaseConnection()
    try:
        result = db.create_or_reactivate_solicitacao(params)
        # Limpar cache relacionado após criação/reativação
        get_solicitacao_by_params_cached.clear()
        return result
    finally:
        db.close()


def limpar_cache_analytics():
    """Limpa todo o cache relacionado ao analytics."""
    try:
        get_crimes_list.clear()
        get_solicitacao_by_params_cached.clear()
        fetch_data_for_model_cached.clear()
        update_solicitacao_status_cached.clear()
        create_or_reactivate_solicitacao_cached.clear()
        logger.info("Cache do analytics limpo com sucesso")
    except Exception as e:
        logger.error(f"Erro ao limpar cache do analytics: {e}")