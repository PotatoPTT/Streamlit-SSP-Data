"""
Gerenciamento de jobs e solicitações da API.
"""

import json
import os
from utils.config.logging import get_logger
from utils.ml.config import MODELS_OUTPUT_DIR

logger = get_logger("JOBS")


def get_pending_job(db_conn):
    """Busca a primeira solicitação pendente no banco de dados."""
    # Prioriza solicitações cujo parametro 'metodo' = 'kmeans' para executar primeiro
    query = """
        SELECT id, parametros
        FROM solicitacoes_modelo
        WHERE status = 'PENDENTE'
        ORDER BY (CASE WHEN parametros->>'metodo' = 'kmeans' THEN 0 ELSE 1 END), data_solicitacao
        LIMIT 1;
    """
    result = db_conn.fetch_all(query)
    if result:
        job_id, params = result[0]  # O segundo valor já é um dict
        return job_id, params       # Retorna diretamente, sem json.loads()
    return None, None


def validate_existing_models(db_conn):
    """Valida modelos existentes no startup e corrige inconsistências."""
    logger.info("Validando modelos existentes...")
    
    try:
        # 1) Qualquer CONCLUIDO com artefato ausente -> marcar como FALHOU
        rows = db_conn.fetch_all("SELECT id, parametros, arquivo FROM solicitacoes_modelo WHERE status = 'CONCLUIDO';")
        
        for r in rows:
            job_id = r[0]
            params = r[1]
            arquivo_blob = r[2]  # Coluna arquivo (bytea)
            
            # parametros pode retornar como dict (jsonb) ou como texto
            try:
                if isinstance(params, str):
                    params = json.loads(params)
            except Exception:
                params = None

            # Verificar se existe modelo no arquivo OU no banco de dados
            has_file_artifact = False
            has_db_artifact = arquivo_blob is not None and len(arquivo_blob) > 0
            
            # Tentar reconstruir o nome do arquivo esperado para verificar se existe no disco
            if params and isinstance(params, dict):
                try:
                    from utils.ml.file_manager import generate_model_filename
                    filename = generate_model_filename(params)
                    full_path = str(MODELS_OUTPUT_DIR / filename)
                    has_file_artifact = os.path.exists(full_path)
                except Exception:
                    full_path = None

            # Se não tem artefato nem no arquivo nem no banco, marcar como FALHOU
            if not has_file_artifact and not has_db_artifact:
                msg = f"Artefato não encontrado nem no arquivo nem no banco de dados"
                logger.warning(f"[startup] Job {job_id} marcado como FALHOU: {msg}")
                db_conn.update_solicitacao_status(job_id, 'FALHOU', mensagem_erro=msg)
            else:
                if has_file_artifact and has_db_artifact:
                    logger.debug(f"[startup] Job {job_id}: artefato encontrado no arquivo E no banco")
                elif has_file_artifact:
                    logger.debug(f"[startup] Job {job_id}: artefato encontrado apenas no arquivo")
                else:
                    logger.debug(f"[startup] Job {job_id}: artefato encontrado apenas no banco de dados")

        # 2) Qualquer PROCESSANDO -> definir como PENDENTE (para tentar novamente)
        processing_rows = db_conn.fetch_all("SELECT id FROM solicitacoes_modelo WHERE status = 'PROCESSANDO';")
        for r in processing_rows:
            jid = r[0]
            logger.info(f"[startup] Job {jid} estava PROCESSANDO; repondo para PENDENTE.")
            db_conn.update_solicitacao_status(jid, 'PENDENTE')
            
        logger.info("Validação de modelos existentes concluída.")
        
    except Exception as e:
        logger.exception(f"[startup] Erro ao validar solicitacoes: {e}")


def process_job(db_conn, job_id, params):
    """Processa um job específico."""
    from utils.ml.data_processor import fetch_data_for_job, prepare_and_clean_data, validate_time_series_data
    from utils.ml.trainer import train_and_find_best_model, validate_model_training_params
    from utils.ml.file_manager import save_model_and_blob
    
    logger.info(f"\n[+] Nova solicitação encontrada (ID: {job_id}).")
    db_conn.update_solicitacao_status(job_id, 'PROCESSANDO')
    logger.info(f"  -> Status atualizado para PROCESSANDO.")

    try:
        # 1. Buscar dados do banco
        time_series_df = fetch_data_for_job(db_conn, params)
        validate_time_series_data(time_series_df)
        
        # 2. Preparar e limpar dados (lógica do Clustering Project)
        crime_name = params.get('crime', 'Unknown')
        time_series_data, city_names, cleaning_stats = prepare_and_clean_data(
            time_series_df, crime_name)
        
        if time_series_data is None or city_names is None:
            raise ValueError("Falha na preparação e limpeza dos dados")
        
        logger.info(f"Dados limpos: {len(city_names)} municípios válidos")
        
        # 3. Validar parâmetros e treinar modelo (busca melhor K de 3 a 15)
        metodo = params.get('metodo', 'kmeans')
        validate_model_training_params(metodo)
        
        model, scaler_type, best_k, best_score = train_and_find_best_model(
            time_series_data, city_names, metodo, crime_name)
        
        if model is None:
            raise ValueError("Falha no treinamento do modelo")
        
        logger.info(f"Modelo treinado: K={best_k}, Silhueta={best_score:.4f}")
        
        # 4. Salvar o modelo com estatísticas
        save_model_and_blob(
            db_conn, job_id, params, model, scaler_type, 
            best_k, best_score, city_names, cleaning_stats)

        # 5. Atualizar status para CONCLUIDO
        db_conn.update_solicitacao_status(job_id, 'CONCLUIDO')
        logger.info(f"  -> Status finalizado: CONCLUIDO.")
        
        return True

    except Exception as e:
        error_message = f"Erro no processamento do job {job_id}: {e}"
        logger.exception(f"[!] ERRO: {error_message}")
        raise  # Re-raise para ser tratado no nível superior