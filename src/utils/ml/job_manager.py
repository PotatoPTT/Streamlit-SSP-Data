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
        rows = db_conn.fetch_all("SELECT id, parametros FROM solicitacoes_modelo WHERE status = 'CONCLUIDO';")
        
        for r in rows:
            job_id = r[0]
            params = r[1]
            
            # parametros pode retornar como dict (jsonb) ou como texto
            try:
                if isinstance(params, str):
                    params = json.loads(params)
            except Exception:
                params = None

            # Tentar reconstruir o nome do arquivo esperado
            if params and isinstance(params, dict):
                try:
                    from utils.ml.file_manager import generate_model_filename
                    filename = generate_model_filename(params)
                    full_path = str(MODELS_OUTPUT_DIR / filename)
                except Exception:
                    full_path = None
            else:
                full_path = None

            if not full_path or not os.path.exists(full_path):
                msg = f"Artefato não encontrado (esperado: {full_path})"
                logger.warning(f"[startup] Job {job_id} marcado como FALHOU: {msg}")
                db_conn.update_solicitacao_status(job_id, 'FALHOU', mensagem_erro=msg)

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
    from utils.ml.data_processor import fetch_data_for_job
    from utils.ml.trainer import train_and_find_best_model
    from utils.ml.file_manager import save_model_and_blob
    
    logger.info(f"\n[+] Nova solicitação encontrada (ID: {job_id}).")
    db_conn.update_solicitacao_status(job_id, 'PROCESSANDO')
    logger.info(f"  -> Status atualizado para PROCESSANDO.")

    try:
        # 1. Buscar e transformar os dados
        time_series_df = fetch_data_for_job(db_conn, params)

        # 2. Treinar e encontrar o melhor modelo (respeitando o método solicitado)
        metodo = params.get('metodo', 'kmeans')
        logger.info(f"  -> Método solicitado: {metodo}")
        model, scaler, best_k, best_score = train_and_find_best_model(
            time_series_df, metodo=metodo)

        # 3. Salvar o modelo e o scaler
        save_model_and_blob(db_conn, job_id, params, model, scaler, best_k, best_score)

        # 4. Atualizar status para CONCLUIDO
        db_conn.update_solicitacao_status(job_id, 'CONCLUIDO')
        logger.info(f"  -> Status finalizado: CONCLUIDO.")
        
        return True

    except Exception as e:
        error_message = f"Erro no processamento do job {job_id}: {e}"
        logger.exception(f"[!] ERRO: {error_message}")
        raise  # Re-raise para ser tratado no nível superior