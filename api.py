import sys
import time
import threading
from datetime import datetime
from pathlib import Path

# Adiciona o diretório raiz ao path para encontrar os módulos
ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / 'src'
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(SRC_DIR))

from src.utils.data.connection import DatabaseConnection
from src.utils.config.logging import get_logger
from src.utils.ml.config import POLLING_INTERVAL_SECONDS
from src.utils.ml import (
    get_pending_job, validate_existing_models, process_job
)

# Configuração do arquivo de lock
LOCK_FILE_PATH = ROOT_DIR / 'configs' / 'api.lock'
LOCK_UPDATE_INTERVAL = 30  # Atualiza o lock a cada 30 segundos
STARTUP_WAIT_SECONDS = 15  # Tempo de espera inicial para garantir que tudo esteja pronto


def update_lock_file():
    """Thread que atualiza o arquivo de lock a cada 30 segundos."""
    while True:
        try:
            LOCK_FILE_PATH.parent.mkdir(exist_ok=True)
            
            # Verifica se foi solicitada a parada
            if LOCK_FILE_PATH.exists():
                with open(LOCK_FILE_PATH, 'r') as f:
                    content = f.read().strip()
                if content == "STOP":
                    # Para o thread e sinaliza para o main loop parar
                    return
            
            # Atualiza com timestamp atual
            with open(LOCK_FILE_PATH, 'w') as f:
                f.write(datetime.now().isoformat())
                
        except Exception as e:
            logger = get_logger("LOCK")
            logger.error(f"Erro ao atualizar lock file: {e}")
        
        time.sleep(LOCK_UPDATE_INTERVAL)


def should_stop():
    """Verifica se a API deve parar baseado no arquivo de lock."""
    try:
        if LOCK_FILE_PATH.exists():
            with open(LOCK_FILE_PATH, 'r') as f:
                content = f.read().strip()
            return content == "STOP"
    except Exception:
        pass
    return False


def main():
    """Loop principal da API."""
    # Inicia a thread do lock file em background
    lock_thread = threading.Thread(target=update_lock_file, daemon=True)
    lock_thread.start()
    
    try:
        # Setup do logging
        logger = get_logger("API")
        logger.info("--- API de Treinamento de Modelos iniciada ---")
        logger.info(f"Verificando solicitações a cada {POLLING_INTERVAL_SECONDS} segundos...")
    except Exception as e:
        logger = get_logger("API")
        logger.error(f"Erro no setup: {e}")
        return

    logger.info(f"Aguardando {STARTUP_WAIT_SECONDS} segundos para garantir que tudo esteja pronto...")
    time.sleep(STARTUP_WAIT_SECONDS)

    # Validação inicial dos modelos existentes
    try:
        db_start = DatabaseConnection()
        validate_existing_models(db_start)
        logger.info("Validação de modelos concluída")
    except Exception as e:
        logger.exception(f"Erro durante validação inicial: {e}")
    finally:
        try:
            db_start.close()
        except Exception:
            pass

    # Loop principal
    while True:
        # Verifica se deve parar
        if should_stop():
            logger.info("Comando de parada recebido")
            break
            
        db = None
        try:
            db = DatabaseConnection()
            job_id, params = get_pending_job(db)

            if job_id:
                logger.info(f"Processando job: {job_id}")
                try:
                    process_job(db, job_id, params)
                    logger.info(f"Job {job_id} processado com sucesso")
                except Exception as e:
                    error_message = f"Erro no processamento do job {job_id}: {e}"
                    logger.exception(f"[!] ERRO: {error_message}")
                    
                    # Fecha a conexão atual para abortar a transação falha
                    if db:
                        db.close()
                    
                    # Reabre a conexão para garantir que o update seja em uma nova transação
                    db = DatabaseConnection()
                    db.update_solicitacao_status(job_id, 'FALHOU', mensagem_erro=str(e))
            else:
                # Se não houver jobs, apenas espera
                time.sleep(POLLING_INTERVAL_SECONDS)

        except (KeyboardInterrupt, SystemExit):
            logger.info("--- API encerrada pelo usuário ---")
            break
        except Exception as e:
            logger.exception(f"Erro inesperado no loop principal: {e}")
            logger.info("Aguardando antes de tentar novamente...")
            time.sleep(POLLING_INTERVAL_SECONDS * 2)
        finally:
            if db:
                db.close()
    
    # Remove o arquivo de lock ao encerrar
    try:
        if LOCK_FILE_PATH.exists():
            LOCK_FILE_PATH.unlink()
            logger.info("Arquivo de lock removido")
    except Exception as e:
        logger.error(f"Erro ao remover arquivo de lock: {e}")


if __name__ == "__main__":
    main()
