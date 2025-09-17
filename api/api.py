import sys
import time
from pathlib import Path

# Adiciona o diretório raiz ao path para encontrar os módulos
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from src.utils.database.connection import DatabaseConnection
from src.utils.api import (
    setup_logging, get_logger, POLLING_INTERVAL_SECONDS,
    get_pending_job, validate_existing_models, process_job
)


def main():
    """Loop principal da API."""
    # Setup do logging
    logger = setup_logging()
    
    logger.info("--- API de Treinamento de Modelos iniciada ---")
    logger.info(f"Verificando solicitações a cada {POLLING_INTERVAL_SECONDS} segundos...")
    logger.info(f"Esperando 30s para inicializar completamente...")
    time.sleep(30)  # Espera inicial para garantir que o DB esteja pronto
    
    # Validação inicial dos modelos existentes
    try:
        db_start = DatabaseConnection()
        validate_existing_models(db_start)
    except Exception as e:
        logger.exception(f"Erro durante validação inicial: {e}")
    finally:
        try:
            db_start.close()
        except Exception:
            pass

    # Loop principal
    while True:
        db = None
        try:
            db = DatabaseConnection()
            job_id, params = get_pending_job(db)

            if job_id:
                try:
                    process_job(db, job_id, params)
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
            logger.info("\n--- API encerrada pelo usuário. ---")
            break
        except Exception as e:
            logger.exception(f"\n[!] Erro inesperado no loop principal: {e}")
            logger.info("Aguardando antes de tentar novamente...")
            time.sleep(POLLING_INTERVAL_SECONDS * 2)
        finally:
            if db:
                db.close()


if __name__ == "__main__":
    main()
