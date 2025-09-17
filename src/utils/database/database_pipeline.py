import os
import pandas as pd
from utils.database.connection import DatabaseConnection
from utils.api.config import get_logger

logger = get_logger("DATABASE")

class DatabasePipeline:
    db = None
    df = None
    def __init__(self, processed_data_path=None):
        self.processed_data_path = processed_data_path or os.path.join('ssp_data_processed', 'merged_with_coords.csv')
        self.df = pd.read_csv(self.processed_data_path)

    def run(self):
        logger.info("=== Início do Pipeline de Inserção no Banco de Dados ===")
        try:
            self.db = DatabaseConnection()
            if self.db is None or  self.df is None:
                raise ValueError("Conexão com o banco de dados ou DataFrame não inicializados corretamente.")
            self.db.insert_all(self.df)
            logger.info('Dados enviados para o banco com sucesso!')
        finally:
            if self.db is not None:
                self.db.close()
        logger.info("=== Pipeline de Inserção no Banco de Dados concluído ===")

if __name__ == '__main__':
    logger.warning("Este script não está rodando dentro do pipeline ssp_pipeline.py.")
    pipeline = DatabasePipeline()
    pipeline.run()
