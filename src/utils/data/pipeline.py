import os
import pandas as pd
from utils.data.connection import DatabaseConnection
from utils.config.logging import get_logger

logger = get_logger("DATABASE")

class DatabasePipeline:
    db = None
    df = None
    def __init__(self, processed_data_path=None):
        self.processed_data_path = processed_data_path or os.path.join('ssp_data_processed', 'merged_with_coords.csv')
        self.df = pd.read_csv(self.processed_data_path)

    def run(self):
        logger.info("=== Início do Pipeline de Inserção no Banco de Dados ===")
        if self.df is None:
            raise ValueError("DataFrame não inicializado corretamente.")
        with DatabaseConnection() as db:
            db.insert_all(self.df)
            logger.info('Dados enviados para o banco com sucesso!')
        logger.info("=== Pipeline de Inserção no Banco de Dados concluído ===")

if __name__ == '__main__':
    logger.warning("Este script não está rodando dentro do pipeline ssp_pipeline.py.")
    pipeline = DatabasePipeline()
    pipeline.run()
