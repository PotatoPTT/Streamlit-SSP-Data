from utils.data.ssp_pipeline import SSPDataPipeline
from utils.data.pipeline import DatabasePipeline
from utils.config.logging import get_logger
import os
import sys

# __file__ está em src/utils/core/pipeline_runner.py
# Precisamos ir para a RAIZ do projeto (subir 3 níveis: core -> utils -> src -> raiz)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

# Adicionar src/ ao path para imports funcionarem
SRC_DIR = os.path.join(ROOT_DIR, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Mudar para o diretório raiz do projeto
os.chdir(ROOT_DIR)

logger = get_logger("PIPELINE")
logger.info(f"Diretório de trabalho definido para: {ROOT_DIR}")


class PipelineRunner:
    def run(self):
        pipeline = SSPDataPipeline()
        # Supondo que pipeline.run() retorna os anos alterados
        anos_alterados = pipeline.run()
        logger.info(f"Anos alterados: {anos_alterados}")

        # Usar o mesmo caminho que o processador usou para salvar
        processed_data_path = './output/ssp_data_processed/merged_with_coords.csv'
        pipeline = DatabasePipeline(processed_data_path=processed_data_path)
        pipeline.run()

        logger.info("=== Pipeline completo ===")


if __name__ == '__main__':
    PipelineRunner().run()
