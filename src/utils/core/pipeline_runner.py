from utils.data.ssp_pipeline import SSPDataPipeline
from utils.data.pipeline import DatabasePipeline
from utils.config.logging import get_logger
import os
# Garante que o diretório de trabalho seja o root do projeto
os.chdir(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

logger = get_logger("PIPELINE")


class PipelineRunner:
    def run(self):
        pipeline = SSPDataPipeline()
        # Supondo que pipeline.run() retorna os anos alterados
        anos_alterados = pipeline.run()
        logger.info(f"Anos alterados: {anos_alterados}")

        pipeline = DatabasePipeline()
        pipeline.run()

        logger.info("=== Pipeline completo ===")


if __name__ == '__main__':
    PipelineRunner().run()
