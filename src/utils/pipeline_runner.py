from src.utils.download.ssp_pipeline import SSPDataPipeline
from src.utils.database.database_pipeline import DatabasePipeline
from src.utils.graph.graph_pipeline import GraphPipeline
import logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)


class PipelineRunner:
    def run(self):
        pipeline = SSPDataPipeline()
        pipeline.run()

        pipeline = DatabasePipeline()
        pipeline.run()

        graph_pipeline = GraphPipeline()
        graph_pipeline.run()


if __name__ == '__main__':
    logging.warning("Este script não está rodando dentro do pipeline ssp_pipeline.py.")
    PipelineRunner().run()
