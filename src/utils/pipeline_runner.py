from src.utils.download.ssp_pipeline import SSPDataPipeline
from src.utils.database.database_pipeline import DatabasePipeline
from src.utils.graph.graph_pipeline import GraphPipeline
import logging
import os
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)


class PipelineRunner:
    def run(self):
        pipeline = SSPDataPipeline()
        # Supondo que pipeline.run() retorna os anos alterados
        anos_alterados = pipeline.run()
        logging.info(f"Anos alterados: {anos_alterados}")

        pipeline = DatabasePipeline()
        pipeline.run()

        # Gera mapas apenas para anos alterados ou se não existir mapa para o ano
        graph_pipeline = GraphPipeline()
        anos_para_gerar = set()
        # Sempre gera mapas para anos alterados
        if anos_alterados:
            anos_para_gerar.update(anos_alterados)
        # Remove duplicatas e força geração para anos alterados, mesmo se pasta existir
        for ano in range(2025, 2000, -1):
            if ano not in anos_para_gerar:
                ano_dir = os.path.join(graph_pipeline.output_dir, str(ano))
                if not os.path.exists(ano_dir) or not os.listdir(ano_dir):
                    anos_para_gerar.add(ano)
        if anos_para_gerar:
            # Cria uma única instância e chama run para todos os anos
            graph_pipeline.run(years_list=sorted(anos_para_gerar))
        else:
            logging.info("Nenhum ano alterado ou faltando mapa. Nada a gerar.")
        logging.info("=== Pipeline completo ===")


if __name__ == '__main__':
    logging.warning(
        "Este script não está rodando dentro do pipeline ssp_pipeline.py.")
    PipelineRunner().run()
