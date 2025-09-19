"""
Substituição do antigo plotter folium por uma implementação leve que delega
à visualização Plotly (analytics_plots.plot_maps_crime_counts_plotly).

Isso remove toda a lógica de geração de HTML/folium e mantém uma API
compatível para chamadores que esperam um método `plot_maps_by_year_and_crime_db`.
"""

from utils.data.connection import DatabaseConnection
from utils.visualization.analytics_plots import plot_maps_crime_counts_plotly
from utils.config.logging import get_logger

logger = get_logger("MAPS")


class MapPlotter:
    """Wrapper leve: busca dados do DB e delega à função Plotly.

    Mantemos a assinatura `plot_maps_by_year_and_crime_db(year_filter)` para
    compatibilidade com `GraphPipeline` e chamadas existentes.
    """

    def __init__(self, output_dir=None):
        # output_dir mantido apenas para compatibilidade com código anterior
        self.output_dir = output_dir

    def plot_maps_by_year_and_crime_db(self, year_filter=None):
        """Busca dados e chama o plotter Plotly (render inline no Streamlit).

        Observação: esta função agora renderiza inline e não salva arquivos HTML.
        """
        db = DatabaseConnection()
        try:
            df = db.get_map_data(year=year_filter)
            if df is None or df.empty:
                logger.warning('Nenhum dado encontrado no banco para o filtro.')
                return

            # Chama a função Plotly que renderiza no Streamlit
            plot_maps_crime_counts_plotly(df, year=year_filter)

        finally:
            try:
                db.close()
            except Exception:
                pass
