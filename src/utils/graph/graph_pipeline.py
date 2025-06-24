import os
import logging
from utils.graph.mapPlotter import MapPlotter

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)


class GraphPipeline:
    def __init__(self, coords_path=None, output_dir=None):
        default_coords = os.path.join('configs', 'cities_location.csv')
        default_output = os.path.join('output', 'maps')
        self.output_dir = output_dir or default_output
        self.plotter = MapPlotter(
            output_dir=self.output_dir
        )

    def run(self, year_filter=None, years_list=None):
        logging.info("=== Início do Pipeline de Geração de Mapas ===")
        if years_list is not None:
            for year in years_list:
                self.plotter.plot_maps_by_year_and_crime_db(year_filter=year)
        else:
            self.plotter.plot_maps_by_year_and_crime_db(
                year_filter=year_filter)
        logging.info("=== Pipeline de Mapas concluído ===")


if __name__ == '__main__':
    pipeline = GraphPipeline()
    pipeline.run()
