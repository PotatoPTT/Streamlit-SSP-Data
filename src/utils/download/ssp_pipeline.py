from src.utils.download.dataDownloader import SSPDataDownloader, BASE_URL, YEARS, GRUPO_DELITO, TIPO_GRUPO, OUTPUT_DIR, ZIP_FILENAME, MAX_WORKERS, DEBUG, HEADERS
import os
from concurrent.futures import ThreadPoolExecutor
from src.utils.download.dataProcessor import DataProcessor
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)


class SSPDataPipeline:
    def __init__(self):
        self.codes_csv = os.path.join('configs', 'cities_codes.csv')
        self.location_csv = os.path.join('configs', 'cities_location.csv')
        self.downloader = SSPDataDownloader(
            base_url=BASE_URL,
            years=YEARS,
            grupo_delito=GRUPO_DELITO,
            tipo_grupo=TIPO_GRUPO,
            output_dir=OUTPUT_DIR,
            zip_filename=ZIP_FILENAME,
            csv_file_path=self.codes_csv,
            max_workers=MAX_WORKERS,
            debug_mode=DEBUG,
            headers=HEADERS,
            download_everything=False
        )
        self.processor = DataProcessor(
            input_dir=OUTPUT_DIR,
            output_dir_processed=OUTPUT_DIR + '_processed',
            location_csv_path=self.location_csv
        )

    def run(self):
        logging.info("=== Início do Pipeline SSP Data ===")
        loc_file = self.location_csv
        codes_file = self.codes_csv
        needs_location_update = False
        anos_alterados = set()
        if not os.path.exists(loc_file):
            needs_location_update = True
        else:
            # Verifica integridade do arquivo de localização
            try:
                codes_df = pd.read_csv(codes_file)
                loc_df = pd.read_csv(loc_file)
                unique_codes = codes_df['Nome_Municipio'].drop_duplicates(
                ).sort_values().reset_index(drop=True)
                if 'Nome_Municipio' not in loc_df.columns or 'latitude' not in loc_df.columns or 'longitude' not in loc_df.columns:
                    needs_location_update = True
                else:
                    loc_df = loc_df.drop_duplicates('Nome_Municipio')
                    merged = pd.merge(unique_codes.to_frame(),
                                      loc_df, on='Nome_Municipio', how='left')
                    missing = merged[merged['latitude'].isnull(
                    ) | merged['longitude'].isnull()]
                    if len(loc_df) != len(unique_codes) or not missing.empty:
                        needs_location_update = True
            except Exception as e:
                logging.error(
                    f"Erro ao verificar integridade de cities_location.csv: {e}")
                needs_location_update = True

        # Verificar e gerar arquivo de localização de cidades se necessário
        if needs_location_update:
            logging.warning(
                f"Arquivo de localização ausente ou incompleto. Gerando paralelamente aos downloads...")
            with ThreadPoolExecutor(max_workers=2) as executor:
                f_loc = executor.submit(
                    self.downloader.generate_location_file, output_location_path=self.location_csv)
                f_down = executor.submit(self.downloader.download_data)
                anos_alterados = f_down.result()
                f_loc.result()
        else:
            # Download de dados
            anos_alterados = self.downloader.download_data()
        # Processamento usando arquivo de localização existente
        self.processor.process_files()
        logging.info("=== Pipeline concluído com sucesso ===")
        return anos_alterados


if __name__ == '__main__':
    pipeline = SSPDataPipeline()
    pipeline.run()
