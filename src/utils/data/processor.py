import pandas as pd
import os
import warnings
from utils.config.logging import get_logger
warnings.filterwarnings("ignore", category=UserWarning,
                        module="openpyxl")  # Ignorar avisos do openpyxl

# Não muda o diretório aqui - deixa o pipeline_runner fazer isso
# O diretório de trabalho já deve estar definido como a raiz do projeto

logger = get_logger("DATA_PROCESSOR")


class DataProcessor:
    def __init__(self, input_dir, output_dir_processed, location_csv_path=None):
        self.input_dir = input_dir
        self.output_dir_processed = output_dir_processed
        self._ensure_dir(self.output_dir_processed)
        self.location_csv_path = location_csv_path or os.path.join(
            'configs', 'cities_location.csv')

    def _ensure_dir(self, directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f"Diretório criado: {directory_path}")

    def process_files(self):
        logger.info(
            f"Iniciando carregamento e processamento dos arquivos em: {self.input_dir}")
        
        # Verificar se o diretório de entrada existe
        if not os.path.exists(self.input_dir):
            logger.error(f"Diretório de entrada não encontrado: {self.input_dir}")
            logger.error(f"Diretório de trabalho atual: {os.getcwd()}")
            return
        
        # Listar arquivos no diretório
        all_files = os.listdir(self.input_dir)
        logger.info(f"Total de arquivos no diretório: {len(all_files)}")
        
        dfs = []
        for filename in all_files:
            if filename.endswith(('.csv', '.xlsx', '.json')):
                file_path = os.path.join(self.input_dir, filename)
                try:
                    import re
                    match = re.match(
                        r'(.+?)\((\d+)\)-(.+?)\((\d+)\)-(\d+)\((\d{2}-\d{2}-\d{4})\)', filename)
                    if match:
                        regiao = match.group(1)
                        id_regiao = match.group(2)
                        municipio = match.group(3)
                        id_municipio = match.group(4)
                        ano = match.group(5)
                        data_coleta = match.group(6)
                    else:
                        regiao = id_regiao = municipio = id_municipio = ano = data_coleta = None
                        logger.warning(
                            f"Nome de arquivo inesperado: {filename}")
                    if filename.endswith('.csv'):
                        df = pd.read_csv(file_path, dtype=str)
                    elif filename.endswith('.xlsx'):
                        df = pd.read_excel(file_path, dtype=str)
                    else:
                        df = pd.read_json(file_path)
                    df['Nome_Regiao'] = regiao
                    df['ID_Regiao'] = id_regiao
                    df['Nome_Municipio'] = municipio
                    df['ID_Municipio'] = id_municipio
                    df['Ano'] = ano
                    df['Data_Coleta'] = data_coleta
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"✗ Erro ao ler {filename}: {e}")
        
        logger.info(f"Total de arquivos processados com sucesso: {len(dfs)}")
        
        if not dfs:
            logger.error("Nenhum arquivo de dados lido. Abortando processo.")
            return
        combined_df = pd.concat(dfs, ignore_index=True)
        # Substituir '...' por NaN em todo o DataFrame
        combined_df = combined_df.replace('...', pd.NA)
        logger.info(
            f"Dados combinados totalizando {len(combined_df)} linhas.")

        if not os.path.exists(self.location_csv_path):
            logger.error(
                f"Arquivo de localização de cidades não encontrado em: {self.location_csv_path}")
            return
        loc_df = pd.read_csv(self.location_csv_path)
        expected_cols = ['Nome_Municipio', 'latitude', 'longitude']
        if any(col not in loc_df.columns for col in expected_cols):
            logger.error(
                f"Colunas inválidas em {self.location_csv_path}. Esperado: {expected_cols}")
            return
        if 'Nome_Municipio' not in combined_df.columns:
            possiveis = [
                c for c in combined_df.columns if 'municipio' in c.lower()]
            if possiveis:
                combined_df['Nome_Municipio'] = combined_df[possiveis[0]]
            else:
                logger.error(
                    "Nenhuma coluna de município encontrada nos dados combinados.")
                return
        # Merge direto sem tratar nomes, pois os nomes já estão padronizados
        combined_df = combined_df.merge(
            loc_df,
            on='Nome_Municipio',
            how='left'
        )

        # Corrigir valores numéricos com ponto como separador de milhar (ex: 12.008 -> 12008)
        for col in combined_df.columns:
            if col in ['latitude', 'longitude']:
                continue
            # Se a coluna for do tipo object (string), tentar converter removendo pontos
            if combined_df[col].dtype == object:
                # Substituir pontos apenas em strings que parecem números
                combined_df[col] = combined_df[col].apply(lambda x: int(
                    str(x).replace('.', '')) if str(x).replace('.', '').isdigit() else x)
            elif combined_df[col].dtype == float:
                if (combined_df[col].dropna() % 1 == 0).all():
                    combined_df[col] = combined_df[col].astype('Int64')
        output_file = os.path.join(
            self.output_dir_processed, 'merged_with_coords.csv')
        combined_df.to_csv(output_file, index=False)
        logger.info(f"Arquivo final salvo em: {output_file}")


if __name__ == '__main__':
    logger.warning(
        "Este script não está rodando dentro do pipeline ssp_pipeline.py.")
    DOWNLOADED_DATA_DIR = "./output/ssp_data"
    PROCESSED_DATA_DIR = "./output/ssp_data_processed"
    processor = DataProcessor(
        input_dir=DOWNLOADED_DATA_DIR, output_dir_processed=PROCESSED_DATA_DIR)
    processor.process_files()
