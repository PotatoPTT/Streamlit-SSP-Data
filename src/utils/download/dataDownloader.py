import requests
import urllib3
import os
import time
import shutil
import pandas as pd
from datetime import datetime
import concurrent.futures
from threading import Lock
from geopy.geocoders import Nominatim  # type: ignore
from typing import Optional
from geopy.location import Location  # type: ignore
import logging
import re
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)


class SSPDataDownloader:
    def __init__(self, base_url, years, grupo_delito, tipo_grupo, output_dir, zip_filename, csv_file_path, max_workers, debug_mode, headers, download_everything):
        self.base_url = base_url
        self.years = years
        self.grupo_delito = grupo_delito
        self.tipo_grupo = tipo_grupo
        self.output_dir = output_dir
        self.zip_filename = zip_filename
        self.csv_file_path = csv_file_path
        self.max_workers = max_workers
        self.debug_mode = debug_mode
        self.headers = headers
        self.download_everything = download_everything
        self.arquivos_baixados_count = 0
        self.count_lock = Lock()

    def _ensure_dir(self, directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logging.info(f"Diretório criado: {directory_path}")

    def _load_municipios_data(self):
        try:
            df = pd.read_csv(self.csv_file_path)
            expected_cols = ['Nome_Regiao', 'ID_Regiao',
                             'Nome_Municipio', 'ID_Municipio']
            missing_cols = [
                col for col in expected_cols if col not in df.columns]
            if missing_cols:
                logging.error(
                    f"Colunas ausentes no arquivo CSV '{self.csv_file_path}': {missing_cols}")
                logging.error(f"Colunas encontradas: {list(df.columns)}")
                logging.error(
                    f"Por favor, verifique se o arquivo CSV contém as colunas: {', '.join(expected_cols)}.")
                return None
            logging.info(
                f"Dados dos municípios carregados de '{self.csv_file_path}'. Total de municípios: {len(df)}")
            return df
        except FileNotFoundError:
            logging.error(
                f"Arquivo CSV não encontrado em '{self.csv_file_path}'. Certifique-se de que o upload foi feito corretamente.")
            return None
        except Exception as e:
            logging.error(
                f"Erro ao ler o arquivo CSV '{self.csv_file_path}': {e}")
            return None

    def _sanitize_filename(self, name):
        # Permite letras (inclusive acentuadas), números, espaço, underline, hífen e apóstrofo
        # Remove apenas caracteres inválidos para nomes de arquivos no Windows
        return re.sub(r"[^\w\s\-áàâãéèêíïóôõöúçñÁÀÂÃÉÈÊÍÏÓÔÕÖÚÇÑ']", '_', str(name), flags=re.UNICODE).rstrip()

    def _download_single_file(self, year, municipio_row, data_atual_formatada):
        regiao_nome = municipio_row['Nome_Regiao']
        regiao_id = municipio_row['ID_Regiao']
        municipio_nome = municipio_row['Nome_Municipio']
        id_municipio = municipio_row['ID_Municipio']

        params = {
            "ano": year,
            "grupoDelito": self.grupo_delito,
            "tipoGrupo": self.tipo_grupo,
            "idGrupo": id_municipio
        }

        regiao_nome_s = self._sanitize_filename(regiao_nome)
        municipio_nome_s = self._sanitize_filename(municipio_nome)
        base_filename_prefix = f"{regiao_nome_s}({regiao_id})-{municipio_nome_s}({id_municipio})-{year}("
        base_filename = f"{regiao_nome_s}({regiao_id})-{municipio_nome_s}({id_municipio})-{year}({data_atual_formatada})"
        log_prefix = f"Ano {year}, Mun {municipio_nome_s}({id_municipio})"

        # Aviso se o nome original tiver caracteres não seguros
        unsafe_pattern = r'[\\/:*?\"<>|]'
        original_filename = f"{regiao_nome}({regiao_id})-{municipio_nome}({id_municipio})-{year}({data_atual_formatada})"
        if re.search(unsafe_pattern, original_filename):
            logging.warning(
                f"{log_prefix}: Nome de arquivo contém caracteres não seguros: '{original_filename}'. Será salvo como: '{base_filename}'")

        for fname in os.listdir(self.output_dir):
            if fname.startswith(base_filename_prefix) and not fname.endswith(f"{data_atual_formatada}).xlsx"):
                try:
                    os.remove(os.path.join(self.output_dir, fname))
                    if self.debug_mode:
                        logging.info(
                            f"{log_prefix}: Arquivo antigo removido: {fname}")
                except Exception as e:
                    logging.warning(
                        f"{log_prefix}: Falha ao remover arquivo antigo {fname}: {e}")

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    self.base_url, headers=self.headers, params=params, timeout=60, verify=False)

                if response.status_code == 200:
                    if not response.content or len(response.content) < 50:
                        logging.warning(
                            f"{log_prefix}: OK (200) mas conteúdo vazio/pequeno. Pulando.")
                        return f"{log_prefix}: Vazio"

                    content_type = response.headers.get(
                        'Content-Type', '').lower()
                    ext = '.bin'
                    if 'application/json' in content_type:
                        ext = '.json'
                    elif 'text/csv' in content_type:
                        ext = '.csv'
                    elif 'excel' in content_type or 'spreadsheetml' in content_type or 'application/vnd.ms-excel' in content_type:
                        ext = '.xlsx'

                    final_filename = f"{base_filename}{ext}"
                    filepath = os.path.join(self.output_dir, final_filename)

                    with open(filepath, 'wb') as f:
                        f.write(response.content)

                    with self.count_lock:
                        self.arquivos_baixados_count += 1
                    if self.debug_mode:
                        logging.info(
                            f"{log_prefix}: SALVO como {final_filename} ({len(response.content)} bytes)")
                    return f"{log_prefix}: Salvo"

                elif response.status_code == 404:
                    logging.warning(f"{log_prefix}: Sem dados (404)")
                    return f"{log_prefix}: 404"
                elif response.status_code == 500:
                    logging.warning(f"{log_prefix}: Erro no servidor (500)")
                    # Retry para erro 500
                elif response.status_code == 204:
                    logging.warning(f"{log_prefix}: Sem conteúdo (204)")
                    return f"{log_prefix}: 204"
                else:
                    logging.warning(
                        f"{log_prefix}: Erro HTTP {response.status_code}")
                    # Retry para outros erros
            except requests.exceptions.Timeout:
                logging.warning(
                    f"{log_prefix}: Timeout na requisição (tentativa {attempt}/{max_retries}).")
            except requests.exceptions.SSLError as e:
                logging.warning(
                    f"{log_prefix}: ERRO SSL (mesmo com verify=False): {e} (tentativa {attempt}/{max_retries})")
            except requests.exceptions.RequestException as e:
                logging.warning(
                    f"{log_prefix}: Falha na requisição: {e} (tentativa {attempt}/{max_retries})")
            if attempt < max_retries:
                time.sleep(2)
        logging.error(f"{log_prefix}: Falha após {max_retries} tentativas.")
        return f"{log_prefix}: Falha após {max_retries} tentativas."

    def _compress_downloaded_files(self):
        if self.arquivos_baixados_count > 0 and os.path.exists(self.output_dir) and os.listdir(self.output_dir):
            print(f"\nCompactando arquivos em {self.zip_filename}...")
            try:
                shutil.make_archive(self.zip_filename.replace(
                    '.zip', ''), 'zip', self.output_dir)
                print(f"Pasta compactada com sucesso: {self.zip_filename}")
            except Exception as e:
                print(f"Erro ao compactar arquivos: {e}")
        elif self.arquivos_baixados_count == 0:
            print("\nNenhum arquivo foi baixado, então nada para compactar.")
        else:
            print(
                f"\nO diretório de saída '{self.output_dir}' não existe ou está vazio. Nada para compactar.")

    def _get_expected_filename(self, year, municipio_row, data_atual_formatada):
        regiao_nome = municipio_row['Nome_Regiao']
        regiao_id = municipio_row['ID_Regiao']
        municipio_nome = municipio_row['Nome_Municipio']
        id_municipio = municipio_row['ID_Municipio']
        regiao_nome_s = self._sanitize_filename(regiao_nome)
        municipio_nome_s = self._sanitize_filename(municipio_nome)
        base_filename = f"{regiao_nome_s}({regiao_id})-{municipio_nome_s}({id_municipio})-{year}({data_atual_formatada})"
        # Considera possíveis extensões
        for ext in ['.csv', '.xlsx', '.json', '.bin']:
            candidate = os.path.join(self.output_dir, f"{base_filename}{ext}")
            if os.path.exists(candidate):
                return candidate
        # Se não existe, retorna o nome padrão .csv
        return os.path.join(self.output_dir, f"{base_filename}.csv")

    def _is_file_valid(self, filepath):
        if not os.path.exists(filepath):
            return False
        try:
            if os.path.getsize(filepath) < 50:
                return False
            # Para CSV, tenta ler
            if filepath.endswith('.csv'):
                df = pd.read_csv(filepath)
                if df.empty:
                    return False
        except Exception:
            return False
        return True

    def _get_missing_files(self, year, municipios_df, data_atual_formatada):
        missing = []
        for _, row in municipios_df.iterrows():
            fname = self._get_expected_filename(
                year, row, data_atual_formatada)
            if not self._is_file_valid(fname):
                missing.append((year, row))
        return missing

    def _download_files_for_year(self, year, municipios_df, data_atual_formatada):
        missing = self._get_missing_files(
            year, municipios_df, data_atual_formatada)
        if not missing:
            logging.info(
                f"Todos os arquivos de {year} já existem e são válidos. Nenhum download necessário.")
            return
        logging.info(
            f"Baixando {len(missing)} arquivos faltantes/invalidos para o ano {year}...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(
                self._download_single_file, year, row, data_atual_formatada) for year, row in missing]
            completed_count = 0
            total_tasks = len(futures)
            for future in concurrent.futures.as_completed(futures):
                completed_count += 1
                if completed_count % 50 == 0 or completed_count == total_tasks:
                    logging.info(
                        f"Progresso: {completed_count}/{total_tasks} tarefas de download processadas para {year}.")

    def download_data(self):
        municipios_df = self._load_municipios_data()
        if municipios_df is None or municipios_df.empty:
            logging.error(
                "Nenhum dado de município para processar. Abortando.")
            return

        self._ensure_dir(self.output_dir)
        data_atual_formatada = datetime.now().strftime("%d-%m-%Y")

        logging.info(
            f"Iniciando downloads paralelos para a pasta: {self.output_dir}")
        logging.info(f"Anos: de {self.years.start} até {self.years.stop + 1}")
        logging.info(
            f"Máximo de workers (downloads simultâneos): {self.max_workers}")
        logging.warning(
            "AVISO: A verificação do certificado SSL está desabilitada.")
        logging.info("-" * 30)

        if self.download_everything:
            logging.info(
                "Modo DOWNLOAD_EVERYTHING ativado: baixando todos os arquivos, mesmo que já existam.")
            anos_recentes = [self.years]
            anos_antigos = []
        else:
            ano_atual = datetime.now().year
            ano_baixar_novamente = ano_atual + 1 #! - 2
            anos_antigos = [y for y in self.years if y < ano_baixar_novamente]
            anos_recentes = [
                y for y in self.years if y >= ano_baixar_novamente]

        # Para anos antigos, só baixa o que faltar
        for year in anos_antigos:
            self._download_files_for_year(
                year, municipios_df, data_atual_formatada)

        # Para anos recentes (ano atual e futuros), baixa tudo normalmente
        if anos_recentes:
            logging.info(f"Baixando normalmente para anos: {anos_recentes}")
            futures = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for year in anos_recentes:
                    for index, row in municipios_df.iterrows():
                        futures.append(executor.submit(
                            self._download_single_file, year, row, data_atual_formatada))

                completed_count = 0
                total_tasks = len(futures)
                for future in concurrent.futures.as_completed(futures):
                    completed_count += 1
                    if completed_count % 50 == 0 or completed_count == total_tasks:
                        logging.info(
                            f"Progresso: {completed_count}/{total_tasks} tarefas de download processadas (anos recentes).")

        logging.info("-" * 30)
        logging.info(f"Processamento de download paralelo concluído.")
        logging.info(
            f"Total de arquivos baixados com sucesso: {self.arquivos_baixados_count}")
        if self.debug_mode:
            self._compress_downloaded_files()

    def generate_location_file(self, output_location_path=None):
        """
        Gera um CSV com Nome_Municipio, latitude e longitude usando geopy.
        Faz verificação de integridade: se já existe cities_location.csv, verifica se está completo e sem nulos.
        Se faltar algum município ou houver nulos, baixa apenas os que faltam ou estão nulos.
        """
        output_path = output_location_path or os.path.join(
            'configs', 'cities_location.csv')
        df = self._load_municipios_data()
        if df is None or df.empty:
            logging.error(
                "Nenhum dado de município para gerar localização. Abortando.")
            return
        unique_mun = df['Nome_Municipio'].drop_duplicates(
        ).sort_values().reset_index(drop=True)
        # Tenta carregar cities_location.csv existente
        if os.path.exists(output_path):
            loc_df = pd.read_csv(output_path)
            if 'Nome_Municipio' in loc_df.columns and 'latitude' in loc_df.columns and 'longitude' in loc_df.columns:
                # Verifica se todos os municípios estão presentes e sem nulos
                loc_df = loc_df.drop_duplicates('Nome_Municipio')
                merged = pd.merge(unique_mun.to_frame(), loc_df,
                                  on='Nome_Municipio', how='left')
                missing = merged[merged['latitude'].isnull() |
                                 merged['longitude'].isnull()]
                if len(loc_df) == len(unique_mun) and missing.empty:
                    logging.info(
                        f"Arquivo de localização já está completo ({len(loc_df)} municípios). Nenhum download necessário.")
                    return
                else:
                    logging.info(
                        f"Arquivo cities_location.csv incompleto: {len(missing)} municípios faltando ou com dados nulos. Baixando apenas os necessários...")
                    municipios_para_baixar = missing['Nome_Municipio'].tolist()
                    records = loc_df.to_dict('records')
            else:
                logging.warning(
                    "Arquivo cities_location.csv inválido. Gerando tudo novamente.")
                municipios_para_baixar = unique_mun.tolist()
                records = []
        else:
            municipios_para_baixar = unique_mun.tolist()
            records = []
        geolocator = Nominatim(user_agent="ssp_data_locations", scheme="http")
        total = len(municipios_para_baixar)
        for idx, muni in enumerate(municipios_para_baixar, 1):
            try:
                loc = geolocator.geocode(
                    f"{muni}, São Paulo, Brasil")  # type: ignore
                if isinstance(loc, Location):
                    lat, lon = loc.latitude, loc.longitude
                else:
                    lat, lon = (None, None)
            except Exception:
                lat, lon = (None, None)
            records.append(
                {'Nome_Municipio': muni, 'latitude': lat, 'longitude': lon})
            if idx % 50 == 0 or idx == total:
                logging.info(
                    f"Progresso localização: {idx}/{total} municípios processados.")
            time.sleep(1)
        # Junta com os já existentes (sem sobrescrever os válidos)
        if os.path.exists(output_path) and 'loc_df' in locals() and not loc_df.empty:
            # Atualiza apenas os municípios que estavam faltando
            loc_df = loc_df.set_index('Nome_Municipio')
            for rec in records:
                loc_df.loc[rec['Nome_Municipio']] = [
                    rec['latitude'], rec['longitude']]
            out_df = loc_df.reset_index()
        else:
            out_df = pd.DataFrame(records)
        out_df.to_csv(output_path, index=False)
        logging.info(f"Arquivo de localização salvo em: {output_path}")


# --- Configurações ---
BASE_URL = "https://www.ssp.sp.gov.br/v1/OcorrenciasMensais/ExportarMensal"
YEARS = range(2025, 2000, -1)  # De 2025 até 2001
GRUPO_DELITO = 6
TIPO_GRUPO = "MUNICÍPIO"
OUTPUT_DIR = "./ssp_data"
ZIP_FILENAME = "./ssp_data.zip"
CSV_FILE_PATH = os.path.join("configs", "cities_codes.csv")
MAX_WORKERS = 10
DOWNLOAD_EVERYTHING = False  # Se True, baixa tudo mesmo que já tenha arquivos válidos
DEBUG = False
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "authorization": "",
}

if __name__ == '__main__':
    start_time = time.time()

    downloader = SSPDataDownloader(
        base_url=BASE_URL,
        years=YEARS,
        grupo_delito=GRUPO_DELITO,
        tipo_grupo=TIPO_GRUPO,
        output_dir=OUTPUT_DIR,
        zip_filename=ZIP_FILENAME,
        csv_file_path=CSV_FILE_PATH,
        max_workers=MAX_WORKERS,
        debug_mode=DEBUG,
        headers=HEADERS,
        download_everything=DOWNLOAD_EVERYTHING
    )
    print("AVISO: Este script não está rodando dentro do pipeline ssp_pipeline.py.")
    downloader.download_data()
    downloader.generate_location_file()

    end_time = time.time()
    print(f"\nTempo total de execução: {end_time - start_time:.2f} segundos.")
