import psycopg2
import pandas as pd
import io
import streamlit as st
import json
import time
from typing import Optional
from utils.config.logging import get_logger
from utils.config.constants import MESES

logger = get_logger("DB")


class DatabaseConnection:
    def __init__(self):
        self._connection_params = {
            "dbname": st.secrets["POSTGRES_DB"],
            "user": st.secrets["POSTGRES_USER"],
            "password": st.secrets["POSTGRES_PASSWORD"],
            "host": st.secrets["POSTGRES_HOST"],
            "port": st.secrets.get("POSTGRES_PORT", 5432),
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        }
        self._connect()

    def _connect(self):
        """Estabelece uma nova conexão com o banco de dados."""
        self.conn = psycopg2.connect(**self._connection_params)
        self.cur = self.conn.cursor()
        logger.debug("Nova conexão estabelecida com o banco de dados")

    def _ensure_connection(self):
        """Verifica e reconecta se a conexão foi perdida."""
        try:
            # Testa se a conexão está ativa
            if self.conn.closed:
                logger.warning("Conexão fechada detectada, reconectando...")
                self._connect()
                return
            
            # Executa um comando simples para verificar a saúde da conexão
            self.cur.execute("SELECT 1")
            self.cur.fetchone()
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logger.warning(f"Conexão perdida ({e}), reconectando...")
            try:
                self.close()
            except:
                pass
            self._connect()

    def __enter__(self):
        """Support 'with DatabaseConnection() as db' usage."""
        return self

    def __exit__(self, exc_type, exc, tb):
        # Ensure connection is closed when exiting a with-block.
        try:
            self.close()
        except Exception:
            # don't raise from __exit__
            pass
        # Do not suppress exceptions
        return False

    def close(self):
        """Fecha a conexão com o banco de dados."""
        try:
            if hasattr(self, 'cur') and self.cur:
                self.cur.close()
        except:
            pass
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
        except:
            pass

    def fetch_all(self, query, params=None):
        self._ensure_connection()
        self.cur.execute(query, params or ())
        return self.cur.fetchall()

    def fetch_one(self, query, params=None):
        """Execute a query and return a single row (or None)."""
        self._ensure_connection()
        self.cur.execute(query, params or ())
        return self.cur.fetchone()

    def fetch_df(self, query, params=None, columns=None):
        """Execute query and return a pandas DataFrame with optional columns.

        Usage: df = db.fetch_df(query, params, columns=[...])
        """
        self._ensure_connection()
        rows = self.fetch_all(query, params)
        if columns:
            return pd.DataFrame(rows, columns=columns)
        return pd.DataFrame(rows)

    def execute(self, query, params=None, commit=False):
        self._ensure_connection()
        """Execute a statement (INSERT/UPDATE/DELETE). Optionally commit."""
        self.cur.execute(query, params or ())
        if commit:
            self.conn.commit()
        return self.cur.rowcount

    def copy_from_stringio(self, string_io, table_name, columns=None, commit=True):
        """Helper for COPY from an in-memory StringIO into a table.

        string_io: file-like with CSV/text content
        table_name: target table
        columns: optional list of columns to specify in COPY
        """
        cols_part = f"({', '.join(columns)})" if columns else ''
        sql = f"COPY {table_name} {cols_part} FROM STDIN WITH (FORMAT text)"
        try:
            with self.conn.cursor() as cur:
                cur.copy_expert(sql, string_io)
            if commit:
                self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erro no copy_from_stringio: {e}")
            return False

    def fetch_time_series_data(self, params):
        """
        Função unificada para buscar dados de séries temporais.

        Args:
            params: Dicionário com parâmetros:
                - data_inicio: Data de início no formato 'YYYY-MM'
                - data_fim: Data de fim no formato 'YYYY-MM'
                - crime: Nome do crime
                - regiao: Nome da região ou 'Todas'

        Returns:
            pd.DataFrame: DataFrame pivotado com municípios como índice e datas como colunas
        """
        logger.info(
            f"Buscando dados para o período de {params['data_inicio']} a {params['data_fim']} na região '{params['regiao']}' para o crime '{params['crime']}'...")

        query = """
            SELECT
                m.nome AS municipio,
                TO_CHAR((o.ano || '-' || LPAD(o.mes::text, 2, '0') || '-01')::date, 'YYYY-MM') AS ano_mes,
                SUM(o.quantidade) AS quantidade
            FROM ocorrencias o
            JOIN municipios m ON o.municipio_id = m.id
            JOIN regioes r ON m.regiao_id = r.id
            JOIN crimes c ON o.crime_id = c.id
            WHERE (o.ano || '-' || LPAD(o.mes::text, 2, '0') || '-01')::date BETWEEN TO_DATE(%s, 'YYYY-MM') AND TO_DATE(%s, 'YYYY-MM')
            AND c.natureza = %s
        """
        sql_params = [params['data_inicio'],
                      params['data_fim'], params['crime']]

        if params['regiao'] != 'Todas':
            query += " AND r.nome = %s"
            sql_params.append(params['regiao'])

        query += """
            GROUP BY m.nome, ano_mes
            ORDER BY m.nome, ano_mes;
        """

        df = pd.DataFrame(self.fetch_all(query, tuple(sql_params)), columns=[
                          'municipio', 'ano_mes', 'quantidade'])

        if df.empty:
            raise ValueError("A consulta de dados não retornou resultados.")

        # Pivotar para formato de série temporal
        time_series_df = df.pivot_table(
            index='municipio', columns='ano_mes', values='quantidade').fillna(0)

        logger.info(
            f"Dados transformados: {time_series_df.shape[0]} municípios e {time_series_df.shape[1]} meses.")
        return time_series_df

    def validate_time_series_data(self, time_series_df):
        """Valida se os dados de série temporal são adequados para análise."""
        if time_series_df.empty:
            raise ValueError("DataFrame de séries temporais está vazio.")

        if time_series_df.shape[1] < 2:
            raise ValueError(
                "Série temporal muito curta para análise (menos de 2 pontos de dados).")

        if time_series_df.shape[0] < 2:
            raise ValueError(
                "Número insuficiente de municípios para análise de clusters (menos de 2).")

        return True

    def insert_regioes(self, regioes_df: pd.DataFrame):
        regioes = regioes_df[['ID_Regiao', 'Nome_Regiao']].drop_duplicates()
        db_regioes = {row[0]: row[1]
                      for row in self.fetch_all('SELECT id, nome FROM regioes;')}
        data_to_upsert = []
        for _, row in regioes.iterrows():
            regiao_id = int(row['ID_Regiao'])
            nome = row['Nome_Regiao']
            if regiao_id not in db_regioes or db_regioes[regiao_id] != nome:
                data_to_upsert.append((regiao_id, nome))
        if data_to_upsert:
            self.cur.executemany('''
                INSERT INTO regioes (id, nome)
                VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE SET nome = EXCLUDED.nome;
            ''', data_to_upsert)
            self.conn.commit()

    def insert_municipios(self, municipios_df: pd.DataFrame):
        municipios = municipios_df[['ID_Municipio', 'Nome_Municipio',
                                    'ID_Regiao', 'latitude', 'longitude']].drop_duplicates()
        db_municipios = {row[0]: (row[1], row[2], row[3], row[4]) for row in self.fetch_all(
            'SELECT id, nome, regiao_id, latitude, longitude FROM municipios;')}
        data_to_upsert = []
        for _, row in municipios.iterrows():
            mun_id = int(row['ID_Municipio'])
            nome = row['Nome_Municipio']
            regiao_id = int(row['ID_Regiao'])
            latitude = float(row['latitude']) if not pd.isna(
                row['latitude']) else None
            longitude = float(row['longitude']) if not pd.isna(
                row['longitude']) else None
            db_tuple = db_municipios.get(
                mun_id, (None, None, None, None, None))
            if (mun_id not in db_municipios or
                db_tuple[0] != nome or
                db_tuple[1] != regiao_id or
                db_tuple[2] != latitude or
                    db_tuple[3] != longitude):
                data_to_upsert.append(
                    (mun_id, nome, regiao_id, latitude, longitude))
        if data_to_upsert:
            self.cur.executemany('''
                INSERT INTO municipios (id, nome, regiao_id, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET nome = EXCLUDED.nome, regiao_id = EXCLUDED.regiao_id, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude;
            ''', data_to_upsert)
            self.conn.commit()

    def insert_crimes(self, crimes_df: pd.DataFrame):
        crimes = crimes_df[['Natureza']].drop_duplicates()
        db_crimes = set(row[0] for row in self.fetch_all(
            'SELECT natureza FROM crimes;'))
        data_to_insert = [(row['Natureza'],) for _, row in crimes.iterrows(
        ) if row['Natureza'] not in db_crimes]
        if data_to_insert:
            self.cur.executemany('''
                INSERT INTO crimes (natureza)
                VALUES (%s)
                ON CONFLICT (natureza) DO NOTHING;
            ''', data_to_insert)
            self.conn.commit()

    def get_crime_map(self):
        self.cur.execute('SELECT id, natureza FROM crimes;')
        return {n: i for i, n in self.cur.fetchall()}

    def copy_ocorrencias(self, df: pd.DataFrame, crime_map: dict, ano=None):
        if ano is None:
            anos = sorted(df['Ano'].unique())
        elif isinstance(ano, (list, tuple, set)):
            anos = list(ano)
        else:
            anos = [ano]
        
        # Verificar quais colunas de meses existem no DataFrame
        colunas_disponiveis = set(df.columns)
        meses_no_df = [mes for mes in MESES if mes in colunas_disponiveis]
        
        if not meses_no_df:
            logger.error(f"Nenhuma coluna de mês encontrada no DataFrame. Colunas disponíveis: {list(df.columns)}")
            logger.error(f"Meses esperados: {MESES}")
            raise ValueError("Colunas de meses não encontradas no DataFrame")
        
        logger.info(f"Colunas de meses encontradas: {meses_no_df}")
        
        for a in anos:
            a_int = int(a)
            df_ano = df[df['Ano'] == a]
            db_ocorrencias = {(row[0], row[1], row[2], row[3]): row[4] for row in self.fetch_all(
                'SELECT ano, mes, municipio_id, crime_id, quantidade FROM ocorrencias WHERE ano = %s;', (a_int,))}
            ocorrencias_dict = {}
            for _, row in df_ano.iterrows():
                for i, mes in enumerate(meses_no_df, 1):
                    # Ajustar índice se nem todos os meses estão presentes
                    mes_numero = MESES.index(mes) + 1
                    quantidade = row[mes]
                    if pd.isna(quantidade):
                        continue
                    key = (int(row['Ano']), mes_numero, int(
                        row['ID_Municipio']), crime_map[row['Natureza']])
                    if key not in db_ocorrencias or db_ocorrencias[key] != int(quantidade):
                        ocorrencias_dict[key] = int(
                            quantidade)  # sobrescreve duplicatas
            ocorrencias_data = [(*k, v) for k, v in ocorrencias_dict.items()]
            logger.info(
                f'COPY ocorrências do ano {a} ({len(ocorrencias_data)} registros diferentes)...')
            if not ocorrencias_data:
                logger.info(f'Nenhum dado diferente para inserir no ano {a}.')
                continue
            # Criar CSV em memória
            output = io.StringIO()
            for row in ocorrencias_data:
                output.write('\t'.join(map(str, row)) + '\n')
            output.seek(0)
            temp_table = f"ocorrencias_temp_{a_int}"
            try:
                with self.conn.cursor() as cur:
                    # Cria tabela temporária
                    cur.execute(f'''
                        CREATE TEMP TABLE {temp_table} (
                            ano INT,
                            mes INT,
                            municipio_id INT,
                            crime_id INT,
                            quantidade INT
                        ) ON COMMIT DROP;
                    ''')
                    # COPY para tabela temporária
                    cur.copy_expert(
                        f'''COPY {temp_table} (ano, mes, municipio_id, crime_id, quantidade) FROM STDIN WITH (FORMAT text)''', output)
                    # Upsert para tabela final
                    cur.execute(f'''
                        INSERT INTO ocorrencias (ano, mes, municipio_id, crime_id, quantidade)
                        SELECT ano, mes, municipio_id, crime_id, quantidade FROM {temp_table}
                        ON CONFLICT (ano, mes, municipio_id, crime_id)
                        DO UPDATE SET quantidade = EXCLUDED.quantidade;
                    ''')
                self.conn.commit()
                logger.info(
                    f'Sucesso ao inserir ocorrências do ano {a} via COPY + upsert.')
            except Exception as e:
                self.conn.rollback()
                logger.error(
                    f'Erro ao inserir ocorrências do ano {a} via COPY + upsert: {e}')

    def get_map_data(self, year=None, crime=None):
        """
        Retorna dados para plotagem do mapa, incluindo:
        - Nome_Municipio, latitude, longitude, Ano, Natureza, mês, quantidade
        Pode filtrar por ano e/ou crime.
        """
        query = '''
            SELECT m.nome as Nome_Municipio, m.latitude, m.longitude, o.ano as Ano, c.natureza as Natureza, o.mes, o.quantidade
            FROM ocorrencias o
            JOIN municipios m ON o.municipio_id = m.id
            JOIN crimes c ON o.crime_id = c.id
            WHERE (%s IS NULL OR o.ano = %s)
              AND (%s IS NULL OR c.natureza = %s)
        '''
        params = [year, year, crime, crime]
        self.cur.execute(query, params)
        rows = self.cur.fetchall()
        # Monta DataFrame
        df = pd.DataFrame(rows, columns=[
            'Nome_Municipio', 'latitude', 'longitude', 'Ano', 'Natureza', 'mes', 'quantidade'
        ])
        return df

    def get_solicitacao_by_params(self, params: dict):
        """
        Busca uma solicitação de modelo pelos parâmetros.
        Retorna o dicionário com id, status, parametros (decodificados) e mensagem_erro.
        """
        self._ensure_connection()
        
        params_json = json.dumps(params, sort_keys=True)
        query = '''
            SELECT id, status, parametros, mensagem_erro
            FROM solicitacoes_modelo
            WHERE parametros = %s;
        '''
        self.cur.execute(query, (params_json,))
        result = self.cur.fetchone()
        if result:
            try:
                parametros = json.loads(result[2]) if result[2] else None
            except Exception:
                parametros = None
            return {
                "id": result[0],
                "status": result[1],
                "parametros": parametros,
                "mensagem_erro": result[3]
            }
        return None

    def create_solicitacao(self, params: dict):
        """
        Cria uma nova solicitação de modelo com status 'PENDENTE'.
        Retorna o ID da nova solicitação.
        Limpa automaticamente o cache do Streamlit relacionado às solicitações.
        """
        self._ensure_connection()
        
        params_json = json.dumps(params, sort_keys=True)
        # Inserir a solicitação, mas se já existir (único por parametros),
        # reativar caso o status atual seja 'EXPIRADO' ou 'FALHOU'
        # Sempre retorna o id (novo ou existente).
        query = '''
            INSERT INTO solicitacoes_modelo (parametros, status)
            VALUES (%s, 'PENDENTE')
            ON CONFLICT (parametros) DO UPDATE
            SET status = CASE 
                WHEN solicitacoes_modelo.status IN ('EXPIRADO', 'FALHOU') THEN EXCLUDED.status 
                ELSE solicitacoes_modelo.status 
            END,
                mensagem_erro = CASE 
                    WHEN solicitacoes_modelo.status IN ('EXPIRADO', 'FALHOU') THEN NULL 
                    ELSE solicitacoes_modelo.mensagem_erro 
                END,
                data_atualizacao = NOW()
            RETURNING id;
        '''
        try:
            self.cur.execute(query, (params_json,))
            result = self.cur.fetchone()
            self.conn.commit()

            # Limpar cache do Streamlit automaticamente após criar/reativar solicitação
            self._clear_streamlit_cache()

            return result[0] if result else None
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erro ao criar/reativar solicitação: {e}")
            return None

    def _clear_streamlit_cache(self):
        """
        Limpa o cache do Streamlit relacionado às solicitações.
        Método privado chamado automaticamente após operações que afetam solicitações.
        """
        try:
            # Importar apenas se streamlit estiver disponível
            import streamlit as st

            # Tentar importar e limpar as funções de cache específicas
            try:
                from utils.ui.analytics.utils import (
                    get_solicitacao_by_params_cached,
                    get_solicitacao_by_params_processing
                )

                get_solicitacao_by_params_cached.clear()
                get_solicitacao_by_params_processing.clear()

                # Ativar flag para usar TTL baixo na próxima verificação
                if hasattr(st, 'session_state'):
                    st.session_state.force_refresh_models = True

                logger.debug(
                    "Cache do Streamlit limpo automaticamente após operação de solicitação")

            except ImportError:
                # Se as funções não estiverem disponíveis, não há problema
                logger.debug(
                    "Funções de cache não disponíveis, continuando sem limpeza")
                pass

        except ImportError:
            # Se streamlit não estiver disponível (ex: execução em background), não há problema
            logger.debug(
                "Streamlit não disponível, continuando sem limpeza de cache")
            pass
        except Exception as e:
            # Não queremos que erro de cache quebre a operação principal
            logger.warning(f"Erro ao limpar cache automaticamente: {e}")
            pass

    def update_solicitacao_status(self, solicitacao_id: int, status: str, mensagem_erro: Optional[str] = None):
        """
        Atualiza o status e/ou mensagem de erro de uma solicitação.
        A coluna de arquivo (bytea) é gerenciada por store_model_blob.
        Limpa automaticamente o cache do Streamlit relacionado às solicitações.
        """
        query = '''
            UPDATE solicitacoes_modelo
            SET status = %s,
                mensagem_erro = %s,
                data_atualizacao = NOW()
            WHERE id = %s;
        '''
        try:
            self._ensure_connection()
            self.cur.execute(query, (status, mensagem_erro, solicitacao_id))
            self.conn.commit()

            # Limpar cache do Streamlit automaticamente após atualizar status
            self._clear_streamlit_cache()

            return True
        except Exception as e:
            try:
                self._ensure_connection()
                self.conn.rollback()
            except:
                pass
            logger.error(
                f"Erro ao atualizar status da solicitação {solicitacao_id}: {e}")
            return False

    # --- Artifact storage helpers ---
    def store_model_blob(self, solicitacao_id: int, filename: str, blob: bytes):
        """
        Store the artifact bytes directly in the solicitacoes_modelo.arquivo column for the given solicitacao.
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self._ensure_connection()
                
                # Update the solicitacao record with the blob (arquivo bytea)
                self.cur.execute('''
                    UPDATE solicitacoes_modelo
                    SET arquivo = %s,
                        data_atualizacao = NOW()
                    WHERE id = %s;
                ''', (psycopg2.Binary(blob), solicitacao_id))
                self.conn.commit()
                
                logger.info(f"Modelo armazenado com sucesso no banco (tentativa {retry_count + 1}/{max_retries})")
                return True
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                retry_count += 1
                logger.warning(f"Erro de conexão ao salvar modelo (tentativa {retry_count}/{max_retries}): {e}")
                
                try:
                    self._ensure_connection()
                    self.conn.rollback()
                except:
                    pass
                
                if retry_count >= max_retries:
                    logger.error(f"Falha ao salvar modelo após {max_retries} tentativas")
                    return False
                    
                # Aguarda antes de tentar novamente
                time.sleep(2 ** retry_count)  # Backoff exponencial: 2s, 4s, 8s
                
            except Exception as e:
                try:
                    self._ensure_connection()
                    self.conn.rollback()
                except:
                    pass
                logger.error(
                    f"Erro ao salvar artefato na solicitacao {solicitacao_id}: {e}")
                return False
        
        return False

    def fetch_model_blob_by_solicitacao(self, solicitacao_id: int):
        """
        Returns the bytes stored in solicitacoes_modelo.arquivo for the given solicitacao id, or None if not present.
        """
        try:
            self.cur.execute(
                'SELECT arquivo FROM solicitacoes_modelo WHERE id = %s;', (solicitacao_id,))
            row = self.cur.fetchone()
            if row and row[0] is not None:
                return bytes(row[0])
            return None
        except Exception as e:
            logger.error(
                f"Erro ao buscar artefato por solicitacao {solicitacao_id}: {e}")
            return None

    def insert_all(self, df: pd.DataFrame):
        logger.info("Iniciando inserção de dados no banco...")
        logger.info("Inserindo regioes...")
        self.insert_regioes(df)
        logger.info(f"Inseridas {len(df['ID_Regiao'].unique())} regioes.")
        logger.info("Inserindo municipios...")
        self.insert_municipios(df)
        logger.info(
            f"Inseridos {len(df['ID_Municipio'].unique())} municipios.")
        logger.info("Inserindo crimes...")
        self.insert_crimes(df)
        logger.info(
            f"Inserido {len(df['Natureza'].unique())} tipos de crimes.")
        logger.info("Inserindo ocorrencias...")
        crime_map = self.get_crime_map()
        self.copy_ocorrencias(df, crime_map)
        logger.info(f"Inseridas {len(df) * 12} ocorrencias (1 por mes).")
        logger.info("Todas as inserções concluídas.")
