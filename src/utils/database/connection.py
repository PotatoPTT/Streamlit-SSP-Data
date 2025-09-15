import psycopg2
import pandas as pd
import logging
import io
import streamlit as st
import json

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)


class DatabaseConnection:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=st.secrets["POSTGRES_DB"],
            user=st.secrets["POSTGRES_USER"],
            password=st.secrets["POSTGRES_PASSWORD"],
            host=st.secrets["POSTGRES_HOST"],
            port=st.secrets.get("POSTGRES_PORT", 5432)
        )
        self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()

    def fetch_all(self, query, params=None):
        self.cur.execute(query, params or ())
        return self.cur.fetchall()

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
        meses = ['Janeiro', 'Fevereiro', 'Marco', 'Abril', 'Maio', 'Junho',
                 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        if ano is None:
            anos = sorted(df['Ano'].unique())
        elif isinstance(ano, (list, tuple, set)):
            anos = list(ano)
        else:
            anos = [ano]
        for a in anos:
            a_int = int(a)
            df_ano = df[df['Ano'] == a]
            db_ocorrencias = {(row[0], row[1], row[2], row[3]): row[4] for row in self.fetch_all(
                'SELECT ano, mes, municipio_id, crime_id, quantidade FROM ocorrencias WHERE ano = %s;', (a_int,))}
            ocorrencias_dict = {}
            for _, row in df_ano.iterrows():
                for i, mes in enumerate(meses, 1):
                    quantidade = row[mes]
                    if pd.isna(quantidade):
                        continue
                    key = (int(row['Ano']), i, int(
                        row['ID_Municipio']), crime_map[row['Natureza']])
                    if key not in db_ocorrencias or db_ocorrencias[key] != int(quantidade):
                        ocorrencias_dict[key] = int(
                            quantidade)  # sobrescreve duplicatas
            ocorrencias_data = [(*k, v) for k, v in ocorrencias_dict.items()]
            logging.info(
                f'COPY ocorrências do ano {a} ({len(ocorrencias_data)} registros diferentes)...')
            if not ocorrencias_data:
                logging.info(f'Nenhum dado diferente para inserir no ano {a}.')
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
                logging.info(
                    f'Sucesso ao inserir ocorrências do ano {a} via COPY + upsert.')
            except Exception as e:
                self.conn.rollback()
                logging.error(
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
        """
        params_json = json.dumps(params, sort_keys=True)
        query = '''
            SELECT id, status, caminho_artefato, mensagem_erro
            FROM solicitacoes_modelo
            WHERE parametros = %s;
        '''
        self.cur.execute(query, (params_json,))
        result = self.cur.fetchone()
        if result:
            return {
                "id": result[0],
                "status": result[1],
                "caminho_artefato": result[2],
                "mensagem_erro": result[3]
            }
        return None

    def create_solicitacao(self, params: dict):
        """
        Cria uma nova solicitação de modelo com status 'PENDENTE'.
        Retorna o ID da nova solicitação.
        """
        params_json = json.dumps(params, sort_keys=True)
        query = '''
            INSERT INTO solicitacoes_modelo (parametros, status)
            VALUES (%s, 'PENDENTE')
            ON CONFLICT (parametros) DO NOTHING
            RETURNING id;
        '''
        try:
            self.cur.execute(query, (params_json,))
            result = self.cur.fetchone()
            self.conn.commit()
            return result[0] if result else None
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Erro ao criar solicitação: {e}")
            return None

    def update_solicitacao_status(self, solicitacao_id: int, status: str, caminho_artefato: str = None, mensagem_erro: str = None):
        """
        Atualiza o status, caminho do artefato e/ou mensagem de erro de uma solicitação.
        """
        query = '''
            UPDATE solicitacoes_modelo
            SET status = %s,
                caminho_artefato = %s,
                mensagem_erro = %s,
                data_atualizacao = NOW()
            WHERE id = %s;
        '''
        try:
            self.cur.execute(
                query, (status, caminho_artefato, mensagem_erro, solicitacao_id))
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            logging.error(
                f"Erro ao atualizar status da solicitação {solicitacao_id}: {e}")
            return False

    def insert_all(self, df: pd.DataFrame):
        logging.info("Iniciando inserção de dados no banco...")
        logging.info("Inserindo regioes...")
        self.insert_regioes(df)
        logging.info(f"Inseridas {len(df['ID_Regiao'].unique())} regioes.")
        logging.info("Inserindo municipios...")
        self.insert_municipios(df)
        logging.info(
            f"Inseridos {len(df['ID_Municipio'].unique())} municipios.")
        logging.info("Inserindo crimes...")
        self.insert_crimes(df)
        logging.info(
            f"Inserido {len(df['Natureza'].unique())} tipos de crimes.")
        logging.info("Inserindo ocorrencias...")
        crime_map = self.get_crime_map()
        self.copy_ocorrencias(df, crime_map)
        logging.info(f"Inseridas {len(df) * 12} ocorrencias (1 por mes).")
        logging.info("Todas as inserções concluídas.")
