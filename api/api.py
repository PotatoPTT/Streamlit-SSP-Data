import pandas as pd
import numpy as np
import time
import joblib
import os
import sys
import json
from sklearn.metrics import silhouette_score
from tslearn.clustering import TimeSeriesKMeans
from tslearn.preprocessing import TimeSeriesScalerMeanVariance

# Adiciona o diretório raiz ao path para encontrar os módulos
# Isso permite que o script seja executado de qualquer lugar
from pathlib import Path

# Use project-relative paths instead of absolute strings
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from src.utils.database.connection import DatabaseConnection

# --- Configurações ---
POLLING_INTERVAL_SECONDS = 15
K_RANGE = range(2, 11)  # Silhouette score não é definido para k=1
MODELS_OUTPUT_DIR = ROOT_DIR / 'output' / 'models'
MODELS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_pending_job(db_conn):
    """Busca a primeira solicitação pendente no banco de dados."""
    # Prioriza solicitações cujo parametro 'metodo' = 'kmeans' para executar primeiro
    query = """
        SELECT id, parametros
        FROM solicitacoes_modelo
        WHERE status = 'PENDENTE'
        ORDER BY (CASE WHEN parametros->>'metodo' = 'kmeans' THEN 0 ELSE 1 END), data_solicitacao
        LIMIT 1;
    """
    result = db_conn.fetch_all(query)
    if result:
        job_id, params = result[0]  # O segundo valor já é um dict
        return job_id, params       # Retorna diretamente, sem json.loads()
    return None, None


def fetch_data_for_job(db_conn, params):
    """Busca os dados de ocorrências com base nos parâmetros da solicitação."""
    print(f"Buscando dados para o período de {params['data_inicio']} a {params['data_fim']} na região '{params['regiao']}' para o crime '{params['crime']}'...")

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
    sql_params = [params['data_inicio'], params['data_fim'], params['crime']]

    if params['regiao'] != 'Todas':
        query += " AND r.nome = %s"
        sql_params.append(params['regiao'])

    query += """
        GROUP BY m.nome, ano_mes
        ORDER BY m.nome, ano_mes;
    """

    df = pd.DataFrame(db_conn.fetch_all(query, tuple(sql_params)), columns=[
                      'municipio', 'ano_mes', 'quantidade'])

    if df.empty:
        raise ValueError("A consulta de dados não retornou resultados.")

    # Pivotar para formato de série temporal
    time_series_df = df.pivot_table(
        index='municipio', columns='ano_mes', values='quantidade').fillna(0)

    print(f"Dados transformados: {time_series_df.shape[0]} municípios e {time_series_df.shape[1]} meses.")
    return time_series_df


def train_and_find_best_model(time_series_df, metodo='kmeans'):
    """Normaliza os dados, treina modelos KMeans para um range de K e retorna o melhor."""
    if time_series_df.shape[1] < 2:
        raise ValueError(
            "Série temporal muito curta para análise (menos de 2 pontos de dados).")

    print("Normalizando as séries temporais...")
    scaler = TimeSeriesScalerMeanVariance(mu=0., std=1.)
    X_scaled = scaler.fit_transform(time_series_df.values)

    best_score = -1
    best_k = -1
    best_model = None

    print(f"Iniciando benchmark para K de {K_RANGE.start} a {K_RANGE.stop - 1} usando metodo={metodo}...")
    for k in K_RANGE:
        if metodo == 'kdba':
            # usar métrica dtw para kdba-like
            model = TimeSeriesKMeans(n_clusters=k, metric="dtw", random_state=42, n_jobs=-1)
        else:
            model = TimeSeriesKMeans(n_clusters=k, metric="euclidean", random_state=42, n_jobs=-1)
        labels = model.fit_predict(X_scaled)

        # Reshape para o formato que silhouette_score espera (2D array)
        X_reshaped = X_scaled.reshape(X_scaled.shape[0], -1)
        score = silhouette_score(X_reshaped, labels)
        print(f"  K={k}, Silhueta={score:.4f}")

        if score > best_score:
            best_score = score
            best_k = k
            best_model = model

    if best_model is None:
        raise RuntimeError("Nenhum modelo foi treinado com sucesso.")

    print(
        f"Melhor modelo encontrado: K={best_k} com score de silhueta de {best_score:.4f}")
    return best_model, scaler, best_k, best_score


def generate_model_path(job_id, params):
    """Gera um nome de arquivo padronizado para o modelo."""
    metodo = params.get('metodo', 'kmeans')
    filename = f"model_{job_id}_{metodo}_{params['data_inicio']}_{params['data_fim']}_{params['regiao']}_{params['crime']}.joblib"
    # Remove caracteres inválidos para nomes de arquivo
    safe_filename = "".join(
        c for c in filename if c.isalnum() or c in ('-', '_', '.'))
    # Retorna apenas o nome do arquivo — o diretório é sempre output/models
    return safe_filename


def main():
    """Loop principal da API."""
    print("--- API de Treinamento de Modelos iniciada ---")
    print(f"Verificando solicitações a cada {POLLING_INTERVAL_SECONDS} segundos...")
    # On startup: validate existing records and normalize states
    try:
        db_start = DatabaseConnection()
        # 1) Any CONCLUIDO with missing artifact -> mark INVALIDO
        rows = db_start.fetch_all("SELECT id, caminho_artefato FROM solicitacoes_modelo WHERE status = 'CONCLUIDO';")
        for r in rows:
            job_id = r[0]
            caminho = r[1]
            # determine full path
            if not caminho:
                msg = 'Artefato ausente (valor vazio)'
                print(f"[startup] Job {job_id} marcado como FALHOU: {msg}")
                db_start.update_solicitacao_status(job_id, 'FALHOU', mensagem_erro=msg)
                continue
            if os.path.isabs(caminho):
                full = caminho
            else:
                full = str(MODELS_OUTPUT_DIR / caminho)
            if not os.path.exists(full):
                msg = f"Artefato não encontrado em: {caminho}"
                print(f"[startup] Job {job_id} marcado como FALHOU: {msg}")
                db_start.update_solicitacao_status(job_id, 'FALHOU', mensagem_erro=msg)

        # 2) Any PROCESSANDO -> set to PENDENTE (so work can be retried)
        processing_rows = db_start.fetch_all("SELECT id FROM solicitacoes_modelo WHERE status = 'PROCESSANDO';")
        for r in processing_rows:
            jid = r[0]
            print(f"[startup] Job {jid} estava PROCESSANDO; repondo para PENDENTE.")
            db_start.update_solicitacao_status(jid, 'PENDENTE')

    except Exception as e:
        print(f"[startup] Erro ao validar solicitacoes: {e}")
    finally:
        try:
            db_start.close()
        except Exception:
            pass

    while True:
        db = None
        try:
            db = DatabaseConnection()
            job_id, params = get_pending_job(db)

            if job_id:
                print(f"\n[+] Nova solicitação encontrada (ID: {job_id}).")
                db.update_solicitacao_status(job_id, 'PROCESSANDO')
                print(f"  -> Status atualizado para PROCESSANDO.")

                try:
                    # 1. Buscar e transformar os dados
                    time_series_df = fetch_data_for_job(db, params)

                    # 2. Treinar e encontrar o melhor modelo (respeitando o método solicitado)
                    metodo = params.get('metodo', 'kmeans')
                    print(f"  -> Método solicitado: {metodo}")
                    model, scaler, best_k, best_score = train_and_find_best_model(
                        time_series_df, metodo=metodo)

                    # 3. Salvar o modelo e o scaler — salva em output/models e registra apenas o filename
                    model_filename = generate_model_path(job_id, params)
                    model_full_path = MODELS_OUTPUT_DIR / model_filename
                    joblib.dump({'model': model, 'scaler': scaler, 'k': best_k,
                                'silhouette': best_score, 'params': params}, str(model_full_path))
                    print(f"  -> Modelo salvo em: {model_full_path}")

                    # 4. Atualizar status para CONCLUIDO armazenando apenas o nome do arquivo
                    db.update_solicitacao_status(
                        job_id, 'CONCLUIDO', caminho_artefato=model_filename)
                    print(f"  -> Status finalizado: CONCLUIDO.")

                except Exception as e:
                    error_message = f"Erro no processamento do job {job_id}: {e}"
                    print(f"[!] ERRO: {error_message}")
                    # Fecha a conexão atual para abortar a transação falha
                    if db:
                        db.close()
                    # Reabre a conexão para garantir que o update seja em uma nova transação
                    db = DatabaseConnection()
                    db.update_solicitacao_status(
                        job_id, 'FALHOU', mensagem_erro=str(e))
            else:
                # Se não houver jobs, apenas espera
                time.sleep(POLLING_INTERVAL_SECONDS)

        except (KeyboardInterrupt, SystemExit):
            print("\n--- API encerrada pelo usuário. ---")
            break
        except Exception as e:
            print(f"\n[!] Erro inesperado no loop principal: {e}")
            print("Aguardando antes de tentar novamente...")
            time.sleep(POLLING_INTERVAL_SECONDS * 2)
        finally:
            if db:
                db.close()


if __name__ == "__main__":
    main()
