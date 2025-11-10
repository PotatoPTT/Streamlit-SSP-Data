"""
Treinamento de modelos de machine learning para a API.
Integrado com lógica do Clustering Project: RobustScaler, teste de K de 2 a 15 e silhouette score.
"""

import numpy as np
from sklearn.preprocessing import RobustScaler
from tslearn.clustering import TimeSeriesKMeans, silhouette_score
from tslearn.utils import to_time_series_dataset
from utils.config.logging import get_logger
from utils.ml.config import K_RANGE, RANDOM_STATE, MAX_ITER, N_INIT, NORMALIZACAO_ALG

logger = get_logger("ML")


def prepare_time_series_data(time_series_data, crime_name):
    """
    Converte dados para formato tslearn.
    Baseado no Clustering Project.

    Args:
        time_series_data (np.array): Dados brutos das séries temporais.
        crime_name (str): Nome do crime para mensagens de log.

    Returns:
        np.array: Dados no formato tslearn, ou None se inválidos.
    """
    try:
        X_tslearn = to_time_series_dataset(time_series_data)
        if X_tslearn is None or X_tslearn.shape[0] == 0 or X_tslearn.shape[1] == 0:
            logger.error(f"Nenhuma série temporal válida para o crime '{crime_name}'.")
            return None
        logger.info(f"Total de séries temporais para '{crime_name}': {X_tslearn.shape[0]}")
        return X_tslearn
    except Exception as e:
        logger.exception(f"ERRO ao preparar séries temporais para '{crime_name}': {e}")
        return None


def normalize_time_series(X_tslearn, metodo, crime_name):
    """
    Normaliza séries temporais usando RobustScaler (do Clustering Project).
    Normaliza cada série individualmente para maior robustez.
    
    Args:
        X_tslearn (np.array): Dados no formato tslearn [n_series, n_timesteps, 1].
        metodo (str): Método de clustering ('kmeans' ou 'kdba').
        crime_name (str): Nome do crime para mensagens de log.
    
    Returns:
        tuple: (X_scaled, scaler_type) - dados normalizados e tipo de scaler usado.
    """
    try:
        normalization_type = NORMALIZACAO_ALG.get(metodo, 'robust')
        
        if normalization_type == 'robust':
            # RobustScaler - normaliza cada série individualmente
            # Remove a última dimensão para trabalhar com [n_series, n_timesteps]
            X_2d = X_tslearn.squeeze()
            
            # Normalizar cada série individualmente
            X_scaled_2d = np.array([
                RobustScaler().fit_transform(X_2d[i, :].reshape(-1, 1)).flatten() 
                for i in range(X_2d.shape[0])
            ])
            
            # Adiciona dimensão de volta para formato tslearn [n_series, n_timesteps, 1]
            X_scaled = X_scaled_2d.reshape(X_tslearn.shape)
            scaler_type = 'robust'
            logger.info(f"Dados normalizados com RobustScaler para '{crime_name}' (método: {metodo}).")
        else:
            # Fallback para RobustScaler se necessário (mantém consistência)
            logger.warning(f"Normalization type '{normalization_type}' não reconhecido, usando RobustScaler como fallback.")
            X_2d = X_tslearn.squeeze()
            
            # Normalizar cada série individualmente
            X_scaled_2d = np.array([
                RobustScaler().fit_transform(X_2d[i, :].reshape(-1, 1)).flatten() 
                for i in range(X_2d.shape[0])
            ])
            
            X_scaled = X_scaled_2d.reshape(X_tslearn.shape)
            scaler_type = 'robust'
            logger.info(f"Dados normalizados com RobustScaler (fallback) para '{crime_name}' (método: {metodo}).")
            
        return X_scaled, scaler_type
        
    except Exception as e:
        logger.exception(f"ERRO ao normalizar dados para '{crime_name}' (método: {metodo}): {e}")
        return X_tslearn, None


def train_kmeans_model(X_scaled, n_clusters, metodo='kmeans'):
    """
    Treina um modelo K-means com parâmetros do Clustering Project.
    
    Args:
        X_scaled (np.array): Dados normalizados no formato tslearn.
        n_clusters (int): Número de clusters.
        metodo (str): 'kmeans' (Euclidean) ou 'kdba' (DTW).
    
    Returns:
        tuple: (model, labels)
    """
    try:
        if metodo == 'kdba':
            # K-means com métrica DTW
            metric = "dtw"
            logger.info(f"Treinando K-means DTW com {n_clusters} clusters...")
        else:
            # K-means com métrica Euclidean
            metric = "euclidean"
            logger.info(f"Treinando K-means Euclidean com {n_clusters} clusters...")
        
        model = TimeSeriesKMeans(
            n_clusters=n_clusters,
            metric=metric,
            max_iter=MAX_ITER,
            random_state=RANDOM_STATE,
            n_init=N_INIT,
            n_jobs=-1,
            verbose=False
        )
        
        model.fit(X_scaled)
        labels = model.labels_
        
        logger.info(f"Clusterização concluída. Clusters únicos: {len(np.unique(labels))}")
        return model, labels
        
    except Exception as e:
        logger.exception(f"ERRO na clusterização ({metodo}): {e}")
        raise


def calculate_silhouette_score(X_scaled, labels, metodo='kmeans'):
    """
    Calcula o score de silhueta para os clusters.
    Integrado com Clustering Project: usa métrica apropriada para cada método.
    
    Args:
        X_scaled (np.array): Dados normalizados [n_series, n_timesteps, 1].
        labels (np.array): Rótulos dos clusters.
        metodo (str): 'kmeans' (Euclidean) ou 'kdba' (DTW).
    
    Returns:
        float: Score de silhueta (-1 a 1, quanto maior melhor).
    """
    try:
        # Validação: precisa ter pelo menos 2 clusters
        if len(np.unique(labels)) < 2:
            logger.warning(f"Menos de 2 clusters únicos encontrados. Retornando score -1.")
            return -1.0
        
        # Definir métrica de silhouette baseada no método de clustering
        # Seguindo a lógica do Clustering Project (silhouette.py linhas 420-427)
        if metodo == 'kdba':
            # K-DBA usa DTW, então silhouette também deve usar DTW
            silhouette_metric = 'dtw'
            X_for_silhouette = X_scaled
            logger.debug(f"Calculando silhouette com métrica DTW para K-DBA")
        else:  # metodo == 'kmeans'
            # K-Means usa Euclidean, então silhouette também usa Euclidean
            silhouette_metric = 'euclidean'
            X_for_silhouette = X_scaled
            logger.debug(f"Calculando silhouette com métrica Euclidean para K-Means")
        
        # Calcular silhouette score usando tslearn (igual ao Clustering Project)
        # tslearn.clustering.silhouette_score aceita formato 3D e diferentes métricas
        score = silhouette_score(
            X_for_silhouette,
            labels,
            metric=silhouette_metric
        )
        
        return score
        
    except Exception as e:
        logger.warning(f"Erro ao calcular silhouette score com método '{metodo}': {e}")
        return -1.0


def find_best_k(X_scaled, metodo, crime_name):
    """
    Encontra o melhor K testando de 2 a 15 usando score de silhueta.
    Baseado no Clustering Project.
    
    Args:
        X_scaled (np.array): Dados normalizados.
        metodo (str): Método de clustering ('kmeans' ou 'kdba').
        crime_name (str): Nome do crime para logging.
    
    Returns:
        tuple: (best_model, best_k, best_score, best_labels)
    """
    best_score = -1
    best_k = -1
    best_model = None
    best_labels = None
    
    logger.info(f"Iniciando busca por melhor K de {K_RANGE.start} a {K_RANGE.stop - 1} para '{crime_name}' (método: {metodo})...")
    
    for k in K_RANGE:
        try:
            model, labels = train_kmeans_model(X_scaled, k, metodo)
            score = calculate_silhouette_score(X_scaled, labels, metodo)
            
            logger.info(f"  K={k}, Silhueta={score:.4f}")
            
            if score > best_score:
                best_score = score
                best_k = k
                best_model = model
                best_labels = labels
                
        except Exception as e:
            logger.warning(f"  K={k} falhou: {e}")
            continue
    
    if best_model is None:
        raise RuntimeError("Nenhum modelo foi treinado com sucesso.")
    
    logger.info(f"✓ Melhor K encontrado: {best_k} com silhueta={best_score:.4f}")
    return best_model, best_k, best_score, best_labels


def train_and_find_best_model(time_series_data, city_names, metodo, crime_name):
    """
    Pipeline completo: testa K de 2 a 15 e retorna o melhor modelo via silhouette.
    Baseado no Clustering Project com RobustScaler.
    
    Args:
        time_series_data (np.array): Dados das séries temporais limpos.
        city_names (list): Lista de nomes das cidades.
        metodo (str): Método de clustering ('kmeans' ou 'kdba').
        crime_name (str): Nome do crime para logging.
    
    Returns:
        tuple: (model, scaler_type, best_k, best_score) ou (None, None, None, None) se falhar.
    """
    logger.info(f"Iniciando treinamento para '{crime_name}' (método: {metodo})...")
    
    # Etapa 1: Preparar dados para formato tslearn
    X_tslearn = prepare_time_series_data(time_series_data, crime_name)
    if X_tslearn is None:
        logger.error("Falha na preparação dos dados para tslearn")
        return None, None, None, None
    
    # Etapa 2: Normalizar dados com RobustScaler
    X_scaled, scaler_type = normalize_time_series(X_tslearn, metodo, crime_name)
    if X_scaled is None or scaler_type is None:
        logger.error("Falha na normalização dos dados")
        return None, None, None, None
    
    # Etapa 3: Buscar melhor K (2 a 15) usando silhouette
    best_model, best_k, best_score, best_labels = find_best_k(X_scaled, metodo, crime_name)
    
    logger.info(f"Treinamento concluído com sucesso: K={best_k}, Silhueta={best_score:.4f}")
    return best_model, scaler_type, best_k, best_score


def validate_model_training_params(metodo):
    """Valida os parâmetros antes do treinamento."""
    if metodo not in ['kmeans', 'kdba']:
        raise ValueError(f"Método '{metodo}' não é suportado. Use 'kmeans' ou 'kdba'.")
    
    if len(K_RANGE) == 0:
        raise ValueError("Range de K está vazio.")
    
    return True