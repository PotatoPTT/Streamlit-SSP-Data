"""
Treinamento de modelos de machine learning para a API.
"""

import numpy as np
from sklearn.metrics import silhouette_score
from tslearn.clustering import TimeSeriesKMeans
from tslearn.preprocessing import TimeSeriesScalerMeanVariance
from utils.api.config import get_logger, K_RANGE
from utils.api.data_processor import validate_time_series_data

logger = get_logger("ML")


def normalize_time_series(time_series_df):
    """Normaliza os dados de séries temporais."""
    logger.info("Normalizando as séries temporais...")
    validate_time_series_data(time_series_df)
    
    scaler = TimeSeriesScalerMeanVariance(mu=0., std=1.)
    X_scaled = scaler.fit_transform(time_series_df.values)
    
    return X_scaled, scaler


def train_kmeans_model(X_scaled, k, metodo='kmeans'):
    """Treina um modelo K-means para um K específico."""
    if metodo == 'kdba':
        # usar métrica dtw para kdba-like
        model = TimeSeriesKMeans(n_clusters=k, metric="dtw", random_state=42, n_jobs=-1)
    else:
        model = TimeSeriesKMeans(n_clusters=k, metric="euclidean", random_state=42, n_jobs=-1)
    
    labels = model.fit_predict(X_scaled)
    return model, labels


def calculate_silhouette_score(X_scaled, labels):
    """Calcula o score de silhueta para os clusters."""
    # Reshape para o formato que silhouette_score espera (2D array)
    X_reshaped = X_scaled.reshape(X_scaled.shape[0], -1)
    return silhouette_score(X_reshaped, labels)


def find_best_k(X_scaled, metodo='kmeans'):
    """Encontra o melhor K usando score de silhueta."""
    best_score = -1
    best_k = -1
    best_model = None
    
    logger.info(f"Iniciando benchmark para K de {K_RANGE.start} a {K_RANGE.stop - 1} usando metodo={metodo}...")
    
    for k in K_RANGE:
        model, labels = train_kmeans_model(X_scaled, k, metodo)
        score = calculate_silhouette_score(X_scaled, labels)
        
        logger.info(f"  K={k}, Silhueta={score:.4f}")

        if score > best_score:
            best_score = score
            best_k = k
            best_model = model

    if best_model is None:
        raise RuntimeError("Nenhum modelo foi treinado com sucesso.")

    logger.info(f"Melhor modelo encontrado: K={best_k} com score de silhueta de {best_score:.4f}")
    return best_model, best_k, best_score


def train_and_find_best_model(time_series_df, metodo='kmeans'):
    """Normaliza os dados, treina modelos KMeans para um range de K e retorna o melhor."""
    # Normalizar dados
    X_scaled, scaler = normalize_time_series(time_series_df)
    
    # Encontrar melhor K
    best_model, best_k, best_score = find_best_k(X_scaled, metodo)
    
    return best_model, scaler, best_k, best_score


def validate_model_training_params(time_series_df, metodo):
    """Valida os parâmetros antes do treinamento."""
    validate_time_series_data(time_series_df)
    
    if metodo not in ['kmeans', 'kdba']:
        raise ValueError(f"Método '{metodo}' não é suportado. Use 'kmeans' ou 'kdba'.")
    
    if len(K_RANGE) == 0:
        raise ValueError("Range de K está vazio.")
    
    return True