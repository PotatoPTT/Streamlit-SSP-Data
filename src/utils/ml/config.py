"""
Configurações para Machine Learning e API de treinamento de modelos.
Integrado com lógica do Clustering Project.
"""

from pathlib import Path

# Configurações principais da API
POLLING_INTERVAL_SECONDS = 15

# Parâmetros do Clustering Project
K_RANGE = range(2, 16)  # Testar K de 2 a 15 e escolher melhor via silhouette
RANDOM_STATE = 42  # Para reprodutibilidade
MAX_ITER = 200  # Máximo de iterações
N_INIT = 10  # Número de inicializações do K-Means
SAMPLE_SIZE_TRAINING_DEFAULT = 100  # Usar 100% dos dados (não fazer amostragem)
THRESHOLD_SERIE_ZERO = 100.0  # Remover séries 100% zeradas

# Amostragem de municípios (sempre 100% conforme especificado)
AMOSTRA_MUNICIPIOS_PERCENT = 100

# Normalização por método (do Clustering Project)
NORMALIZACAO_ALG = {
    'kmeans': 'robust',  # K-Means Euclidean usa RobustScaler
    'kdba': 'robust'     # K-Means DTW usa RobustScaler
}

# Diretórios
ROOT_DIR = Path(__file__).resolve().parents[3]  # Volta 3 níveis: ml -> utils -> src -> root
MODELS_OUTPUT_DIR = ROOT_DIR / 'output' / 'models'

# Garante que o diretório de modelos existe
MODELS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)