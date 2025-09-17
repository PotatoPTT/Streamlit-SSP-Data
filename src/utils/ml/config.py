"""
Configurações para Machine Learning e API de treinamento de modelos.
"""

from pathlib import Path

# Configurações principais da API
POLLING_INTERVAL_SECONDS = 15
K_RANGE = range(2, 11)  # Silhouette score não é definido para k=1

# Diretórios
ROOT_DIR = Path(__file__).resolve().parents[3]  # Volta 3 níveis: ml -> utils -> src -> root
MODELS_OUTPUT_DIR = ROOT_DIR / 'output' / 'models'

# Garante que o diretório de modelos existe
MODELS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)