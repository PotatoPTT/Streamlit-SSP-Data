"""
Configurações para a API de treinamento de modelos.
"""

import logging
from pathlib import Path

# Configurações principais da API
POLLING_INTERVAL_SECONDS = 15
K_RANGE = range(2, 11)  # Silhouette score não é definido para k=1

# Diretórios
ROOT_DIR = Path(__file__).resolve().parents[3]  # Volta 3 níveis: api -> utils -> src -> root
MODELS_OUTPUT_DIR = ROOT_DIR / 'output' / 'models'

# Garante que o diretório de modelos existe
MODELS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging():
    """Configura o sistema de logging da API."""
    logger = logging.getLogger('ssp_api')
    logger.setLevel(logging.INFO)
    
    # Remove handlers existentes para evitar duplicação
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Adiciona handler para stdout se não existir
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def get_logger():
    """Retorna o logger configurado da API."""
    return logging.getLogger('ssp_api')