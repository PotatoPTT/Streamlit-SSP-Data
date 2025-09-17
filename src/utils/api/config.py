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


class ContextLogger:
    """Logger com contexto para identificar diferentes módulos."""
    
    def __init__(self, base_logger, context="API"):
        self.base_logger = base_logger
        self.context = context
    
    def _format_message(self, message):
        return f"[{self.context}] {message}"
    
    def info(self, message):
        self.base_logger.info(self._format_message(message))
    
    def error(self, message):
        self.base_logger.error(self._format_message(message))
    
    def warning(self, message):
        self.base_logger.warning(self._format_message(message))
    
    def debug(self, message):
        self.base_logger.debug(self._format_message(message))
    
    def exception(self, message):
        self.base_logger.exception(self._format_message(message))


def setup_logging():
    """Configura o sistema de logging da API."""
    logger = logging.getLogger('ssp_api')
    logger.setLevel(logging.INFO)
    
    # Remove handlers existentes para evitar duplicação
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Adiciona apenas um handler para console
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('[%(levelname)s] %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # Evita propagação para o logger root (evita duplicação)
        logger.propagate = False
    
    return ContextLogger(logger, "API")


def get_logger(context="API"):
    """Retorna o logger configurado da API com contexto específico."""
    base_logger = logging.getLogger('ssp_api')
    
    # Garantir que o logger está configurado
    if not base_logger.handlers:
        base_logger.setLevel(logging.INFO)
        
        # Adicionar handler para console
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('[%(levelname)s] %(message)s')
        console_handler.setFormatter(formatter)
        base_logger.addHandler(console_handler)
        
        # Evita propagação para o logger root
        base_logger.propagate = False
    
    return ContextLogger(base_logger, context)