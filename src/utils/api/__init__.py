"""
Módulo API para treinamento de modelos de machine learning.

Este módulo contém as funcionalidades necessárias para:
- Processamento de jobs de treinamento
- Gerenciamento de dados e modelos
- Configuração e logging
"""

from .config import setup_logging, get_logger, POLLING_INTERVAL_SECONDS, K_RANGE, MODELS_OUTPUT_DIR
from .job_manager import get_pending_job, validate_existing_models, process_job
from .data_processor import fetch_data_for_job, validate_time_series_data
from .model_trainer import train_and_find_best_model, validate_model_training_params
from .file_manager import generate_model_filename, save_model_and_blob, validate_model_file

__all__ = [
    # Config
    'setup_logging', 'get_logger', 'POLLING_INTERVAL_SECONDS', 'K_RANGE', 'MODELS_OUTPUT_DIR',
    # Job Manager
    'get_pending_job', 'validate_existing_models', 'process_job',
    # Data Processor
    'fetch_data_for_job', 'validate_time_series_data',
    # Model Trainer
    'train_and_find_best_model', 'validate_model_training_params',
    # File Manager
    'generate_model_filename', 'save_model_and_blob', 'validate_model_file'
]