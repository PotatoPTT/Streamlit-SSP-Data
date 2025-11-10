"""
Gerenciamento de arquivos e persistência de modelos.
"""

import joblib
import os
from pathlib import Path
from utils.config.logging import get_logger
from utils.ml.config import MODELS_OUTPUT_DIR

logger = get_logger()


def generate_model_filename(params):
    """Gera um nome de arquivo padronizado para o modelo."""
    metodo = params.get('metodo', 'kmeans')
    filename = f"model_{metodo}_{params['data_inicio']}_{params['data_fim']}_{params['regiao']}_{params['crime']}.joblib"
    
    # Remove caracteres inválidos para nomes de arquivo
    safe_filename = "".join(
        c for c in filename if c.isalnum() or c in ('-', '_', '.'))
    
    return safe_filename


def create_model_payload(model, scaler_type, best_k, best_score, city_names, cleaning_stats, params):
    """
    Cria o payload do modelo para salvamento.
    Integrado com estatísticas de limpeza do Clustering Project e silhouette score.
    """
    return {
        'model': model, 
        'scaler': scaler_type,  # 'robust' ou 'zscore'
        'k': best_k,
        'silhouette': best_score,  # Score de silhueta do melhor K
        'params': params,
        'city_names': city_names,  # Lista de municípios usados no treinamento
        'cleaning_stats': cleaning_stats  # Estatísticas de limpeza de dados
    }


def save_model_to_disk(model_payload, filename):
    """Salva o modelo no disco."""
    model_full_path = MODELS_OUTPUT_DIR / filename
    
    # Garante que o diretório existe
    model_full_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Salva o modelo
    joblib.dump(model_payload, str(model_full_path))
    logger.info(f"  -> Modelo salvo em: {model_full_path}")
    
    return model_full_path


def save_model_to_database(db_conn, job_id, filename, model_full_path):
    """Salva o modelo como blob no banco de dados."""
    try:
        with open(model_full_path, 'rb') as f:
            blob = f.read()
        
        stored = db_conn.store_model_blob(job_id, filename, blob)
        if stored:
            logger.info(f"  -> Modelo armazenado no DB com filename={filename}")
            return True
        else:
            logger.warning("  -> Falha ao armazenar modelo no DB (store_model_blob retornou False)")
            return False
            
    except Exception as e:
        logger.exception(f"  -> Erro ao gravar artefato no DB: {e}")
        return False


def save_model_and_blob(db_conn, job_id, params, model, scaler_type, best_k, best_score, city_names, cleaning_stats):
    """
    Salva o modelo completo (disco + banco de dados) com estatísticas de limpeza e silhouette score.
    Integrado com lógica do Clustering Project.
    """
    # Gerar nome do arquivo
    filename = generate_model_filename(params)
    
    # Criar payload do modelo com estatísticas de limpeza e silhouette
    model_payload = create_model_payload(
        model, scaler_type, best_k, best_score, city_names, cleaning_stats, params)
    
    # Salvar no disco
    model_full_path = save_model_to_disk(model_payload, filename)
    
    # Salvar no banco de dados
    save_model_to_database(db_conn, job_id, filename, model_full_path)
    
    return filename, model_full_path


def validate_model_file(filename):
    """Valida se um arquivo de modelo existe e é válido."""
    if not filename:
        return False, "Nome do arquivo está vazio"
    
    model_path = MODELS_OUTPUT_DIR / filename
    
    if not model_path.exists():
        return False, f"Arquivo não encontrado: {model_path}"
    
    try:
        # Tenta carregar o modelo para validar
        model_data = joblib.load(str(model_path))
        required_keys = ['model', 'scaler', 'k', 'params']
        
        for key in required_keys:
            if key not in model_data:
                return False, f"Chave '{key}' ausente no modelo"
        
        # Validar estatísticas de limpeza (opcional, para compatibilidade com modelos antigos)
        if 'cleaning_stats' not in model_data:
            logger.warning(f"Modelo {filename} não possui 'cleaning_stats' (modelo antigo)")
        
        return True, "Modelo válido"
        
    except Exception as e:
        return False, f"Erro ao carregar modelo: {e}"


def cleanup_old_models(max_models=50):
    """Remove modelos antigos para economizar espaço."""
    try:
        model_files = list(MODELS_OUTPUT_DIR.glob("*.joblib"))
        
        if len(model_files) <= max_models:
            return
        
        # Ordena por data de modificação (mais antigos primeiro)
        model_files.sort(key=lambda x: x.stat().st_mtime)
        
        # Remove os mais antigos
        files_to_remove = model_files[:-max_models]
        
        for file_path in files_to_remove:
            try:
                file_path.unlink()
                logger.info(f"Modelo antigo removido: {file_path.name}")
            except Exception as e:
                logger.warning(f"Erro ao remover {file_path.name}: {e}")
                
    except Exception as e:
        logger.exception(f"Erro durante limpeza de modelos: {e}")