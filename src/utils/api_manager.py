"""
Gerenciador simples da API baseado em arquivo de lock.
"""
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

API_TIMEOUT = 1  # minutos para considerar a API como "não rodando"


def is_api_running():
    """
    Verifica se a API está rodando baseado no arquivo de lock.
    
    Returns:
        bool: True se a API está rodando (arquivo de lock atualizado recentemente)
    """
    lock_file = Path(__file__).resolve().parent.parent.parent / 'configs' / 'api.lock'
    
    if not lock_file.exists():
        return False
    
    try:
        # Lê o conteúdo do arquivo
        with open(lock_file, 'r') as f:
            content = f.read().strip()
        
        # Se contém "STOP", a API deve parar
        if content == "STOP":
            return False
        
        # Converte para datetime
        lock_time = datetime.fromisoformat(content)
        
        # Verifica se foi atualizado nos últimos API_TIMEOUT minutos
        max_age = timedelta(minutes=API_TIMEOUT)
        time_diff = datetime.now() - lock_time
        
        return time_diff <= max_age
        
    except Exception:
        return False


def start_api():
    """
    Inicia a API em um subprocess.
    
    Returns:
        tuple: (success: bool, message: str)
    """
    # Primeiro verifica se já está rodando para evitar duplicatas
    if is_api_running():
        return True, "API já está rodando"
    
    try:
        # Cria o arquivo lock imediatamente para prevenir múltiplas chamadas
        lock_file = Path(__file__).resolve().parent.parent.parent / 'configs' / 'api.lock'
        lock_file.parent.mkdir(exist_ok=True)
        
        # Escreve timestamp atual para marcar que está iniciando
        with open(lock_file, 'w') as f:
            f.write(datetime.now().isoformat())
        
        # Encontra o executável Python correto
        if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
            # Estamos em um venv
            python_exe = sys.executable
        else:
            python_exe = 'python'
        
        # Caminho para o arquivo da API
        api_file = Path(__file__).resolve().parent.parent.parent / 'api.py'
        
        # Inicia a API com saída no console
        process = subprocess.Popen(
            [python_exe, str(api_file)],
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0,
            start_new_session=True if sys.platform != 'win32' else False
        )
        
        return True, f"API iniciada com sucesso (PID: {process.pid})"
        
    except Exception as e:
        # Se falhou, remove o arquivo lock para permitir nova tentativa
        try:
            lock_file = Path(__file__).resolve().parent.parent.parent / 'configs' / 'api.lock'
            if lock_file.exists():
                lock_file.unlink()
        except:
            pass
        return False, f"Erro ao iniciar API: {e}"


def ensure_api_running():
    """
    Garante que a API está rodando. Se não estiver, tenta iniciar.
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if is_api_running():
        return True, "API já está rodando"
    
    return start_api()


def get_api_status():
    """
    Retorna o status atual da API.
    
    Returns:
        tuple: (status: str, message: str)
    """
    if is_api_running():
        return "rodando", "API está rodando normalmente"
    else:
        return "parada", "API não está rodando"


# Para compatibilidade com código existente
def ensure_single_api_instance(mode='subprocess'):
    """Wrapper para ensure_api_running() para compatibilidade."""
    return ensure_api_running()


def stop_api():
    """
    Para a API escrevendo STOP no arquivo de lock.
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        lock_file = Path(__file__).resolve().parent.parent.parent / 'configs' / 'api.lock'
        lock_file.parent.mkdir(exist_ok=True)
        
        with open(lock_file, 'w') as f:
            f.write("STOP")
        
        return True, "Comando de parada enviado para a API"
        
    except Exception as e:
        return False, f"Erro ao parar API: {e}"