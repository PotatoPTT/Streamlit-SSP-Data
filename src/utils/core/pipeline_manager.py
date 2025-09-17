"""
Gerenciador de pipeline para atualização de dados.
"""

import streamlit as st
import os
import time
import sys
import subprocess
from utils.config.logging import get_logger
from utils.ui.dashboard.utils import limpar_cache_dashboard

logger = get_logger("PIPELINE_MANAGER")

# Configurações
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
LOCK_FILE = os.path.join(ROOT_DIR, 'configs', 'update.lock')
COOLDOWN_SECONDS = 60 * 60  # 60 minutos


def is_pipeline_locked():
    """Verifica se o pipeline está em cooldown."""
    if not os.path.exists(LOCK_FILE):
        return False, None
    
    try:
        with open(LOCK_FILE, 'r') as f:
            timestamp = float(f.read().strip())
        
        elapsed = time.time() - timestamp
        if elapsed < COOLDOWN_SECONDS:
            return True, int(COOLDOWN_SECONDS - elapsed)
        
        return False, None
    except Exception as e:
        logger.warning(f"Erro ao verificar lock do pipeline: {e}")
        return False, None


def set_pipeline_lock():
    """Define o timestamp de lock do pipeline."""
    try:
        os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
        with open(LOCK_FILE, 'w') as f:
            f.write(str(time.time()))
        logger.info("Lock do pipeline definido")
    except Exception as e:
        logger.error(f"Erro ao definir lock do pipeline: {e}")


def executar_pipeline_com_output(pipeline_placeholder):
    """Executa o pipeline e mostra o output em tempo real."""
    src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    cmd = [sys.executable, "-m", "utils.pipeline_runner"]
    
    st.session_state['pipeline_output'] = []
    set_pipeline_lock()
    
    logger.info("Iniciando execução do pipeline")
    
    try:
        process = subprocess.Popen(
            cmd, 
            cwd=src_dir, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True, 
            bufsize=1
        )
        
        if process.stdout is not None:
            for line in process.stdout:
                st.session_state['pipeline_output'].append(line)
                output = ''.join(st.session_state['pipeline_output'][-20:])
                pipeline_placeholder.code(output, language="bash")
        
        process.wait()
        
        if process.returncode == 0:
            pipeline_placeholder.success("Pipeline executado com sucesso!")
            set_pipeline_lock()  # Atualiza timestamp ao finalizar
            logger.info("Pipeline executado com sucesso")
            # Limpar cache após atualização
            limpar_cache_dashboard()
        else:
            pipeline_placeholder.error(f"Pipeline falhou com código: {process.returncode}")
            logger.error(f"Pipeline falhou com código: {process.returncode}")
            
    except Exception as e:
        pipeline_placeholder.error(f"Erro ao executar pipeline: {e}")
        logger.error(f"Erro ao executar pipeline: {e}")


def render_pipeline_control(pipeline_placeholder):
    """Renderiza o controle de execução do pipeline."""
    locked, cooldown_left = is_pipeline_locked()
    
    if locked:
        cooldown_msg = f"Aguarde {cooldown_left//60} min para nova atualização." if cooldown_left else "Aguarde o cooldown."
        st.button("🔄 Atualizar Dados", disabled=True, help=cooldown_msg)
        
        info_msg = f"A atualização já foi executada recentemente. Tente novamente em {cooldown_left//60} minutos." if cooldown_left else "O pipeline está em cooldown."
        st.info(info_msg)
    else:
        if st.button("🔄 Atualizar Dados", help="Executa o pipeline completo de atualização de dados"):
            executar_pipeline_com_output(pipeline_placeholder)
        elif st.session_state.get('pipeline_output'):
            output = ''.join(st.session_state['pipeline_output'][-40:])
            pipeline_placeholder.code(output, language="bash")