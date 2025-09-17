import streamlit as st
from utils.database.connection import DatabaseConnection
from utils.analytics_utils import build_model_params
from utils.analytics_ui import (
    render_date_filters, render_location_filter, render_crime_filter,
    render_method_selector, handle_completed_model, handle_pending_processing_model,
    handle_failed_model, handle_expired_model, handle_no_existing_models
)


def show_analytics(df_anos, df_regioes, df_meses_por_ano):
    """Analytics page"""
    st.markdown("# 📊 Análises Preditivas")
    st.info(
        "Selecione os parâmetros abaixo para gerar ou consultar uma análise preditiva.")

    db = DatabaseConnection()

    try:
        # --- Filtros ---
        st.markdown("### Filtros para o Modelo")

        # Renderizar filtros de interface
        ano_inicio, mes_inicio, ano_fim, mes_fim = render_date_filters(df_anos, df_meses_por_ano)
        
        if not mes_fim:
            st.error("Não há meses disponíveis para o ano final selecionado.")
            return

        regiao_selecionada = render_location_filter(df_regioes)
        crime_selecionado = render_crime_filter(db)

        # --- Construir parâmetros do modelo ---
        params = build_model_params(ano_inicio, mes_inicio, ano_fim, mes_fim, 
                                  regiao_selecionada, crime_selecionado)

        # --- Verificação de Solicitações Existentes para os dois métodos ---
        params_k = params.copy()
        params_k['metodo'] = 'kmeans'
        params_d = params.copy()
        params_d['metodo'] = 'kdba'

        solicit_k = db.get_solicitacao_by_params(params_k)
        solicit_d = db.get_solicitacao_by_params(params_d)

        # --- Gerenciar fluxos baseados no status das solicitações ---
        if solicit_k or solicit_d:
            selected_method, selected_solicit = render_method_selector(solicit_k, solicit_d)
            status = selected_solicit['status'] if selected_solicit else None
            
            if status == 'CONCLUIDO':
                handle_completed_model(selected_method, selected_solicit, params, db)
            elif status in ['PENDENTE', 'PROCESSANDO']:
                handle_pending_processing_model(status)
            elif status == 'FALHOU':
                handle_failed_model(selected_method, selected_solicit, params_k, params_d, db)
            elif status == 'EXPIRADO':
                handle_expired_model(selected_method, params_k, params_d, db)
        else:
            handle_no_existing_models(params_k, params_d, db)
            
    finally:
        # Fechar a conexão no final da execução da página
        db.close()

