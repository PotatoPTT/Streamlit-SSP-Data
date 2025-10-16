import streamlit as st
import json
from utils.ui.analytics.utils import (
    build_model_params, get_crimes_list, get_solicitacao_by_params_cached,
    get_solicitacao_by_params_processing
)
from utils.ui.analytics.components import (
    render_date_filters, render_location_filter, render_crime_filter,
    render_method_selector, handle_pending_processing_model,
    process_model_by_status, handle_no_existing_models_cached
)


def show_analytics(df_anos, df_regioes, df_meses_por_ano):
    """Analytics page"""
    st.markdown("# 📊 Agrupamento de Cidades por Perfil Criminal")
    st.info(
        "Selecione os parâmetros abaixo para gerar ou consultar um modelo (agrupamento)")

    # --- Filtros ---
    st.markdown("### Filtros para o Modelo")

    # Renderizar filtros de interface
    ano_inicio, mes_inicio, ano_fim, mes_fim = render_date_filters(
        df_anos, df_meses_por_ano)

    if not mes_fim:
        st.error("Não há meses disponíveis para o ano final selecionado.")
        return

    regiao_selecionada = render_location_filter(df_regioes)

    # Usar versão cacheada para buscar crimes
    crimes_list = get_crimes_list()
    crime_selecionado = st.selectbox("Crime", crimes_list)

    submit_filters = st.button("Aplicar filtros")

    selected_state = st.session_state.get('analytics_selected_params')

    params_current = build_model_params(ano_inicio, mes_inicio, ano_fim, mes_fim,
                                        regiao_selecionada, crime_selecionado)

    params_k_current = params_current.copy()
    params_k_current['metodo'] = 'kmeans'
    params_d_current = params_current.copy()
    params_d_current['metodo'] = 'kdba'

    if submit_filters:
        selected_state = {
            'params': params_current,
            'params_k': params_k_current,
            'params_d': params_d_current
        }
        st.session_state['analytics_selected_params'] = selected_state
        st.session_state['force_refresh_models'] = False

    if not selected_state:
        st.info('Ajuste os filtros e clique em "Aplicar filtros" para consultar ou gerar modelos.')
        return

    params = selected_state['params']
    params_k = selected_state['params_k']
    params_d = selected_state['params_d']

    # Verificar se há uma flag de rerun (para usar TTL baixo)
    use_low_ttl = st.session_state.get('force_refresh_models', False)

    if use_low_ttl:
        # Usar versão com TTL baixo para solicitações em processamento
        solicit_k = get_solicitacao_by_params_processing(
            json.dumps(params_k, sort_keys=True))
        solicit_d = get_solicitacao_by_params_processing(
            json.dumps(params_d, sort_keys=True))
    else:
        # Usar versões cacheadas normais para buscar solicitações
        solicit_k = get_solicitacao_by_params_cached(
            json.dumps(params_k, sort_keys=True))
        solicit_d = get_solicitacao_by_params_cached(
            json.dumps(params_d, sort_keys=True))

    # Resetar flag após verificação
    if use_low_ttl:
        st.session_state.force_refresh_models = False

    # Se alguma solicitação está em processamento, ativar flag para próxima verificação
    if (solicit_k and solicit_k.get('status') in ['PENDENTE', 'PROCESSANDO']) or \
       (solicit_d and solicit_d.get('status') in ['PENDENTE', 'PROCESSANDO']):
        st.session_state.force_refresh_models = True

    # --- Gerenciar fluxos baseados no status das solicitações ---
    if solicit_k or solicit_d:
        selected_method, selected_solicit = render_method_selector(
            solicit_k, solicit_d)
        process_model_by_status(
            selected_method, selected_solicit, params, params_k, params_d)
    else:
        handle_no_existing_models_cached(params_k, params_d)
