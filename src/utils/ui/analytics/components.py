"""
Gerenciamento de fluxos de interface para análises preditivas.
"""

import streamlit as st
import time
from utils.config.logging import get_logger

logger = get_logger("ANALYTICS_UI")


def render_date_filters(df_anos, df_meses_por_ano):
    """Renderiza os filtros de data da interface."""
    from utils.ui.analytics.utils import get_meses_mapping, get_available_months_for_year, filter_end_months

    meses_map, meses_map_inv = get_meses_mapping()
    anos_list = df_anos["ano"].sort_values(ascending=False).tolist()

    st.markdown("##### Período da Análise")
    col_start, col_end = st.columns(2)

    with col_start:
        ano_inicio = st.selectbox("Ano de Início", anos_list, index=0)
        meses_disponiveis_inicio = get_available_months_for_year(
            df_meses_por_ano, ano_inicio)
        meses_nomes_inicio = [meses_map_inv[m]
                              for m in meses_disponiveis_inicio]
        mes_inicio = st.selectbox("Mês de Início", meses_nomes_inicio, index=0)

    with col_end:
        anos_fim_disponiveis = [ano for ano in anos_list if ano >= ano_inicio]
        ano_fim = st.selectbox("Ano de Fim", anos_fim_disponiveis, index=0)

        meses_disponiveis_fim = get_available_months_for_year(
            df_meses_por_ano, ano_fim)
        mes_inicio_num = meses_map[mes_inicio]
        meses_fim_filtrados = filter_end_months(
            meses_disponiveis_fim, ano_fim, ano_inicio, mes_inicio_num)

        meses_nomes_fim = [meses_map_inv[m] for m in meses_fim_filtrados]
        mes_fim_index = len(meses_nomes_fim) - 1 if meses_nomes_fim else 0
        mes_fim = st.selectbox(
            "Mês de Fim", meses_nomes_fim, index=mes_fim_index)

    return ano_inicio, mes_inicio, ano_fim, mes_fim


def render_location_filter(df_regioes):
    """Renderiza o filtro de localização."""
    st.markdown("##### Localização")
    regioes_list = ["Todas"] + df_regioes["nome"].tolist()
    return st.selectbox("Região", regioes_list)


def render_crime_filter(db):
    """Renderiza o filtro de tipo de crime."""
    from utils.ui.analytics.utils import get_crimes_list
    st.markdown("##### Tipo de Crime")
    crimes_list = get_crimes_list()
    return st.selectbox("Natureza do Crime", crimes_list)


def render_method_selector(solicit_k, solicit_d):
    """Renderiza o seletor de método quando há solicitações existentes."""
    from utils.ui.analytics.utils import get_status_label

    options = [
        ('kmeans', get_status_label(solicit_k, 'K-Means')),
        ('kdba', get_status_label(solicit_d, 'K-DBA'))
    ]

    # Escolhe por padrão o K-Means se estiver concluído, senão o primeiro disponível
    default = 0
    if solicit_k and solicit_k['status'] == 'CONCLUIDO':
        default = 0
    elif solicit_d and solicit_d['status'] == 'CONCLUIDO':
        default = 1

    choice = st.selectbox('Modelo disponível', [
                          o[1] for o in options], index=default)
    selected_method = ['kmeans', 'kdba'][[o[1] for o in options].index(choice)]
    selected_solicit = solicit_k if selected_method == 'kmeans' else solicit_d

    return selected_method, selected_solicit


def handle_completed_model(selected_method, selected_solicit, params, db):
    """Gerencia o fluxo quando um modelo está concluído."""
    from utils.ui.analytics.utils import (
        get_model_filename, load_model_from_file_or_db,
        fetch_data_for_model_cached, prepare_municipalities_table
    )
    from utils.visualization.analytics_plots import (
        display_model_metrics, plot_time_series_by_cluster, plot_map_by_cluster
    )

    st.success("Modelo concluído — exibindo resultados.")

    model_filename = get_model_filename(selected_method, params)
    if not model_filename:
        st.error(
            "Artefato do modelo não encontrado (valor vazio). Considere gerar o modelo novamente.")
        return

    try:
        model_data = load_model_from_file_or_db(
            model_filename, selected_solicit, db)
    except FileNotFoundError as e:
        st.error(f"Erro ao carregar modelo: {e}")
        return
    except Exception as e:
        st.error(f"Erro inesperado ao carregar modelo: {e}")
        return

    model = model_data['model']
    scaler = model_data['scaler']
    k = model_data.get('k')
    silhouette = model_data.get('silhouette')

    display_model_metrics(silhouette, k)

    # Buscar dados e gerar visualizações (usando versão cacheada)
    import json
    time_series_df = fetch_data_for_model_cached(
        json.dumps(params, sort_keys=True))

    if not time_series_df.empty:
        labels = model.predict(scaler.transform(time_series_df.values))
        time_series_df_with_labels = time_series_df.copy()
        time_series_df_with_labels['cluster'] = labels

        plot_time_series_by_cluster(time_series_df.copy(), labels)
        plot_map_by_cluster(db, time_series_df_with_labels)

        # Tabela com região
        st.markdown("#### Tabela de Municípios por Cluster")
        display_df = prepare_municipalities_table(
            time_series_df_with_labels, db)
        st.dataframe(display_df)


def handle_pending_processing_model(status):
    """Gerencia o fluxo quando um modelo está pendente ou processando."""
    st.info(
        f"Uma solicitação para este modelo já existe e está com o status: **{status}**.")
    st.write("A página verificará o status a cada 10 segundos. Por favor, aguarde.")

    # Exibir um progresso para melhor UX
    progress_bar = st.progress(0)
    for i in range(10):
        progress_bar.progress((i + 1) / 10)
        time.sleep(1)

    # Ativar flag para usar TTL baixo na próxima verificação
    st.session_state.force_refresh_models = True

    # Cache será limpo automaticamente quando o status for atualizado
    st.rerun()


def handle_failed_model(selected_method, selected_solicit, params_k, params_d, db):
    """Gerencia o fluxo quando um modelo falhou."""
    err = selected_solicit.get('mensagem_erro')
    st.error(f"A última tentativa de gerar este modelo falhou: {err}")

    # Se a mensagem de falha indica artefato ausente, oferece regeneração
    # Atualizado para reconhecer a nova mensagem de erro
    if err and ('não encontrado' in err or 'ausente' in err or 'banco de dados' in err):
        st.warning(
            "O artefato associado a esta solicitação está ausente. Deseja regenerar?")
        if st.button("Regenerar modelo"):
            nova_id = db.create_solicitacao(
                params_k if selected_method == 'kmeans' else params_d)
            if nova_id:
                st.success(
                    f"Solicitação de regeneração criada (ID: {nova_id}).")
                # Cache é limpo automaticamente pela função create_solicitacao
                st.rerun()
            else:
                st.error("Não foi possível criar a solicitação de regeneração.")
    else:
        if st.button("Tentar novamente"):
            nova_id = db.create_solicitacao(
                params_k if selected_method == 'kmeans' else params_d)
            if nova_id:
                st.success(
                    f"Nova solicitação criada com sucesso (ID: {nova_id}). O modelo será treinado em segundo plano.")
                # Cache é limpo automaticamente pela função create_solicitacao
                st.rerun()
            else:
                st.error(
                    "Não foi possível criar uma nova solicitação. Verifique se os parâmetros já não estão pendentes.")


def handle_expired_model(selected_method, params_k, params_d, db):
    """Gerencia o fluxo quando um modelo expirou."""
    st.warning(
        "Esta solicitação expirou. Você pode reativá-la para que o modelo seja reprocessado.")
    if st.button("Reativar solicitação"):
        nova_id = db.create_solicitacao(
            params_k if selected_method == 'kmeans' else params_d)
        if nova_id:
            st.success(f"Solicitação reativada/confirmada (ID: {nova_id}).")
            # Cache é limpo automaticamente pela função create_solicitacao
            st.rerun()
        else:
            st.error(
                "Não foi possível reativar a solicitação. Verifique os parâmetros e tente novamente.")


def handle_no_existing_models(params_k, params_d, db):
    """Gerencia o fluxo quando não há modelos existentes."""
    st.warning("Nenhum modelo encontrado com os parâmetros selecionados. Será criada uma solicitação para ambos os métodos: K-Means e K-DBA.")

    with st.form("form_confirmacao"):
        st.write("Deseja solicitar a criação de novos modelos (K-Means e K-DBA) com estes parâmetros? O processo pode levar alguns minutos.")
        submitted = st.form_submit_button("Sim, criar solicitações")

        if submitted:
            logger.info(
                f"Criando solicitações para K-Means e K-DBA com parâmetros: {params_k}")
            id_k = db.create_solicitacao(params_k)
            id_d = db.create_solicitacao(params_d)
            msgs = []
            if id_k:
                msgs.append(f"K-Means criada (ID: {id_k})")
                logger.info(f"Solicitação K-Means criada com ID: {id_k}")
            else:
                msgs.append("K-Means não criada (pode já existir)")
                logger.warning("Solicitação K-Means não foi criada")
            if id_d:
                msgs.append(f"K-DBA criada (ID: {id_d})")
                logger.info(f"Solicitação K-DBA criada com ID: {id_d}")
            else:
                msgs.append("K-DBA não criada (pode já existir)")
                logger.warning("Solicitação K-DBA não foi criada")
            st.success("; ".join(msgs))

            # Cache é limpo automaticamente pela função create_solicitacao
            # Pequena pausa para garantir que as operações sejam processadas
            time.sleep(1)
            logger.info("Executando rerun após criação de solicitações")
            st.rerun()


# ==================== HANDLERS CACHEADOS ====================

def handle_no_existing_models_cached(params_k, params_d):
    """Versão otimizada com cache para quando não há modelos existentes."""
    from utils.data.connection import DatabaseConnection

    logger.info("Processando nova solicitação de modelo")

    db = DatabaseConnection()
    try:
        handle_no_existing_models(params_k, params_d, db)
    finally:
        db.close()


def process_model_by_status(selected_method, selected_solicit, params, params_k, params_d):
    """Processa o modelo baseado no status da solicitação."""
    from utils.data.connection import DatabaseConnection

    status = selected_solicit['status'] if selected_solicit else None

    # Usar conexão com banco para todas as operações
    db = DatabaseConnection()
    try:
        if status == 'CONCLUIDO':
            handle_completed_model(
                selected_method, selected_solicit, params, db)
        elif status in ['PENDENTE', 'PROCESSANDO']:
            handle_pending_processing_model(status)
        elif status == 'FALHOU':
            handle_failed_model(
                selected_method, selected_solicit, params_k, params_d, db)
        elif status == 'EXPIRADO':
            handle_expired_model(selected_method, params_k, params_d, db)
        else:
            logger.warning(f"Status desconhecido: {status}")
    finally:
        db.close()
