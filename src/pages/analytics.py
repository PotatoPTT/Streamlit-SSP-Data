import streamlit as st
import streamlit.components.v1 as components
import json
import pandas as pd
from utils.ui.analytics.utils import (
    build_model_params, get_solicitacao_by_params_cached,
    get_solicitacao_by_params_processing, get_completed_models,
    get_meses_mapping
)
from utils.ui.analytics.components import (
    render_date_filters, render_location_filter, render_crime_filter,
    render_method_selector, process_model_by_status,
    handle_no_existing_models_cached
)


def show_analytics(df_anos, df_regioes, df_meses_por_ano):
    """Página de Analytics com navegação entre modelos existentes e configuração manual."""
    st.markdown("# 📊 Agrupamento de Cidades por Perfil Criminal")

    tab_labels = ["Modelos concluídos", "Configurar manualmente"]
    st.session_state.setdefault('analytics_active_tab', tab_labels[0])

    _, center_col, _ = st.columns([1, 3, 1])
    with center_col:
        buttons_cols = st.columns(len(tab_labels))
        for tab_label, col in zip(tab_labels, buttons_cols):
            is_active = st.session_state['analytics_active_tab'] == tab_label
            button_type = "primary" if is_active else "secondary"
            if col.button(tab_label, key=f"analytics_tab_btn_{tab_label}", type=button_type, width='stretch'):
                st.session_state['analytics_active_tab'] = tab_label
                st.rerun()

    selected_tab = st.session_state['analytics_active_tab']

    selected_state = st.session_state.get('analytics_selected_params')

    def format_period(start, end):
        if not start and not end:
            return "-"
        return f"{start or '-'} → {end or '-'}"

    def render_models_tab(current_state):
        models_cols = st.columns([1, 14, 1])
        with models_cols[1]:
            with st.container(border=True):
                st.markdown("### Modelos já processados")
                modelos = get_completed_models()

                if not modelos:
                    st.info("Nenhum modelo concluído encontrado até o momento.")
                    if st.button("Ir para configuração manual", key="analytics_go_manual"):
                        st.session_state['analytics_active_tab'] = tab_labels[1]
                        st.rerun()
                    return

                groups = {}
                groups_order = []
                for modelo in modelos:
                    base = modelo.get('base_params') or {}
                    key = (
                        base.get('data_inicio'),
                        base.get('data_fim'),
                        base.get('regiao'),
                        base.get('crime'),
                        base.get('tipo_modelo', 'predicao_ocorrencias')
                    )
                    if key not in groups:
                        groups_order.append(key)
                        groups[key] = {
                            'base_params': base,
                            'modelos': [],
                            'last_update': modelo.get('data_atualizacao'),
                            'ids': []
                        }
                    groups[key]['modelos'].append(modelo)
                    groups[key]['ids'].append(modelo['id'])
                    if hasattr(modelo.get('data_atualizacao'), 'timestamp'):
                        if not hasattr(groups[key]['last_update'], 'timestamp') or modelo.get('data_atualizacao') > groups[key]['last_update']:
                            groups[key]['last_update'] = modelo.get('data_atualizacao')

                def format_methods_summary(group):
                    if not group['modelos']:
                        return '—'
                    parts = []
                    for m in group['modelos']:
                        metodo = (m.get('metodo') or '—').upper() if m.get('metodo') else '—'
                        status = m.get('status', '—')
                        parts.append(f"{metodo}: {status}")
                    return " | ".join(parts)

                label_map = {}
                summary_rows = []
                for key in groups_order:
                    group = groups[key]
                    base = group['base_params'] or {}
                    periodo = format_period(base.get('data_inicio'), base.get('data_fim'))
                    methods_summary = format_methods_summary(group)
                    atualizado_em = group.get('last_update')
                    atualizado_em_str = atualizado_em.strftime("%d/%m/%Y %H:%M") if hasattr(
                        atualizado_em, 'strftime') else str(atualizado_em or '—')

                    label = f"{base.get('regiao', 'Todas')} • {base.get('crime', '—')} • {periodo}"
                    label_map[label] = group

                    summary_rows.append({
                        "Região": base.get('regiao') or '—',
                        "Crime": base.get('crime') or '—',
                        "Período": periodo,
                        "Métodos": methods_summary,
                        "Criado em": atualizado_em_str,
                    })

                default_label = None
                if current_state:
                    params_base = current_state.get('params', {})
                    for label, group in label_map.items():
                        base = group.get('base_params') or {}
                        if all([
                            base.get('data_inicio') == params_base.get('data_inicio'),
                            base.get('data_fim') == params_base.get('data_fim'),
                            base.get('regiao') == params_base.get('regiao'),
                            base.get('crime') == params_base.get('crime')
                        ]):
                            default_label = label
                            break

                labels = list(label_map.keys())
                selected_index = labels.index(default_label) if default_label in labels else 0
                selected_label = st.selectbox(
                    "Selecione um conjunto de parâmetros",
                    labels,
                    index=selected_index
                )

                selected_group = label_map[selected_label]
                base = selected_group.get('base_params') or {}
                periodo = format_period(base.get('data_inicio'), base.get('data_fim'))
                atualizado_em = selected_group.get('last_update')
                atualizado_em_str = atualizado_em.strftime("%d/%m/%Y %H:%M") if hasattr(
                    atualizado_em, 'strftime') else str(atualizado_em or '—')

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**Região:** {base.get('regiao', '—')}")
                    st.markdown(f"**Crime:** {base.get('crime', '—')}")
                    st.markdown(f"**Período:** {periodo}")
                with col2:
                    st.markdown(f"**Métodos disponíveis:**")
                    for modelo in selected_group['modelos']:
                        metodo = (modelo.get('metodo') or '—').upper() if modelo.get('metodo') else '—'
                        status = modelo.get('status', '—')
                        st.markdown(f"- {metodo}: {status}")
                    st.markdown(f"**Criado em:** {atualizado_em_str}")

                if st.button("Exibir Modelo", key=f"abrir_grupo_{selected_group['ids'][0]}"):
                    params_base = {
                        "data_inicio": base.get('data_inicio'),
                        "data_fim": base.get('data_fim'),
                        "regiao": base.get('regiao'),
                        "crime": base.get('crime'),
                        "tipo_modelo": base.get('tipo_modelo', 'predicao_ocorrencias')
                    }
                    params_base = {k: v for k, v in params_base.items() if v is not None}
                    params = params_base.copy()
                    params.setdefault('tipo_modelo', 'predicao_ocorrencias')
                    params_k = params.copy()
                    params_k['metodo'] = 'kmeans'
                    params_d = params.copy()
                    params_d['metodo'] = 'kdba'

                    st.session_state['analytics_selected_params'] = {
                        'params': params,
                        'params_k': params_k,
                        'params_d': params_d
                    }
                    st.session_state['force_refresh_models'] = False
                    st.session_state['analytics_active_tab'] = tab_labels[1]
                    st.session_state['analytics_selected_model_id'] = selected_group['ids'][0]
                    st.session_state['analytics_scroll_target'] = 'resultados-do-modelo'
                    st.rerun()

                st.markdown("#### Resumo de grupos concluídos")
                st.dataframe(pd.DataFrame(summary_rows))

    def render_manual_tab(current_state):
        manual_cols = st.columns([1, 14, 1])
        with manual_cols[1]:
            _, meses_map_inv = get_meses_mapping()

            default_params = current_state.get('params') if current_state else {}
            data_inicio = default_params.get('data_inicio') if default_params else None
            data_fim = default_params.get('data_fim') if default_params else None

            def extract_defaults(data_inicio_val, data_fim_val):
                default_vals = {}
                if data_inicio_val:
                    ano, mes = data_inicio_val.split('-')
                    default_vals['ano_inicio'] = int(ano)
                    default_vals['mes_inicio'] = meses_map_inv.get(int(mes))
                if data_fim_val:
                    ano, mes = data_fim_val.split('-')
                    default_vals['ano_fim'] = int(ano)
                    default_vals['mes_fim'] = meses_map_inv.get(int(mes))
                return default_vals

            defaults = extract_defaults(data_inicio, data_fim)

            with st.container(border=True):
                st.markdown("### Configuração manual do modelo")

                ano_inicio, mes_inicio, ano_fim, mes_fim = render_date_filters(
                    df_anos,
                    df_meses_por_ano,
                    default_ano_inicio=defaults.get('ano_inicio'),
                    default_mes_inicio=defaults.get('mes_inicio'),
                    default_ano_fim=defaults.get('ano_fim'),
                    default_mes_fim=defaults.get('mes_fim')
                )

                if not mes_fim:
                    st.error("Não há meses disponíveis para o ano final selecionado.")
                    return

                regiao_selecionada = render_location_filter(
                    df_regioes, default_regiao=default_params.get('regiao') if default_params else None)

                crime_selecionado = render_crime_filter(
                    default_crime=default_params.get('crime') if default_params else None)

                if crime_selecionado is None:
                    st.warning("Nenhum crime disponível para seleção. Atualize a base de dados e tente novamente.")
                    return

                submit_filters = st.button("Aplicar filtros", key="analytics_apply_filters")

                params_current = build_model_params(
                    ano_inicio, mes_inicio, ano_fim, mes_fim,
                    regiao_selecionada, crime_selecionado)

                params_k_current = params_current.copy()
                params_k_current['metodo'] = 'kmeans'
                params_d_current = params_current.copy()
                params_d_current['metodo'] = 'kdba'

                updated_state = st.session_state.get('analytics_selected_params')
                if submit_filters:
                    updated_state = {
                        'params': params_current,
                        'params_k': params_k_current,
                        'params_d': params_d_current
                    }
                    st.session_state['analytics_selected_params'] = updated_state
                    st.session_state['analytics_selected_model_id'] = None
                    st.session_state['force_refresh_models'] = False

            current_state = st.session_state.get('analytics_selected_params')

            st.markdown('<a id="resultados-do-modelo"></a>', unsafe_allow_html=True)

            with st.container(border=True):
                st.markdown("### Resultados do modelo")

                if not current_state:
                    st.info('Ajuste os filtros e clique em "Aplicar filtros" para consultar ou gerar modelos.')
                    return

                params = current_state['params']
                params_k = current_state['params_k']
                params_d = current_state['params_d']

                use_low_ttl = st.session_state.get('force_refresh_models', False)

                if use_low_ttl:
                    solicit_k = get_solicitacao_by_params_processing(
                        json.dumps(params_k, sort_keys=True))
                    solicit_d = get_solicitacao_by_params_processing(
                        json.dumps(params_d, sort_keys=True))
                else:
                    solicit_k = get_solicitacao_by_params_cached(
                        json.dumps(params_k, sort_keys=True))
                    solicit_d = get_solicitacao_by_params_cached(
                        json.dumps(params_d, sort_keys=True))

                if use_low_ttl:
                    st.session_state.force_refresh_models = False

                if (solicit_k and solicit_k.get('status') in ['PENDENTE', 'PROCESSANDO']) or \
                   (solicit_d and solicit_d.get('status') in ['PENDENTE', 'PROCESSANDO']):
                    st.session_state.force_refresh_models = True

                if solicit_k or solicit_d:
                    selected_method, selected_solicit = render_method_selector(
                        solicit_k, solicit_d)
                    process_model_by_status(
                        selected_method, selected_solicit, params, params_k, params_d)
                else:
                    handle_no_existing_models_cached(params_k, params_d)

            if st.session_state.get('analytics_scroll_target') == 'resultados-do-modelo':
                components.html(
                    """
                    <script>
                        const streamlitFrame = window.parent;
                        const anchor = streamlitFrame.document.getElementById('resultados-do-modelo');
                        if (anchor) {
                            anchor.scrollIntoView({ behavior: 'smooth', block: 'start' });
                        } else {
                            streamlitFrame.location.hash = 'resultados-do-modelo';
                        }
                    </script>
                    """,
                    height=0
                )
                st.session_state['analytics_scroll_target'] = None

    if selected_tab == tab_labels[0]:
        render_models_tab(selected_state)
    else:
        render_manual_tab(selected_state)
