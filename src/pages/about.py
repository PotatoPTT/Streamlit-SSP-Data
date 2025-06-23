import streamlit as st


def show_about():
    """About page"""
    st.markdown("# ℹ️ Sobre o Sistema")
    st.markdown("""
    ## SP Segurança Dashboard
    
    Este dashboard foi desenvolvido como projeto de conclusão de curso para a disciplina de **Projetos em Computação**.
    
    O sistema tem como objetivo a análise e visualização de dados de segurança pública do Estado de São Paulo, utilizando dados oficiais disponibilizados pelo Governo do Estado.
    
    ### Características
    - **Interface Profissional**: Design inspirado em ferramentas como Power BI e Tableau
    - **Dados Oficiais**: Todos os dados utilizados foram retirados do portal oficial da Secretaria de Segurança Pública de São Paulo ([https://www.ssp.sp.gov.br/estatistica/dados-mensais](https://www.ssp.sp.gov.br/estatistica/dados-mensais))
    - **Análises Avançadas**: Ferramentas estatísticas e de machine learning
    - **Relatórios Customizados**: Geração de relatórios personalizados
    - **Visualizações Interativas**: Gráficos e mapas interativos
    - **Modo Escuro**: Interface moderna e agradável
    
    ### Recursos Futuros
    - Mapa interativo com dados georreferenciados
    - Alertas automáticos para anomalias
    - Integração com sistemas de emergência
    - API pública para desenvolvedores
    
    ### Suporte Técnico
    Para suporte técnico e mais informações, entre em contato através dos canais oficiais da disciplina.
    """)
