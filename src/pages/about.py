import streamlit as st


def show_about():
    """About page"""
    st.markdown("# ℹ️ Sobre o Sistema")
    st.markdown("""
    
    Este dashboard foi desenvolvido como projeto de conclusão de curso para a disciplina de **Projetos em Computação**.
    
    O sistema tem como objetivo a análise e visualização de dados de segurança pública do Estado de São Paulo, utilizando dados oficiais disponibilizados pelo Governo do Estado.
    
    ### Características
    - **Interface Intuitiva**: Design inspirado em ferramentas como Power BI e Tableau
    - **Dados Oficiais**: Todos os dados utilizados foram retirados do portal oficial da Secretaria de Segurança Pública de São Paulo ([https://www.ssp.sp.gov.br/estatistica/dados-mensais](https://www.ssp.sp.gov.br/estatistica/dados-mensais))
    - **Visualizações Interativas**: Gráficos e mapas interativos
    
    ### Código Fonte
    O código-fonte deste projeto está disponível em: [https://github.com/PotatoPTT/Streamlit-SSP-Data](https://github.com/PotatoPTT/Streamlit-SSP-Data)
    
    ### Recursos Futuros
    - Análises avançadas com machine learning
    - Análise de séries temporais
    - Relatórios customizados
    
    ### Suporte Técnico
    Para mais informações, entre em contato através do GitHub.
    """)
