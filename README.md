# SSP Data Dashboard

Acesse a versão online: [https://sspdata.streamlit.app/](https://sspdata.streamlit.app/)

Este projeto é um painel interativo desenvolvido com [Streamlit](https://streamlit.io/) para visualização, análise e geração de relatórios sobre dados de ocorrências criminais do Estado de São Paulo.

## Estrutura do Projeto

- **src/app.py**: Arquivo principal. Gerencia a navegação entre páginas, carrega filtros (anos, regiões, municípios) e busca dados do banco de dados para alimentar os gráficos e tabelas.
- **src/pages/**: Contém os módulos das páginas do app (Dashboard, Análises, Relatórios, Sobre).
- **src/utils/**: Funções utilitárias, como geração de gráficos e conexão com o banco de dados.
- **configs/**: Arquivos de configuração, como código dos municípios e localizações de cidades, além do script `_createTables.sql` para criação das tabelas necessárias no banco de dados.
- **output/maps/**: Mapas gerados pelo sistema, são criados caso os dados sejam alterados porem como são demorados para gerar eles servem como um cache.

## Lógica Principal

1. **Navegação**: O app possui uma barra de navegação superior para alternar entre as páginas (Dashboard, Análises, Relatórios, Sobre).
2. **Filtros**: Ao iniciar, o app carrega filtros de ano, região e município a partir do banco de dados.
3. **Consulta de Dados**: As funções de busca consultam o banco de dados PostgreSQL conforme os filtros selecionados, retornando DataFrames para alimentar gráficos e tabelas.
4. **Visualização**: Os dados são apresentados em gráficos interativos (Plotly) e tabelas, permitindo análise visual rápida.
5. **Relatórios**: Geração de relatórios customizados a partir dos dados filtrados.
6. **População do Banco de Dados**: O banco de dados não precisa estar previamente populado. Basta criar as tabelas utilizando o script `configs/configs_createTables.sql`. Ao clicar em "Atualizar Dados" no app, ele irá popular automaticamente o banco com os dados necessários.

## Como Executar Localmente

1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Crie as tabelas do banco de dados PostgreSQL executando o script `configs/configs_createTables.sql`.
3. Configure as credenciais do banco de dados no arquivo `.streamlit/secrets.toml` (veja exemplo abaixo).
4. Execute o app:
   ```bash
   streamlit run src/app.py
   ```

### Exemplo de `.streamlit/secrets.toml`
```toml
[database]
POSTGRES_DB = "ssp_database"
POSTGRES_USER = "SSPData"
POSTGRES_PASSWORD = "password"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
```

## Observações
- O app utiliza cache para otimizar consultas ao banco logo pode demorar um tempo para refletir as mudanças.
- O banco de dados só precisa das tabelas criadas; os dados serão populados automaticamente ao clicar em "Atualizar Dados".
- O layout é otimizado para visualização em desktop.

---
Projeto SSP Data
