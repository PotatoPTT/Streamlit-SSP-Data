# SSP Data Dashboard

Acesse a versão online: [https://sspdata.streamlit.app/](https://sspdata.streamlit.app/)

Plataforma interativa desenvolvida com [Streamlit](https://streamlit.io/) para visualização, análise e clustering de dados de ocorrências criminais do Estado de São Paulo, utilizando técnicas avançadas de Machine Learning para análise de séries temporais.

## Sobre o Projeto

Este sistema integra coleta automática de dados da SSP-SP, processamento robusto de séries temporais e algoritmos de clustering (K-means, K-DBA, K-Shape) para identificar padrões criminais em municípios paulistas. O projeto foi desenvolvido em duas fases:

- **Fase 1**: Construção da infraestrutura base (pipeline de dados, banco de dados, interface de visualização)
- **Fase 2**: Implementação de análise avançada com Machine Learning e clustering de séries temporais

O projeto foi desenvolvido para a materia de Projetos em Computação na Unesp Rio Claro

## Estrutura do Projeto

```
├── diagramas/                  # Pasta contendo os diagramas do projeto atual
├── api.py                      # API de treinamento de modelos ML
├── src/
│   ├── app.py                  # Aplicação principal Streamlit
│   ├── pages/                  # Páginas da aplicação
│   │   ├── dashboard.py        # Visualização de dados e estatísticas
│   │   ├── analytics.py        # Análise de clustering e séries temporais
│   │   └── about.py            # Informações sobre o projeto
│   └── utils/                  # Módulos utilitários
│       ├── config/             # Configurações e logging
│       ├── core/               # Gerenciadores (API, pipeline)
│       ├── data/               # Conexão BD, download e processamento
│       ├── ml/                 # Machine Learning (treinamento, jobs)
│       ├── ui/                 # Componentes de interface
│       └── visualization/      # Gráficos e mapas
├── configs/
│   ├── _createTables.sql       # Script de criação das tabelas
│   ├── cities_codes.csv        # Códigos dos municípios
│   └── cities_location.csv     # Coordenadas geográficas
└── output/
    └── models/                 # Modelos treinados (.joblib)
```

## Funcionalidades

### Dashboard
- Visualização interativa de estatísticas criminais
- Filtros por ano, região, município e tipo de crime
- Gráficos de tendências temporais e distribuição geográfica
- Mapas de calor com geolocalização
- KPIs e métricas comparativas
- Tabelas exportáveis

### Analytics (Machine Learning)
- **Clustering de séries temporais** com três algoritmos:
  - **K-means**: Clustering euclidiano clássico
  - **K-DBA**: Dynamic Time Warping Barycenter Averaging
  - **K-Shape**: Clustering baseado em forma das séries
- Seleção automática do melhor K (2-15) usando Silhouette Score
- Visualização interativa dos clusters identificados
- Fila de jobs assíncrona para processamento em background
- Histórico de modelos treinados

### Pipeline Automático
- Download automático de dados da API SSP-SP
- Processamento e limpeza de dados
- Enriquecimento com informações geográficas
- Detecção inteligente de dados faltantes
- Atualização incremental do banco de dados
- Sistema de cooldown (60 minutos entre atualizações)

### Melhorias Implementadas
- **Front-end responsivo** com design moderno
- **Refatoração completa** do código com arquitetura modular
- **Otimizações de performance** (cache, lazy loading)
- **Sistema de logs** estruturado
- **API REST** para gerenciamento de treinamento

## Tecnologias Utilizadas

- **Frontend**: Streamlit, Plotly
- **Backend**: Python 3.12+
- **Banco de Dados**: PostgreSQL
- **Machine Learning**: scikit-learn, tslearn
- **Processamento**: pandas, numpy
- **Geolocalização**: geopy
- **Persistência de Modelos**: joblib

## Pré-requisitos

- Python 3.12 ou superior
- PostgreSQL 12+
- pip (gerenciador de pacotes Python)

## Como Executar Localmente

### 1. Clone o repositório
```bash
git clone https://github.com/PotatoPTT/Streamlit-SSP-Data.git
cd Streamlit-SSP-Data
```

### 2. Instale as dependências
```bash
pip install -r requirements.txt
```

### 3. Configure o banco de dados PostgreSQL

Crie o banco de dados e execute o script de criação das tabelas:
```bash
psql -U seu_usuario -d postgres -c "CREATE DATABASE ssp_database;"
psql -U seu_usuario -d ssp_database -f configs/_createTables.sql
```

### 4. Configure as credenciais

Crie o arquivo `.streamlit/secrets.toml`:
```toml
[database]
POSTGRES_DB = "ssp_database"
POSTGRES_USER = "seu_usuario"
POSTGRES_PASSWORD = "sua_senha"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
```

### 5. Execute a aplicação

**Opção 1: Apenas o Streamlit**
```bash
streamlit run src/app.py
```

**Opção 2: Streamlit + API de Treinamento (recomendado)**
```bash
# Terminal 1 - API de treinamento
python api.py

# Terminal 2 - Interface Streamlit
streamlit run src/app.py
```

### 6. Popule o banco de dados

- Acesse a aplicação em `http://localhost:8501`
- No Dashboard, clique em **"Atualizar Dados"**
- O sistema irá baixar e processar automaticamente os dados da SSP-SP

## Fluxo de Trabalho

1. **Inicialização**: O app carrega filtros (anos, regiões, municípios) do banco de dados
2. **Visualização**: Dashboard apresenta dados com gráficos interativos e mapas
3. **Atualização**: Pipeline automático sincroniza dados da API SSP-SP
4. **Análise**: Usuário configura parâmetros de clustering na página Analytics
5. **Treinamento**: API processa jobs em background, treinando modelos
6. **Resultado**: Visualização dos clusters e padrões identificados

## Tratamento de Dados para ML

### Remoção de Séries Nulas
Séries temporais com muitos valores ausentes são removidas porque:
- Distorcem algoritmos de clustering (K-means, K-DBA, K-Shape)
- DTW não lida bem com gaps extensos
- Evita viés nos cálculos de distância e centróides

### Robust Scaler
Utilizado em vez de StandardScaler devido a:
- Dados criminais possuem **outliers extremos** (eventos sazonais)
- Usa **mediana e IQR** (mais resistente a outliers)
- Preserva estrutura dos dados sem distorção
- Permite comparação justa entre séries de diferentes magnitudes

## Configurações Avançadas

### Cache e Performance
- Cache de filtros: 30 minutos (1800s)
- Cache de dados processados: otimização Streamlit
- Cooldown de atualização: 60 minutos

### API de Treinamento
- Polling interval: configurável em `src/utils/ml/config.py`
- Modelos salvos em: `output/models/`
- Lock file: `configs/api.lock`

## Observações

- O app utiliza cache agressivo para otimizar performance
- Layout responsivo otimizado para desktop e mobile
- API de treinamento pode rodar em background avulsa (opcional)
- Modelos são persistidos e podem ser reutilizados

## Colaboradores

<table>
  <tr>
    <td align="center">
      <a href="https://github.com/antony-pereira" target="_blank">
        <img src="https://github.com/antony-pereira.png" width="100px;" alt="Antony Pereira"/><br />
        <sub><b>Antony Pereira</b></sub>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/PotatoPTT" target="_blank">
        <img src="https://github.com/PotatoPTT.png" width="100px;" alt="Caio Henrique"/><br />
        <sub><b>Caio Henrique</b></sub>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/k-nylander" target="_blank">
        <img src="https://github.com/k-nylander.png" width="100px;" alt="Kauã Nylander"/><br />
        <sub><b>Kauã Nylander</b></sub>
      </a>
    </td>
  </tr>
</table>