"""
Constantes globais do projeto para evitar duplicação de código.
"""

# Mapeamento de meses
MESES = [
    'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
]

# Mapeamento nome -> número do mês
MESES_MAP = {mes: i + 1 for i, mes in enumerate(MESES)}

# Mapeamento número -> nome do mês
MESES_MAP_INV = {i + 1: mes for i, mes in enumerate(MESES)}

# Tema padrão para gráficos (dark mode)
CHART_THEME = {
    'primary_color': '#1f77b4',
    'text_color': '#fafafa',
    'grid_color': 'rgba(255,255,255,0.1)',
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'paper_bgcolor': 'rgba(0,0,0,0)'
}

# Configurações de mapas
# Raio mínimo e máximo (pixels) para normalização dos marcadores no mapa
MAP_MIN_MARKER_RADIUS = 20
MAP_MAX_MARKER_RADIUS = 200
# Centro inicial do mapa focado no Estado de São Paulo (aproximação)
MAP_CENTER_SP = {'lat': -22.5, 'lon': -49.0}
# Zoom inicial apropriado para visualizar o estado de SP
MAP_INITIAL_ZOOM_SP = 6.2

# Configurações de API
API_TIMEOUT_MINUTES = 1
LOCK_UPDATE_INTERVAL_SECONDS = 30
STARTUP_WAIT_SECONDS = 15

# Configurações do download
BAIXAR_ANOS_ANTERIORES = 0
MAX_WORKERS = 10