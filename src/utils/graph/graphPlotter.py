import plotly.graph_objects as go


def create_monthly_evolution_chart(get_chart_theme):
    """Create themed monthly evolution line chart (placeholder)"""
    theme = get_chart_theme()
    fig = go.Figure()
    fig.add_annotation(
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        text="Aguardando dados da API<br>📈 Gráfico será carregado quando conectado",
        showarrow=False,
        font=dict(size=16, color=theme['text_color']),
        align="center")
    fig.update_layout(title=dict(text="Evolução Mensal",
                                 font=dict(size=18, color=theme['text_color']),
                                 x=0.02),
                      xaxis=dict(title="Mês",
                                 showgrid=True,
                                 gridcolor=theme['grid_color'],
                                 color=theme['text_color'],
                                 showticklabels=False),
                      yaxis=dict(title="Número de Ocorrências",
                                 showgrid=True,
                                 gridcolor=theme['grid_color'],
                                 color=theme['text_color'],
                                 showticklabels=False),
                      plot_bgcolor=theme['plot_bgcolor'],
                      paper_bgcolor=theme['paper_bgcolor'],
                      height=400,
                      showlegend=False,
                      font=dict(color=theme['text_color']))
    return fig


def create_occurrence_types_chart(get_chart_theme):
    """Create themed occurrence types bar chart (placeholder)"""
    theme = get_chart_theme()
    fig = go.Figure()
    fig.add_annotation(
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        text="Aguardando dados da API<br>📊 Gráfico será carregado quando conectado",
        showarrow=False,
        font=dict(size=16, color=theme['text_color']),
        align="center")
    fig.update_layout(title=dict(text="Tipos de Ocorrências",
                                 font=dict(size=18, color=theme['text_color']),
                                 x=0.02),
                      xaxis=dict(title="Quantidade",
                                 showgrid=True,
                                 gridcolor=theme['grid_color'],
                                 color=theme['text_color'],
                                 showticklabels=False),
                      yaxis=dict(title="Tipo de Ocorrência",
                                 color=theme['text_color'],
                                 showticklabels=False),
                      plot_bgcolor=theme['plot_bgcolor'],
                      paper_bgcolor=theme['paper_bgcolor'],
                      height=400,
                      showlegend=False,
                      font=dict(color=theme['text_color']))
    return fig
