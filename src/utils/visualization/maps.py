import os
import folium
import pandas as pd
from utils.data.connection import DatabaseConnection

# Importa o logger contextualizado
from utils.config.logging import get_logger
logger = get_logger("MAPS")


class MapPlotter:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def _get_normalization_params(self, values):
        values = values.astype(float)
        min_val = values[values > 0].min() if (values > 0).any() else 0
        max_val = values.max() if not values.empty else 1
        #! é dificil determinar os valores mínimos e máximos de raio
        #! já que são paulo tem MUUUIIITOOO crime
        min_radius = 2
        max_radius = 35
        return min_val, max_val, min_radius, max_radius

    def _norm_radius(self, val, min_val, max_val, min_radius, max_radius):
        if val <= 0 or max_val == min_val:
            return min_radius
        return min_radius + (max_radius - min_radius) * (val - min_val) / (max_val - min_val)

    def _add_month_layer(self, m, month, idx, month_df, crime, offsets, offset_factor, month_colors, min_val, max_val, min_radius, max_radius):
        fg = folium.FeatureGroup(name=month, show=(month == 'Janeiro'))
        color = month_colors[(idx-1) % len(month_colors)]
        for _, row in month_df.iterrows():
            lat = row['latitude']
            lon = row['longitude']
            municipio = row['Nome_Municipio']
            value = row['quantidade']
            try:
                value_num = float(value)
            except Exception:
                value_num = 0
            if pd.notnull(lat) and pd.notnull(lon) and value_num > 0:
                dlat, dlon = offsets[idx-1]
                lat_offset = lat + dlat * offset_factor
                lon_offset = lon + dlon * offset_factor
                popup = f"{municipio}<br>{crime}: {value} ({month})"
                folium.CircleMarker(
                    location=[lat_offset, lon_offset],
                    radius=self._norm_radius(
                        value_num, min_val, max_val, min_radius, max_radius),
                    color=color,
                    fill=True,
                    fill_opacity=0.7,
                    popup=popup
                ).add_to(fg)
        m.add_child(fg)

    def plot_maps_by_year_and_crime_db(self, year_filter=None):
        """
        Plota mapas usando dados vindos do banco de dados, normalizando o tamanho dos círculos.
        """
        db = DatabaseConnection()
        months = [
            'Janeiro', 'Fevereiro', 'Marco', 'Abril', 'Maio', 'Junho',
            'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
        ]
        month_colors = [
            '#e6194b', '#3cb44b', '#ffe119', '#4363d8',
            '#f58231', '#911eb4', '#46f0f0', '#f032e6',
            '#bcf60c', '#fabebe', '#008080', '#e6beff'
        ]
        offsets = [
            (-1, -1), (-1, 0), (-1, 1), (-1, 2),
            (0, -1), (0, 0), (0, 1), (0, 2),
            (1, -1), (1, 0), (1, 1), (1, 2)
        ]
        offset_factor = 0.01
        df = db.get_map_data(year=year_filter)
        if df.empty:
            logger.warning('Nenhum dado encontrado no banco para o filtro.')
            return
        years = [year_filter] if year_filter is not None else df['Ano'].unique()
        crimes = df['Natureza'].unique()
        for year in years:
            logger.info(f"=== Início do Pipeline de Geração de Mapas ===")
            logger.info(f"Gerando mapas para o ano {year}...")
            year_df = df[df['Ano'] == year]
            year_dir = os.path.join(self.output_dir, str(year))
            if not os.path.exists(year_dir):
                os.makedirs(year_dir)
            for crime in crimes:
                crime_df = year_df[year_df['Natureza'] == crime]
                if crime_df.empty:
                    continue
                m = folium.Map(location=[-21.8, -49.1], zoom_start=7)
                min_val, max_val, min_radius, max_radius = self._get_normalization_params(
                    crime_df['quantidade'])
                for idx, month in enumerate(months, 1):
                    month_df = crime_df[crime_df['mes'] == idx]
                    self._add_month_layer(m, month, idx, month_df, crime, offsets, offset_factor,
                                          month_colors, min_val, max_val, min_radius, max_radius)
                folium.LayerControl(collapsed=False).add_to(m)
                crime_filename = crime.replace(
                    '/', '_').replace(' ', '_').replace('(', '').replace(')', '')
                map_path = os.path.join(year_dir, f"{crime_filename}.html")
                m.save(map_path)
                logger.debug(
                    f"Mapa do crime '{crime}' do ano {year} salvo em {map_path}")
            logger.info(f"Mapas para o ano {year} gerados com sucesso!")
        logger.info("=== Todos os mapas foram gerados com sucesso! ===")
        db.close()
