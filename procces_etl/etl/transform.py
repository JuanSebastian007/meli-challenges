import pandas as pd
import numpy as np
from datetime import timedelta


class ETLTransformer:
    def __init__(self, taps_source: pd.DataFrame, prints_source: pd.DataFrame, pays_source: pd.DataFrame):
        """
        Inicializa una instancia de la clase.

        Parámetros:
        taps_source (str): Ruta o fuente de datos para los taps.
        prints_source (str): Ruta o fuente de datos para los prints.
        pays_source (str): Ruta o fuente de datos para los pays.
        """
        self.taps_source = taps_source
        self.prints_source = prints_source
        self.pays_source = pays_source

    def unnested_columns(self):
        """
        Desanida las columnas 'event_data' en los DataFrames 'taps_source' y 'prints_source', 
        y luego elimina las columnas originales 'event_data'.

        Este método realiza las siguientes operaciones:
        1. Desanida la columna 'event_data' en 'taps_source' y 'prints_source', 
           expandiendo su contenido en nuevas columnas.
        2. Elimina la columna original 'event_data' de ambos DataFrames.

        Atributos:
            self.taps_source (pd.DataFrame): DataFrame que contiene la columna 'event_data' 
                                             que será desanidada.
            self.prints_source (pd.DataFrame): DataFrame que contiene la columna 'event_data' 
                                               que será desanidada.

        Returns:
            None
        """
        self.taps_source = self.taps_source.join(self.taps_source['event_data'].apply(pd.Series))
        self.prints_source = self.prints_source.join(self.prints_source['event_data'].apply(pd.Series))

        self.taps_source = self.taps_source.drop(columns=['event_data'])
        self.prints_source = self.prints_source.drop(columns=['event_data'])

    def convert_dtypes_standarized(self):
        """
        Convierte los tipos de datos de las columnas en los DataFrames de origen a tipos estandarizados.

        Esta función realiza las siguientes conversiones:
        - Convierte la columna 'pay_date' de `pays_source` a tipo datetime.
        - Convierte la columna 'user_id' de `pays_source`, `prints_source` y `taps_source` a tipo string.
        - Convierte la columna 'day' de `prints_source` y `taps_source` a tipo datetime.
        - Convierte la columna 'position' de `prints_source` y `taps_source` a tipo entero.
        - Convierte la columna 'value_prop' de `prints_source` y `taps_source` a minúsculas y elimina espacios en blanco al inicio y al final.

        Returns:
            None
        """
        self.pays_source['pay_date'] = pd.to_datetime(self.pays_source['pay_date'])
        self.pays_source['user_id'] = self.pays_source['user_id'].astype(str)

        self.prints_source['user_id'] = self.prints_source['user_id'].astype(str)
        self.prints_source['day'] = pd.to_datetime(self.prints_source['day'])
        self.prints_source['position'] = self.prints_source['position'].astype(int)

        self.taps_source['user_id'] = self.taps_source['user_id'].astype(str)
        self.taps_source['day'] = pd.to_datetime(self.taps_source['day'])
        self.taps_source['position'] = self.taps_source['position'].astype(int)

        self.prints_source['value_prop'] = self.prints_source['value_prop'].str.lower().str.strip()
        self.taps_source['value_prop'] = self.taps_source['value_prop'].str.lower().str.strip()

    def transform_prints_clicked(self):
        """
        Transforma los datos de impresiones y clics de la última semana.

        Esta función realiza las siguientes operaciones:
        1. Calcula la fecha mínima y máxima de la última semana basada en los datos de impresiones.
        2. Filtra las impresiones y los clics de la última semana.
        3. Realiza un merge entre las impresiones y los clics basándose en 'user_id', 'day' y 'value_prop'.
        4. Añade una columna 'clicked' que indica si hubo un clic (True) o no (False) para cada impresión.
        5. Elimina columnas innecesarias y renombra la columna 'position_x' a 'position'.

        Returns:
            DataFrame: Un DataFrame con las impresiones de la última semana y una columna adicional 'clicked' que indica si hubo un clic.
        """
        fecha_min = self.prints_source['day'].max() - timedelta(weeks=1)
        fecha_max = self.prints_source['day'].max()

        prints_last_week = self.prints_source[self.prints_source['day'] >= fecha_min]
        taps_last_week = self.taps_source[(self.taps_source['day'] >= fecha_min) & (self.taps_source['day'] <= fecha_max)]

        print_clicked = pd.merge(prints_last_week, taps_last_week, on=['user_id', 'day', 'value_prop'], how='left', indicator=True)
        print_clicked['clicked'] = np.where(print_clicked['_merge'] == 'both', True, False)
        print_clicked = print_clicked.drop(columns=['_merge', 'position_y'])
        print_clicked = print_clicked.rename(columns={'position_x': 'position'})

        return print_clicked

    def transform_accumulate_data(self, print_clicked):
        """
        Transforma y acumula datos de impresiones, clics y pagos en un periodo de 3 semanas.

        Args:
            print_clicked (pd.DataFrame): DataFrame que contiene los datos de impresiones con una columna 'day'.

        Returns:
            tuple: Una tupla que contiene tres DataFrames:
                - prints_3_prev_weeks_cumulate (pd.DataFrame): DataFrame acumulado de impresiones de las 3 semanas previas.
                - taps_3_prev_weeks_cumulate (pd.DataFrame): DataFrame acumulado de clics de las 3 semanas previas.
                - pays_3_prev_weeks_cumulate (pd.DataFrame): DataFrame acumulado de pagos de las 3 semanas previas.

        El método realiza las siguientes operaciones:
            1. Obtiene los días únicos del DataFrame `print_clicked`.
            2. Genera tuplas de fechas que representan el rango de 3 semanas previas para cada día único.
            3. Ordena y reinicia los índices de los DataFrames `prints_source`, `taps_source` y `pays_source`.
            4. Para cada rango de fechas, filtra los datos de impresiones, clics y pagos dentro del rango.
            5. Calcula las cantidades acumuladas de vistas, clics y pagos previos.
            6. Filtra los datos para mantener solo aquellos correspondientes al día final del rango.
            7. Concatena los resultados en DataFrames acumulados y los retorna.
        """
        valores_unicos_day = print_clicked['day'].unique()
        tuplas_dates = [(fecha, fecha - timedelta(weeks=3)) for fecha in valores_unicos_day]

        self.prints_source = self.prints_source.sort_values(by='day', ascending=True).reset_index(drop=True)
        self.taps_source = self.taps_source.sort_values(by='day', ascending=True).reset_index(drop=True)
        self.pays_source = self.pays_source.sort_values(by='pay_date', ascending=True).reset_index(drop=True)

        prints_3_prev_weeks_list = []
        taps_3_prev_weeks_list = []
        pays_3_prev_weeks_list = []

        for end_date, start_date in tuplas_dates:
            prints_3_prev_weeks = self.prints_source[(self.prints_source['day'] >= start_date) & (self.prints_source['day'] <= end_date)].copy()
            taps_3_prev_weeks = self.taps_source[(self.taps_source['day'] >= start_date) & (self.taps_source['day'] <= end_date)].copy()
            pays_3_prev_weeks = self.pays_source[(self.pays_source['pay_date'] >= start_date) & (self.pays_source['pay_date'] <= end_date)].copy()

            prints_3_prev_weeks['quantity_views_prev_print'] = prints_3_prev_weeks.groupby(['user_id', 'value_prop']).cumcount()
            taps_3_prev_weeks['quantity_clicked_prev_print'] = taps_3_prev_weeks.groupby(['user_id', 'value_prop']).cumcount()
            pays_3_prev_weeks['import_accumulates_prev_print'] = pays_3_prev_weeks.groupby(['user_id', 'value_prop'])['total'].cumsum()

            prints_3_prev_weeks = prints_3_prev_weeks[prints_3_prev_weeks['day'] == end_date]
            taps_3_prev_weeks = taps_3_prev_weeks[taps_3_prev_weeks['day'] == end_date]
            pays_3_prev_weeks = pays_3_prev_weeks[pays_3_prev_weeks['pay_date'] == end_date]

            prints_3_prev_weeks_list.append(prints_3_prev_weeks)
            taps_3_prev_weeks_list.append(taps_3_prev_weeks)
            pays_3_prev_weeks_list.append(pays_3_prev_weeks)

        prints_3_prev_weeks_cumulate = pd.concat(prints_3_prev_weeks_list, ignore_index=True)
        taps_3_prev_weeks_cumulate = pd.concat(taps_3_prev_weeks_list, ignore_index=True)
        pays_3_prev_weeks_cumulate = pd.concat(pays_3_prev_weeks_list, ignore_index=True)

        return prints_3_prev_weeks_cumulate, taps_3_prev_weeks_cumulate, pays_3_prev_weeks_cumulate

    def merge_data(self, print_clicked, prints_3_prev_weeks_cumulate, taps_3_prev_weeks_cumulate, pays_3_prev_weeks_cumulate):
        """
        Combina múltiples DataFrames en uno solo, uniendo los datos basados en columnas comunes y rellenando valores faltantes.

        Parámetros:
        -----------
        print_clicked : pd.DataFrame
            DataFrame que contiene los datos de impresiones y clics.
        prints_3_prev_weeks_cumulate : pd.DataFrame
            DataFrame que contiene la acumulación de impresiones de las 3 semanas previas.
        taps_3_prev_weeks_cumulate : pd.DataFrame
            DataFrame que contiene la acumulación de clics de las 3 semanas previas.
        pays_3_prev_weeks_cumulate : pd.DataFrame
            DataFrame que contiene la acumulación de pagos de las 3 semanas previas.

        Retorna:
        --------
        pd.DataFrame
            DataFrame resultante de la combinación de los DataFrames de entrada, con valores faltantes rellenados con 0.
        """
        result = pd.merge(print_clicked, 
                          prints_3_prev_weeks_cumulate[['day', 'user_id', 'value_prop', 'quantity_views_prev_print']], 
                          on=['day', 'user_id', 'value_prop'], 
                          how='left')

        result = pd.merge(result, 
                          taps_3_prev_weeks_cumulate[['day', 'user_id', 'value_prop', 'quantity_clicked_prev_print']], 
                          on=['day', 'user_id', 'value_prop'], 
                          how='left')

        result = pd.merge(result, 
                          pays_3_prev_weeks_cumulate[['pay_date', 'user_id', 'value_prop', 'import_accumulates_prev_print']], 
                          left_on=['day', 'user_id', 'value_prop'], 
                          right_on=['pay_date', 'user_id', 'value_prop'], 
                          how='left')

        result = result.drop(columns=['pay_date'])
        result = result.fillna(0)

        return result