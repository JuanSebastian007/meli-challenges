import pandas as pd

class DataFrameSaver:
    def __init__(self, dataframe: pd.DataFrame):
        """
        Inicializa una instancia de la clase.

        Args:
            dataframe (pd.DataFrame): El DataFrame que se utilizará en la instancia.
        """
        self.dataframe = dataframe

    def save_to_csv(self, file_path: str):
        """
        Guarda el DataFrame en un archivo CSV.

        Args:
            file_path (str): La ruta del archivo donde se guardará el CSV.

        Returns:
            None
        """
        self.dataframe.to_csv(file_path, index=False)