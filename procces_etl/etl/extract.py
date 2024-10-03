import pandas as pd

class DataExtractor:
    def __init__(self, file_path: str):
        """
        Inicializa una instancia de la clase.

        Args:
            file_path (str): La ruta del archivo que se utilizará en la instancia.
        """
        self.file_path = file_path

    def extract(self):
        """
        Extrae datos de un archivo según su formato.

        Este método verifica la extensión del archivo especificado en `self.file_path`
        y llama al método de extracción correspondiente según el formato del archivo.

        Returns:
            object: Los datos extraídos del archivo.

        Raises:
            ValueError: Si el formato del archivo no es compatible.
        """
        if self.file_path.endswith('.csv'):
            return self._extract_csv()
        elif self.file_path.endswith('.json'):
            return self._extract_json()
        else:
            raise ValueError("Unsupported file format")

    def _extract_csv(self):
        return pd.read_csv(self.file_path)

    def _extract_json(self):
        return pd.read_json(self.file_path, lines=True)