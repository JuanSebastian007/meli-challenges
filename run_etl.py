from procces_etl.etl.extract import DataExtractor
from procces_etl.etl.transform import ETLTransformer
from procces_etl.etl.load import DataFrameSaver

def main():
    """
    Función principal para ejecutar el proceso ETL (Extract, Transform, Load).

    Esta función realiza las siguientes operaciones:
    1. Extracción de datos:
        - Extrae datos de un archivo JSON de taps.
        - Extrae datos de un archivo JSON de prints.
        - Extrae datos de un archivo CSV de pays.

    2. Transformación de datos:
        - Desanida columnas de los datos extraídos.
        - Convierte los tipos de datos a un formato estandarizado.
        - Transforma los datos de prints para identificar los clics.
        - Acumula datos de las tres semanas previas para prints, taps y pays.
        - Fusiona los datos transformados y acumulados en un solo DataFrame.

    3. Carga de datos:
        - Guarda el DataFrame resultante en un archivo CSV.

    Returns:
         None
    """
    taps_source = DataExtractor('procces_etl/data/raw/taps.json').extract()
    prints_source = DataExtractor('procces_etl/data/raw/prints.json').extract()
    pays_source = DataExtractor('procces_etl/data/raw/pays.csv').extract()

    transform_data = ETLTransformer(taps_source, prints_source, pays_source)
    transform_data.unnested_columns()
    transform_data.convert_dtypes_standarized()
    print_clicked = transform_data.transform_prints_clicked()
    prints_3_prev_weeks_cumulate, taps_3_prev_weeks_cumulate, pays_3_prev_weeks_cumulate = transform_data.transform_accumulate_data(print_clicked)
    result = transform_data.merge_data(print_clicked, prints_3_prev_weeks_cumulate, taps_3_prev_weeks_cumulate, pays_3_prev_weeks_cumulate)

    saver = DataFrameSaver(result)
    saver.save_to_csv('procces_etl/data/processed/result.csv')

if __name__ == "__main__":
    main()