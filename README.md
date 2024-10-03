
# Mercado Libre Challenge

Este proyecto es una solución para el desafío técnico de Mercado Libre.

## Descripción

El objetivo del proyecto es desarrollar un proceso de ETL para un algoritmo de Machine Learning que analiza el comportamiento de los usuarios en la plataforma de Mercado Pago, que busca predecir el orden en que se debe mostrar una Propuestas de Valor en un carrusel llamado "Descubri Mas"

## Requisitos

- Python 3.8+
- Pipenv
## Instalación

1. Instala virtualenv si no lo tienes instalado:
```bash
pip install virtualenv
```
2. Crea un entorno virtual:
```bash
virtualenv venv
```
3. Activa el entorno virtual:
```bash
cd venv
source bin/activate
```
4. Clona el repositorio:
```bash
git clone https://github.com/JuanSebastian007/meli-challenges.git
cd meli-challenges
```
5. Instala las dependencias:
```bash
pip install -r requirements.txt
```
6. Ejecutar el proyecto 
```bash
python run_etl.py
```
## *NOTA: Los resultados se guardan en la ruta data/processed/result.csv
*NOTA*: En caso de que reciba error por la configuracion del python path, lo puede configurar de la siguiente forma:
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)
```
## Estructura de carpeta
```plaintext
mercado_libre_challenge/
├── data/
│   ├── raw/ #Aqui estan los datos crudos.
│   ├── processed/  # Aquí se guardan los datos finales
├── data_anaslisis/
│   ├── analisis_exploratorio.ipynb
│   ├── analisis_exploratorio.pdf
├── etl/ #Proceso del ETL
│   ├── __init__.py
│   ├── extract.py
│   ├── load.py
│   ├── transform.py
├── tests/ #Pruebas unitarias
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_load.py
│   ├── test_transform.py
|   ├── coverage.txt #Cobertura de pruebas unitarias
├── requirements.txt
├── run_etl.py
└── README.md
```