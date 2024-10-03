
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
    cd mercado_libre_challenge
    ```
5. Instala las dependencias:
    ```bash
    pip install -r requirements.txt
    ```
6. Ejecutar el proyecto 
```bash
    python run_etl.py
```
*NOTA*: En caso de que reciba error por la configuracion del python path, lo puede configurar de la siguiente forma:
```bash
    export PYTHONPATH=$PYTHONPATH:$(pwd)
```