# dataJobs_project

Se espera crear un modelo para predecir las habilidades más demandadas en el mercado laboral. Esto puede ayudar a los profesionales, estudiantes y la industria en general a tomar decisiones informadas sobre qué habilidades desarrollar y mejorar.

El objetivo principal del proyecto es analizar las ofertas laborales en el campo de los datos y con el propósito de identificar patrones y tendencias que revelen las habilidades más demandadas en el mercado laboral actual. Esta investigación puede ayudar a las personas interesadas en este ámbito o que quieran meterse a este, ayudando a determinar si es la predicción deseada como una decisión laboral. Todo esto mediante el dataset de kaggle [Data Jobs Listings - Glassdoor](https://www.kaggle.com/datasets/andresionek/data-jobs-listings-glassdoor?select=glassdoor.csv).

## Archivos del repositorio

EDA_project.ipynb: Esta todo el analisis exploratorio de las tablas usadas que son "glassdoor.csv", "glassdoor_benefits_highlights.csv" y "glassdoor_salary_salaries.csv" (descargar estos csv para la ejecucion del codigo).

ETL_project.pdf: Es el documento en el cual estan descritas las fases realizadas y explicadas a detalles.

dataJobs_script.py: Este el codigo principal en el que se realizo la conexion a postgresql, se cargaron los datos a la BD, se reemplazaron los valores nulos en algunas columnas, campos vacios y normalizaciones para los titulos del trabajo. 

df_tocsv_and_transfomations.py: Aqui se tuvo que usar pandas para poder incrustar correctamente el csv y donde se realizo la mayor transformación de datos para la normalizacion del jobTitle.

dimesions_script.py: Aqui se hace la inserción de datos en las ds dimensiones que se va a hacer uso mas en el futuro del proyecto.

project_dashboard.pdf: Es el dashboard que realice en power bi, con tres graficas por ahora.

## ¿Como correr los scripts?

1. Clone el repositorio con `https://github.com/VinkeArtunduaga/dataJobs_project.git`
2. Instale python 3.11
3. Instale la base de datos PostgreSQL
4. Para las librerias es necesario hacer `pip install psycopg2`, `pip install pandas` y seran instaladas, tambien se usan json y csv pero se supone que estan predeterminadas.
5. Crear un usuario y contraseña para el uso de postgreSQL
6. Crear una database en pgAdmin 4 llamada ETL (asi fue como le puse el nombre a la mia pero se puede cambiar)
7. Cambie las configuraciones de la conexion a la base de datos segun el usuario, contraseña y database asignados
8. Corra primero el codigo de df_tocsv_and_transformations.py en la terminal mediante `python df_tocsv_and_transformations.py`
9. Luego `python dataJobs_script.py` para de esta manera crear la tabla principal con sus normalizaciones y limpieza de nulos.
10. Para luego correr el de la creacion de las dimesiones `python dimensions_script.py`

## En caso de querer realizar el proceso de EDA:

1. Descargar Jupyter para mas facilidad con Jupyter lab
2. Al ya tener descargadas las librerias de json y pandas o no haber podido ser instaladas ejecutar en la terminal `pip install pandas` y `pip install json`
3. Cambiar la direccion de donde se encuentran los archivos csv de glassdoor.csv, glassdoor_benefits_highlights.csv y glassdoor_salary_salaries.csv.
4. Al ejecutar cada uno de los bloques o de corrido deberia apreciarse el analisis.

## Para la segunda parte del proyecto hice una carpeta llama API ahi estan todo lo que se realizo.

