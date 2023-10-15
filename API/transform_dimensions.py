import psycopg2
import json

#Carga de la configuracion de los datos de la database
with open('C:/Users/kevin/ETL/Postgres/kaggle/dataJobs_project/db_config.json') as config_file:
    config = json.load(config_file)

try:
    conn = psycopg2.connect(
        host='localhost',
        user = config['user'],
        password = config['password'],
        database='ETL'
    )

    print("Conexion exitosa uwu")

    cursor = conn.cursor()

    #Creacion de la tabla dataJobs_glassdoor
    create_table = """
        CREATE TABLE IF NOT EXISTS datajobs_salaries (
            location VARCHAR,
            job_title VARCHAR,
            publisher_name VARCHAR,
            publisher_link VARCHAR,
            min_salary FLOAT,
            max_Salary FLOAT,
            median_Salary FLOAT,
            salary_period VARCHAR,
            salary_currency VARCHAR
        )
    """

    cursor.execute(create_table)
    conn.commit()
    print("Tabla creada")

    insert_salaries = """
        COPY datajobs_salaries(
            location, job_title, publisher_name, publisher_link,
            min_salary, max_salary, median_salary, salary_period,
            salary_currency
        ) FROM 'C:/Users/kevin/ETL/Postgres/kaggle/dataJobs_project/API/job_salaries.csv' DELIMITER ',' CSV HEADER ENCODING 'ISO-8859-1'
    """
    cursor.execute(insert_salaries)
    conn.commit()
    print("Salarios agregados")

    delete_publisherLink = """
        ALTER TABLE datajobs_salaries
        DROP COLUMN publisher_link;
    """
    cursor.execute(delete_publisherLink)
    conn.commit()
    print("Columna publisher_link eliminada")

except Exception as ex:
    print("Error:", ex)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Conexión finalizada u.u")