import psycopg2
import csv
import json

#Carga de la configuracion de los datos de la database
with open('db_config.json') as config_file:
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

    create_table_salaries = """
        CREATE TABLE IF NOT EXISTS datajobs_salaries (
            id SERIAL PRIMARY KEY,
            salary_id INTEGER,
            index INTEGER,
            salary_reports INTEGER,
            job_title VARCHAR,
            pay_period VARCHAR,
            payPercentil_10 FLOAT,
            payPercentil_90 FLOAT,
            payPercentil_50 FLOAT,
            salary_type VARCHAR
        );
    """
    cursor.execute(create_table_salaries)
    conn.commit()

    sql_salaries = """
        COPY datajobs_salaries (salary_id, index, salary_reports, job_title, pay_period, payPercentil_10, payPercentil_90, payPercentil_50, salary_type)
        FROM 'C:/Users/kevin/ETL/Postgres/kaggle/dataJobs_project/glassdoor_salary_salaries.csv' DELIMITER ',' CSV HEADER;
    """
    cursor.execute(sql_salaries)
    conn.commit()
    print("Datos de salaries insertados")

    # Eliminar las filas con NULL en la columna 'index'
    delete_null_index = """
        DELETE FROM datajobs_salaries
        WHERE index IS NULL;
    """
    cursor.execute(delete_null_index)
    conn.commit()
    print("Filas con NULL en 'index' eliminadas")

    # Actualizar los valores NULL en 'payPercentil_50' a 0
    update_null_payPercentil_50 = """
        UPDATE datajobs_salaries
        SET payPercentil_50 = 0
        WHERE payPercentil_50 IS NULL;
    """
    cursor.execute(update_null_payPercentil_50)
    conn.commit()
    print("Valores NULL en 'payPercentil_50' actualizados a 0")

    create_table_benefits = """
        CREATE TABLE IF NOT EXISTS datajobs_benefitsHighlights (
            id SERIAL PRIMARY KEY,
            benefits_id INTEGER,
            highlighted_phrase VARCHAR,
            highlighted_icon VARCHAR,
            highlighted_name VARCHAR,
            index INTEGER,
            count_reviews INTEGER
        );
    """
    cursor.execute(create_table_benefits)
    conn.commit()

    sql_benefits = """
        COPY datajobs_benefitsHighlights (benefits_id, highlighted_phrase, highlighted_icon, highlighted_name, index, count_reviews)
        FROM 'C:/Users/kevin/ETL/Postgres/kaggle/dataJobs_project/glassdoor_benefits_highlights.csv' DELIMITER ',' CSV HEADER;
    """
    cursor.execute(sql_benefits)
    conn.commit()
    print("Datos de benefits insertados")

    # Eliminar las filas con NULL en la columna index de benefits
    delete_null_index_benefits = """
        DELETE FROM datajobs_benefitsHighlights
        WHERE index IS NULL;
    """
    cursor.execute(delete_null_index_benefits)
    conn.commit()
    print("Filas con NULL en 'index' de benefits eliminadas")

except Exception as ex:
    print("Error:", ex)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Conexión finalizada u.u")