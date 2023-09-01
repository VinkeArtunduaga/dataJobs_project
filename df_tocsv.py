import pandas as pd
import psycopg2

df = pd.read_csv("C:/Users/kevin/ETL/Postgres/kaggle/dataJobs_project/dataJobs_all.csv", delimiter=',')

# Guardado del dataset mediante pandas
output_filename = 'dataJobs_all.csv'
df.to_csv(output_filename, index=False)
print(f"CSV guardado como '{output_filename}' exitosamente.")

# Categorias mapeadas
category_mapping = {

    "Technology/IT": [
        "Java Team Lead", "Software Engineer", "Developer", "Technical Support Engineer",
        "AI Engineer", "Cloud Engineer", "Data Analyst", "Security Data Scientist",
        "Data Engineer", "Data Center Technician (DCO)",
        "Software Developer within Automotive", "Cloud Engineer/DevOps Engineer",
        "System Analyst/Backend Developer", "Junior Big Data Engineer",
        "Sr Software Engineer, Ad tech", "Software Engineer – Networks"
    ],
    
    "Management": ["Manager", "Director", "Lead"],

    "Data Science/Analytics": [
        "Data Scientist", "Statistician", "Market Research Analyst",
        "Big Data Analyst (m/w)"
    ],
    "Business": [
        "Sales Manager", "Account Manager", "Business Development",
        "Marketing Manager", "Product Manager", "Digital Marketing",
        "Finance Analyst", "Financial Manager", "Consultant", "Advisor",
        "Associate Practice Engagement Manager",
        "Proactive Monitoring Engineer-Commerce Cloud",
        "Agile Business Analyst", "Associate"
    ],
    "Healthcare/Science": [
        "Doctor", "Nurse", "Medical Scientist", "Biomedical Scientist",
        "Clinical Research Associate", "Genetic Counselor",
        "Senior Research Scientist – Antibody reagents: in vivo biology",
        "Public Health Specialist Manager",
        "Clinical Scientist in Pharma Research and Early Development (m/f/d)"
    ],
    "Engineering": [
        "Engineer", "Project Engineer", "Geotechnical Engineer",
        "Controls Engineer", "Energy Engineer", "Aerospace Engineer",
        "Structural and Foundation Engineer",
        "BMS Controls Engineer", "Energy Engineer",
        "Automotive Data Analyst", "Construction Engineer",
        "Structural and Foundation Engineer",
        "Associate, Big Data Analyst (m/w)", "Sr.Equipment Engineer"
    ],
    "Education": [
        "Assistant Professor", "Lecturer", "Educational Consultant",
        "Assistant Professor - Computer Sciences",
        "Territory Manager (Ponta Grossa/PR)"
    ],
    "Logistics/Supply Chain": ["Supply Chain Analyst", "Logistics Manager"],

    "Design": ["Designer", "Art Director", "Interior Designer"],

    "Retail": ["Retail Manager"],

    "Research": [
        "Research Scientist", "Research Analyst",
        "Research Scientist - Computer Vision",
        "Senior Research Scientist", "Assistant Professor - Computer Sciences"
    ],

    "Human Resources": ["HR Manager", "Recruiter"],

    "Architecture/Construction": ["Architect", "Construction Engineer"],

    "Manufacturing": [
        "Manufacturing Engineer", "Quality Control Analyst"
    ],
    "Food Industry": ["Executive Chef", "Food Quality Analyst"],

    "Tourism/Hospitality": [
        "Tourism Coordinator", "Hospitality Manager", "Travel Consultant"
    ],
    "Language/Linguistics": ["Linguist", "Language Teacher"],

    "Transportation": [
        "Transportation Planner", "Aviation Engineer", "Logistics Analyst",
        "Roving Project Manager", "Project Manager - Mechanical + Electrical",
        "Territory Manager (Outdoor Sales Consultant)"
    ]
}

# Crear una nueva columna jobTitle_normalized
df['jobTitle_normalized'] = ""

# Realizar las transformaciones y mapeo a categorías
for cat, titles in category_mapping.items():
    matching_rows = df['header.jobTitle'].str.contains('|'.join(titles), case=False)
    df.loc[matching_rows, 'jobTitle_normalized'] = cat
    print(f"Registrados {matching_rows.sum()} títulos en la categoría {cat}")

# Asignar la categoría "Other" a los títulos no mapeados
other_rows = df['jobTitle_normalized'].isnull()
df.loc[other_rows, 'jobTitle_normalized'] = "Other"
print(f"Registrados {other_rows.sum()} títulos en la categoría Other")

# Conexión a la base de datos 
conn = psycopg2.connect(
    database = "ETL",
    user = "postgres",
    password = "colamer13",
    host = "localhost"
)

cursor = conn.cursor()

# Actualizar la columna jobTitle_normalized 
for index, row in df.iterrows():
    header_jobtitle = row['jobTitle_normalized']
    update_query = f"""
        UPDATE dataJobs_glassdoor
        SET jobTitle_normalized = %s
        WHERE id = %s
    """
    # Supongo que tu base de datos tiene una columna 'id'
    cursor.execute(update_query, (header_jobtitle, index + 1))
    print(f"Actualizada fila {index + 1} en la base de datos")

conn.commit()
cursor.close()
conn.close()