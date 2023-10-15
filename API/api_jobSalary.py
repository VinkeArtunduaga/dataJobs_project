import requests
import pandas as pd

# Lista de títulos de trabajo que deseas consultar
job_titles = [".net software engineer", "business data analyst",
"agile project manager", "business analyst h/f",
"analyst", ".net developer",
"area manager", "senior clinical research associate",
"area sales manager", "data analyst (m/w/d)",
"associate consultant", "technical manager",
"associate product manager", "production manager",
"associate software engineer", "agile business analyst",
"business analyst", "software support engineer",
"backend engineer", "network engineer",
"brand manager", "senior qa engineer",
"business analyst (m/w/d)", "principal software engineer",
"business consultant", "product manager (m/w/d)",
"business development analyst", "data analist",
"business development executive", "senior data engineer",
"business intelligence analyst", "software development engineer",
"business intelligence engineer", "senior java software engineer",
"business manager", "senior machine learning engineer",
"business systems analyst", "customer success manager",
"c++ software engineer", "finance analyst",
"clinical research associate", "qa automation engineer",
"cloud engineer", ".net developer",
"computer vision engineer", "data scientist",
"construction project manager", "security engineer",
"customer project manager", "senior big data engineer",
"data engineer", "pricing analyst",
"data scientist", "manager",
"data analyst h/f", "test analyst",
"data analyst", "data engineer",
"data architect", "assistant project manager",
"data engineer (m/w/d)", "junior product manager",
"data scientist h/f", "test automation engineer",
"database administrator", "technical project manager",
"database engineer", "senior consultant",
"datenbankadministrator (m/w/d)", "business operations analyst",
"delivery manager", "database developer",
"development manager", "bi analyst",
"devops engineer", "application developer",
"etl developer", "scientist",
"engineer", "technical account manager",
"engineering manager", "junior software engineer",
"field service engineer", "business process analyst",
"finance manager", "data quality analyst",
"full stack engineer", "construction manager",
"it business analyst", "marketing manager",
"infrastructure engineer", "project director",
"java engineer", "software engineer in test",
"java software engineer", "system engineer",
"junior business analyst", "product manager",
"junior data analyst", "functional analyst",
"junior data scientist", "frontend engineer",
"junior project manager", "senior developer",
"junior research fellow", "senior database administrator",
"key account manager", "software test engineer",
"lead data scientist", "application engineer",
"lead engineer", "assistant manager",
"machine learning engineer", "account manager",
"marketing data analyst", "product designer",
"mechanical engineer", "mobile developer",
"mobile application developer", "it manager",
"operations manager", "senior software developer",
"oracle database administrator", "manufacturing engineer",
"project manager", "program manager",
"product analyst", "application support engineer",
"product development manager", "digital marketing manager",
"product engineer", "full-stack software engineer",
"product manager", "data scientist",
"product marketing manager", "qa engineer",
"product owner", "it support engineer",
"product owner h/f", "assistant professor",
"product owner", "software developer",
"product specialist", "technical support engineer",
"produktmanager (m/w/d)", "product manager",
"project engineer", "big data engineer",
"project manager", "project manager",
"electrical engineer", "product manager",
"projectleider", "integration engineer",
"python developer", "lead software engineer",
"quality analyst", "senior it project manager",
"quality assurance engineer", "full stack software engineer",
"quality engineer", "junior software developer",
"quantitative analyst", "business system analyst",
"regional sales manager", "country manager",
"research analyst", "pre-sales engineer",
"research associate", "project coordinator",
"research scientist", "data scientist",
"software engineer", "software qa engineer",
"sql developer", "project manager",
"sales engineer", "digital project manager",
"senior account manager", "systems analyst",
"senior business analyst", "it project manager",
"senior data analyst", "financial analyst",
"senior data scientist", "senior product manager",
"senior database engineer", "data scientist",
"senior devops engineer", "projectmanager",
"senior electrical engineer", "sr software engineer",
"senior financial analyst", "associate project manager",
"senior program manager", "actuarial analyst",
"senior project manager", "business development manager",
"senior research fellow", "senior analyst",
"senior software engineer", "devops engineer",
"senior systems engineer", "software engineering manager",
"service delivery manager", "service engineer",
"site reliability engineer", "technical product manager",
"software development engineer in test", "data analyst",
"software development manager", "senior project engineer",
"software engineer (java)", "it engineer",
"software engineer - java", "principal data scientist",
"software engineer", "research manager",
"software engineer intern", "quality manager",
"software engineer java", "software engineer",
"backend software engineer", "solutions engineer",
"senior project manager", "m/w/d",
"sr. product manager", "project engineer",
"sr. software engineer", "sr. software engineer",
"senior java engineer", "staff software engineer",
"lead data engineer", "statistician",
"embedded software engineer", "structural engineer",
"operations analyst", "support engineer",
"sales manager", "system analyst",
"process engineer", "systems"]

# URL y cabeceras
url = "https://job-salary-data.p.rapidapi.com/job-salary"
headers = {
    "X-RapidAPI-Key": "fad835f1d6mshd94834117030601p1fa5afjsn90f78286b6aa",
    "X-RapidAPI-Host": "job-salary-data.p.rapidapi.com"
}

# Crear una lista para almacenar los DataFrames de cada consulta
data_frames = []

# Iterar a través de los títulos de trabajo
for job_title in job_titles:
    querystring = {"job_title": job_title, "location": "usa"}
    
    response = requests.get(url, headers=headers, params=querystring)

    # Comprueba si la solicitud fue exitosa (código de respuesta 200)
    if response.status_code == 200:
        # Convierte la respuesta en un DataFrame de Pandas
        data = response.json()
        df = pd.DataFrame(data['data'])
        
        # Agrega el DataFrame a la lista
        data_frames.append(df)
    else:
        print(f"La solicitud para el trabajo {job_title} no se completó correctamente. Código de respuesta:", response.status_code)

# Concatena todos los DataFrames en uno solo
final_df = pd.concat(data_frames)

# Ejemplo de visualización de las primeras filas
print("Primeras 5 filas del DataFrame:")
print(final_df.head())

final_df.to_csv('job_salaries.csv', index=False)
print("Los datos se han guardado en 'combined_jobs_extended.csv'")


