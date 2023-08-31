import pandas as pd

df = pd.read_csv("C:/Users/kevin/ETL/Postgres/kaggle/dataJobs_glassdoor.csv", delimiter=',')

output_filename = 'dataJobs_all.csv'
df.to_csv(output_filename, index=False)

print(f"CSV guardado como '{output_filename}' exitosamente.")
