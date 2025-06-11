import pandas as pd
from connect import get_connection

# Connexion
conn = get_connection()
cursor = conn.cursor()

# Liste des vues à exporter
views = [
    "VW_AVG_AGE_BY_PATHOLOGY",
    "VW_TOP_MEDICATION_BY_PATHOLOGY",
    "VW_ROOMS_BY_PATHOLOGY",
    "VW_DOCTOR_SPECIALTY_BY_PATHOLOGY",
    "VW_PATIENTS_ONE_NIGHT",
    "VW_EMPTY_ROOMS"
]

# Base cible
database = "BASE_SOCLE"
schema = "PUBLIC"

for view in views:
    query = f"SELECT * FROM {database}.{schema}.{view}"
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(data, columns=columns)
    df.to_csv(f"{view.lower()}.csv", index=False)
    print(f"✅ Exporté : {view.lower()}.csv")

cursor.close()
conn.close()
