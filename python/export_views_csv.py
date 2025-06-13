# export_views.py
import os
from pathlib import Path
import pandas as pd
from connect import get_connection

def export_views_to_csv(
    views: list[str],
    database: str,
    schema: str,
    output_dir: str = "csv"
) -> None:
    """
    Exporte chaque vue de la liste `views` depuis la base <database>.<schema>
    vers un fichier CSV dans le dossier `output_dir`.
    """
    # Assure-toi que le dossier existe
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Connexion
    conn = get_connection()
    cursor = conn.cursor()

    for view in views:
        query = f"SELECT * FROM {database}.{schema}.{view}"
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(data, columns=columns)
        file_path = output_path / f"{view.lower()}.csv"
        df.to_csv(file_path, index=False)
        print(f"✅ Exporté : {file_path}")

    cursor.close()
    conn.close()
