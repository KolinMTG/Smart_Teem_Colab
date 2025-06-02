"""Permet de générer des fichier SQL automatiquement pour les traitements de données simples à partir d'un fichier Excel de mapping."""

import pandas as pd
import os

def lire_mapping_excel(chemin_fichier:str, index_feuille:int=0) -> pd.DataFrame:
    # Lire avec deux lignes d'en-tête
    df = pd.read_excel(chemin_fichier, header=[0,1], sheet_name=index_feuille)
    # Pour plus de lisibilité, on peut aplatir les noms en concaténant les deux niveaux
    print(df)
    df.columns = ['_'.join([str(i) for i in col if str(i) != 'nan']).strip() for col in df.columns]
    return df

def filtrer_et_nettoyer(df):
    # Filtrer les lignes où la colonne "CIBLE_Règle de gestion" contient "Alimentation directe"
    # Si ta colonne s'appelle 'CIBLE_Règle de gestion' ou 'TARGET_Règle de gestion', adapte ici :
    col_regle = 'CIBLE_Règle de gestion'  # ou 'TARGET_Règle de gestion'

    # Filtrer les lignes
    df_filtre = df[df[col_regle] == "Alimentation directe"]

    # Garder uniquement les colonnes voulues
    colonnes_a_garder = [
        'SOURCE_TABLE',
        'SOURCE_COLONNE',
        'CIBLE_TABLE',
        'CIBLE_COLONNE'
    ]

    # Si certaines colonnes sont absentes, on les ignore pour éviter erreur
    colonnes_existantes = [col for col in colonnes_a_garder if col in df_filtre.columns]
    df_result = df_filtre[colonnes_existantes]
    return df_result

def generer_sql(df:pd.DataFrame, dir_path:str, db_source:str = "BASE_STAGING", db_cible="BASE_WORK")->None:
    """Etant donné un directory, génère différents ficheir nommés _insert_{nom_table.lower()}.sql
    qui contient les insertions SLQ pour chaques tables du mapping présent
    format du df attendu:
    SOURCE_TABLE, SOURCE_COLONNE, CIBLE_TABLE, CIBLE_COLONNE
    Attention cette fonction n'effectue que des ajouts automatiques
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    for table in df['CIBLE_TABLE'].unique():
        # creer un fichier _insert_{nom_table.lower()}.sql
        df_table = df[df['CIBLE_TABLE'] == table]
        nom_fichier = f"{dir_path}/_insert_{table.lower()}.sql"
        with open(nom_fichier, 'w') as f:
            requete = f"INSERT INTO {db_cible}.{table} (\n"

            # On récupère les colonnes cibles pour la table
            colonnes_cibles = df_table['CIBLE_COLONNE'].tolist()
            requete += '    ' + ',\n    '.join(colonnes_cibles) + "\n)\n"

            # Partie SELECT
            requete += "SELECT\n"
            # On récupère les colonnes sources pour la table
            colonnes_sources = df_table['SOURCE_COLONNE'].tolist()
            select_lines = [
                f"    {colonnes_sources[i]} AS {colonnes_cibles[i]}"
                for i in range(len(colonnes_sources))
            ]
            requete += ',\n'.join(select_lines) + "\n"
            # On ajoute la table source
            requete += f"FROM {db_source}.{df_table['SOURCE_TABLE'].iloc[0]};\n"

            f.write(requete)
            print(f"Requête SQL générée pour la table {table} dans le fichier {nom_fichier}")
            


if __name__ == "__main__":
    df = lire_mapping_excel(r"C:\Users\colin\Documents\ETUDE\UTC semestre 4\NF26\Smart-Teem\nf26_smart_teem\Data Hospital\Hopital Mapping VF.xlsx", index_feuille=1)
    df = filtrer_et_nettoyer(df)
    generer_sql(df, r"C:\Users\colin\Documents\ETUDE\UTC semestre 4\NF26\Smart-Teem\nf26_smart_teem\sql\_stg_to_wrk")
