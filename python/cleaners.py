# cleaners.py
import pandas as pd


def explore_table(df: pd.DataFrame, table_name: str, threshold_outlier: dict = None):
    print(f"\n=== 🧹 Exploration de la table: {table_name} ===")

    # 1️⃣ Taille de la table
    print(f"- Nombre de lignes: {df.shape[0]}, Nombre de colonnes: {df.shape[1]}")

    # 2️⃣ Valeurs manquantes
    print("\n--- Valeurs manquantes par colonne ---")
    missing = df.isnull().sum()
    missing = missing[missing > 0]
    if missing.empty:
        print("Aucune valeur manquante détectée.")
    else:
        print(missing)

    # 3️⃣ Lignes dupliquées
    nb_duplicates = df.duplicated().sum()
    print(f"\n--- Lignes dupliquées ---\nNombre de lignes dupliquées: {nb_duplicates}")

    # 4️⃣ Types de colonnes
    print("\n--- Types de colonnes ---")
    print(df.dtypes)

    # 5️⃣ Valeurs aberrantes
    if threshold_outlier:
        print("\n--- Recherche de valeurs aberrantes ---")
        for col, (low, high) in threshold_outlier.items():
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                nb_low = (df[col] < low).sum()
                nb_high = (df[col] > high).sum()
                print(f"  {col}: {nb_low} en dessous de {low}, {nb_high} au dessus de {high}")
            else:
                print(f"⚠️  Colonne {col} non numérique ou absente — skip.")

    print(f"=== ✅ Fin de l’exploration de {table_name} ===\n")


# ➜ fonctions avancées réutilisables si besoin

def detect_outliers(df: pd.DataFrame, col: str, low: float, high: float):
    """Retourne les valeurs outliers d'une colonne numérique."""
    if col not in df.columns:
        raise ValueError(f"Colonne {col} non trouvée dans le DataFrame.")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Colonne {col} n'est pas numérique.")

    outliers = df[(df[col] < low) | (df[col] > high)]
    return outliers


def detect_missing(df: pd.DataFrame):
    """Retourne les colonnes contenant des valeurs manquantes."""
    missing = df.isnull().sum()
    return missing[missing > 0]


def detect_duplicates(df: pd.DataFrame):
    """Retourne les lignes dupliquées."""
    return df[df.duplicated(keep=False)]
