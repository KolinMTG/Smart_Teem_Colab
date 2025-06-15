
# Projet SID Snowflake

## Objectif  
Ce projet installe et configure la base SID sur Snowflake. Il contient les scripts SQL et Python pour créer les bases et tables, ainsi que le script d’installation automatisé.

## Prérequis  
- Python 3.8 ou plus  
- Accès à un compte Snowflake avec les droits nécessaires  
- Un environnement virtuel Python recommandé

## Configuration initiale Snowflake (à exécuter une seule fois)

Avant de lancer le script Python d’installation, il faut créer le rôle, la base, le warehouse et les permissions avec ces commandes SQL dans Snowflake (via Snowflake Web dans un WorkSheet ou SnowSQL) :

```sql
CREATE ROLE IF NOT EXISTS ROLE_PROJECT_SID;
GRANT ROLE ROLE_PROJECT_SID TO USER YOURUSERNAME;
CREATE DATABASE IF NOT EXISTS BASE_SID;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ROLE_PROJECT_SID;
GRANT ALL PRIVILEGES ON DATABASE BASE_SID TO ROLE ROLE_PROJECT_SID;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE ROLE_PROJECT_SID;
```

## Installation

1. Cloner le dépôt  
2. Créer un fichier `.env` à la racine du projet avec les paramètres suivants :  
```

SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=BASE_SID

````

3. Installer les dépendances Python  
```bash
pip install -r requirements.txt
````

## Utilisation

Lancer le script d’installation pour créer les bases et tables sur Snowflake :

```bash
python install_sid.py
```

---

**Important** :

* Ne jamais commiter le fichier `.env` contenant des informations sensibles.
* Vérifier que le rôle Snowflake utilisé a bien les droits pour créer les objets nécessaires.


## Aiflow

La version de Airflow utilisée pour le développement des DAGs est 2.11.0. Les fichiers des DAGs se trouvent dans le dossier python du répertoire du projet. Il est donc nécessaire de modifier le fichier de configuration de Airflow pour indiquer leur emplacement. Dans le fichier `airflow/airflow.cfg`, on doit avoir :
```
dags_folder = <chemin absolu vers le répertoire du projet>/python
```

Pour lancer Airflow, exécuter les commandes suivante dans deux terminaux différents :
```
export AIRFLOW_HOME=${PWD}/airflow
airflow scheduler
```
et
```
export AIRFLOW_HOME=${PWD}/airflow
airflow webserver
```

L’interface web de Airflow est alors accessible à l’URL localhost:8080.
