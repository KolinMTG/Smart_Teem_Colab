# Projet de développement d'un système d'information décisionnel

## Objectif et contexte du projet

Ce projet a pour objectif le développement d’un **Système d’Information Décisionnel** (SID) permettant de suivre l’activité d’un établissement de santé.

Il s’inscrit dans le cadre de l’UV NF26 de l’Université de Technologie de Compiègne (UTC) et est réalisé en collaboration avec _Smart Teem_, entreprise spécialisée dans la conception et la mise en œuvre de solutions sur mesure en data et IA.
Lien vers [_Smart Teem_](https://smartteem.com/)


_Smart Teem_ joue un rôle central dans ce projet en définissant les objectifs du projet et en accompagnant les différentes équipes d'étudiant dans le choix des outils et des architectures.


Le projet est réalisé en **1 mois par une équipe de 7 étudiants**, avec pour objectif pédagogique de se familiariser avec les technologies de traitement et de visualisation de données à travers la mise en place d’une chaîne décisionnelle complète.

Les outils utilisés sont :
- **Snowflake** – entrepôt de données cloud
- **Apache Airflow** – orchestration et automatisation des workflows
- **Power BI** – visualisation et création de tableaux de bord interactifs

⚠️ **Disclaimer :** Il s’agit d’une **version démo** du projet qui présente le travail réalisé par l’équipe.
Cette version n’a pas vocation à être complète ou pleinement fonctionnelle, en raison des différentes restrictions et contraintes appliquées dans le cadre du projet académique.

## Organisation du projet

Le projet est découpé en 4 lots d'une semaine chacuns : 

### Lot 1 - Installation et prépation
- Mise en place de l’environnement de travail et configuration des différents outils imposés
- Développement d’une solution adaptée aux besoins définis par Smart Teem
- Réflexions sur l'organisation du groupe

### Lot 2 - Début du développement du SID
- Création des tables dans Snowflake
- Alimentation initiale de la base de données

### Lot 3 - Traitement et orchestration
- Nettoyage et préparation des données
- Définition et déploiement des DAGs sous AIrflow

### Lot 4 - Visualisation et reporting
- Développement d’un tableau de bord interactif dans Power BI
- Mise en forme des indicateurs clés pour le suivi d’activité

La structure globale des tables et l'organisation du projet est résumé ci-dessous
[Pipeline_and_organisation](documents/pipeline_and_organisation.png)


## Environnement de travail

### 1. Prérequis

Liste des logiciels, outils et versions nécessaires pour exécuter le projet :  

- **Système d’exploitation :** Windows / macOS / Linux  
- **Python :** version recommandée ≥ 3.9  
- **Snowflake :**  
  - `snowflake-connector-python==3.15.0`  
  - `snowflake-snowpark-python==1.25.0`  
- **Apache Airflow :**  
  - `apache-airflow==2.7.1`  
  - Providers : `apache-airflow-providers-common-sql`, `apache-airflow-providers-ftp`, `apache-airflow-providers-http`, `apache-airflow-providers-imap`, `apache-airflow-providers-sqlite`  
- **Power BI :** version récente  
- **Autres librairies Python :**  
  - `rich` (version compatible avec Airflow & Flask-Limiter)  

---

### 2. Installation

Il est recommandé de créer un **environnement virtuel Python** pour isoler les dépendances du projet :  

```bash
# Créer un environnement virtuel
python -m venv venv

# Activer l'environnement
source venv/bin/activate  # Linux / macOS
venv\Scripts\activate     # Windows

# Installer les dépendances via le fichier requirements.txt
pip install -r requirements.txt
```
Le fichier `requirements.txt` contient toutes les librairies et dépendances nécessaires pour le projet (Airflow, Snowflake, Rich, etc.)

### 3. Configuration spécifique

#### Snowflake :
Configurer la connexion au compte Snowflake et créer les tables nécessaires pour l’ingestion des données.
Un guide d'installation et de configuration est disponible : `documents/README_snowflake.md`

#### Apache Airflow :
Initialiser la base Airflow et lancer les DAGs :
```bash
airflow db init
airflow scheduler
airflow webserver
```
#### Power BI
Connecter les sources de données (Snowflake ou fichiers CSV/Excel) pour visualiser les indicateurs du tableau de bord.
Assurez-vous d'avoir une version récente de Power BI.

## Structure du projet
Ce repository s'organise en 3 documents principaux : 
- `documents` contient les différentes éléments de documentation concernant les choix pour la structure du projet, par exemple l'oganisation des différentes tables
- `python` contient les différents codes sources python
- `sql` contient les différents codes sources sql qui sont exectué via les codes pythons

### Dossier `sql` 
Le dossier `sql` est contient les codes qui permettent : 
- La créations des différentes tables du projet : `_create`
- La destruction des tables, renseigné dans le dossier `_reinitialisation` sous le nom `_reinit`
- L'insertion d'information dans les tables via les dossier `_stg_to_wrk` et `_wrk_to_soc` sous le nom `_insert`
- Le suivit technique des différentes manipulation de la base de donnée dans le dossier `_tch`
- Les calculs via des vue sql permetant la création d'un tableau de bord interactif dans le dossier `_views`

### Dossier `python`

Le dossier `python` contient deux sous-dossiers : `logs` et `csv`. Ces sous-dossiers présentent uniquement des exemples de logs et de données générés au format CSV à partir des vues SQL. Toutes les informations présentes dans ces fichiers sont fictives et n’ont d’intérêt que pour illustrer le fonctionnement du projet.

Le dossier `python` contient également tous les codes sources nécessaires à l’exécution correcte des scripts SQL présentés précédemment, en particulier :  

- **`connect.py`** : gère la connexion automatique à Snowflake dès que le fichier `.env` est présent.  
- **`launch_load_*.py`, `insert_*.py`, `install_*.py` et `load_*.py`** : exécutent des scripts SQL pour gérer la base de données.  
- **`dag_*.py`** : gèrent les DAGs via Apache Airflow.  
- **`log_config.py`** : configure un logger adapté au projet.  

L’ensemble de ces fichiers est utilisé dans **`main.py`**, qui permet d’exécuter l’intégralité de la pipeline lorsque des données journalières sont fournies au format CSV (données de l’hôpital).  

> Le dossier contenant les données journalières n’est pas fourni dans ce repository pour des raisons évidentes de confidentialité.




## Auteurs

- Nadia Guillaumot – nadia.guillaumot@etu.utc.fr - https://github.com/Nadiaglmt
- Rim Moumni – rim.moumni@etu.utc.fr - 
- Colin Manyri – colin.manyri@etu.utc.fr - https://github.com/KolinMTG
- Daniel Treluyer – daniel.treluyer@etu.utc.fr
- Lojaïn Rhafiri – lojain.rhafiri@etu.utc.fr
- Dina Mouayed – dina.mouayed@etu.utc.fr
- Estelle Pham – estelle.pham@etu.utc.fr
