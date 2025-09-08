# Decision Support Information System Development Project

## Objective and Project Context

The objective of this project is to develop a **Decision Support Information System** (DSS) to monitor the activity of a healthcare facility.

This project is carried out within the framework of the **NF26 course** at the **Université de Technologie de Compiègne (UTC)** in collaboration with _Smart Teem_, a company specialized in designing and implementing tailor-made solutions in data and AI.  
Link to [_Smart Teem_](https://smartteem.com/)

_Smart Teem_ plays a central role in this project by defining the objectives and supporting the student teams in selecting tools and architectures.

The project is developed over **1 month by a team of 7 students**, with the pedagogical objective of becoming familiar with data processing and visualization technologies through the implementation of a complete decision-making pipeline.

The tools used are:
- **Snowflake** – cloud data warehouse  
- **Apache Airflow** – workflow orchestration and automation  
- **Power BI** – visualization and interactive dashboard creation  

⚠️ **Disclaimer:** This is a **demo version** of the project showcasing the work carried out by the team.  
It is not intended to be complete or fully functional, due to the restrictions and constraints applied in the academic context.

---

## Project Organization

The project is divided into 4 weekly work packages:

### Work Package 1 - Installation and Preparation
- Setting up the working environment and configuring the required tools  
- Developing a solution adapted to the needs defined by Smart Teem  
- Team organization discussions  

### Work Package 2 - Beginning of DSS Development
- Creating tables in Snowflake  
- Initial data loading into the database  

### Work Package 3 - Data Processing and Orchestration
- Cleaning and preparing the data  
- Defining and deploying DAGs with Airflow  

### Work Package 4 - Visualization and Reporting
- Developing an interactive dashboard in Power BI  
- Designing key performance indicators (KPIs) for activity monitoring  

The overall table structure and project organization are summarized below:  
[Pipeline_and_organisation](documents/pipeline_and_organisation.png)

---

## Working Environment

### 1. Prerequisites

List of software, tools, and versions required to run the project:  

- **Operating System:** Windows / macOS / Linux  
- **Python:** recommended version ≥ 3.9  
- **Snowflake:**  
  - `snowflake-connector-python==3.15.0`  
  - `snowflake-snowpark-python==1.25.0`  
- **Apache Airflow:**  
  - `apache-airflow==2.7.1`  
  - Providers: `apache-airflow-providers-common-sql`, `apache-airflow-providers-ftp`, `apache-airflow-providers-http`, `apache-airflow-providers-imap`, `apache-airflow-providers-sqlite`  
- **Power BI:** recent version  
- **Other Python libraries:**  
  - `rich` (compatible with Airflow & Flask-Limiter)  

---

### 2. Installation

It is recommended to create a **Python virtual environment** to isolate the project dependencies:  

```bash
# Create a virtual environment
python -m venv venv

# Activate the environment
source venv/bin/activate  # Linux / macOS
venv\Scripts\activate     # Windows

# Install dependencies via requirements.txt
pip install -r requirements.txt
```

The `requirements.txt` file contains all the libraries and dependencies needed for the project (Airflow, Snowflake, Rich, etc.).

### 3. Specific Configuration

#### Snowflake:
Configure the Snowflake account connection and create the necessary tables for data ingestion.  
An installation and configuration guide is available here: `documents/README_snowflake.md`

#### Apache Airflow:
Initialize the Airflow database and launch the DAGs:  
```bash
airflow db init
airflow scheduler
airflow webserver
```

#### Power BI
Connect the data sources (Snowflake or CSV/Excel files) to visualize the KPIs in the dashboard.  
Make sure you have a recent version of Power BI installed.

---

## Project Structure
This repository is organized into 3 main folders:  
- `documents` contains documentation related to project design choices, such as table organization  
- `python` contains the Python source code  
- `sql` contains the SQL source code executed via Python scripts  

### `sql` Folder
The `sql` folder contains scripts for:  
- Creating the project tables: `_create`  
- Dropping/resetting tables: `_reinitialisation/_reinit`  
- Inserting data into tables: `_stg_to_wrk` and `_wrk_to_soc` (`_insert` scripts)  
- Technical monitoring of database operations: `_tch`  
- SQL views for building the interactive dashboard: `_views`  

### `python` Folder
The `python` folder contains two subfolders: `logs` and `csv`. These only include **example logs and CSV data** generated from SQL views.  
All the data in these files is **fictitious** and only meant to illustrate the project workflow.  

It also contains all the Python source code required to correctly execute the SQL scripts, including:  

- **`connect.py`**: handles automatic connection to Snowflake when the `.env` file is present  
- **`launch_load_*.py`, `insert_*.py`, `install_*.py`, `load_*.py`**: execute SQL scripts to manage the database  
- **`dag_*.py`**: manage DAGs with Apache Airflow  
- **`log_config.py`**: configures a custom logger for the project  

All these files are orchestrated by **`main.py`**, which runs the complete pipeline when daily hospital data is provided in CSV format.  

> The folder containing daily hospital data is **not included in this repository** for confidentiality reasons.  

---

## Authors

- Nadia Guillaumot – nadia.guillaumot@etu.utc.fr – https://github.com/Nadiaglmt  
- Rim Moumni – rim.moumni@etu.utc.fr  
- Colin Manyri – colin.manyri@etu.utc.fr – https://github.com/KolinMTG  
- Daniel Treluyer – daniel.treluyer@etu.utc.fr  
- Lojaïn Rhafiri – lojain.rhafiri@etu.utc.fr  
- Dina Mouayed – dina.mouayed@etu.utc.fr  
- Estelle Pham – estelle.pham@etu.utc.fr  

