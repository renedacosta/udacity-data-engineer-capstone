# udacity-data-engineer-capstone
This Repository contains an implementation of the Capstone Project for Udacity Data Engineering Nanodegree
The immigration dataset along with the temperature dataset to develop analytics table to gain insight on immigration trends in the US based on city temperatures.
Using a postgres database the data is first ingested into staging tables and then normalized into a star schema for analytics.
A summary table is created for quick analytics

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Project structure
The project folder is structured as follows 
```
.
├── README.md
├── LICENSE
├── .gitignore
├── Capstone Project Template.ipynb <-- This jupyter notebook file contains an overview and write up of the project
├── main.py                   <-- This is the main python execution script 
├── commands                  <-- This folder contains shell commands for running airflow 
|    └── start.sh             <-- Shell script to start airflow
├── dags                      <-- This folder contains the dags to be run
├── data                      <-- This folder contains data files
├── helpers                   <-- This folder contains helper function pipelines with airflow 
|    └── stage_files.py       <-- This python script is used to stage the data into postres
├── images                    <-- This folder contains images used in the template file 
└── sql                       <-- This folder contains sql scripts that are fun for the ETL
```
## Installation
To run this project the following dependencies are needed:
- jupyter notebook
- psycopg2

```python
pip install -r requirements.txt
```

## Usage
```bash
python main
```

## Environment variables
- DBHOST: database host server
- DBUSER: database user
- DBPW: database user password
- DB: database name

## Project write up 
The full project write up is provided in [Capstone Project Template.ipynb](Capstone&#32;Project&#32;Template.ipynb)
