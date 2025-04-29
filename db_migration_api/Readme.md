# Data Engineering Challenge – API and Azure SQL Integration

## Overview

This repository contains the solution to a technical challenge for a Data Engineering position at Globant. The objective is to demonstrate the ability to:

- Build an API that ingests data from CSV files into a SQL Server database in batches (1,000 rows per batch).
- Create an Azure Function with HTTP-triggered endpoints that query the ingested data and return JSON responses.

## Technologies Used

| Component         | Technology                         |
|-------------------|-------------------------------------|
| Language          | Python                              |
| API Framework     | FastAPI                             |
| Cloud Platform    | Microsoft Azure                     |
| Database          | Azure SQL Server                    |
| Cloud Service     | Azure Functions                     |
| Libraries Used    | pandas, pyodbc, dotenv, os, json, logging, azure.functions |

## Repository Structure
```bash
.
├── db_migration_api/                # API to read CSVs and ingest data
    │   ├── app/
        │   ├── main.py                   # Main logic for ingestion
        │   ├── .env                      # Environment variables (not included)
        │   ├── requirements.txt
        │   └── data/                     # Input CSV files (included)
            │   ├── departments.csv
            │   ├── hired_employees.csv
            │   └── jobs.csv
        │   ├── Readme.md                 #This Fie
        │   └── Query.txt                 # Querys used in the develop

├── AZ_function_CodeChallenge/   # Azure Function app
│   ├── function_app.py          # HTTP Triggered function
│   ├── requirements.txt
│   ├── .venv                    # Environment variables (not included)
│   └── ...
├── 
├── README.md                    # This file
└── .gitignore
```

## Run Locally
Python 3.13 for the ingestion API.

Python 3.11 for the Azure Function (required for Azure SDK compatibility).

pip package manager installed.

## CSV Files
Three input files (.csv) are included in the /data directory and used by the ingestion process to populate SQL Server tables

## API Logic
Loads CSVs using pandas

Inserts data into SQL Server using pyodbc

Uses batching (1,000 rows per insert)

Includes basic try/except error handling

## Azure Function Response
It has three endpoints triggered via HTTP:
* Query 1 for the summary_challenge - Returns a Json response with a subset of data
* Query 2 for the avg_challenge - Returns a Json response with a subset of data
* COmbined Query that - Returns a Json response with a message of an action being successfully made

## Notes
Azure credentials are not shared in this repo.

The SQL Server is hosted privately on Azure and cannot be accessed externally.

The .env file is excluded via .gitignore and should be created manually.

Basic error handling is included in the batch logic. No additional testing or advanced logging is implemented.


## Future Improvements
* Add unit tests for both ingestion and query logic
* Implement pagination or filters for the query endpoint
