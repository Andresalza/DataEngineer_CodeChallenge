from fastapi import FastAPI
import pandas as pd

app = FastAPI()


departments_columns = ["id", "department"]
jobs_columns = ["id", "job"]
hired_employees_columns = ["id", "name", "datetime", "department_id", "job_id"]


departments_df = pd.read_csv('data/departments.csv', header=None, names=departments_columns)
jobs_df = pd.read_csv('data/jobs.csv', header=None, names=jobs_columns)
hired_employees_df = pd.read_csv('data/hired_employees.csv', header=None, names=hired_employees_columns)


@app.get("/departments")
def get_departments():
    departments = departments_df.to_dict(orient="records")
    return {"departments": departments}