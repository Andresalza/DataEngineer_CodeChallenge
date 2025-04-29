import azure.functions as func
import logging
import pyodbc
import pandas as pd
import os
import json


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
@app.function_name(name='hired_employees_summary')
@app.route(route="hired_employees_summary")
def hired_employees_summary(req: func.HttpRequest) -> func.HttpResponse:

    #SQL connections
    server_name = os.getenv("server_name")
    database_name = os.getenv("database_name")
    user_id = os.getenv("user_id")
    password = os.getenv("password")
    driver = '{ODBC Driver 18 for SQL Server}' 
    connection_string = "Driver={};Server=tcp:{}.database.windows.net,1433;Database={};Uid={};Pwd={};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;".format(driver,server_name,database_name,user_id,password)

    #Conection to SQL
    cnx=pyodbc.connect(connection_string)
    cursor=cnx.cursor()

    select_query = """
      WITH temp1 AS(
            SELECT   he.[id]
                    ,he.[name]
                    ,CONVERT(datetime, REPLACE(he.datetime,'z',''), 126) AS 'datetime'
                    ,he.[department_id]
                    ,he.[job_id]
                    ,de.[department]
                    ,jo.[job]
            FROM [dbo].[hired_employees] AS he 
            INNER JOIN [dbo].[departments] AS de 
            ON he.department_id = de.id
            INNER JOIN [dbo].[jobs] AS jo 
            ON he.job_id = jo.id
            WHERE he.[datetime] IS NOT NULL
        ) 
        SELECT
            department,
            job,
            SUM(CASE WHEN MONTH(datetime) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN MONTH(datetime) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN MONTH(datetime) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN MONTH(datetime) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS Q4
        FROM
        Temp1
        WHERE
            YEAR(datetime) = 2021
        GROUP BY
            department,job
        ORDER BY
            department DESC,job DESC;
    """
    df_results = pd.read_sql(select_query,cnx)

    cnx.close()

    df_results_json = df_results.to_json(orient='records')

    return func.HttpResponse(df_results_json,mimetype="application/json", status_code=200)

@app.function_name(name='above_avg_hired_empl')
@app.route(route="above_avg_hired_empl")
def above_avg_hired_empl(req: func.HttpRequest) -> func.HttpResponse:

    #SQL connections
    server_name = os.getenv("server_name")
    database_name = os.getenv("database_name")
    user_id = os.getenv("user_id")
    password = os.getenv("password")
    driver = '{ODBC Driver 18 for SQL Server}' 
    connection_string = "Driver={};Server=tcp:{}.database.windows.net,1433;Database={};Uid={};Pwd={};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;".format(driver,server_name,database_name,user_id,password)

    #Conection to SQL
    cnx=pyodbc.connect(connection_string)
    cursor=cnx.cursor()

    select_query = """
      --Get all employee counts by department for 2021
        WITH dept_hire_counts AS (
            SELECT 
                d.id,
                d.department,
                COUNT(
                    CASE 
                        WHEN e.id IS NOT NULL 
                        AND e.datetime IS NOT NULL 
                        AND YEAR(CONVERT(datetime, REPLACE(e.datetime, 'Z', ''), 126)) = 2021 
                        THEN 1 
                        ELSE NULL 
                    END
                ) AS hired
            FROM 
                departments AS d
            LEFT JOIN 
                hired_employees AS e 
                ON d.id = e.department_id
            GROUP BY 
                d.id, d.department
        ),

        -- Calculate the mean hiring count across all departments for 2021
        avg_hire_count AS (
            SELECT 
                AVG(CAST(hired AS FLOAT)) AS mean_hired
            FROM 
                dept_hire_counts
        )
        -- Get departments that hired more than the mean
        SELECT 
            dhc.id,
            dhc.department,
            dhc.hired
        FROM 
            dept_hire_counts AS dhc
        CROSS JOIN 
            avg_hire_count AS ahc
        WHERE 
            dhc.hired > ahc.mean_hired
        ORDER BY 
            dhc.hired DESC;
    """
    df_results = pd.read_sql(select_query,cnx)

    cnx.close()

    df_results_json = df_results.to_json(orient='records')

    return func.HttpResponse(df_results_json,mimetype="application/json", status_code=200)

@app.function_name(name='combined_tables_creation')
@app.route(route="combined_tables_creation")
def combined_tables_creation(req: func.HttpRequest) -> func.HttpResponse:

    #SQL connections
    server_name = os.getenv("server_name")
    database_name = os.getenv("database_name")
    user_id = os.getenv("user_id")
    password = os.getenv("password")
    driver = '{ODBC Driver 18 for SQL Server}' 
    connection_string = "Driver={};Server=tcp:{}.database.windows.net,1433;Database={};Uid={};Pwd={};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;".format(driver,server_name,database_name,user_id,password)

    #Conection to SQL
    cnx=pyodbc.connect(connection_string)
    cursor=cnx.cursor()
    cursor.execute("TRUNCATE TABLE dbo.avg_challenge")
    cnx.commit()
    cursor.execute("TRUNCATE TABLE dbo.summary_challenge")
    cnx.commit()
    cursor.execute("""INSERT INTO dbo.avg_challenge (id,department,hired) 
                   
                        WITH dept_hire_counts AS (
                            SELECT 
                                d.id,
                                d.department,
                                COUNT(
                                    CASE 
                                        WHEN e.id IS NOT NULL 
                                        AND e.datetime IS NOT NULL 
                                        AND YEAR(CONVERT(datetime, REPLACE(e.datetime, 'Z', ''), 126)) = 2021 
                                        THEN 1 
                                        ELSE NULL 
                                    END
                                ) AS hired
                            FROM 
                                departments AS d
                            LEFT JOIN 
                                hired_employees AS e 
                                ON d.id = e.department_id
                            GROUP BY 
                                d.id, d.department
                        ),
                        avg_hire_count AS (
                            SELECT 
                                AVG(CAST(hired AS FLOAT)) AS mean_hired
                            FROM 
                                dept_hire_counts
                        )
                        SELECT 
                            dhc.id,
                            dhc.department,
                            dhc.hired
                        FROM 
                            dept_hire_counts AS dhc
                        CROSS JOIN 
                            avg_hire_count AS ahc
                        WHERE 
                            dhc.hired > ahc.mean_hired
                        ORDER BY 
                            dhc.hired DESC;
                   
                """)
    cnx.commit()
    cursor.execute("""INSERT INTO dbo.summary_challenge (department,job,Q1,Q2,Q3,Q4) 
                   
                        WITH temp1 AS(
                            SELECT   he.[id]
                                    ,he.[name]
                                    ,CONVERT(datetime, REPLACE(he.datetime,'z',''), 126) AS 'datetime'
                                    ,he.[department_id]
                                    ,he.[job_id]
                                    ,de.[department]
                                    ,jo.[job]
                            FROM [dbo].[hired_employees] AS he 
                            INNER JOIN [dbo].[departments] AS de 
                            ON he.department_id = de.id
                            INNER JOIN [dbo].[jobs] AS jo 
                            ON he.job_id = jo.id
                            WHERE he.[datetime] IS NOT NULL
                        ) 
                        SELECT
                            department,
                            job,
                            SUM(CASE WHEN MONTH(datetime) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Q1,
                            SUM(CASE WHEN MONTH(datetime) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS Q2,
                            SUM(CASE WHEN MONTH(datetime) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS Q3,
                            SUM(CASE WHEN MONTH(datetime) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS Q4
                        FROM
                        Temp1
                        WHERE
                            YEAR(datetime) = 2021
                        GROUP BY
                            department,job
                        ORDER BY
                            department DESC,job DESC;
                   
                """)
    cnx.commit()

    cnx.close()
    
    success_message = {
    "status": "success",
    "message": "Query executed successfully",
    "details": "tables summary_challenge and avg_challenge are now updated"
    }
    
    json_response = json.dumps(success_message)


    return func.HttpResponse(json_response,mimetype="application/json", status_code=200)