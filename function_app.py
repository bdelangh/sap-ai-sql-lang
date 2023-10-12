import azure.functions as func
import logging
import os
import openai
import json
import urllib
import pyodbc

from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.sql_database import SQLDatabase
from langchain.llms import AzureOpenAI

# SQL info
sql_server     = os.environ['SQL_URL']
sql_database   = os.environ['SQL_DB']
sql_username   = os.environ['SQL_USER']
sql_pwd        = os.environ['SQL_PASS']
sql_driver     = 'ODBC Driver 17 for SQL Server'

#Log SQL DB Info
def logDatabaseEnvironment():
    logging.info("Logging Database Environment")
    logging.info(f"SQL Server   = {sql_server}")
    logging.info(f"SQL Database = {sql_database}")
    logging.info(f"SQL Username/Password = {sql_username} {sql_pwd}")
    logging.info(f"SQL Driver = {sql_driver}")

#Log OpenAi Info
def logOpenAIEnvironment():
    logging.info("Logging OpenAI Environment")
    openAI_APIType    = os.getenv("OPENAI_API_TYPE")
    openAI_APIBase    = os.getenv("OPENAI_API_BASE")
    openAI_APIKey     = os.getenv("OPENAI_API_KEY")
    openAI_APIVersion = os.getenv("OPENAI_API_VERSION")
    logging.info(f"OpenAI_API_TYPE = {openAI_APIType}")
    logging.info(f"OpenAI_API_BASE = {openAI_APIBase}")
    logging.info(f"OpenAI_API_KEY = {openAI_APIKey}")
    logging.info(f"OpenAI_API_VERSION = {openAI_APIVersion}")

app = func.FunctionApp()

# Retrieve the prompt and return the SQL result
@app.function_name(name="ProcessSQLPrompt")
@app.route(route="sqlprompt", auth_level=func.AuthLevel.ANONYMOUS)
def processPrompt(req: func.HttpRequest) -> func.HttpResponse:
     logging.info('Python HTTP trigger function processed a request.')

     prompt = req.params.get('prompt')
     if not prompt:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            prompt = req_body.get('prompt')

     if prompt:
        result = generateSQL(prompt)
        logging.info('Returning the result')
        return func.HttpResponse(result, status_code=200)
     else:
        return func.HttpResponse(
             "Pass a prompt in the query string or in the request body for the correct result.",
             status_code=200
        )


# Get information about your data, and use it translate natural language to SQL code with OpenAI to then execute it on your data
def generateSQL(query):
    
    # Connect to your database using ODBC
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + sql_server +';DATABASE=' + sql_database + ';UID=' + sql_username +';PWD=' + sql_pwd + ';')

    try:
        # Execute the query to retrieve the column information
        with conn.cursor() as cursor:
            sql = "SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS"
            cursor.execute(sql)
            result_set = cursor.fetchall()

            # Extract the column names from the cursor description
            column_names = [column[0] for column in cursor.description]

            # Extract the column names from each row and convert to dictionary
            result_list = [dict(zip(column_names, row)) for row in result_set]

        # Format the result set as a JSON string
        result_set_json = json.dumps(result_list)

        # Define the OpenAI prompt
        prompt = f"# Here are the columns in the database:\n# {result_set_json}\n### Generate a single T-SQL query for the following question using the information about the database: {query}\n\nSELECT"
        logging.info(f"prompt : {prompt}")

        # Setting API Key and API endpoint for OpenAI
        openai.api_type    = os.environ["OPENAI_API_TYPE"]
        openai.api_base    = os.environ["OPENAI_API_BASE"]
        openai.api_version = os.environ["OPENAI_API_VERSION"]
        openai.api_key     = os.environ["OPENAI_API_KEY"]
        deployment_name    = os.environ["OPENAI_API_MODEL"]
        #gpt-35-turbo
         
        logging.info('Sending an SQL generation request to OpenAI')
        response = openai.Completion.create(
            engine=deployment_name,
            prompt=prompt,
            temperature=0,
            max_tokens=200,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
            stop=["#",";"])
        
        logging.info('### RESPONSE FROM OPENAI ###')
        logging.info(json.dumps(response))

        # Retrieve the generated SQL Query
        sqlquery = f'SELECT{response.choices[0].text}'
        sqlquery = sqlquery.replace("\n", " ")
        logging.info(f'SQLQuery: {sqlquery}')

        # Execute the SQL query
        cursor.execute(sqlquery)
        logging.info('Query executed')
        #final_result = str(cursor.fetchall())
        final_result = cursor.fetchall()
        logging.info('SQL result fetched')

        # Parse final_result
        logging.info(f'Final Result : {final_result}')
        #logging.info(f"Intermediary : {final_result[0]}")
        final_output = final_result[0][0]
        logging.info(f'Final Output after Parsing : {final_output}')        

        # Return JSON Object
        #return (f'Question: {query} \nSQL Query: {sqlquery} \n\nGenerated Response: {final_output}')
        responseJSON = { 
                            "Question" : query,
                            "SQLQuery" : sqlquery,
                            "Response" : str(final_output)
                        }

        return json.dumps(responseJSON)


    finally:
        conn.close()

#### LangChain Implementation
#### See https://docs.langchain.com/docs/
#### See https://python.langchain.com/en/latest/
@app.function_name(name="ProcessSQLLangChain")
@app.route(route="langprompt", auth_level=func.AuthLevel.ANONYMOUS)
def processSQLLangChain(req: func.HttpRequest) -> func.HttpResponse:
     logging.info('LangChain prompt invoked')

     prompt = req.params.get('prompt')
     if not prompt:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            prompt = req_body.get('prompt')

     if prompt:
        logging.info(f'This is your prompt: {prompt}')
        
        #Connect to the SQL Database
        def db_instance():
            #Creating SQLAlchemy connection sting
            params = urllib.parse.quote_plus('DRIVER='+sql_driver+';SERVER=tcp:'+sql_server+';PORT=1433;DATABASE='+sql_database+';UID='+sql_username+';PWD='+sql_pwd+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
            conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
            db_instance = SQLDatabase.from_uri(conn_str)
            return db_instance

        logDatabaseEnvironment()
        logging.info("Connecting to database ...")
        db = db_instance()
        logging.info("... Database Connection established")
        
        logging.info("Getting OpenAI Environment")
        logOpenAIEnvironment()
        logging.info("Creating Large Language Model")
        llm = AzureOpenAI(deployment_name="text-davinci-003", model_name="text-davinci-003") # replace with your details

        # LangChain Agent
        toolkit = SQLDatabaseToolkit(db=db, llm = llm)

        logging.info("Creating LangChain SQL Agent")
        agent_executor = create_sql_agent(
            llm= llm,
            toolkit=toolkit,
            verbose=True,
            top_k = 5
        )

        # Execute LangChain
        logging.info("Running LangAgent with received prompt")
        response = agent_executor.run(prompt)

        logging.info("--- Response")
        logging.info(response)
        logging.info("--- End of Response ---")

        return func.HttpResponse(f"Hello, This is your response from LangChain : {response}")
     else:
        return func.HttpResponse(
             "Pass a prompt in the query string or in the request body for the correct result.",
             status_code=200
        )