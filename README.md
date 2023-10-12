# Querying SAP Data using Natural Language

If a company owns an SAP System, this SAP system is an important source of valuable informantion. So making this data available to to end users is very important. However not every end user has SQL knowledge. This is where openAI can step in and help by converting Natural Language in SQL.

This blog hints at 2 methods on how to do this. 

This blog assumes the SAP data has been extracted towards a SQL table in Azure. As tooling we used the [SAP CDC Connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-sap-change-data-capture)  from Azure Data Factory. This blog can also be seen as an extenstion to the [SAP & Data Microhack](https://github.com/thzandvl/microhack-sap-data). In the microhack you can find documentation on how to extract the used data from a S4Hana Fully Activated Appliance which can be found on SAP CAL.

To enable openAI to know which data is available, we need to provide information from the data dictionary. We can do this ourselves by reading out the available tables from the data dictionary and providing it as an input for the openAI prompt. 

We can use a tool like [LangChain](https://docs.langchain.com/docs/) which executes this step automatically for us.

Since in the end we want to make the NL to SQL functionality available in a chatbot, this functionality is built as an Azure Function which can be called by the ChatBot.

## Reading out the data dictionary
To read out the data dictionary, the following code is used :
``` python
sql = "SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS"
cursor.execute(sql)
result_set = cursor.fetchall()

# Extract the column names from the cursor description
column_names = [column[0] for column in cursor.description]

# Extract the column names from each row and convert to dictionary
result_list = [dict(zip(column_names, row)) for row in result_set]
```

This list of column names can then be passed in the openAI prompt:
``` python
prompt = f"# Here are the columns in the database:\n# {result_set_json}\n### Generate a single T-SQL query for the following question using the information about the database: {question}\n\nSELECT"
```

This prompt is then passed to the openAI request.

```python
# Generate text using the OpenAI API
response = openai.Completion.create(
    engine=deployment_engine,
    prompt=prompt,
    max_tokens=200,
    n=1,
    #stop=None,
    stop = ["#",";"],
    temperature=0,
)
```

The SQL Statement is retrieved from the openAI response and executed on the database.
```python
sqlquery = f'SELECT{response.choices[0].text}'
...
cursor.execute(sqlquery)
```

After some formatting the result can be presented as output, which could be hadled by the chatbot.

```python
# Parse final_result
logging.info(f'Final Result : {final_result}')
#logging.info(f"Intermediary : {final_result[0]}")
final_output = final_result[0][0]
logging.info(f'Final Output after Parsing : {final_output}')        

# Return JSON Object

responseJSON = { 
                    "Question" : query,
                    "SQLQuery" : sqlquery,
                    "Response" : str(final_output)
                }

return json.dumps(responseJSON)
```

As a test, let's try the following question : 

```
What is the average difference in days between BillingDocumentDate and PaymentDate?
```

Generated SQL Statement:

```sql
SELECT AVG(DATEDIFF(day, SalesOrderHeaders.BILLINGDOCUMENTDATE, Payments.PaymentDate)) AS avg_days_diff FROM SalesOrderHeaders JOIN Payments ON SalesOrderHeaders.SALESDOCUMENT = Payments.SalesOrderNr
```

Notice that openAI retrieved the tables and fields to join on. 
This SQL Statement can then be launched on the database.

As a final response this results in :
```json
{ 
    "Question": "What is the average difference in days between BillingDocumentDate and PaymentDate?",
    "SQLQuery": "SELECT AVG(DATEDIFF(day, SalesOrderHeaders.BILLINGDOCUMENTDATE, Payments.PaymentDate)) AS avg_days_diff FROM SalesOrderHeaders JOIN Payments ON SalesOrderHeaders.SALESDOCUMENT = Payments.SalesOrderNr",
    "Response": "44.0" 
}
```
You can verify the response be executing the SQL Statement directly against the DB.

# Using LangChain
[LangChain](https://docs.langchain.com/docs/) makes the whole simpler for us. We just need to point it to our database.

```python
def db_instance():
    #Creating SQLAlchemy connection sting
    params = urllib.parse.quote_plus('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ pwd+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    db_instance = SQLDatabase.from_uri(conn_str)
    return db_instance
```
Create a OpenAI Model (LLM - Large Language Model)

```python
logging.info("Creating Large Language Model")
llm = AzureOpenAI(deployment_name="text-davinci-003", model_name="text-davinci-003") # replace with your details
```

And then create an executor based on the DB connection and the created openAI model:

```python
# LangChain Agent
toolkit = SQLDatabaseToolkit(db=db, llm = llm)

agent_executor = create_sql_agent(
    llm= llm,
    toolkit=toolkit,
    verbose=True,
    top_k = 5
)
```
The executor can then execute the question

```python
response = agent_executor.run(prompt)
```

As a test we run the same question as above. Below you can find the LangChain trace:
```
Entering new AgentExecutor chain...
Action: list_tables_sql_db
Action Input: ""
Observation: Payments, SalesOrderHeaders, CDSViewsToExtract, SalesOrderItems
Thought: I need to look at the schema of the SalesOrderHeaders, Payments tables to see if they contain the relevant information I need.
Action: schema_sql_db
Action Input: "Payments, SalesOrderHeaders"
Observation:
CREATE TABLE [Payments] (
        [PaymentNr] NVARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SalesOrderNr] NVARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [CustomerNr] NVARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [CustomerName] NVARCHAR(80) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [PaymentDate] DATE NULL, 
        [PaymentValue] DECIMAL(15, 2) NULL, 
        [Currency] NVARCHAR(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
)

/*
3 rows from Payments table:
PaymentNr       SalesOrderNr    CustomerNr      CustomerName    PaymentDate     PaymentValue    Currency
33425   3869    EWM17-CU01      EWM Domestic Customer 01        2020-11-15      210.00  None
33420   4531    0017100003      Domestic Customer US 3  2021-11-29      29000.00        None
33428   3757    USCU_S14        Cityscape Cycles        2020-12-02      2640.00 None
*/


CREATE TABLE [SalesOrderHeaders] (
        [BILLINGCOMPANYCODE] NVARCHAR(4) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [BILLINGDOCUMENTDATE] DATE NULL, 
        [COUNTRY] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [CREATIONDATE] DATE NULL, 
        [CREATIONTIME] TIME NULL, 
        [CREDITCONTROLAREA] NVARCHAR(4) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [CUSTOMERACCOUNTGROUP] NVARCHAR(4) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [CUSTOMERGROUP] NVARCHAR(2) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [CUSTOMERNAME] NVARCHAR(80) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [DISTRIBUTIONCHANNEL] NVARCHAR(2) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [LASTCHANGEDATE] DATE NULL, 
        [LASTCHANGEDATETIME] DECIMAL(21, 0) NULL, 
        [ORGANIZATIONDIVISION] NVARCHAR(2) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [PRICINGDATE] DATE NULL, 
        [PURCHASEORDERBYCUSTOMER] NVARCHAR(35) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SALESDISTRICT] NVARCHAR(6) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SALESDOCUMENT] NVARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SALESDOCUMENTPROCESSINGTYPE] NVARCHAR(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SALESDOCUMENTTYPE] NVARCHAR(4) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SALESGROUP] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SALESOFFICE] NVARCHAR(4) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SALESORGANIZATION] NVARCHAR(4) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SDDOCUMENTCATEGORY] NVARCHAR(4) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [SOLDTOPARTY] NVARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [TOTALNETAMOUNT] DECIMAL(15, 2) NULL, 
        [TRANSACTIONCURRENCY] NVARCHAR(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [CITYNAME] NVARCHAR(35) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
        [POSTALCODE] NVARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
)

/*
3 rows from SalesOrderHeaders table:
BILLINGCOMPANYCODE      BILLINGDOCUMENTDATE     COUNTRY CREATIONDATE    CREATIONTIME    CREDITCONTROLAREA       CUSTOMERACCOUNTGROUP    CUSTOMERGROUP   CUSTOMERNAME    DISTRIBUTIONCHANNEL     LASTCHANGEDATE  LASTCHANGEDATETIME      ORGANIZATIONDIVISION  PRICINGDATE     PURCHASEORDERBYCUSTOMER SALESDISTRICT   SALESDOCUMENT   SALESDOCUMENTPROCESSINGTYPE     SALESDOCUMENTTYPE       SALESGROUP      SALESOFFICE     SALESORGANIZATION       SDDOCUMENTCATEGORY      SOLDTOPARTY     TOTALNETAMOUNTRANSACTIONCURRENCY      CITYNAME        POSTALCODE
1710    2020-07-03      US      2020-07-08      None    A000    KUNA    Z1      Greenhigh Bikes 10      2020-07-08      20200708164437  00      2020-07-03      4500038319      US0003  3384    None    TA      None    None    1710    C       USCU_S09      107214.00       USD     NASHVILLE       37201
1710    2019-03-20      US      2019-03-24      None    1000    CUST    01      Domestic US Customer 1  10      2019-06-17      20190617191930  00      2019-03-20      324TG10_2-161   None    1181    None    TA      None    None    1710    C    0017100001       0.00    USD     Atlanta 30315-1402
1710    2019-06-25      US      2019-06-25      None    A000    KUNA    Z2      Skymart Corp    10      None    20190625170954  00      2019-06-25      4500100006      US0004  1277    None    TA      None    None    1710    C       USCU_L01     85170.00 USD     New york        10007
*/


Thought: I should use a query to get the average difference between payment date and billing document date.
Action: query_checker_sql_db
Action Input: SELECT AVG(DATEDIFF(day, PaymentDate, BILLINGDOCUMENTDATE)) FROM Payments p JOIN SalesOrderHeaders s ON p.SalesOrderNr = s.SALESDOCUMENT
Observation:

SELECT AVG(DATEDIFF(day, PaymentDate, BILLINGDOCUMENTDATE)) 
FROM Payments p 
INNER JOIN SalesOrderHeaders s 
ON p.SalesOrderNr = s.SALESDOCUMENT WHERE PaymentDate IS NOT NULL AND BILLINGDOCUMENTDATE IS NOT NULL;

Thought: The query looks correct, I should execute it.
Action: query_sql_db
Action Input: SELECT AVG(DATEDIFF(day, PaymentDate, BILLINGDOCUMENTDATE)) FROM Payments p JOIN SalesOrderHeaders s ON p.SalesOrderNr = s.SALESDOCUMENT WHERE PaymentDate IS NOT NULL AND BILLINGDOCUMENTDATE IS NOT NULL;
Observation: [(-44,)]Thought: I now know the final answer

Final Answer: The average difference between payment date and billing document date is -44 days.

> Finished chain.

```
Note that langchain itself reads out the data dictionary. You can also nicely follow the thought chain Langchain is going through to find the answer.