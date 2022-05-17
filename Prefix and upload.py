from sre_constants import SUCCESS
from warnings import catch_warnings
from snowflake.connector.pandas_tools import write_pandas, pd_writer
import snowflake.connector
from io import StringIO
from azure.storage.blob import BlobServiceClient, PublicAccess
import pandas as pd
container_name = "testcont"
blob_service_client = BlobServiceClient.from_connection_string('###############')
    
ctx = snowflake.connector.connect(
    user='########',
    password='##########',
    account='########',
    warehouse="########",
    )
cs = ctx.cursor()

cs.execute("SELECT current_version()")
cs.execute('USE ROLE SYSADMIN')
cs.execute("USE DATABASE PYTHONTR")
cs.execute('USE SCHEMA PUBLIC')
input_excel = blob_service_client.get_blob_client(container= container_name, blob = 'UMD.NCREIF.xlsx')

# Storing the Excel file to a dataframe

df = pd.ExcelFile(input_excel.download_blob().readall())

# Filter out the Sheet Names which starts with ~~~~

filter_col = [col for col in df.sheet_names if col.startswith('UMD.')]

# Getting the List of Filtered Column Names

df2 = pd.read_excel(df, filter_col)

# Running a loop for all the tabs in the excel sheet

for cols in filter_col:

    # Removing the UMD. prefix
    
    tname= cols.lstrip('UMD.')
    
    # Converting the dictionary to a Dataframe , as write pandas and snowflake was giving error !
    
    cnvrtd = df2[cols] 

    # Drop Table if exist / Otherwise it will append 
    
    truncprint = ('DROP TABLE  IF EXISTS "{0}"'.format(tname))
    
    # Executing the drop table command 
    
    cs.execute(truncprint)

    # Wring dataframe to the snowflake without the prefix(UMD.) in the sheets tab

    write_pandas(ctx, cnvrtd, tname, auto_create_table=1)

# Closing Connection

cs.close()
ctx.close()
