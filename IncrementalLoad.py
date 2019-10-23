### 1. Import modules
import sqlalchemy as db
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import google.auth
import pymysql
import datetime
print('Importación de modulos correcta')

## Make clients ##
credentials = service_account.Credentials.from_service_account_file('path_to_Credential')
project_id = 'Id of project'
bqclient = bigquery.Client(
    credentials=credentials,
    project=project_id,
)

## Make Query ##
sql_bq = '''SELECT MAX(datevariable) FROM Dataset'''
valor = (
    bqclient.query(sql_bq)
    .result()
    .to_dataframe()
)
v = str(v.iloc[interval])  #Filter desirable value of date, for example, without the hour it would be year-month-day
print('Lectura de datos MySQL correcta')

## Connection to MySQL ##
engine = db.create_engine('mysql+pymysql://User:Key@IP')
#If you're running with Jupyter you need mysql + pymysql, elsewhere when executing as .py just use mysql
sql_sg = """ SELECT desirableVariables
FROM Dataset WHERE datevariable > '{}' 
""".format(v)
df_from_query = pd.read_sql_query(sql_sg, engine)
print('Lectura de datos MySQL correcta')

#Validation in case there is no new data to append
if df_from_query.shape[0] != 0:
    table_id = 'Dataset to Append to'
    schema = [{'name': 'variable1', 'type': 'STRING'}, 
                 {'name': 'variable2', 'type': 'FLOAT'},
                 {'name': 'variable3', 'type': 'INTEGER'}, 
                 ....
                 {'name': 'variableN', 'type': 'DESIREDTYPE'}]
    df_from_query.to_gbq(table_id, project_id=project_id, if_exists='append', table_schema=schema, credentials=credentials)
    print('Proceso de carga incremental fue correcto')
### Refresh data from n previous days
    v2 = datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
    v3 = v2 - datetime.timedelta(days=n,hours=v2.hour,minutes=v2.minute,seconds=v2.second)
    #the last line is to assure your are taking data from the beginning of the day, for example, 2019-10-25 00:00:00#

    ### Data from the last n days
    sql_sgt = """ SELECT desiredVariables
    FROM Dataset WHERE datevariable >= '{fecha1}' AND datevariable <= '{fecha2}' 
    """.format(fecha1=v3,fecha2=v)
    df_n_days = pd.read_sql_query(sql_sgt, engine)
    print('Lectura de datos con n dias anteriores correcta')

    ### Update table in GCP with temporal table
    table_id_t = 'Dataset_n_days'
    schema = [{'name': 'variable1', 'type': 'STRING'}, 
                 {'name': 'variable2', 'type': 'FLOAT'},
                 {'name': 'variable3', 'type': 'INTEGER'}, 
                 ....
                 {'name': 'variableN', 'type': 'DESIREDTYPE'}]
    df_n_days.to_gbq(table_id_t, project_id=project_id, if_exists='replace', table_schema=schema, credentials=credentials)
    print('Guardado de tabla temporal correcto')

    ##Merge Dataset and Temporal Table##
    sql_merge = '''
       MERGE Dataset D
       USING Dataset_n_days DND
       ON D.id = DND.id
       WHEN MATCHED 
       THEN UPDATE 
       SET D.variable1 = DND.variable1, 
           D.variable2 = DND.variable2,
           ....,
           D.variableN = DND.variableN
        WHEN NOT MATCHED THEN
        INSERT (id) VALUES (id) 
        '''
    query_job = bqclient.query(sql_merge, location='location_of_GCP_services')
    query_job.result() 
    print('Proceso de unifacion de datos correcto')
    print('El proceso concluyo con exito')
else:
    print('No hay datos nuevos para agregar')