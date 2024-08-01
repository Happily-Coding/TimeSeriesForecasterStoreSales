#Note that while we could provide a method to create tables, its better to do it as a script, the reason is that you shouldnt hard code creating the tables since you should do it only once, or when for some reason you want to recreate them.
import os
from sql_utils import execute_queries
from dotenv import load_dotenv
import psycopg
import dataset_properties

properties_per_dataset = dataset_properties.properties_by_csv_sql_dataset
sql_table_creation_queries = [dataset_properties.create_sql_table_script for dataset_properties in dataset_properties.properties_by_csv_sql_dataset.values()]

#create_last_row_table_query = f'''
#CREATE TABLE IF NOT EXISTS last_loaded_rows (
#    table_name TEXT UNIQUE NOT NULL,
#    row INTEGER NOT NULL
#);
#COMMENT ON TABLE last_loaded_rows IS 'The last row of the csv file that was read by spark'
#'''

try:
    load_dotenv('.\db_settings.env') #Load the database configuration
    db_host = os.getenv('db_host')
    db_port = os.getenv('db_port')
    db_name = os.getenv('db_name')
    db_data_engineer_user = os.getenv('db_data_engineer_user')
    db_data_engineer_password = os.getenv('db_data_engineer_password')
    data_engineering_connection = psycopg.connect(f"host={db_host} port={db_port} dbname={db_name} user={db_data_engineer_user} password={db_data_engineer_password}")

    execute_queries(sql_table_creation_queries,data_engineering_connection)
    data_engineering_connection.close()
except Exception as e:
    print(f'There was an error while trying to create the database tables, {e}')
else:
    print(f'Sucesfully created the tables in host={db_host} port={db_port} db_name={db_name} . Queries used:')
    for query in sql_table_creation_queries: #Print the queries in an easily readable format without \n symbols
        print(query)