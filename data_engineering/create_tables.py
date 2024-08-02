#Note that while we could provide a method to create tables, its better to do it as a script, the reason is that you shouldnt hard code creating the tables since you should do it only once, or when for some reason you want to recreate them.
from sql_utils import execute_queries
import dataset_properties
from db_connector import data_engineering_connection, db_host, db_port, db_name

properties_per_dataset = dataset_properties.properties_by_csv_sql_dataset
sql_table_creation_queries = [dataset_properties.create_sql_table_script for dataset_properties in dataset_properties.properties_by_csv_sql_dataset.values()]

try:
    execute_queries(sql_table_creation_queries,data_engineering_connection)
    data_engineering_connection.close() #TODO MAKE DB CONNECTOR CREATE A SEPARATE CONNECTION PER REQUEST, SINCE EACH CONNECTION SHOULD BE CLOSED.
except Exception as e:
    print(f'There was an error while trying to create the database tables, {e}')
else:
    print(f'Sucesfully created the tables in host={db_host} port={db_port} db_name={db_name} . Queries used:')
    for query in sql_table_creation_queries: #Print the queries in an easily readable format without \n symbols
        print(query)