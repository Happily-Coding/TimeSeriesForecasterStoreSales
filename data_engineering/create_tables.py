"""
A python script to create the required db tables
while we could provide a method to create tables instead,
its better to do it as an independent script,
since you shouldnt hard code creating the tables since you should do it only once,
or when for some reason you want to recreate them.
"""
import dataset_properties
from db_interfacing import db_interface

properties_per_dataset = dataset_properties.properties_by_csv_sql_dataset
sql_table_creation_queries = [dataset_properties.create_sql_table_script for dataset_properties in dataset_properties.properties_by_csv_sql_dataset.values()]

try:
    db_interface.execute_engineering_queries(sql_table_creation_queries)
except Exception as e:
    print(f'There was an error while trying to create the database tables, {e}')