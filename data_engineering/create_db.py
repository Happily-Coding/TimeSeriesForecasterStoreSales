#TODO, separate db creation and user creation? The user might already exist.
import os
from dotenv import load_dotenv
import psycopg
from sql_utils import create_db_if_not_exists, create_engineering_user_if_not_exists_and_allowed, execute_query
from typing import cast, LiteralString


#Load the database info, its stored in .env files because its simple, allows it to be easily overriden in production, and helps prevent accidentally liking confidential info.
load_dotenv('.\db_settings.env') #Load the database configuration
db_host = os.getenv('db_host')
db_port = os.getenv('db_port')
db_admin_user = os.getenv('db_admin_user')
db_admin_password = os.getenv('db_admin_password')
db_data_engineer_user = os.getenv('db_data_engineer_user')
db_data_engineer_password = os.getenv('db_data_engineer_password')
db_name = os.getenv('db_name')

try:
    database_server_connection = psycopg.connect(f"host={db_host} port={db_port} user={db_admin_user} password={db_admin_password}")
    database_server_connection.autocommit = True

    create_db_query = f'CREATE DATABASE {db_name}'
    execute_query(create_db_query, database_server_connection)
    database_server_connection.close()

    database_connection = psycopg.connect(f"host={db_host} port={db_port} user={db_admin_user} password={db_admin_password} dbname={db_name}")
    create_engineering_user_if_not_exists_and_allowed(database_connection, db_data_engineer_user, db_data_engineer_password, db_name)
    database_connection.close()

except Exception as e:
    print('There was a problem while connecting to the database server or creating the database:')
    print(e)

else:
    print(f'Sucessfully created the postgres db: {db_name} at host={db_host} port={db_port} and the postgres user: {db_data_engineer_user}')