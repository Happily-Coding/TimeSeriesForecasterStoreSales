#TODO, separate db creation and user creation? The user might already exist.
import psycopg
from sql_utils import create_engineering_user_if_not_exists_and_allowed, create_postgres_db
from db_connector import db_name, db_host, db_port, db_admin_user, db_admin_password, db_data_engineer_user, db_data_engineer_password

try:
    create_postgres_db(db_host, db_port, db_admin_user, db_admin_password, db_name)
    database_connection = psycopg.connect(f"host={db_host} port={db_port} user={db_admin_user} password={db_admin_password} dbname={db_name}")
    create_engineering_user_if_not_exists_and_allowed(database_connection, db_data_engineer_user, db_data_engineer_password, db_name)
    database_connection.close()
except Exception as e:
    print('There was a problem while connecting to the database server or creating the database:')
    print(e)
else:
    print(f'Sucessfully created the postgres db: {db_name} at host={db_host} port={db_port} and the postgres user: {db_data_engineer_user}')