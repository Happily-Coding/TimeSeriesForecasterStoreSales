""" A file containing the db_interface that should be used,
    and its definition containing the logic making it work.
"""
from typing import Iterable, LiteralString
import psycopg
#TODO rename, split, or remove create_engineering_user_if... from sql_utils
import sql_utils
from sql_utils import create_postgres_db
class DBInterface:
    """
    A class to allow modifiying our postgres database
    - Without coupling other classes to a particular database implementation,
        - allowing to change only this class if the implementation changes
        - allowing usage of this class without having to know its inner workings.
        - allowing us to keep the db configuration/credentials in a single place.
            - preventing other classes from needing to obtain it.
    """
    def __init__(
        self,
        db_host,
        db_port,
        db_admin_user,
        db_admin_password,
        db_data_engineer_user,
        db_data_engineer_password,
        db_name
        ):
        self.db_host = db_host
        self.db_port = db_port
        self.db_admin_user = db_admin_user
        self.db_admin_password = db_admin_password
        self.db_data_engineer_user = db_data_engineer_user
        self.db_data_engineer_password = db_data_engineer_password
        self.db_name = db_name
    
    #TODO: verify if its worth adding a "try except raise from" to connect or if the original message is clear enough.
    def _make_data_engineering_connection(self):
        return psycopg.connect(f"host={self.db_host} port={self.db_port} dbname={self.db_name} user={self.db_data_engineer_user} password={self.db_data_engineer_password}")
    
    def _make_database_administrative_connection(self):
        return psycopg.connect(f"host={self.db_host} port={self.db_port} user={self.db_admin_user} password={self.db_admin_password} dbname={self.db_name}")
        
    def create_db(self):
        create_postgres_db(self.db_host, self.db_port, self.db_admin_user, self.db_admin_password, self.db_name)

    def create_engineering_user_if_not_exists_and_allowed(self, verbose=True):
        try:
            administrative_connection = self._make_database_administrative_connection()
            sql_utils.create_engineering_user_if_not_exists_and_allowed(
                administrative_connection,
                self.db_data_engineer_user,
                self.db_data_engineer_password,
                self.db_name
                )
            administrative_connection.close()
        except Exception as ex:
            raise Exception('There was a problem creating a database') from ex
        else:
            if verbose:
                print(f'Sucessfully created the postgres db: {self.db_name} at host={self.db_host} port={self.db_port} and the postgres user: {self.db_data_engineer_user}')
    
    def execute_engineering_queries(self, queries, verbose=True):
        engineering_connection = self._make_data_engineering_connection()
        sql_utils.execute_queries(queries, engineering_connection)
        engineering_connection.close()
        
        if verbose:
            print(f'Sucesfully created the tables in host={self.db_host} port={self.db_port} db_name={self.db_name} . Queries used:')
            for query in queries: #Print the queries in an easily readable format without \n symbols
                print(query)
    
    def execute_multi_valued_query(self, query:LiteralString, values:Iterable):
        """
        Executes efficiently a query that needs to be applied to multiple rows with diferent values
        The most common examples being ALTER and UPDATE queries
        """
        engineering_connection = self._make_data_engineering_connection()
        with engineering_connection.cursor() as cursor: #Automatically close the cursor when done
            cursor.executemany(query, values)
            engineering_connection.commit()
            engineering_connection.close()
        
    
    

import os
from dotenv import load_dotenv

load_dotenv('.\db_settings.env') #Load the database configuration into
db_interface = DBInterface(
    os.getenv('db_host'),
    os.getenv('db_port'),
    os.getenv('db_admin_user'),
    os.getenv('db_admin_password'),
    os.getenv('db_data_engineer_user'),
    os.getenv('db_data_engineer_password'),
    os.getenv('db_name')
)
