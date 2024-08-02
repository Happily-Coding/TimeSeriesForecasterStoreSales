import psycopg
from typing import Iterable, Literal, cast, LiteralString
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

from order_utils import create_desc_filter

def create_db_if_not_exists(dbless_connection, db_name) -> None:
    """ Creates and executes a query to create a database of a certain name if it doesn't exist.
    dbless_connection: A database server connection with autocommit=True, and no database specified.
    """
    
    #Postgres doesn't support IF NOT EXISTS clause in CREATE DATABASE, this is a work arround using a subquery.
    #Queries are expected to be literal string, strings that are created directly, and as such dont have user input.
    create_db_if_not_exists_query:LiteralString = cast(LiteralString, f"""
    SELECT 'CREATE DATABASE {db_name}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{db_name}')
    """)

    execute_query(create_db_if_not_exists_query, conn=dbless_connection)

    

def create_engineering_user_if_not_exists_and_allowed(sql_connection, user_to_create, user_to_create_password, db_name):
    user_creation_query = f"""
    -- Create a user, data_engineer, that can create databases.
    CREATE USER {user_to_create} LOGIN CONNECTION LIMIT -1 ENCRYPTED PASSWORD '{user_to_create_password}';

    -- Allow data_engineer to connect to the database. (could lead to issues if the database does not exist yet)
    GRANT CONNECT ON DATABASE {db_name} TO {user_to_create};

    -- Allow data_engineer to create, list, and access tables and other schema elements.
    GRANT CREATE, USAGE ON SCHEMA public TO {user_to_create};

    --Make data_engineer cant accidentally delete/update rows in current tables
    REVOKE DELETE, UPDATE ON ALL TABLES IN SCHEMA "public" FROM {user_to_create};

    -- And make sure you they cant delete/update rows in future tables
    ALTER DEFAULT PRIVILEGES IN SCHEMA "public" REVOKE DELETE, UPDATE ON TABLES FROM {user_to_create};
    """
    execute_query(user_creation_query, sql_connection)



def execute_query(sql_query:str|LiteralString, conn: psycopg.connection.Connection) -> None:
    """Executes any query using an existing connection, properly handling errors by preventing changes and printing errors.
       If the query needs autocommit, for example for creating database, make sure that your conn.autocommit = True
    """
    cur = conn.cursor()
    try:
        cur.execute(cast(LiteralString, sql_query))
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur._query.query}") # type: ignore
        conn.rollback()
    else:
        conn.commit()
    
    cur.close()


def execute_queries(sql_queries: Iterable[LiteralString|str], conn: psycopg.connection.Connection) -> None:
    """Execute multiple queries, commiting the results only if they are all sucesfull."""
    literal_sql_queries:list[LiteralString] = [cast(LiteralString, sql_query) for sql_query in sql_queries]
    cur: psycopg.cursor.Cursor = conn.cursor()
    for literal_sql_query in literal_sql_queries:
        try:
            cur.execute(literal_sql_query)
        except Exception as e:
            print(f"{type(e).__name__}: {e}")
            print(f"Query: {cur._query.query}") # type: ignore <-ignore that _query could be of none type since it will never happen here.
            conn.rollback()
            cur.close()
            break
    conn.commit()

def create_tables_that_dont_exist(db_info, creation_queries):
    """TODO REMOVE AND USE EXECUTE_QUERIES INSTEAD"""
    for query in creation_queries:
        # Connect to the database created and create a table
        connection:psycopg.Connection = psycopg.connect(**db_info)
        execute_query(query, connection)
        connection.close()
        
        
def get_last_sql_table_entry(spark:SparkSession, spark_sql_options:dict, sql_table_name:str, keys_to_order_sql_by:Iterable):
    last_table_entry:SparkDataFrame = (
        spark.read.format('jdbc')
        .options(**spark_sql_options)
        .option('query', f'SELECT * FROM {sql_table_name} ORDER BY {create_desc_filter(keys_to_order_sql_by)} LIMIT 1')
        .load()
    )
    return last_table_entry

def make_create_columns_if_not_exists_statement(table_name:str, sql_column_strings:Iterable[str]) -> LiteralString:
    """
    The returned statement will look like this:
    ALTER TABLE table_name 
    ADD COLUMN IF NOT EXISTS col_name_1,
    ADD COLUMN IF NOT EXISTS col_name_2,
    ADD COLUMN IF NOT EXISTS col_name_2
    """
    alter_table_statement = f'ALTER TABLE {table_name} \n'
    for column_with_type_statement in sql_column_strings:
        alter_table_statement = alter_table_statement + f'ADD COLUMN IF NOT EXISTS {column_with_type_statement},\n'
    alter_table_statement = alter_table_statement[:-2] #remove trailing coma and \n
    print(alter_table_statement)
    alter_table_statement = cast(LiteralString, alter_table_statement)
    return alter_table_statement

def create_table_columns_if_not_exist(table_name:str, sql_column_strings:Iterable[str], connection:psycopg.connection.Connection):
    alter_table_statement = make_create_columns_if_not_exists_statement(table_name, sql_column_strings)
    with connection.cursor() as cursor:
        cursor.execute(alter_table_statement)
    connection.commit()

def make_update_columns_with_values_statement(table_to_store_in:str, columns_to_store:Iterable[str]) -> LiteralString:
    """
    The returned statment will look like this: 
    UPDATE table_name SET col_name = %, col_name2 = %s, col_name3 = %s
    """
    update_query = f'UPDATE {table_to_store_in} SET'
    
    #Add col_name = %, col_name2 = %s ... %s will be replaced later by values
    for column_to_store_name in columns_to_store:
        update_query = update_query + f' {column_to_store_name} = %s,'
    update_query = update_query[:-1] #remove trailing coma.
    update_query = cast(LiteralString, update_query)
    
    return update_query

def make_where_each_column_equals_values_statement(identity_columns:Iterable[str]) -> LiteralString:
    """
    The returned statement will look like this:
    WHERE col_name = %s, col_name_2 = %s, col_name3 = %s
    %s will be replaced later by values you pass on cursor.execute or cursor.executemany.
    """
    where_statement = 'WHERE '
    for identity_column in identity_columns:
        where_statement = where_statement + f'{identity_column} = %s, '
    where_statement = where_statement[:-2] #remove trailing coma and space
    where_statement = cast(LiteralString, where_statement)
    return where_statement

def create_postgres_db(db_host, db_port, db_admin_user, db_admin_password, db_name):
    database_server_connection = psycopg.connect(f"host={db_host} port={db_port} user={db_admin_user} password={db_admin_password}")
    database_server_connection.autocommit = True

    create_db_query = f'CREATE DATABASE {db_name}'
    execute_query(create_db_query, database_server_connection)
    database_server_connection.close()