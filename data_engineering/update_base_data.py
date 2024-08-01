from base_data_management import BaseDataManager
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import psycopg

#TODO MOVER TODA LA PARTE DE SETUP A UNA CLASE COMPARTIDA CON VARIABLES ESTATICAS PARA TODO ESTO PARA NO REPETIR TANTO AL PEDO

os.environ['PYSPARK_PYTHON'] = '..\.venv\scripts\python.exe'#sys.executable
#os.environ['PYSPARK_DRIVER_PYTHON'] = '..\.venv\scripts\python.exe' #sys.executable
#Download the postgres jdbc driver and use it with spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.7.3 pyspark-shell'

#Load the database info, its stored in .env files because its simple, allows it to be easily overriden in production, and helps prevent accidentally liking confidential info.
load_dotenv('.\db_settings.env') #Load the database configuration
db_host = os.getenv('db_host')
db_port = os.getenv('db_port')
db_admin_user = os.getenv('db_admin_user')
db_admin_password = os.getenv('db_admin_password')
db_data_engineer_user = os.getenv('db_data_engineer_user')
db_data_engineer_password = os.getenv('db_data_engineer_password')
db_name = os.getenv('db_name')

spark_session_builder:SparkSession.Builder = SparkSession.builder # type: ignore <-Ignore a wrong pylance warning, and make type detection work properly
spark:SparkSession = (
    spark_session_builder
    .master('local[3]')
    .appName('TimeSeriesForecastStoreSales Data Engineering')
    .getOrCreate()
)

spark_sql_options = {
    'driver': "org.postgresql.Driver",
    'url': f"jdbc:postgresql://{db_host}:{db_port}/{db_name}",
    'user': db_data_engineer_user,
    'password': db_data_engineer_password,
    'format':'jdbc'
}
data_engineering_connection = psycopg.connect(f"host={db_host} port={db_port} dbname={db_name} user={db_data_engineer_user} password={db_data_engineer_password}")

base_data_manager = BaseDataManager(spark, spark_sql_options, data_engineering_connection)

base_data_manager.update_all_base_data()
