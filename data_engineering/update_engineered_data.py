#CURRENTLY TESTING EACH METHOD TO MAKE SRUE THEY WORK
import pyspark
import data_engineering_management
from data_engineering_management import DataEngineeringManager
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

data_engineering_manager = DataEngineeringManager(spark, spark_sql_options, data_engineering_connection)

oil_features = data_engineering_management.create_oil_prices_feature_group()

current_oil_data = data_engineering_manager.get_current_data_in_dataset_of_name('oil_price_by_date')
current_oil_data.select()
current_oil_data.show(2)

transformed_oil_data = oil_features.pipeline.fit(current_oil_data).transform(current_oil_data)
transformed_oil_data.show(2)


#data_engineering_manager.store_engineered_features(oil_features, transformed_oil_data)

#engineer_features_and_store(oil_features, current_oil_dataset)

#data_engineering_manager.engineer_and_store_all_features()

