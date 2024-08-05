from pyspark.sql import SparkSession
import spark_utils

class SparkInterface:
    """A class to interact with the spark backend, more easily, and without having to know dataset details"""
    def __init__(self, spark:SparkSession, spark_sql_options:dict[str,str]):
        self.spark = spark
        self.spark_sql_options = spark_sql_options
    
    def get_current_data_in_sql_table(self, table_name:str):
        """Get the data in a sql table as a lazy spark dataframe."""
        return spark_utils.get_current_data_in_sql_table(self.spark, self.spark_sql_options, table_name)

def _make_spark_interface():
    from dotenv import load_dotenv
    load_dotenv('.\db_settings.env') #Load the database configuration into
    
    import os
    #Fix an error about spark with python, probably necessary because we are not in the same folder as the .venv folder
    os.environ['PYSPARK_PYTHON'] = '..\.venv\scripts\python.exe'#sys.executable
    #os.environ['PYSPARK_DRIVER_PYTHON'] = '..\.venv\scripts\python.exe' #sys.executable
    #Download the postgres jdbc driver and use it with spark
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.7.3 pyspark-shell'
    
    db_host = os.getenv('db_host')
    db_port = os.getenv('db_port')
    db_data_engineer_user = os.getenv('db_data_engineer_user')
    db_data_engineer_password = os.getenv('db_data_engineer_password')
    db_name = os.getenv('db_name')

    spark_sql_options = {
        'driver': "org.postgresql.Driver",
        'url': f"jdbc:postgresql://{db_host}:{db_port}/{db_name}",
        'user': db_data_engineer_user,
        'password': db_data_engineer_password,
        'format':'jdbc'
    }
    
    spark_session_builder:SparkSession.Builder = SparkSession.builder # type: ignore <-Ignore a wrong pylance warning, and make type detection work properly
    spark:SparkSession = (
        spark_session_builder
        .master('local[3]')
        .appName('TimeSeriesForecastStoreSales Data Engineering')
        .getOrCreate()
    )
    
    return SparkInterface(
        spark,
        spark_sql_options
    )

spark_interface = _make_spark_interface()