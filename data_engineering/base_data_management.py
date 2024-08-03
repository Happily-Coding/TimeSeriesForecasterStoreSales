from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from psycopg import Connection
from typing import cast, LiteralString
from csv_sql_dataset_utils import CsvSqlDatasetProperties
import dataset_properties
from csv_utils import get_csv_rows_skipping
from db_interfacing import db_interface #TODO consider assigning it as a variable instead of using directly from import
#Though its probably unnecesary since you could just overwrite it by assigning a variable in python.
#It could take a db_interface on the constructor though.

class BaseDataManager:
    def __init__(self, spark:SparkSession, spark_sql_options:dict[str,str]):
        self.spark = spark
        self.spark_sql_options = spark_sql_options

    #TODO remove remaining sql implementation form here?
    def append_rows_pyscopg(self, table_to_store_in, df:DataFrame):
        """Insert new rows into an existing database table. Requires the table and columns to exist, and all other columns to be nullable/have a default value"""
        column_names = df.columns
        update_query = cast(LiteralString, f"INSERT INTO {table_to_store_in} ({', '.join(column_names)}) VALUES ({('%s, '*len(column_names))[:-2]})")
        try:
            column_values = df.collect() #Might want to limit how much of the dataset to collect here
            db_interface.execute_multi_valued_query(update_query, column_values)
        except Exception as e:
            print(f'There was an error storing database features of table {table_to_store_in}.
                  With update query: {update_query}
                  Will cancel the operation and safely roll back any changes. The error was:\n {e}')
            
    def get_new_csv_data(self, csv_file_path:str, sql_col_csv_equivalents:dict[str,str], current_sql_table_name:str) -> DataFrame:
        stored_data = self.spark.read.format('jdbc').options(**self.spark_sql_options).option('dbtable', current_sql_table_name).load().select(list(sql_col_csv_equivalents.keys()))
        stored_data_with_col_names_as_csv = stored_data.withColumnsRenamed(sql_col_csv_equivalents)
        
        csv_schema =  stored_data_with_col_names_as_csv.schema
        number_of_stored_rows = stored_data.count()
        new_csv_data = self.spark.read.csv(
            path=get_csv_rows_skipping(self.spark, csv_file_path, number_of_stored_rows), # type: ignore <- Ignoring this type error because it doesnt actually generate any problem.
            header=True, #Let spark know to ignore header
            schema=csv_schema
        )
        new_csv_data_with_sql_col_names = new_csv_data.withColumnsRenamed({v:k for k, v in sql_col_csv_equivalents.items()})
        return new_csv_data_with_sql_col_names

    def get_new_data_from_csv_sql_dataset_of_properties(self, dataset_props:CsvSqlDatasetProperties):
        return self.get_new_csv_data(dataset_props.csv_file_path, dataset_props.sql_col_csv_equivalents, dataset_props.sql_table_name)
    
    def get_new_data_from_csv_dataset_of_name(self, name):
        return self.get_new_data_from_csv_sql_dataset_of_properties(dataset_properties.properties_by_csv_sql_dataset[name])

    def update_sql_with_new_csv_data(self, csv_file_path:str, csv_to_sql_cols:dict[str,str], current_sql_table_name:str):
        data = self.get_new_csv_data(csv_file_path, csv_to_sql_cols, current_sql_table_name)
        self.append_rows_pyscopg(current_sql_table_name, data)
        return data
        
    def update_csv_sql_dataset_of_properties(self, dataset_props:CsvSqlDatasetProperties):
        """calls update_sql_with_new_csv_data without having manually obtain the parameters from csv_sql_dataset_properties"""
        self.update_sql_with_new_csv_data(dataset_props.csv_file_path, dataset_props.sql_col_csv_equivalents, dataset_props.sql_table_name)
    
    def update_csv_sql_dataset_of_name(self, name):
        """calls update_sql_with_new_csv_data without having to manually search for the dataset properties """
        self.update_csv_sql_dataset_of_properties(dataset_properties.properties_by_csv_sql_dataset[name])
    
    """The consumer classes shouldnt need to worry about what type of dataset a dataset is, or even what its stored under, just that it exists
       Plus this allows us to change the implementation without the consumer being impacted.
    """
    def get_new_stores_base_data(self):
        return self.get_new_data_from_csv_dataset_of_name('stores')
    
    def update_stores_base_data(self):
        return self.update_csv_sql_dataset_of_name('stores')

    def get_new_oil_base_data(self):
        return self.get_new_data_from_csv_dataset_of_name('oil_price_by_date')
    
    def update_oil_base_data(self):
        return self.update_csv_sql_dataset_of_name('oil_price_by_date')

    def get_new_sales_base_data(self):
        return self.get_new_data_from_csv_dataset_of_name('sales_agg_by_date_store_productfamily')
    
    def update_sales_base_data(self):
        return self.update_csv_sql_dataset_of_name('sales_agg_by_date_store_productfamily')
    
    def get_new_events_base_data(self):
        return self.get_new_data_from_csv_dataset_of_name('events_by_date')
    
    def update_events_base_data(self):
        return self.update_csv_sql_dataset_of_name('events_by_date')
    
    def get_new_transactions_base_data(self):
        return self.get_new_data_from_csv_dataset_of_name('transactions_agg_by_date_store')
    
    def update_transactions_base_data(self):
        return self.update_csv_sql_dataset_of_name('transactions_agg_by_date_store')
    
    def update_all_base_data(self):#Could try except all of them to make sure a failure in one stops all as long as they raise the exception, but is it benefitial?
        self.update_stores_base_data()
        self.update_oil_base_data()
        self.update_sales_base_data()
        self.update_events_base_data()
        self.update_transactions_base_data()

    