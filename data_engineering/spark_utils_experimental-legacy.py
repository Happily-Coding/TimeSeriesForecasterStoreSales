from pyspark.ml import Transformer
from pyspark.sql.dataframe import DataFrame
from psycopg import Connection
from typing import cast, LiteralString
import sql_utils

class PostgresExistingRecordsFilter(Transformer):
    """Filters (removes) from the dataframe it recieves any records that already exist in a postgres database"""
    
    def __init__(self, spark, spark_sql_options, table_name, matching_rows_condition):
        '''matching_rows_filter: a filter that should evaluate to true for records that are the same on both tables. string for the join column name, a list of column names, a join expression (Column), or a list of Columns. If on is a string or a list of strings indicating the name of the join column(s), the column(s) must exist on both sides '''
        self.existing_records_reader = spark.read.format('jdbc').options(**spark_sql_options).option('dbtable', table_name)
        self.matching_rows_condition = matching_rows_condition
        
        
    def _transform(self, dataset:DataFrame) -> DataFrame:
        #example matching_rows condition csv_dataset.col('id_col').equalTo(csv_dataset.col('id_col')
        #could probably be expressed as 'id_col' in this case.
        return dataset.join(self.existing_records_reader.load(), self.matching_rows_condition, 'left_anti' )
    

class PostgresStoredFeatureUpdater(Transformer):
    """
    Update the values of existing rows for a certain column, with the values of those rows in the current dataframe.
    The condition should '' or WHERE .... (where included)
    Probably key for filling features you just added to the table, if you wanted to do it without storing the entire dataframe again.
    """
    def __init__(self, connection_to_use:Connection, table_to_store_in:str, col_to_store:str, col_to_store_in_type:str, condition_for_storage:str):
        self.col_to_store = col_to_store
        self.table_to_store_in = table_to_store_in
        self.connection_to_use = connection_to_use
        self.col_to_store_in_type = col_to_store_in_type
        self.condition_for_storage = condition_for_storage
        
    def _transform(self, dataset:DataFrame) -> DataFrame:
        #Regretably you cannot add a column with values. You need to create it empty, and update all records to have the new vlaues.
        try:
            #Make sure the column exists if missing, probably belongs elsewhere, likely in the registration step which could be at the start of the pipeline.
            #Maybe this could be changed to store_registered_features
            sql_utils.execute_query(f'ALTER TABLE {self.table_to_store_in} ADD COLUMN IF NOT EXISTS {self.col_to_store} {self.col_to_store_in_type}', self.connection_to_use)
            
            #Update the database with the current values of the column in the dataframe
            with self.connection_to_use.cursor() as cursor: #Automatically close the cursor when done
                update_query = cast(LiteralString, f"UPDATE {self.table_to_store_in} SET {self.col_to_store} = %s {self.condition_for_storage}")
                column_values = dataset.select(self.col_to_store).collect() #Might want to limit how much of the dataset to collect here
                print(f'update_query: {update_query}, column_values: {column_values}')
                cursor.executemany(update_query, column_values)
                self.connection_to_use.commit()
        except Exception as e:
            print(f'There was an error storing a database feature. Continuing without saving. Error: \n {e}')
            self.connection_to_use.rollback()

        return dataset
    
class JDBCDataFrameStorer(Transformer):
    """
    Store all the values of the current dataframe using a JCDBC connector.
    Is mostly meant for storing new data to keep the database up to date:
        By default append rows in this dataframe to, which will work perfectly for storing data found in the previous step to be new (it will not store any rows if there isnt new data if paired with it). use mode='overwrite' to update existing data, entirely overriding the new table. DANGEROUS.
    If you are interested in adding a new feature to an existing database table see PostgresStoredFeatureUpdater
    If you are interested in updating an existing feature, that is probably the best alternative too.

    """
    def __init__(self, spark_sql_options:dict, dbtable:str, mode='append'):
        #Todo consider generating the reader here and just using it in transform.
        self.spark = spark
        self.spark_sql_options = spark_sql_options
        self.dbtable = dbtable
        self.mode = mode
        
    def _transform(self, dataset:DataFrame) -> DataFrame:
        #Regretably you cannot add a column with values. You need to create it empty, and update all records to have the new vlaues.
        dataset.write.format('jdbc').options(**self.spark_sql_options).option('dbtable', self.dbtable).save(mode=self.mode)
        return dataset