from pyspark.sql.dataframe import DataFrame
from typing import cast, Dict, LiteralString
import sql_utils

class FeatureStorer():

    def __init__(self, connection_to_use):
        self.connection_to_use = connection_to_use
        

    def store_features(self, dataset:DataFrame, table_to_store_in:str, column_types:dict[str,str]):
        #Regretably you cannot add a column with values. You need to create it empty, and update all records to have the new vlaues.
        try:
            #Make sure all columns exist.
            for column_name, column_type in column_types.items():
                sql_utils.execute_query(f'ALTER TABLE {table_to_store_in} ADD COLUMN IF NOT EXISTS {column_name} {column_type}', self.connection_to_use) #Could probably all add columns be concatenated
            
            #THERE WILL NOT BE ANY ENTRIES AT FIRST< WILLL UPDATE WORK? OR SHOULD WE JUST USE DROP TABLE IF EXISTS AND CREATE TABLE AND INSERT INTO?
            #NO! Reason: You'd loose the base features which in some cases may not be recoverable. Droping feature columns shouldnt be a problem but it may not be so easy to distinguish them from crafted features currently.
            #This is specially cause some of them should be inserted into the main table.
            #Column types should indicate which columns to drop and re_insert, so it should be ok.
                
            
            #Update the database with the current values of the column in the dataframe
            with self.connection_to_use.cursor() as cursor: #Automatically close the cursor when done
                #The update query needs to look like this: UPDATE oil_prices SET date = '2013-01-01', dcoilwtico = 1.0, row_number = 1
                
                update_query = cast(LiteralString, f"UPDATE {table_to_store_in} SET ({self.col_to_store}) = %s {self.condition_for_storage}")
                column_values = dataset.select(self.col_to_store).collect() #Might want to limit how much of the dataset to collect here
                print(f'update_query: {update_query}, column_values: {column_values}')
                cursor.executemany(update_query, column_values)
                self.connection_to_use.commit()
        except Exception as e:
            print(f'There was an error storing a database feature. Continuing without saving. Error: \n {e}')
            self.connection_to_use.rollback()
            
                #POSIBLE PROBLEMA SI QUIERO GUARDAR SOLO ALGUNAS COLUMNAS: SI NO MANTENGO LA ROW NUMBER COLUMN SPARK PUEDE LLEGAR A METER LAS COSAS EN DISTINTO ORDEN QUE EL QUE PUSO LAS ORIGINALES>