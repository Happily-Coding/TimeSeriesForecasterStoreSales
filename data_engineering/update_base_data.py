from base_data_management import BaseDataManager
from db_connector import spark, data_engineering_connection, spark_sql_options

base_data_manager = BaseDataManager(spark, spark_sql_options, data_engineering_connection)
base_data_manager.update_all_base_data()

#create_last_row_table_query = f'''
#CREATE TABLE IF NOT EXISTS last_loaded_rows (
#    table_name TEXT UNIQUE NOT NULL,
#    row INTEGER NOT NULL
#);
#COMMENT ON TABLE last_loaded_rows IS 'The last row of the csv file that was read by spark'
#'''