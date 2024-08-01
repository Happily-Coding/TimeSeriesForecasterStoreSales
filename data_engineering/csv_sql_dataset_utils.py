"""Utility methods to deal with datasets that are obtained from csv and loaded into SQL"""
import pyspark
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame, Row as SparkRow
from csv_utils import read_filtered_csv
from sql_utils import get_last_sql_table_entry
from filter_utils import create_newer_rows_filter
from order_utils import create_desc_filter

from dataclasses import dataclass
@dataclass
class CsvSqlDatasetProperties():
    csv_file_path:str
    sql_table_name:str
    create_sql_table_script:str
    sql_col_csv_equivalents:dict[str,str]
    #columns_that_define_newness:list[str]
    #It could be reasoanble to have a list of column names as they are expected in the csv file, por make base_columns a dict
    #To ensure that if any unexpected column changes happen in the csv file no bugs occur.


    
def get_csv_new_rows(spark:SparkSession, spark_sql_options:dict[str,str], sql_table_name:str, columns_to_order_by:list[str], csv_file_path:str)-> SparkDataFrame:
    #Get a dataframe from the sql table. If there are entries in the table, it will contain the last one, otherwise it will be empty
    dataframe_with_possible_last_entry:SparkDataFrame = get_last_sql_table_entry(
        spark=spark,
        spark_sql_options=spark_sql_options,
        sql_table_name=sql_table_name,
        keys_to_order_sql_by=columns_to_order_by
    )
    sql_table_schema = dataframe_with_possible_last_entry.schema
    
    csv_data = spark.read.csv(
            csv_file_path,
            header=True, #Let spark know to ignore header
            schema=sql_table_schema
    )

    #If there are rows stored in sql, to find the new entries in the csv, filter the df.
    last_sql_entry_as_row:SparkRow|None = dataframe_with_possible_last_entry.first()
    if last_sql_entry_as_row !=None:
        newer_rows_filter = create_newer_rows_filter(
            newest_row=last_sql_entry_as_row,
            columns_to_filter_by=columns_to_order_by
        )
        csv_data = csv_data.filter(newer_rows_filter)

    return csv_data


def load_csv_datasets_new_data(spark, spark_sql_options:dict, csv_sql_dataset_properties:dict[str, CsvSqlDatasetProperties]) -> None:
    """For each csv data, get the new data and load it into sql """
    for dataset_name, dataset_properties in csv_sql_dataset_properties.items():
        new_dataset_data: SparkDataFrame = get_csv_new_rows(
            spark,
            spark_sql_options,
            dataset_properties.sql_table_name,
            dataset_properties.columns_that_define_newness,
            dataset_properties.csv_file_path
        )#.limit(100)
        new_dataset_data.write.mode('append').format('jdbc').options(**spark_sql_options).option('dbtable', dataset_properties.sql_table_name).save()