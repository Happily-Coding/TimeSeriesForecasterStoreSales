"""A utility class for creating string filters, that can be used for spark dataframes"""
from pyspark.sql import SparkSession, Row as SparkRow

def create_newer_rows_filter(newest_row:SparkRow|dict, columns_to_filter_by:list[str]):
    """Creates a string filter checking that each column in columns_to_filter_by has a descending value newer than the value of newewst_row"""
    filter_string_per_column = []
    for column_to_filter_by in columns_to_filter_by:
        filter_string_of_column = f"({column_to_filter_by} > '{newest_row[column_to_filter_by]}')"
        filter_string_per_column.append(filter_string_of_column)

    full_filter = ' AND '.join(filter_string_per_column)
    return full_filter
