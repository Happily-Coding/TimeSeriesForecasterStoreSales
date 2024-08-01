from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

def read_filtered_csv(spark:SparkSession, csv_path:str, schema, filter_string:str, has_header_to_ignore:bool=True)-> SparkDataFrame:
    filtered_csv:SparkDataFrame = (
        spark.read.csv(
            csv_path,
            header=True, #Let spark know to ignore header
            schema=schema
        )
        .filter(filter_string)
    )
    return filtered_csv


def get_csv_rows_skipping(spark:SparkSession, file_path:str, rows_to_skip:int, column_separator=',') -> RDD[str]:
    """
    My most efficient implementation for getting rows from a csv file by skipping rows
    Reads the csv lines packing them with a row number, uses that number to filter, and then returns the original row
    The biggest limitation is that you
    """
    #Read the csv as a text file
    unfiltered_csv_rows = spark.sparkContext.textFile(file_path)
    
    if rows_to_skip == 0:
        return unfiltered_csv_rows
    
    csv_rows = (
        unfiltered_csv_rows
        .zipWithIndex() #Make each row a tuple of row_value, row_number
        .filter(lambda row: row[1] >= rows_to_skip) #Filter rows with more row number
        .map(lambda row_row_number_tuple: row_row_number_tuple[0])
    )
    return csv_rows


def get_csv_rows_numbered_skipping(spark:SparkSession, file_path:str, rows_to_skip:int, column_separator=',') -> RDD[str]:
    """
    Returns the rows of a csv  skipping rows until row number reaches a certain number. 
    Its my most efficient implementation for getting rows from a csv file skipping rows, and keeping the row number. 
    Read a csv as text lines using rdd synthax.
    During the rdd stage add a column number to each row, and skip rows based on that number. 
    Could be more efficient that get_numbered_csv_rows, since you filter before calling to csv, but could not make a diference since spark is lazy, and its less clean imo.
    Considering obth results look the same when using header=True in the csv dataframe creation, i'd wager there shouldnt be a difference, but there is. its faster.
    """
    csv_rows = (
        spark
        .sparkContext
        .textFile(file_path) #Read the csv as a text file
        .zipWithIndex() #Make each row a tuple of row_value, row_number
        .filter(lambda row: row[1] >= rows_to_skip) #Filter rows with more row number
        .map(lambda row_row_number_tuple: f'{row_row_number_tuple[1]}{column_separator}{row_row_number_tuple[0]}') #Add 
            #Row.fromSeq(row.toSeq() + row_number) ) #Make the rows just the row value again.
    )
    return csv_rows

def get_csv_rows_numbered(spark:SparkSession, file_path:str, column_separator=',') -> RDD[str]:
    """Return the rows of a csv file, with an additional column at the start, that is the number the row was at the csv
       The additional column will start from 0 but that value will be assigned to the header row if there is any
       It shouldn't be used if you need to filter based on row number, since you can do it more efficiently with get_csv_rows_numbered_skipping
    """
    csv_rows = (
        spark
        .sparkContext
        .textFile(file_path) #Read the csv as a text file
        .zipWithIndex() #Make each row a tuple of row_value, row_number
        .map(lambda row_row_number_tuple: f'{row_row_number_tuple[1]}{column_separator}{row_row_number_tuple[0]}')
    )
    return csv_rows
