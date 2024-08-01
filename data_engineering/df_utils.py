from pyspark.sql.dataframe import DataFrame
from functools import reduce

def concat_spark_dfs(*dfs):
    """takes all the objects that you passed as parameters and reduces with python reduce them using unionAll"""
    return reduce(DataFrame.unionAll, dfs)

def find_matching_rows(df_to_filter:DataFrame, reference_df:DataFrame,  matching_columns:dict[str,str]):
    from pyspark.sql.functions import col
    for main_df_column, linked_df_column in matching_columns.items():
        df_to_filter = df_to_filter.filter(col(main_df_column).isin(reference_df[linked_df_column]))
    
    return df_to_filter

def split_dataframe_sequentially(df:DataFrame, row_number_col:str, number_of_splits:int) -> list[DataFrame]:
    total_rows = df.count()
    rows_per_split = total_rows // number_of_splits
    
    df_splits:list[DataFrame] = []
    for number_of_split in range(number_of_splits):
        split_start = rows_per_split * number_of_split
        split_end = rows_per_split * (number_of_split + 1)
        split_df = df.select(row_number_col).filter(f'({row_number_col} >= {split_start}) & ({row_number_col} < {split_end})')
        df_splits.append(split_df)

    return df_splits