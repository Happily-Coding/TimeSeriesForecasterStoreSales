from typing import Iterable, List, Type
from colorama import init
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCols
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, DataType
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
                                                                  
class ColumnSelector(Transformer):
    """A custom transformer that selects some columns from the original dataframe
    Its mostly useful to prevent errors 
    when adding a column that already exists but should be recreated from scratch.
    """
    def __init__(self, columns_to_select: List[str]|List[Column]|str|Column):
        super(ColumnSelector, self).__init__()
        self.columns_to_select = columns_to_select
    
    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.select(self.columns_to_select)
    
class ColumnDropper(Transformer):
    """A custom transformer that drops some columns from the original dataframe
    """
    def __init__(self, columns_to_drop: Iterable[str|Column]):
        super(ColumnDropper, self).__init__()
        self.columns_to_drop = columns_to_drop

    
    def _transform(self, dataset: DataFrame) -> DataFrame:
        for item in self.columns_to_drop:
            dataset = dataset.drop(item)
        return dataset



#MIGHT BE BEST TRANSFORMED INTO TWO STEPS, one unvectorizing, and one casting to another value if we want
#vector_to_float = F.udf(lambda x : float(x[0]),FloatType())
class VectorFirstValueExtractor(Transformer):
    """
    A custom transformer that transforms a column vector into the first value of each vector.
    Mostly meant to be used for unwrapping pointlesly wrapped columns into their real form.
    spark column type specification may be unnecesary
    
    Example usage:
    unvectorizer = VectorFirstValueExtractor([
        ('oil_price_scaled_0_to_1', float, FloatType())
        
    ]).fit(dataset).transform(dataset)
    """
    def __init__(self, columns_to_extract_with_type: Iterable[tuple[str, Type, DataType|str]]):
        super(VectorFirstValueExtractor, self).__init__()
        self.columns_to_extract_with_type = columns_to_extract_with_type
        
        self.columns_to_extract_with_type_extractor = []
        for column_name, column_python_type, column_spark_type in self.columns_to_extract_with_type:
            self.columns_to_extract_with_type_extractor.append((
                column_name,
                F.udf(lambda x : column_python_type(x[0]),column_spark_type)
            ))
    
    def _transform(self, dataset: DataFrame) -> DataFrame:
        for column_name, transformation_function in self.columns_to_extract_with_type_extractor:
            dataset = dataset.withColumn(
                column_name,
                transformation_function(column_name)
            ) 
        return dataset
    
class VectorToArrayTransformer(Transformer):
    """
    A custom transformer that transforms a vector into an array. 
    This or splitting in columns is necessary for storing vectors in a postgres sql database.
    This is usually prefered since in reality its a single feature, and otherwise the number of features climbs too much.
    """
    from pyspark.ml.functions import vector_to_array
    def __init__(self, columns_to_transform: Iterable[str]):
        super(VectorToArrayTransformer, self).__init__()
        self.columns_to_transform = columns_to_transform
    
    def _transform(self, dataset: DataFrame) -> DataFrame:
        for column_name in self.columns_to_transform:
            dataset = dataset.withColumn(
                column_name,
                vector_to_array(column_name) # type: ignore <-- Ignore type warning, since the method actually supports str type.
            ) 
        return dataset

""" 

transformed_oil_data_arr = transformed_oil_data.withColumn(
    "oil_price_vect", vector_to_array("oil_price_vect")
    )\
    .withColumn("oil_price_scaled_0_to_1", vector_to_array("oil_price_scaled_0_to_1")
    )
    
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
vector_to_float = F.udf(lambda x : float(x[0]),FloatType())
transformed_oil_data_single_values = transformed_oil_data.withColumn(
    "oil_price_vect", vector_to_float("oil_price_vect")
    )\
    .withColumn("oil_price_scaled_0_to_1", vector_to_float("oil_price_scaled_0_to_1")
    )
"""


def get_current_data_in_sql_table(spark:SparkSession, spark_sql_options:dict[str,str], table_name:str):
    """Get the data in a sql table as a lazy spark dataframe."""
    stored_data = spark.read.format('jdbc').options(**spark_sql_options).option('dbtable', table_name).load()
    return stored_data