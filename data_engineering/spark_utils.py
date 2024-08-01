from typing import Iterable, List
from colorama import init
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCols
from pyspark.sql import DataFrame, Column
                                                                  
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
        