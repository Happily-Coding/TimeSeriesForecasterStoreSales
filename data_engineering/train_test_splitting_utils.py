from dataclasses import dataclass
from pyspark.sql.dataframe import DataFrame
from df_utils import concat_spark_dfs
from spark_interfacing import SparkInterface

@dataclass
class TrainTestSplit:
    """A class to group an identifier, train data, and test data."""
    identifier: str
    train_data: DataFrame
    test_data: DataFrame
    

def make_kfold_train_test_splits(sequentially_split_dataset:list[DataFrame], split_base_name:str) -> list[TrainTestSplit]:
    """Makes Train, Test Kfolds from a sequentially split dataset
    see scikit-learn.org/stable/auto_examples/model_selection/plot_cv_indices.html#visualize-cross-validation-indices-for-many-cv-objects
    """
    k_fold_train_test_splits:list[TrainTestSplit] = []

    for split_number in range(len(sequentially_split_dataset)):
        dataset_splits_to_assign = sequentially_split_dataset.copy()
        test_split = dataset_splits_to_assign.pop(split_number)
        train_splits = dataset_splits_to_assign
        train_split = concat_spark_dfs(train_splits)
        k_fold_train_test_split = TrainTestSplit(f'{split_base_name}_{split_number}', train_split, test_split)
        k_fold_train_test_splits.append(k_fold_train_test_split)
    
    return k_fold_train_test_splits

class SparkTableStorageHandler():
    """a storage handler that uses pyspark to store a table.
       mostly useful for tables that need to be overwritten for storage.
    """
    def __init__(self, spark_interface:SparkInterface, save_mode:str='overwrite'):
        self.spark_interface = spark_interface
        self.save_mode = save_mode
        
    def store_table(self, dataframe:DataFrame, table_name:str):
        dataframe.write.format('jdbc').options(**self.spark_interface.spark_sql_options).option('dbtable', table_name).save(mode=self.save_mode)

class TrainTestSplitDataset():
    """A storable train test split """
    
    def __init__(self, train_test_split:TrainTestSplit, train_storer:SparkTableStorageHandler, test_storer:SparkTableStorageHandler):
        
    
    def store(self):
        train_storer.
