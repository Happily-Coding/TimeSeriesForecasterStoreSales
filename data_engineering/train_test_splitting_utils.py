from dataclasses import dataclass
from pyspark.sql.dataframe import DataFrame
from df_utils import concat_spark_dfs, find_matching_rows
from spark_interfacing import SparkInterface

@dataclass
class TrainTestSplit:
    """
    A fragmentation of the dataset in train and test data.
    Used for example to represent one of the kfolds produced by kfold splitting for kfold crossvalidation.
    """
    original_table_name:str
    general_split_type:str
    fold_id:str|object
    train_data: DataFrame
    test_data: DataFrame
    
    def _get_identifier(self, split_part_name):
        return f'{self.original_table_name}_{self.general_split_type}_{split_part_name}_{self.fold_id}'
    
    def get_train_identifier(self):
        """
        Get an identifier for the train fragment of this split, which includes the table_name, split type, split_id, and 'train' word.
        Its the suggested table name for a train test split
        """
        return self._get_identifier('train')
    
    def get_test_identifier(self):
        """
        Get an identifier for the test fragment of this split, which includes the table_name, split type, split_id, and 'test' word.
        Its the suggested table name for a train test split
        """
        return self._get_identifier('test')
    
def make_linked_data_split(dimension_table_name, dimension_table, main_split:TrainTestSplit, linked_keys) ->TrainTestSplit:
    return TrainTestSplit(
        dimension_table_name,
        main_split.general_split_type,
        main_split.fold_id,
        find_matching_rows(dimension_table, main_split.train_data, linked_keys),
        find_matching_rows(dimension_table, main_split.test_data, linked_keys),
    )
    
    
def make_kfold_train_test_splits(sequentially_split_dataset:list[DataFrame], dataset_name:str) -> list[TrainTestSplit]:
    """Makes Train, Test Kfolds from a sequentially split dataset
    see scikit-learn.org/stable/auto_examples/model_selection/plot_cv_indices.html#visualize-cross-validation-indices-for-many-cv-objects
    """
    k_fold_train_test_splits:list[TrainTestSplit] = []

    for split_number in range(len(sequentially_split_dataset)):
        dataset_splits_to_assign = sequentially_split_dataset.copy()
        test_split = dataset_splits_to_assign.pop(split_number)
        train_splits = dataset_splits_to_assign
        train_split = concat_spark_dfs(train_splits)
        k_fold_train_test_split = TrainTestSplit(dataset_name, 'kfold',split_number, train_split, test_split)
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
