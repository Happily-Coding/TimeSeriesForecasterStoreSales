from dataclasses import dataclass
from pyspark.sql.dataframe import DataFrame
from df_utils import concat_spark_dfs

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