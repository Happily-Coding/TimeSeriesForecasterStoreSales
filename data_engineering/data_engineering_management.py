from dataclasses import dataclass
from re import U
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from psycopg import Connection
from typing import Iterable, cast, LiteralString
from csv_sql_dataset_utils import CsvSqlDatasetProperties
from feature_group_factory import FeatureGroup
from dataset_properties import get_sql_table_name_of_dataset_of_name
from csv_utils import get_csv_rows_skipping
from sql_utils import create_table_columns_if_not_exist, make_update_columns_with_values_statement, make_where_each_column_equals_values_statement
from df_utils import find_matching_rows, split_dataframe_sequentially
from train_test_splitting_utils import TrainTestSplit, make_kfold_train_test_splits
from itertools import chain
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline, Transformer
from spark_utils import ColumnDropper, ColumnSelector, VectorFirstValueExtractor
from pyspark.sql.types import FloatType
from typing import LiteralString

@dataclass
class EngineerableFeatureGroup:
    pipeline:Pipeline
    engineered_columns_names:list[str]
    engineered_columns_sql_definitions:list[str] #We could merge this and the following one into a tuple, but its probably not worth it or convenient
    identity_columns:list[str]
    sql_table_to_store_at:str

#Might want to have this and other similar ones in engineered feature groups.py or something maybe renamed to Engineerablefeature group or something
def create_oil_prices_feature_group() -> EngineerableFeatureGroup:
    base_column_selector = ColumnSelector(['date','oil_price']) #Could be a dropper for columns in output columns instead. or error handling if its possible for existing columns
    assembler = VectorAssembler(inputCols=['oil_price'],outputCol='oil_price_vect', handleInvalid='keep') #Make sure null oil prices dont prevent the pipeline from being fit
    scaler = MinMaxScaler(inputCol='oil_price_vect', outputCol="oil_price_scaled_0_to_1")
    unvectorizer = VectorFirstValueExtractor([
        ('oil_price_scaled_0_to_1', float, FloatType())
        
    ])
    dropper = ColumnDropper(['oil_price_vect']) #TODO 
    

    pipeline = Pipeline(stages=[base_column_selector, assembler, scaler, unvectorizer, dropper])
    
    return EngineerableFeatureGroup(
        pipeline,
        ['oil_price_vect', 'oil_price_scaled_0_to_1'],
        ['oil_price_vect FLOAT []', 'oil_price_scaled_0_to_1 FLOAT'],
        ['date'],
        get_sql_table_name_of_dataset_of_name('oil_price_by_date')
    )

def create_store_feature_group() -> EngineerableFeatureGroup:
    from pyspark.ml.feature import OneHotEncoder, StringIndexer
    
    cat_cols = ['city', 'state', 'type', 'cluster']
    
    names_of_cat_cols_turned_numeric_names = [f'{cat_col}_numeric' for cat_col in cat_cols]
    sql_definition_of_cat_cols_turned_numeric = [f'INTEGER {cat_col_numeric_name}' for cat_col_numeric_name in names_of_cat_cols_turned_numeric_names]
    
    names_of_cat_cols_ohe = [f'{cat_col}_ohe' for cat_col in cat_cols]
    sql_definitions_of_cat_cols_ohe = [f'LIST {cat_cols_ohe_name}' for cat_cols_ohe_name in names_of_cat_cols_ohe]
    
    #pyspark requires columns to be numerical in order to OHE them. Indexer assigns them a number based on frequency order.
    columns_indexer = StringIndexer(inputCols=cat_cols, outputCols=names_of_cat_cols_turned_numeric_names)
    ohe = OneHotEncoder(inputCols=names_of_cat_cols_turned_numeric_names, outputCols=names_of_cat_cols_ohe)
    pipeline = Pipeline(stages=[columns_indexer, ohe])
    
    return EngineerableFeatureGroup(
        pipeline,
        [*chain(names_of_cat_cols_turned_numeric_names, names_of_cat_cols_ohe)],
        [*chain(sql_definition_of_cat_cols_turned_numeric, sql_definitions_of_cat_cols_ohe)],
        ['id'],
        get_sql_table_name_of_dataset_of_name('stores'),
    )
    #CREO QUE EL ENGINEERED FEATURE GROUP DEBERIA TENER UN METODO PARA GUARDAR SU INFO. POR OTRO LADO podria tomar un componente que se encargue del storage como parametro y simplemente callearlo.

class DataEngineeringManager:
    def __init__(self, spark:SparkSession, spark_sql_options:dict[str,str], sql_connection:Connection):
        self.spark = spark
        self.spark_sql_options = spark_sql_options
        self.sql_connection = sql_connection

    #There are pipelines that generate a pyspark dataframe
    #Where they are created, they are called, and call a method to store their engineered columns in the dataframe with UPDATE, 
    #They also store the last date of engineering/pipeline version along with the last row that existed then , so that when they are next called they can verify the feature that wasnt engineered
    
    def get_current_data_in_sql_table(self, table_name:str):
        """Get the data in a sql table as a lazy spark dataframe."""
        stored_data = self.spark.read.format('jdbc').options(**self.spark_sql_options).option('dbtable', table_name).load()
        return stored_data

    def get_current_data_in_dataset_of_name(self, dataset_name):
        """Syntactic sugar for get_current)_data_in_sql_table, that gets the sql table name from the dataset properties config."""
        sql_table_name = get_sql_table_name_of_dataset_of_name(dataset_name)
        return self.get_current_data_in_sql_table(sql_table_name)
            
    #def create_feature_group_table_if_not_exists(self, feature_group:FeatureGroup)
            
    def update_columns(self, table_to_store_in:str, df_with_values_to_store:DataFrame, columns_to_store:Iterable[str], identity_columns:Iterable[str]):
        """
        Update a sql table, with the values provided in df_with_values_to_store.
        Identifies the rows to update with the value of the identity columns inside the df_with_values_to_store.
        TODO Most of the logic of this method could be reused and probably should be moved to sql_utils.
        """
        with self.sql_connection.cursor() as cursor: #Automatically close the cursor when done
            update_statement = make_update_columns_with_values_statement(table_to_store_in, columns_to_store)
            where_statement = make_where_each_column_equals_values_statement(identity_columns)
            full_statement = f'{update_statement} {where_statement}'
            
            #Prepare the list of columns we need, in the order we need it to replace %s correctly.
            names_of_columns_in_required_order = [*chain(columns_to_store, identity_columns)]
            
            #Get a view of the dataframe with the columns we need in the order we need them to replace %s
            df_with_values_to_store_in_order = df_with_values_to_store.select(names_of_columns_in_required_order)
            
            #Get the values used to replace the placeholders
            column_values = df_with_values_to_store_in_order.collect() #Might want to limit how much of the dataset to collect here
            
            #Execute the sql query to update the column values.
            cursor.executemany(full_statement, column_values)
            self.sql_connection.commit()

    def get_linked_train_test_split(self, linked_dataset_name:str, main_train_test_split:TrainTestSplit, matching_columns:dict[str,str]):
        """
        matching_columns: dict{col_of_dataset_to_filter:col_of_main_train_test_split}
        """
        linked_table_name = get_sql_table_name_of_dataset_of_name(linked_dataset_name)
        linked_table = self.get_current_data_in_sql_table(linked_table_name)
        
        train_matching_rows_of_linked_table = find_matching_rows(linked_table, main_train_test_split.train_data, matching_columns)
        test_matching_rows_of_linked_table = find_matching_rows(linked_table, main_train_test_split.test_data, matching_columns)

        linked_train_test_split = TrainTestSplit(
            f'{main_train_test_split.identifier}_{linked_table_name}',
            train_matching_rows_of_linked_table,
            test_matching_rows_of_linked_table
        )
        return linked_train_test_split
    
    def store_cross_val_sets(self, list_of_cross_val_folds_per_dataset_name:list[dict[str, TrainTestSplit]], entity_name:str):
        for fold_number, cross_val_folds_per_dataset_name in enumerate(list_of_cross_val_folds_per_dataset_name):
            for dataset_name, cross_val_folds in cross_val_folds_per_dataset_name.items():
                #Probably should be handled by a dataset writer object of a custom class instead. We could use postges instead, though we would need to get the creation script
                #Of the original dataset + all the modifications with the feature engineering, which would prove a bit complex and probably unnecesairy
                # We are yielding control of the creation details here by using spark.
                train_fold_storage_name = f'{entity_name}_train_cv_fold_{fold_number}_{dataset_name}' #We are just happily ignoring the fold string name.
                cross_val_folds.train_data.write.format('jdbc').options(**self.spark_sql_options).option('dbtable', train_fold_storage_name).save(mode='overwrite')
                
                test_fold_storage_name = f'test_cv_fold_{fold_number}_{dataset_name}'
                cross_val_folds.test_data.write.format('jdbc').options(**self.spark_sql_options).option('dbtable', test_fold_storage_name).save(mode='overwrite')
        
    def store_engineered_features(self, feature_group:EngineerableFeatureGroup, engineered_features:DataFrame):
        create_table_columns_if_not_exist(
            feature_group.sql_table_to_store_at,
            feature_group.engineered_columns_sql_definitions,
            self.sql_connection #Warning could be the wrong type since its a dict entry not a direct string.
        )
        self.update_columns(
            feature_group.sql_table_to_store_at,
            engineered_features,
            feature_group.engineered_columns_names,
            feature_group.identity_columns,
        )
        #could have error handling, but im not sure if we want it here tbh. probably best handled by parent.
        
        

    def engineer_features_and_store(self, feature_group:EngineerableFeatureGroup, dataset_for_pipeline:DataFrame):
        engineered_features = feature_group.pipeline.fit(dataset_for_pipeline).transform(dataset_for_pipeline)
        self.store_engineered_features(feature_group, engineered_features)

    def get_sales_cross_val_sets(self) -> list[dict[str, TrainTestSplit]]:
        """
        Make a crossvalidation set for the entity sale, and use it to also create the crossvalidation set for its dimensions.
        Its necessary since ML models should be trained exclusively on training data, be it of the fact or the dimension.
        """
        from pyspark.sql.functions import col
        full_sales_data = self.get_current_data_in_dataset_of_name(dataset_name='sales')
        full_sales_data_sequential_splits = split_dataframe_sequentially(full_sales_data, 'id', 5)
        sales_data_kfold_splits = make_kfold_train_test_splits(full_sales_data_sequential_splits, 'sales') #Will make splits named sales_1, sales_2 ...
        
        data_per_dataset_of_dataset_splits:list[dict[str,TrainTestSplit]] = []
        for sales_data_split in sales_data_kfold_splits:
            current_split_data_per_dataset:dict[str,TrainTestSplit] = {}
            
            current_split_data_per_dataset['sales_agg_by_date_store_productfamily'] = sales_data_split 

            #Se podrian mover estos metodos a dataset properties y que dejen de ser properties tal vez, aunque no se porque seria auto_referencial, y eso no se puede hacer
            #Aunque podria usar setters para que no sea auto referencial
            current_split_data_per_dataset['oil'] = self.get_linked_train_test_split(
                'oil',
                sales_data_split,
                {'date':'date'}
            )
            
            current_split_data_per_dataset['stores'] = self.get_linked_train_test_split(
                'stores',
                sales_data_split,
                {'id':'store_id'}
            )
            
            current_split_data_per_dataset['events_by_date'] = self.get_linked_train_test_split(
                'events_by_date',
                sales_data_split,
                {'date':'date'}
            )
            
            current_split_data_per_dataset['transactions_agg_by_date_store'] = self.get_linked_train_test_split(
                'events_by_date',
                sales_data_split,
                {'date':'date'}
            )
            
            data_per_dataset_of_dataset_splits.append(current_split_data_per_dataset)
        
        return data_per_dataset_of_dataset_splits
        
        
    def engineer_and_store_all_features(self):
        current_oil_dataset = self.get_current_data_in_dataset_of_name('oil_prices')
        self.engineer_features_and_store(create_oil_prices_feature_group(), current_oil_dataset)
        
        current_stores_dataset = self.get_current_data_in_dataset_of_name('stores')
        self.engineer_features_and_store(create_store_feature_group(), current_stores_dataset)
        
        #... same for all datasets, again, would make sense to have engineer and store be a feature of dataset properties,
        # the only reason for it not to be is to be able to handle different storage uses, etc, which would require passing those to the dataset properties creation
        #Though it would kind of make sense if they are datasets . TODO consider.
        
        sales_fact_dims_dataset_folds: list[dict[str, TrainTestSplit]] = self.get_sales_cross_val_sets()
        self.store_cross_val_sets(sales_fact_dims_dataset_folds, 'sales_agg_by_date_store_productfamily') #Maybe should get the table name
        #Each crossvalset could just have a method to store its own data. Not doing it allows easier centralization of the way of getting and storing the data though we could have all of them call a single method and just chance the implementation of that one if we want something else.
        

#TODO Make callable methods to engiener each feature group
        
        
        
    