from dataclasses import dataclass
from pkg_resources import _ProviderFactoryType
from pyspark.sql.dataframe import DataFrame
from typing import Iterable
from dataset_properties import get_sql_table_name_of_dataset_of_name
from sql_utils import create_table_columns_if_not_exist, make_update_columns_with_values_statement, make_where_each_column_equals_values_statement
from df_utils import find_matching_rows, split_dataframe_sequentially
from train_test_splitting_utils import TrainTestSplit, make_kfold_train_test_splits
from itertools import chain
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline
from spark_utils import ColumnDropper, ColumnSelector, VectorFirstValueExtractor, get_current_data_in_sql_table
from pyspark.sql.types import FloatType
from db_interfacing import DBInterface 
from spark_interfacing import SparkInterface
import db_interfacing
import spark_interfacing

@dataclass
class FeatureGroupMaterializerImplementation:
    identity_columns:list[str]
    pipeline:Pipeline
    engineered_columns_names:list[str]
    engineered_columns_sql_definitions:list[str] #We could merge this and the following one into a tuple, but its probably not worth it or convenient
    sql_table_to_store_at:str
    
    def make_features(self, base_dataset:DataFrame):
        return self.pipeline.fit(base_dataset).transform(base_dataset)

class FeatureGroupStorageHandlerImplementation:
    def __init__(
        self,
        feature_group_table_name:str,
        feature_group_columns_to_store:Iterable[str],
        engineered_columns_definition:list[str],
        identity_columns:list[str],
        source_dataset_table_name:str, #May want to change for a component that is a source dataset provider.
        db_interface:DBInterface,
        spark_interface:SparkInterface
    ):
        self.table_to_store_in_name = feature_group_table_name
        self.columns_to_store = feature_group_columns_to_store
        self.identity_columns = identity_columns
        self.engineered_columns_definition = engineered_columns_definition
        self.source_dataset_table_name = source_dataset_table_name
        self.db_interface = db_interface
        self.spark_interface = spark_interface

    def get_current_data_in_source_storage(self):
        return self.spark_interface.get_current_data_in_sql_table(self.source_dataset_table_name)

    def get_current_data_in_target_storage(self):
        return self.spark_interface.get_current_data_in_sql_table(self.table_to_store_in_name)
    
    def update_columns(self, df_with_values_to_store:DataFrame):
        """
        Update a sql table, with the values provided in df_with_values_to_store.
        Identifies the rows to update with the value of the identity columns inside the df_with_values_to_store.
        TODO Most of the logic of this method could be reused and probably should be moved to sql_utils.
        """
        update_statement = make_update_columns_with_values_statement(self.table_to_store_in_name, self.columns_to_store)
        where_statement = make_where_each_column_equals_values_statement(self.identity_columns)
        full_statement = f'{update_statement} {where_statement}'
        
        #Prepare the list of columns we need, in the order we need it to replace %s correctly.
        names_of_columns_in_required_order = [*chain(self.columns_to_store, self.identity_columns)]
        
        #Get a view of the dataframe with the columns we need in the order we need them to replace %s
        df_with_values_to_store_in_order = df_with_values_to_store.select(names_of_columns_in_required_order)
        
        #Get the values used to replace the placeholders
        column_values = df_with_values_to_store_in_order.collect() #Might want to limit how much of the dataset to collect here
        
        #Execute the sql query to update the column values.
        self.db_interface.execute_multi_valued_query(full_statement, column_values)


    def get_values_for_linked_train_test_split(self, linked_table:DataFrame, linked_table_name:str, main_train_test_split:TrainTestSplit, matching_columns:dict[str,str]):
        """
        matching_columns: dict{col_of_dataset_to_filter:col_of_main_train_test_split}
        Gets a train dataset, and test dataset for this table, based on the values of the identity columns in the train and test sets of another dataset.
        Meant to be used when you have a main entity, and a feature group of the dimensions, and want to create a train test split of the main entity
        Which will of course require the same division on the dimension.
        """
        train_matching_rows_of_linked_table = find_matching_rows(linked_table, main_train_test_split.train_data, matching_columns)
        test_matching_rows_of_linked_table = find_matching_rows(linked_table, main_train_test_split.test_data, matching_columns)

        linked_train_test_split = TrainTestSplit(
            f'{main_train_test_split.identifier}_{linked_table_name}',
            train_matching_rows_of_linked_table,
            test_matching_rows_of_linked_table
        )
        return linked_train_test_split

    def store_engineered_features(self, engineered_features:DataFrame):
        self.db_interface.create_table_columns_if_not_exist(
            self.table_to_store_in_name,
            self.engineered_columns_definition,
        )
        self.update_columns(
            engineered_features
        )

    def store_cross_val_sets(self, list_of_cross_val_folds_per_dataset_name:list[dict[str, TrainTestSplit]], entity_name:str):
        for fold_number, cross_val_folds_per_dataset_name in enumerate(list_of_cross_val_folds_per_dataset_name):
            for dataset_name, cross_val_folds in cross_val_folds_per_dataset_name.items():
                #Probably should be handled by a dataset writer object of a custom class instead. We could use postges instead, though we would need to get the creation script
                #Of the original dataset + all the modifications with the feature engineering, which would prove a bit complex and probably unnecesairy
                # We are yielding control of the creation details here by using spark.
                train_fold_storage_name = f'{entity_name}_train_cv_fold_{fold_number}_{dataset_name}' #We are just happily ignoring the fold string name.
                #Could be moved to spark_utils/interface, but tbh doesnt make much sense. nvm it kinda does TODO do , because you are worring about the implementation of the save place
                #With the current approach.
                cross_val_folds.train_data.write.format('jdbc').options(**self.spark_interface.spark_sql_options).option('dbtable', train_fold_storage_name).save(mode='overwrite')
                
                test_fold_storage_name = f'test_cv_fold_{fold_number}_{dataset_name}'
                cross_val_folds.test_data.write.format('jdbc').options(**self.spark_interface.spark_sql_options).option('dbtable', test_fold_storage_name).save(mode='overwrite')

    

@dataclass
class FeatureGroup:
    feature_group_materializer:FeatureGroupMaterializerImplementation
    feature_storage:FeatureGroupStorageHandlerImplementation
    
    def engineer_features_and_store(self):
        dataset_for_pipeline = self.feature_storage.get_current_data_in_source_storage()
        engineered_features = self.feature_group_materializer.pipeline.fit(dataset_for_pipeline).transform(dataset_for_pipeline)
        self.feature_storage.store_engineered_features(engineered_features)
        
def make_standard_feature_group(
    source_dataset_table_name:str, #May want to change for a component that is a source dataset provider.
    identity_columns:list[str],
    pipeline:Pipeline,
    engineered_columns_names:list[str],
    engineered_columns_sql_definitions:list[str], #We could merge this and the following one into a tuple, but its probably not worth it or convenient
    feature_group_storage_table_name:str,
    db_interface:DBInterface,
    spark_interface:SparkInterface
) -> FeatureGroup:
    """A shorthand method to make a feature group using this project's standard implementation for materializing and storage with pyspark and the db_interface."""
    feature_maker = FeatureGroupMaterializerImplementation(
        identity_columns,
        pipeline,
        engineered_columns_names,
        engineered_columns_sql_definitions,
        feature_group_storage_table_name
    ) 
    feature_storer = FeatureGroupStorageHandlerImplementation(
        feature_group_storage_table_name,
        engineered_columns_names,
        engineered_columns_sql_definitions,
        identity_columns,
        source_dataset_table_name,
        db_interface,
        spark_interface
    )
    feature_group = FeatureGroup(
        feature_maker,
        feature_storer
    )
    return feature_group
        
class DataEngineeringManager:
    def __init__(self, db_interface:DBInterface, spark_interface:SparkInterface):
        self.db_interface= db_interface
        self.spark_interface = spark_interface
        
    def make_standard_feature_group(
        self,
        source_dataset_table_name:str, #May want to change for a component that is a source dataset provider.
        identity_columns:list[str],
        pipeline:Pipeline,
        engineered_columns_names:list[str],
        engineered_columns_sql_definitions:list[str], #We could merge this and the following one into a tuple, but its probably not worth it or convenient
        feature_group_storage_table_name:str,
    ) -> FeatureGroup:
        return make_standard_feature_group(
        source_dataset_table_name, #May want to change for a component that is a source dataset provider.
        identity_columns,
        pipeline,
        engineered_columns_names,
        engineered_columns_sql_definitions, #We could merge this and the following one into a tuple, but its probably not worth it or convenient
        feature_group_storage_table_name,
        self.db_interface,
        self.spark_interface
        )


    #Might want to have this and other similar ones in engineered feature groups.py or something maybe renamed to Engineerablefeature group or something
    def create_oil_prices_feature_group(self) -> FeatureGroup:
        base_column_selector = ColumnSelector(['date','oil_price']) #Could be a dropper for columns in output columns instead. or error handling if its possible for existing columns
        assembler = VectorAssembler(inputCols=['oil_price'],outputCol='oil_price_vect', handleInvalid='keep') #Make sure null oil prices dont prevent the pipeline from being fit
        scaler = MinMaxScaler(inputCol='oil_price_vect', outputCol="oil_price_scaled_0_to_1")
        unvectorizer = VectorFirstValueExtractor([
            ('oil_price_scaled_0_to_1', float, FloatType())
            
        ])
        dropper = ColumnDropper(['oil_price_vect'])
        pipeline = Pipeline(stages=[base_column_selector, assembler, scaler, unvectorizer, dropper])
        
        oil_feature_group = self.make_standard_feature_group(
            'oil_price_by_date',#get_sql_table_name_of_dataset_of_name('oil_price_by_date')
            ['date'],
            pipeline,
            ['oil_price_scaled_0_to_1'],
            [ 'oil_price_scaled_0_to_1 FLOAT'],
            'oil_price_by_date'
        )
        
        return oil_feature_group


    def create_store_feature_group(self) -> FeatureGroup:
        cat_cols = ['city', 'state', 'type', 'cluster']
        
        names_of_cat_cols_turned_numeric_names = [f'{cat_col}_numeric' for cat_col in cat_cols]
        sql_definition_of_cat_cols_turned_numeric = [f'INTEGER {cat_col_numeric_name}' for cat_col_numeric_name in names_of_cat_cols_turned_numeric_names]
        
        names_of_cat_cols_ohe = [f'{cat_col}_ohe' for cat_col in cat_cols]
        sql_definitions_of_cat_cols_ohe = [f'LIST {cat_cols_ohe_name}' for cat_cols_ohe_name in names_of_cat_cols_ohe]
        
        #pyspark requires columns to be numerical in order to OHE them. Indexer assigns them a number based on frequency order.
        columns_indexer = StringIndexer(inputCols=cat_cols, outputCols=names_of_cat_cols_turned_numeric_names)
        ohe = OneHotEncoder(inputCols=names_of_cat_cols_turned_numeric_names, outputCols=names_of_cat_cols_ohe)
        pipeline = Pipeline(stages=[columns_indexer, ohe])
        
        return make_standard_feature_group(
            'stores',#get_sql_table_name_of_dataset_of_name('stores')
            ['id'],
            pipeline,
            [*chain(names_of_cat_cols_turned_numeric_names, names_of_cat_cols_ohe)],
            [*chain(sql_definition_of_cat_cols_turned_numeric, sql_definitions_of_cat_cols_ohe)],
            'stores',#get_sql_table_name_of_dataset_of_name('stores')
            self.db_interface,
            self.spark_interface
        )


    def engineer_and_store_all_features(self):
        feature_groups = [
            self.create_oil_prices_feature_group(),
            self.create_store_feature_group()
        ]
        for feature_group in feature_groups:
            feature_group.engineer_features_and_store()
        
        #Create cross_validation table.
        #Apply feature engineering to that table. Simply add a parameter to feature_group_maker?
        
        #make_sales_entity
        #full_sales_data = self.get_current_data_in_dataset_of_name(dataset_name='sales')
        #full_sales_data_sequential_splits = split_dataframe_sequentially(full_sales_data, row_number_col='id', number_of_splits=5)
        #sales.entity.make_and_store_cross_val_sets(df_splits)
            #make_cross_val
        
        #TODO: Instead, make an entity or data_view class in that represents sales, and its related tables, and has the characteristics and methods to create and store them.
        #Keep in mind it will not be related to feature groups directly since it needs all features.
        #At the same time it will be related to feature groups since they will need to be re-created for each split.
        #full_sales_data = self.get_current_data_in_dataset_of_name(dataset_name='sales')
        #full_sales_data_sequential_splits = split_dataframe_sequentially(full_sales_data, row_number_col='id', number_of_splits=5)
        #sales_data_kfold_splits = make_kfold_train_test_splits(sequentially_split_dataset=full_sales_data_sequential_splits, split_base_name='sales') #Will make splits named sales_1, sales_2 ...

data_engineering_manager:DataEngineeringManager = DataEngineeringManager(db_interfacing.db_interface, spark_interfacing.spark_interface)


class PysparkTableProvider():
    """A provider for data in a table
    This particular implementation returns data in a pyspark dataframe.
    """
    def __init__(
        self, 
        name_of_table_to_provide_for:str,
        spark_interface:SparkInterface
    ):
        self.name_of_table_to_provide_for = name_of_table_to_provide_for
        self.spark_interface = spark_interface
        
    def get_data(self) ->DataFrame:
        return self.spark_interface.get_current_data_in_sql_table(self.name_of_table_to_provide_for)
        

LINKED DIMENSION ISNT CORRECTLY IMPLEMENTED, it shouldnt take feature groups, it should just take a table provider. 
In this case it should be a provider for the dimension table. The reason is that feature group wouldnt have all the columns.
We could likely use just a pysparktable provider with the dimension name.
class LinkedDimension():
    """
        A dimension linked to an entity
        linked_keys: a dict of key_from_the_dimension:key in the entity.
    """
    def __init__(self, dimensional, linked_keys:dict[str,str]):
        self.dimensional_feature_group = dimensional_feature_group
        self.linked_keys = linked_keys
        
    def get_cross_validation_set(self, entity_cross_val_set:TrainTestSplit):
        #linked_table:DataFrame, linked_table_name:str, main_train_test_split:TrainTestSplit, matching_columns:dict[str,str]
        train_matching_rows_of_linked_table = find_matching_rows(linked_table, main_train_test_split.train_data, matching_columns)
        test_matching_rows_of_linked_table = find_matching_rows(linked_table, main_train_test_split.test_data, matching_columns)

        linked_train_test_split = TrainTestSplit(
            f'{main_train_test_split.identifier}_{linked_table_name}',
            train_matching_rows_of_linked_table,
            test_matching_rows_of_linked_table
        )
        return linked_train_test_split
        

class Entity():
    """
    An entity, which has base data in a table, and additional data found in other tables.
    Since an entity is a unified state of info from multiple tables, when generating a cross_validation_set, all tables need to respect the same split.
    As of right now the main point of the entity is to be able to create and store crossvalidation sets.
    """
    
    def __init__(
        self,
        main_table_provider:PysparkTableProvider,
        main_table_number_id_cols:str,
        #dimensions_with_link:tuple[FeatureGroup,dict[str,str]],
        linked_dimensions:list[LinkedDimension]
    ):
        self.main_table_number_id_cols = main_table_number_id_cols
        self.main_table_provider = main_table_provider
        self.linked_dimensions = linked_dimensions
        
    def make_main_cross_validation_sets(self):
        """A hard coded implementation to make cross_validation_splits. could be any generic component capable of producing splits"""
        data = self.main_table_provider.get_data()
        splits  = split_dataframe_sequentially(
            data,
            self.main_table_number_id_cols,
            5
        )
        return splits
    
    def get_linked_cross_validation_sets(self, cross_validation_sets):
        for linked_dimension in linked_dimensions:
            get_cross_validation_set

    def get_cross_validation_set():
        

#TODO: Refactor. Should probably belong to a unifier entity class, which has a dictionary indicating which datasets it relates too or something.
#def get_sales_cross_val_sets(self) -> list[dict[str, TrainTestSplit]]:
"""
Make a crossvalidation set for the entity sale, and use it to also create the crossvalidation set for its dimensions.
Its necessary since ML models should be trained exclusively on training data, be it of the fact or the dimension.
"""
"""                 
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
"""