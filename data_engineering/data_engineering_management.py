from dataclasses import dataclass
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
class FeatureGroup:
    """A group of features that can be made and stored."""
    pipeline: Pipeline
    name_of_table_to_store_features_in:str #TODO replace by table provider
    name_of_features_to_store:Iterable[str]
    engineered_columns_definition:list[str]
    identity_columns:list[str]
    source_dataset_table_name:str #May want to change for a component that is a source dataset provider.
    db_interface:DBInterface
    spark_interface:SparkInterface
    
    def make_features(self, base_dataset:DataFrame):
        return self.pipeline.fit(base_dataset).transform(base_dataset)
    
    def engineer_features_and_store(self):
        dataset_for_pipeline = self.get_current_data_in_source_storage()
        engineered_features = self.make_features(dataset_for_pipeline)
        self.store_engineered_features(engineered_features)

    def get_current_data_in_source_storage(self):
        return self.spark_interface.get_current_data_in_sql_table(self.source_dataset_table_name)

    def get_current_data_in_target_storage(self):
        return self.spark_interface.get_current_data_in_sql_table(self.name_of_table_to_store_features_in)
    
    def update_columns(self, df_with_values_to_store:DataFrame):
        """
        Update a sql table, with the values provided in df_with_values_to_store.
        Identifies the rows to update with the value of the identity columns inside the df_with_values_to_store.
        TODO Most of the logic of this method could be reused and probably should be moved to sql_utils.
        """
        update_statement = make_update_columns_with_values_statement(self.name_of_table_to_store_features_in, self.name_of_features_to_store)
        where_statement = make_where_each_column_equals_values_statement(self.identity_columns)
        full_statement = f'{update_statement} {where_statement}'
        
        #Prepare the list of columns we need, in the order we need it to replace %s correctly.
        names_of_columns_in_required_order = [*chain(self.name_of_features_to_store, self.identity_columns)]
        
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
            self.name_of_table_to_store_features_in,
            self.engineered_columns_definition,
        )
        self.update_columns(
            engineered_features
        )

class KFoldSplitDataset():
    """A dataset representing all the kfold splits of a base table"""
    def __init__(self, source_table_name:str, target_table_base_name:str, row_number_col, spark_interface:SparkInterface):
        self.source_table_name = source_table_name
        self.target_table_base_name = target_table_base_name
        self.row_number_col :str = row_number_col
        self.spark_interface = spark_interface
        
    def get_full_base_data(self) ->DataFrame:
        return self.spark_interface.get_current_data_in_sql_table('sales')
    
    def get_sequential_dataset_parts(self) -> list[DataFrame]:
        return split_dataframe_sequentially(self.get_full_base_data(), self.row_number_col, number_of_splits=5)
    
    def get_train_test_splits(self) ->list[TrainTestSplit]:
        return make_kfold_train_test_splits(self.get_sequential_dataset_parts(), split_base_name=self.source_table_name) #Will make splits named source_table_name_1, source_table_name_2 ...

    def store_this_splits(self, splits:list[TrainTestSplit]) -> None:
        for split in splits: #Train test split should probably handle the logic to store, with a method store.
            train_storage_table_name = f'{split.identifier}_train'
            split.train_data.write.format('jdbc').options(**self.spark_interface.spark_sql_options).option('dbtable', train_storage_table_name).save(mode='overwrite')
            
            test_storage_table = f'{split.identifier}_test'
            split.test_data.write.format('jdbc').options(**self.spark_interface.spark_sql_options).option('dbtable', test_storage_table).save(mode='overwrite')

    def store_splits(self) ->None:
        self.store_this_splits(self.get_train_test_splits())

        
class DataEngineeringManager:
    def __init__(self, db_interface:DBInterface, spark_interface:SparkInterface):
        self.db_interface= db_interface
        self.spark_interface = spark_interface
        
    def _make_feature_group(
        self,
        source_dataset_table_name:str, #May want to change for a component that is a source dataset provider.
        identity_columns:list[str],
        pipeline:Pipeline,
        engineered_columns_names:list[str],
        engineered_columns_sql_definitions:list[str], #We could merge this and the following one into a tuple, but its probably not worth it or convenient
        feature_group_storage_table_name:str,
    ) -> FeatureGroup:
        """Makes a feature group passing data engineering manager's db and spark interface"""
        return FeatureGroup(
        pipeline,
        source_dataset_table_name, #May want to change for a component that is a source dataset provider.
        engineered_columns_names,
        engineered_columns_sql_definitions, #We could merge this and the following one into a tuple, but its probably not worth it or convenient
        identity_columns,
        feature_group_storage_table_name,
        self.db_interface,
        self.spark_interface
        )
    
    def _make_split_feature_group(
        self,
        original_feature_group:FeatureGroup, 
        split_id:str,
    ) -> FeatureGroup:
        """Makes a feature group exactly like the original, except its data source and target are prefixed by the split_id,
           In order to get the features stored in the split specific table, and store in a split specific table.
        """
        return FeatureGroup(
            original_feature_group.pipeline,
            f'{split_id}_{original_feature_group.name_of_table_to_store_features_in}',
            original_feature_group.name_of_features_to_store,
            original_feature_group.engineered_columns_definition,
            original_feature_group.identity_columns,
            f'{split_id}_{original_feature_group.source_dataset_table_name}',
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
        
        oil_feature_group = self._make_feature_group(
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
        
        return self._make_feature_group(
            'stores',#get_sql_table_name_of_dataset_of_name('stores')
            ['id'],
            pipeline,
            [*chain(names_of_cat_cols_turned_numeric_names, names_of_cat_cols_ohe)],
            [*chain(sql_definition_of_cat_cols_turned_numeric, sql_definitions_of_cat_cols_ohe)],
            'stores',#get_sql_table_name_of_dataset_of_name('stores')
        )
    
    def make_sales_splits_dataset(self):
        return KFoldSplitDataset(
            'sales',
            'sales',
            'id',
            self.spark_interface
        )

    def engineer_and_store_all_features(self):
        feature_groups = [
            self.create_oil_prices_feature_group(),
            self.create_store_feature_group()
        ]
        for feature_group in feature_groups:
            feature_group.engineer_features_and_store()
            
        sales_splits_dataset = self.make_sales_splits_dataset()
        
        
        #Todo make a train test splitter
        
        full_sales_data_sequential_splits = 
        sales_data_kfold_splits = make_kfold_train_test_splits(sequentially_split_dataset=full_sales_data_sequential_splits, split_base_name='sales') #Will make splits named sales_1, sales_2 ...

        #Grab the main dataset > k fold split > grab the splits > 

        #grab original dataset > kfold split > store filtered data> make each feature group engineer storing in filtered data table.
            #do this by making a db_interface that points to prefixed tables?
            #Would need 1 db interface per split but thats not too bad.
            #Store could also have a table prefix, or name and data as parameter.
            #Could also have feature group, and create feature gorup methods take a prefix parameter which makes a lot of sense.
            #May make sense to pass db_interface or spark interface as parameters, otherwise  youd need to make multiple feature groups per feature group
            #Which seems spagetthi.
        
        #I think having the store method have a name 1 and name 2 parameter is possibly best.
        # you probably shouldnt have to make different objects to change a single parameter.
        #But shouldnt you ... a parameter is what defines an object, and the feature group would otherwise represent to the wrong stuff...
        #Thinking about that its probably best to add it as a parameter to constructor.
        

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
        
        #
        #
data_engineering_manager:DataEngineeringManager = DataEngineeringManager(db_interfacing.db_interface, spark_interfacing.spark_interface)



#If we need to split our df differently we can implement different splitters maintaining the signature, and generalize the method signature to expect anything with split_df
#The simplest case by far would be something that returns splits directly and doesnt even need the input df or just ignores it, though it seems like bad practice to me.
class KFoldDFSplitter():
    """Kfold splits a pyspark df."""
    def __init__(self, row_number_col:str, prefix:str, splits:int):
        self.row_number_col =row_number_col
        self.prefix = prefix
        self.splits = splits
        
    def split_df(self, df:DataFrame)->list[TrainTestSplit]:
        sequential_df_parts = split_dataframe_sequentially(df,self.prefix, self.splits)
        df_kfold_splits = make_kfold_train_test_splits(sequential_df_parts, split_base_name=self.prefix) #Will make splits named sales_1, sales_2 ...
        return df_kfold_splits



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

class ReferencedRowsFinder():
    """
    Finds all rows in a dataframe that were referenced in another one.
    referenced_cols: a dict of col_name_of_df_to_filter:matching_key_in_reference_df. generally dim_key:fact_key.
    """
    
    def __init__(self, referenced_cols:dict[str,str]):
        self.referenced_cols = referenced_cols

    def find_referenced_rows(self, df_to_filter:DataFrame, reference_df:DataFrame):
        return find_matching_rows(df_to_filter, reference_df, self.referenced_cols)

#Maybe could be called linked dimension, and have a method to store the splits, and maybe even be part of an entity which has multiple linked dimensions.
class TrainTestSplitter():
    def __init__(
        self,
        dataset_provider:PysparkTableProvider,
        dataset_splitter:KFoldDFSplitter,
        dimension_provider:PysparkTableProvider,
        dimension_name:str,
        referenced_rows_finder:ReferencedRowsFinder
    ):
        self.dataset_provider = dataset_provider
        self.dataset_splitter = dataset_splitter
        self.dimension_provider = dimension_provider
        self.dimension_name = dimension_name
        self.referenced_rows_finder = referenced_rows_finder
        #FALTA algo para storear.
    
    def make_train_test_splits(self):
        dataset = self.dataset_provider.get_data()
        dimension = self.dimension_provider.get_data()
        splits = self.dataset_splitter.split_df(dataset)
        dimension_splits = []
        for dataset_split in splits: #AGAIN, WE ARE STORING TRAIN TEST SPLITS INSTEAD OF USING THEM DIRECTLY!
            dimension_splits.append(TrainTestSplit(
                f'{dataset_split.identifier}_{self.dimension_name}',
                self.referenced_rows_finder.find_referenced_rows(dimension, dataset_split.train_data),
                self.referenced_rows_finder.find_referenced_rows(dimension, dataset_split.test_data)
            ))
        
        


class LinkedDimension():
    """
        A dimension linked to an entity
        linked_keys: a dict of key_from_the_dimension:key in the entity.
    """
    #TODO REFACTOR METHODS TO USE A NAMEDTABLE/TABLEPROVIDER INSTEAD OF TABLE NAME AND spark_interface? Or at least change spark interface to a table provider?
    def __init__(self, dimension_table_name:str, linked_keys:dict[str,str], spark_interface:SparkInterface):
        self.dimension_table_name = dimension_table_name
        self.spark_interface = spark_interface
        self.linked_keys = linked_keys
        
    def get_cross_validation_set(self, entity_cross_val_split:TrainTestSplit):
        #linked_table:DataFrame, linked_table_name:str, main_train_test_split:TrainTestSplit, matching_columns:dict[str,str]
        dimension_table = self.spark_interface.get_current_data_in_sql_table(self.dimension_table_name)
        train_matching_rows_of_linked_table = find_matching_rows(dimension_table, entity_cross_val_split.train_data, self.linked_keys)
        test_matching_rows_of_linked_table = find_matching_rows(dimension_table, entity_cross_val_split.test_data, self.linked_keys)

        linked_train_test_split = TrainTestSplit(
            f'{main_train_test_split.identifier}_{linked_table_name}', #lo correcto seria el identifier del train test split sin table name. 
            train_matching_rows_of_linked_table,
            test_matching_rows_of_linked_table
        )
        return linked_train_test_split
        

# class Entity():
#     """
#     An entity, which has base data in a table, and additional data found in other tables.
#     Since an entity is a unified state of info from multiple tables, when generating a cross_validation_set, all tables need to respect the same split.
#     As of right now the main point of the entity is to be able to create and store crossvalidation sets.
#     """
    # def __init__(
    #     self,
    #     main_table_provider:PysparkTableProvider,
    #     main_table_number_id_cols:str,
    #     #dimensions_with_link:tuple[FeatureGroup,dict[str,str]],
    #     linked_dimensions:list[LinkedDimension]
    # ):
    #     self.main_table_number_id_cols = main_table_number_id_cols
    #     self.main_table_provider = main_table_provider
    #     self.linked_dimensions = linked_dimensions
        
    # def make_main_cross_validation_sets(self):
    #     """A hard coded implementation to make cross_validation_splits. could be any generic component capable of producing splits"""
    #     data = self.main_table_provider.get_data()
    #     splits  = split_dataframe_sequentially(
    #         data,
    #         self.main_table_number_id_cols,
    #         5
    #     )
    #     return splits
    
    # def get_linked_cross_validation_sets(self, cross_validation_sets):
    #     for linked_dimension in linked_dimensions:
    #         get_cross_validation_set

    # def get_cross_validation_set():
        

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

""" def store_cross_val_sets(self, list_of_cross_val_folds_per_dataset_name:list[dict[str, TrainTestSplit]], entity_name:str):
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
 """