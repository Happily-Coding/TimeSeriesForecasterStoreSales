from typing import Callable
import pyspark
import pyspark.mllib
from pyspark.sql import SparkSession
from dataclasses import dataclass
from csv_utils import get_csv_rows_skipping
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

@dataclass
class FeatureGroup:
    """
    Attributes:
        name (str): The name of the feature group. Will be the base name for its table.
        features_with_type (dict[str, str]): The name of each variable produced by the table, along with its type.
    """
    name:str
    features_with_type:dict[str,str]
    maker:Callable

class FeatureGroupFactory:
    def __init__(self, spark:SparkSession, spark_sql_options:dict):
        self.spark = spark
        self.spark_sql_options = spark_sql_options #Could be replaced for storing a base jdbc reader

    feature_groups = {}
    
    
    def make_new_base_oil_features(self, rows_to_skip, dfs:dict[str,DataFrame]):
        base_oil_features = self.spark.read.csv(
            path=get_csv_rows_skipping(self.spark, '../dataset/oil.csv', rows_to_skip), # type: ignore <- Ignoring this type error because it doesnt actually generate any problem.
            header=True, #Let spark know to ignore header
            schema=self.spark.read.format('jdbc').options(**self.spark_sql_options).option('dbtable', 'oil_prices').load().select(['date', 'dcoilwtico']).schema
        )#.filter(f'row_number > {rows_to_skip}')
        dfs['new_base_oil'] = base_oil_features
        return base_oil_features
    
    
    def make_not_multirow_oil_features(self, dfs:dict[str,DataFrame]):
        new_base_oil_features  = dfs['new_base_oil']
        single_row_engineered_oil_features = new_base_oil_features.withColumns({'double_of_oil_price': new_base_oil_features.oil_price * 2})
        dfs['single_row_engineered_oil_features_new_rows'] = single_row_engineered_oil_features
        return single_row_engineered_oil_features
    
    def get_full_non_multi_row_oil(self, dfs:dict[str,DataFrame]):
        single_row_engineered_oil_features_new_rows = dfs['single_row_engineered_oil_features']
        #ACA HAY UN PROBLEMA< SI APARECIO UNA NUEVA FEATURES EN NOT MULTIROW-OIL-FEATURES NO VA A ESTAR.
        #Para solucionarlo puedo hacer que not multirow una todas las rows si hay una columna missing o aniadir este proceso como step pero es complicar las cosas al pedo todo esto. Aumentaria mucho el costo mental para el poco value que da.
        #Uno no deberia tener que actualizar muy seguido las features y incluso si lo tiene que hacer puede simplemente anadir solo las columnas de las nuevas a partir de data consultada de la database.
        #Aparte todo esto para poder aprovechar lo que estoy metiendo para no tener que nunca queriar a la db, lo cual es medio estupido.
        #Al final no era tan ilogico lo que decia spark de sacar todo procesar y overridear huh. aunque tbh overridear entero era un poco peligroso, y es mejor overridear manualmente dropeando column y reponiendo las nuevas.
        #O al menos nunca modificando las que son mi info posta.
        
        #stored_multirow_oil_features = 
        
    
    def make_engineered_oil_features(self, dfs:dict[str,DataFrame]):
        #OJO ESTO ESTA MAL, ESTAMOS HACIENDO MIN MAX SCALING CON SOLO PARTE DE LA DATA POSIBLEMENTE.
        base_oil_features = dfs['base_oil']
        assembler = VectorAssembler(inputCols=['oil_price'],outputCol='oil_price_vect')
        scaler = MinMaxScaler(inputCol='oil_price_vect', outputCol="oil_price_scaled_0_to_1")
        pipeline = Pipeline(stages=[assembler, scaler])
        engineered_oil_features = pipeline.fit(base_oil_features).transform(base_oil_features)
        dfs['engineered_oil'] = engineered_oil_features
        return engineered_oil_features
    
    def oil_aggregate_features(self, dfs:dict[str,DataFrame]):
        '''Assumes that all dfs have the full features and not just new ones'''
        from pyspark.sql.functions import min, mean, max 
        
        full_oil_features = dfs['engineered_oil']
        
        multirow_oil_features = full_oil_features.select(
            min('oil_price'),
            mean('oil_price'),
            max('oil_price')
        )
        
        return multirow_oil_features
        #Normally you'd join a date table here.
        #.avg()
        
        
    
    #GRAN PROBLEMA CON ESTE APPROACH
    #SI NO QUIERO RE-BUSCAR las features base, no van a estar disponibles en la chain de df. excepto que de alguna forma haga que los pasos que no estan se hagan con info en la database. 
    # SUENA IFFY. Ignorar el problema ya que no va a existir en csv?
    # Y que en data source caras se van a encargar de bajarlas?

    
    feature_group_makers = {
        'new_base_oil':make_new_base_oil_features,
        'new_non_multi_col_oil':make_not_multirow_oil_features,
        'full_non_multi_row_oil':get_full_non_multi_row_oil,
        'engineered_oil':make_engineered_oil_features
    }
    

    feature_groups['base_oil_features'] =  FeatureGroup(
            name='oil',
            features_with_type=dict(
                date='date',
                oil_price='int'
            ),
            maker = make_new_base_oil_features
    )
    
    

        





        
