#A stage represents a set of tasks that can be executed together in a single wave of computation, resulting in a more efficient execution of the Spark job.
#Stage: Jobs that can be executed in paralel
#pyspark.sql.dataframe.DataFrame


#By default, Spark jobs are executed serially. Starting from CDAP 6.1.0, jobs can be executed in parallel by setting the  pipeline.spark.parallel.sinks.enabled = True
#Like any runtime argument, this can be set at the pipeline, namespace, or instance level. 
#You will need a larger cluster to take full advantage of the parallelism, and you will need to be ok with source data being read multiple times.
# This means that caching cannot be relied upon to prevent multiple reads from a source. In most situations, it is not worth caching anything in a pipeline with parallel execution enabled. As such, parallel execution should only be used if multiple reads from the source is not a problem.
#Also, if your cluster does not have enough resources to run the entirety of the jobs at the same time, one job may receive more resources than the other, causing them to complete in different amounts of time.
#lrPipeline = Pipeline()

#df.withColumn('col_to_use', code_to_creat_col)

#lrPipeline.setStages([vectorizer, lr])
#


#Key requirements for etl process
# It must be at least partially sequential (aka, feature B may need feature A so feature A needs to be available)
# It must obtain only the info required for each step at each step. (aka, it must not load feature a1, a2, a3 when it only needs feature a1 to work)

#Algunas features cambian el valor de todas sus rows cada vez que se a;nade una row.
    #Ej average
    #Ej normalized
    #Ej median imputed
    #Ej normalized and imputted
    #Ej min,
    #Ej max
    
#Para guardar el estado en un punto historico necesitarian versionamiento, aunque probablemente no vale la pena
#Pero para guardar su estado mas nuevo necesito la finalizacion de features anteriores.
    


# Some features require other features to be fully created in order to work, for example if i need to do something on a normalized feature, the normalization requires all cells.
    # Normalization is a special feature (but not so special, others are like it), in the sense that the value needs to be updated when new features are added
    # For this kind of feature maybe we need a table per table
    # sales_by_point (store_id, sales_average, )
    # sales_normalized (sale_date, store_nbr, transactions_normalized, last_normalized_id)
    # O mejor una normalized con fecha?
    # sales_normalized_last_id_100
    # O mejor
    # normalized_sales_table
    # sales_normalized_table_name, last_sale_id_for_this_normalization
    # Podria la mas current ser simplemente normalized_sales, y renombrarse antes de comitear una nueva, si es que se quieren conservar copias.
    # El principal problema es que si tengo features que dependen de normalization necesito tambien 
    # Tambien podria quedarse solo el ultimo valor


from sklearn.pipeline import Pipeline
from pyspark.sql import DataFrame as PySparkDataFrame

class Procedure:
    def __init__(self, required_features, ):
        self.name = 
        
class feature_provider:
    procedures = []
    

#Option 0, execute query directly for obtaining features, then for 
from pyspark.sql import SparkSession
spark_session_builder:SparkSession.Builder = SparkSession.builder # type: ignore <-Ignore a wrong pylance warning, and make type detection work properly
spark:SparkSession = (
    spark_session_builder
    .master('local[3]')
    .appName('TimeSeriesForecastStoreSales Data Engineering')
    .getOrCreate()
)
#Select after load might mean that it grabs all columns and then filters the dataframe.
src = spark.read.format('jdbc').options(**spark_sql_options).option('dbtable', dataset_properties.sql_table_name).load().select(column_name)
src.write.format('jdbc').options(**spark_sql_options).option('dbtable', dataset_properties.sql_table_name)
src.write()





#Option 1 hava a hnadler
#Allows changing the implementation with less issues, simply use a different handler.
class DBFeatureHandler():
    def __init__(self, db_options):
        self.db_options = db_options
        
    def get_feature(self, feature_name:str, feature_value:PySparkDataFrame):
    
    def load_feature(self, table_name, feature_name:str, feature_value:PySparkDataFrame):
        



def load_feature(feature_name):
    return

        
feature_handler = DBFeatureHandler(None)
    



DBFeatureHandler.load_feature('normalized_sales', normalize(get_feature(sales, sales)))



transformers_for_feature = dict(
    sales = dict(
        normalized_sales = normalize_feature('sales',)
    )
)

def normalize_db_feature(sql_connection, ):
    
def create_numerical_features():