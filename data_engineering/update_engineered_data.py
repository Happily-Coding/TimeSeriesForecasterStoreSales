import data_engineering_management
from data_engineering_management import DataEngineeringManager
from db_connector import spark, spark_sql_options, data_engineering_connection
#CURRENTLY TESTING EACH METHOD TO MAKE SRUE THEY WORK

data_engineering_manager = DataEngineeringManager(spark, spark_sql_options, data_engineering_connection)

oil_features = data_engineering_management.create_oil_prices_feature_group()

current_oil_data = data_engineering_manager.get_current_data_in_dataset_of_name('oil_price_by_date')
current_oil_data.select()
current_oil_data.show(2)

transformed_oil_data = oil_features.pipeline.fit(current_oil_data).transform(current_oil_data)
transformed_oil_data.show(2)


data_engineering_manager.store_engineered_features(oil_features, transformed_oil_data)

#engineer_features_and_store(oil_features, current_oil_dataset)

#data_engineering_manager.engineer_and_store_all_features()

