import data_engineering_management
from data_engineering_management import DataEngineeringManager
from db_interfacing import db_interface
from spark_interfacing import spark_interface
from db_connector import spark, spark_sql_options, data_engineering_connection
#CURRENTLY TESTING EACH METHOD TO MAKE SRUE THEY WORK

data_engineering_manager = DataEngineeringManager(db_interface, spark_interface)

oil_features = data_engineering_manager.create_oil_prices_feature_group()
oil_features



current_oil_data = oil_features.get_current_data_in_source_storage()
current_oil_data.select()
current_oil_data.show(2)

transformed_oil_data = oil_features.pipeline.fit(current_oil_data).transform(current_oil_data)
transformed_oil_data.show(2)


data_engineering_manager.store_engineered_features(oil_features, transformed_oil_data)

#engineer_features_and_store(oil_features, current_oil_dataset)

#data_engineering_manager.engineer_and_store_all_features()

