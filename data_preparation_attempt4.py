
import pandas as pd
import re
from pandas.tseries.offsets import MonthEnd
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from typing import NamedTuple
import data_preparation_utils as prep_utils

def download_dataset():
    prep_utils.download_kaggle_competition_dataset('./.kaggle/kaggle.json', 'store-sales-time-series-forecasting', './dataset')


def get_train_dataset(length:int, drop_sales=True) -> pd.DataFrame:
    """
    Returns the train dataset
    Its formatted as a base dataframe, which will determine the end number of rows
    With the rest of the suplemental dataframes as attributes
    This way it can be fed into a scikitlearn pipeline and is even compatible with cross validate
    And the suplemental data can be used inside the pipeline, to add columns, etc.
    """
    dataset = pd.read_csv('./dataset/train.csv', index_col='id')

    if length in [ None, 'all', 'full']:
        length = len(dataset)

    dataset = dataset.head(length)
    dataset.attrs['stores_df'] = pd.read_csv('./dataset/stores.csv', index_col='store_nbr')
    dataset.attrs['oil_df'] = pd.read_csv('./dataset/oil.csv')
    dataset.attrs['transactions_df'] = pd.read_csv('./dataset/transactions.csv')
    dataset.attrs['special_days_df'] = pd.read_csv('./dataset/holidays_events.csv')
    
    if drop_sales:
        dataset.attrs['sales'] = dataset.pop('sales') #Make y available for feature engineering, but remove it from the features
    
    return dataset


class OtherDataFrames(NamedTuple):
    elements_to_predict_x_base_df: pd.DataFrame
    sample_submission_df: pd.DataFrame

def get_other_dfs()->OtherDataFrames:
    elements_to_predict_x_base_df = pd.read_csv('./dataset/test.csv', index_col='id')
    sample_submission_df = pd.read_csv('./dataset/sample_submission.csv')
    return OtherDataFrames(elements_to_predict_x_base_df,sample_submission_df)


def rename_raw_dfs_cols(dataset:pd.DataFrame) ->pd.DataFrame:
    """Rename the columns of the dataset so they are more easily understandable
       Please note that while usually its a good idea to require the raw values as parameters to provide more flexibility
       Here we need to have a single variable, the input X as parameter, since this method will be part of the scikitlearn pipeline.
       And so we use a dataclass instead.
    """
    dataset.rename(columns={'family':'product_family', 'onpromotion':'products_of_family_on_promotion'}, inplace=True)
    dataset.attrs['oil_df'].rename(columns={'dcoilwtico':'oil_price'}, inplace=True)
    dataset.attrs['stores_df'].rename(columns={'type':'store_type', 'cluster':'store_cluster', 'city':'store_city', 'state':'store_state'}, inplace=True)
    dataset.attrs['transactions_df'].rename(columns={'transactions':'all_products_transactions'}, inplace=True)
    dataset.attrs['special_days_df'].rename(columns={'type':'day_type', 'locale':'special_day_locale_type', 'locale_name':'special_day_locale','description':'special_day_reason', 'transferred':'special_day_transferred'}, inplace=True)
    return dataset


def merge_data_sources(dataset:pd.DataFrame, merge_oil:bool, merge_stores:bool, merge_special_days:bool, merge_transactions:bool)->pd.DataFrame:
    """Add relevant columns from the auxiliary dataframes into a features dataset, be it the train set or the test set.
       Please note that while usually its a good idea to have the required variables as parameters to provide more flexibility
       Here we need to have a single variable, since this method will be part of the scikitlearn pipeline, so we use the dataset.
       We return a pandas dataframe because once the additional data is merged we no longer need to use a special class.
    """
    attrs = dataset.attrs #Save the attributes, since they are lost on dataset merges, and we need them

    if merge_oil:
        dataset = dataset.merge(attrs['oil_df'], on='date',how='left')

    if merge_stores:
        dataset = dataset.merge(attrs['stores_df'], on=['store_nbr'], how='left')
        
    if merge_transactions:
        dataset = dataset.merge(attrs['transactions_df'], on=['date', 'store_nbr'], how='left')
    
    if merge_special_days:
        dataset = dataset.merge(attrs['special_days_df'], on=['date'], how='left')

    dataset.attrs = attrs #Recover the attributes

    return dataset

def reorder_features_dataset(features_df):
    """Reorder the columns in the feature dataframe so the table becomes easier to understand and inspect. Does not affect the rows."""
    return features_df[[
                'store_nbr', 'store_city', 'store_state', 'store_type', 'store_cluster',
                'days_since_start', 'day_of_week','day_of_month', 'is_15th', 'is_last_day_of_month', 'day_of_year',  
                'day_type', 'special_day_reason', 'special_day_offset', 'special_day_transferred', 'special_day_reason_subtype', 'special_day_locale_type', 'special_day_locale',
                'oil_price', 'all_products_transactions', 
                'product_family', 'products_of_family_on_promotion',
                'sales'
            ]]

def one_hot_encode_necessary_features(features_df, names_of_columns_to_ohe): #TODO make it be done using scklearn one hot encoder?
    """One hot encodes the columns, using their name as prefix, and adding them into the same place the original was, while removing the original """
    #holiday_transferred may be better vectorized. Day type might be too. holiday_locale_type too.
    #This is because their values might have meaning in the order.
    
    #features_df.store_cluster = features_df.store_cluster.astype('int32')

    for name_of_column_to_ohe in names_of_columns_to_ohe:
        index_of_col_to_ohe = features_df.columns.get_loc(name_of_column_to_ohe)
        one_hot_encoded_column:pd.DataFrame = pd.get_dummies(features_df[name_of_column_to_ohe], dummy_na=True, prefix=name_of_column_to_ohe)
        
        # Split the dataframe into two parts: before and after the position of the original column
        columns_before_col_to_ohe = features_df.iloc[:, :index_of_col_to_ohe]
        columns_after_col_to_ohe = features_df.iloc[:, index_of_col_to_ohe+1:] #Exclude the current column

        # Concatenate the two parts with the one-hot encoded dataframe in between
        features_df = pd.concat([columns_before_col_to_ohe, one_hot_encoded_column, columns_after_col_to_ohe], axis=1)

    return features_df

from sklearn.base import BaseEstimator
import numpy as np
class CustomOneHotEncoder(BaseEstimator):
    """A custom one hot encoder 
    To allow creating pefixed onehot encoded columns in place and
    To find to make sure the columns on fit match the columns on transform, even when the present categories arent the same.
    Keep in mind it requires a pandas dataframe as input
    """
    def __init__(self, names_of_columns_to_ohe, output_type='pandas'):
        # Initialize with the column names to be one-hot encoded
        self.names_of_columns_to_ohe = names_of_columns_to_ohe
        # Initialize a dictionary to store the unique categories in each column
        self.categories = {}
        # Set the desired output type
        self.output_type = output_type

    def fit(self, features_df:pd.DataFrame, y=None):
        if not isinstance(features_df, pd.DataFrame):
            raise ValueError(f"CustomOneHotEncoder requires a pandas DataFrame as input, but found: {type(features_df)}" )

        #find the unique categories in each column, to use it for all further transforms
        #And ensure that the features to create are always the same
        for name_of_column_to_ohe in self.names_of_columns_to_ohe:
            self.categories[name_of_column_to_ohe] = features_df[name_of_column_to_ohe].unique()
        return self

    def transform(self, features_df):
        #create new columns for each unique category in the original columns
        #then drop the original column as it has been one-hot encoded
        for name_of_column_to_ohe in self.names_of_columns_to_ohe:
            for category in self.categories[name_of_column_to_ohe]:
                features_df[name_of_column_to_ohe + '_' + str(category)] = (features_df[name_of_column_to_ohe] == category).astype(int)
            features_df.drop(name_of_column_to_ohe, axis=1, inplace=True)

        if self.output_type == 'pandas':
            return features_df
        elif self.output_type =='np':
            return features_df.values
        else:
            raise ValueError(f"CustomOneHotEncoder can only output 'pandas' (pandas DataFrame) or 'np' (NumPy array) but output_type is set to: {self.output_type}")

    def set_output(self, transform):
        self.output_type = transform
    
def process_numerical_features(features_df, normalizers, is_test_data): #Should take the normalizer as parameter.
    """Either normalize or standardize variables depending on their distribution. Also handle missing values where necessary. """
    numerical_variable_names = ['oil_price', 'all_products_transactions']

    #Normalization is a scaling technique in which values are shifted and rescaled so that they end up ranging between 0 and 1. It is also known as Min-Max scaling.
    #Its more robust than standarization since it doesnt require the distribution to be gaussian.
    #MinMaxScaler is a scikitlearn normalizer.

    for variable_name in numerical_variable_names: #TODO MAKE IT CHECK IF ITS TEST AND TRANSFORM INSTEAD OF FIT TRANSFORM
        features_df[variable_name + '_standardized'] = normalizers[variable_name].fit_transform(features_df[variable_name])
        #features_df[variable_name + '_normalized'] = normalizers[variable_name].fit_transform(features_df[variable_name]) #hadd double brakcets


    return features_df
    #TODO #return the dataset and the normalizer

def fill_missing_transactions(features_df):
    """Fill transactions missing values with 0. Since it seems they weren't registered when the store didn't exist 
       Let the model know we filled it by adding a feature indicating it.
    """
    features_df['all_transactions_filled'] = features_df.all_products_transactions.isna()
    features_df.all_products_transactions = features_df.all_products_transactions.fillna(0)
    return features_df

def fill_missing_oil_values(features_df):
    # Oil price highly varies over time, making average imputing inaddecuate.
    # While we could use the average between the previous day and the next day, that wouldnt work for forecasting.
    # Instead just use the last known price, sinceFill down
    features_df['oil_price'] = features_df['oil_price'].ffill()#.fillna(method='ffill')
    features_df['oil_price'] = features_df['oil_price'].bfill() #Make sure that if the first elements are blank they still get a price (the price after them).
    #features_df['oil_price'].interpolate(limit_direction="both")
    return features_df


def refine_special_day_reason(features_df):
    """Assign the same reason to special days that have the same reason but with a small variation, storing otherwise lost information in other columns"""
    
    # Create a function to extract the offset in any reasons that end with -number or +number
    # Function to process the special_day_offset column
    def get_special_day_offset(value):
        if type(value) == str:
            match = re.search(r'([-+]\d+)$', value)
            if match:
                # Return the number with the sign as an integer
                return int(match.group())

            # If the condition is not met, return NaN
        return 0
    
    #Use the function to create a special_day_offset_column, and convert it from int64 to int32 to save space. 
    features_df['special_day_offset'] = features_df['special_day_reason'].apply(get_special_day_offset).astype('int32')


    # Now that the data that is not stored elsewhere has been stored create a function to clean the special_day_reason column
    def process_special_day_reason_value(value):
        '''A function to process the special_day_reason elements that can be applied element-wise using pandas.'''

        if type(value) == str:
            #If the reason contains a locale, eliminate the locale part of the reason, since its already stored in locale column.
            prefixes = ['Fundacion', 'Independencia', 'Provincializacion', 'Cantonizacion', 'Translado']
            for prefix in prefixes:
                if value.startswith(prefix):
                    value = prefix
            
            # If if value ends with - followed by any number or + followed by any number, aka if it has an offset, eliminate the offset part of the reason, since its already stored in the offset column.  
            match = re.search(r'([-+]\d+)$', value) # Check if value ends with - followed by any number or + followed by any number
            if match:
                return value.replace(match.group(), '') # Return the string without the number and sign
            
            # If the reason is mundial de futbol brasil, eliminate additional detaisl since they are already stored in the reason_subtype column. #Todo verify synthax
            if 'Mundial de futbol Brasil' in value: #value.contains('Mundial de futbol Brasil'):
                return 'Mundial de futbol Brasil'
        
        # If the reason does not need any preprocessing just return it
        return value

    #And now apply that function element-wise to cleanup the special_day_reason column to not store redundant info and unify reasons that are actually the same.
    features_df['special_day_reason'] = features_df['special_day_reason'].apply(process_special_day_reason_value)
    return features_df


def replace_date_with_date_related_columns(features_df):
    """"Removes date, and adds many date_related_columns """
    #Providing a date_time format speeds the method up.https://www.kaggle.com/code/kuldeepnpatel/to-datetime-is-too-slow-on-large-dataset
    features_df['date'] = pd.to_datetime(features_df['date'],format='%Y-%M-%d')#%Y-%M-%d # = yyyy-mm-dd, ex= 2013-01-21
    #Calculate the absolute date (from unix epoch), which is a standard start date used by many siystems and libraries working with time series data.
    #While this would work its probably worth for the nn to learn.
    #UNIX_EPOCH = pd.Timestamp("1970-01-01")
    #features_df['absolute_day_number'] = (features_df['date'] - UNIX_EPOCH) // pd.Timedelta('1D')

    #Calculate the number of days since the first date in the dataset. Change the type from int64 to in32 since it won't have nulls.
    features_df['days_since_start'] = ( (features_df['date'] - features_df['date'].min()) // pd.Timedelta('1D') ).astype('int32')

    features_df['day_of_year'] = features_df['date'].dt.dayofyear
    features_df['day_of_month'] = features_df['date'].dt.day
    features_df['day_of_week'] = features_df['date'].dt.dayofweek

    features_df['is_15th'] = (features_df['date'].dt.day == 15).astype(int)
    features_df['is_last_day_of_month'] = (features_df['date'] == features_df['date'] + MonthEnd(1)).astype(int)

    features_df = features_df.drop(columns=['date'])
    return features_df



def prepare_features_df(features_df:pd.DataFrame, stores_df, oil_df, transactions_df, special_days_df, normalizers):
    """Call all the preprocessing methods in order, and prepare a features dataframe for deep learning, be it the train set or the test set"""
    #features_df = merge_data_sources(features_df, stores_df, oil_df, transactions_df, special_days_df) #Careful, do not move this later, since the rest of the methods need access to all the columns.
    features_df = reorder_features_dataset(features_df) #Careful, do not move this later, the rest of the methods try to respect the order of the dataframe, so theyll end up in messy places otherwise.
    features_df = fill_missing_oil_values(features_df)     
    features_df = refine_special_day_reason(features_df)
    features_df = process_numerical_features(features_df) #Rename or include fill missing oil values here?
    features_df = one_hot_encode_necessary_features(features_df) #Careful moving this earlier since one hot encoding properly requires categorical variables to have been processed.
    features_df = replace_date_with_date_related_columns(features_df) #Careful, moving calling this earlier could be problematic since it eliminates date column.

    return features_df


import numpy as np

def rolling_window_dataset(df:pd.DataFrame, window_size):
    """
    Window the dataset so that it can be used for training a neural network.
    Creates a rolling window, aka if a series on the dataframe was 1,2,3 and window size was 2 the new series that will be on another column on a dataframe will be [1,2], [2,3]
    It creates 'lag features' which can be used in the prediction of the future values, in addition to the values of the variables.
    """
    #Create shifted versions of the entire dataframe. One shifted version per day in the window after the first one. 
        
    dataframes = [df]
    for lag in range(1, window_size):
        with pd.option_context('future.no_silent_downcasting', True): #Avoid a pandas warning about tchanges in future behavior
        #Create the shifted dataframe, making sure it keeps the appropiate dtypes
            df_shifted = df.shift(lag)
            df_shifted = df_shifted.infer_objects(copy=False).bfill() #remove na values from starter rows which dont have where to get lag from
            df_shifted = df_shifted.astype(df.dtypes)#Might be unnecessary because of infer objects
            dataframes.append(df_shifted.add_suffix(f'_lag_{lag}'))
    df['row_was_lagg_backfilled'] = df.index < window_size #Add a column indicating wether the row had its lagg backfilled
            
    # Remove rows that will make rows that lack values on joining with other dfs.
    #dataframes[0] = dataframes[0].iloc[window_size-1:]

    #Horizontally concatenate all the shifted dataframes.
    df_concat = pd.concat(dataframes, axis=1)

    return df_concat
    #We could add some lead_time like this:
    #def make_lags(ts, lags, lead_time=1): #https://www.kaggle.com/code/ryanholbrook/forecasting-with-machine-learning
    # return pd.concat(
    #     {
    #         f'y_lag_{i}': ts.shift(i)
    #         for i in range(lead_time, lags + lead_time)
    #     },
    #     axis=1)   

def drop_target(df:pd.DataFrame):
    """Drop the target for prediction from the values
       It is convenient/necessary for having windowing inside the pipeline
       But must be droped for the model to be valid.
       This method is used by the pipeline to achieve that.
    """
    df_without_target = df.drop(columns='sales')
    return df_without_target


#perform fourier transform?
#Inpute missing values in other columns? 

#TODO train_val_split, probably the most sensible approach for a simple time series analysis is to use the last data for validation,
#though i can imagine other approaches which could lead to better peformance, for example masking like bert does for text data.
#Or creating crossval sets by skipping part of the time before prediction

from sklearn.pipeline import FunctionTransformer
from sklearn.pipeline import Pipeline

def create_merging_pipeline()->Pipeline:
    """Create a pipeline for renaming columns and merging dataframes
       We rename them to make the merging code and end result clearer.
       We do it separately from the main pipeline in order to be able to use scikitlearn cross_validate.
       cross_validate enforces the input having the same number of rows as the target value.
       Without prior preprossesing that would not be the case, since we start with tuple of tables, and not the final table.
       We could also think about passing the params as kw args, but i dont think cross validate supports kw args for prediction stage.
    """
    merging_pipeline = Pipeline([
        ('rename_columns', FunctionTransformer(rename_raw_dfs_cols)),
        ('merge_dataframes', FunctionTransformer(merge_data_sources))
    ])
    return merging_pipeline

from sklearn.model_selection import KFold
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.compose import TransformedTargetRegressor
from xgboost import XGBRegressor

def create_pipeline(window_size=3, verbose=False):
    """Create a pipeline for data processing
       Keep in mind that pipelines are extremely practical, and easy to debug.
       You can call parts of the pipeline for debugging purposes by adding a list slicer next to it, for example pipeline[:1].fit_transform(dataset) or by name of the step.
       You can also call the pipeline with 'verbose=True' to analyze the time taken by each step.
       You can use ipython.display to display a graph of the pipeline steps too. Though it seems functiontransformer isnt really named with this way of defining them, you can create a class extending function transformer instead.
       Please note that while we could save processing time by separating the processing pipeline from the predictor for optuna trials, we wouldnt be able to tune the processing if we did that.
       Unless we cache the preprocessing results, and grab them dinamically, but it seems overly complicated unless necessary, plus I don't know if optuna caches something at some point or not, so it may be pointless.
       An interesting alternative would be to create a feature store, and on tuning just decide which features tp grab features from the store.
       In that case a separate pipeline for creating the features to store could be built TODO: move the ideas to separate notes.
    """
    cat_cols_to_ohe = [ 'store_nbr', 'store_cluster', 'product_family', 'store_city','store_state', 'store_type', 'day_type', 'special_day_locale_type', 'special_day_locale',
                    'special_day_reason',  'special_day_transferred'#,'special_day_reason_subtype', 
                    ]
    numerical_features_to_min_max_scale = ['oil_price', 'all_products_transactions']

    

    pipeline = Pipeline([
        ('fill_missing_oil_values', FunctionTransformer(fill_missing_oil_values)), #Could be achievable with sklearn most likely
        ('fill_missing_transactions', FunctionTransformer(fill_missing_transactions)),
        ('refine_special_day_reason', FunctionTransformer(refine_special_day_reason)), #Isnt placed where the column came from.
        ('replace_date_with_date_related_columns', FunctionTransformer(replace_date_with_date_related_columns)), #Careful, moving calling this earlier could be problematic since it eliminates date column.
        #('reorder_features', FunctionTransformer(reorder_features_dataset)), #The order of columns cant change in the middle of the pipeline if runnign cross validate.
        ('prepare_features', ColumnTransformer([
            ('standardize_numerical_features', MinMaxScaler(), numerical_features_to_min_max_scale), 
            ('prepare_categorical_columns', CustomOneHotEncoder(cat_cols_to_ohe), cat_cols_to_ohe),
        ], remainder='passthrough', sparse_threshold=0, n_jobs=3, verbose_feature_names_out=False).set_output(transform='pandas')),
        ('window_dataset', FunctionTransformer(rolling_window_dataset, kw_args={'window_size': window_size})),
        ('drop_target', FunctionTransformer(drop_target)),
        ('model', XGBRegressor()) #instead of linear regressor
    ], verbose=verbose)

    #return TransformedTargetRegressor(regressor=pipeline, transformer=FunctionTransformer(trim_target_to_laggable_range, kw_args={'window_size':2}))

    return pipeline
    #TODO batch the pipeline if possible.
