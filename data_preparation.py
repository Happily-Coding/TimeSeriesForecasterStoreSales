
import pandas as pd
import re
from pandas.tseries.offsets import MonthEnd
#from scipy.stats import shapiro
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from typing import NamedTuple
import data_preparation_utils as prep_utils

def download_dataset():
    prep_utils.download_kaggle_competition_dataset('./.kaggle/kaggle.json', 'store-sales-time-series-forecasting', './dataset')

def get_raw_dfs() ->tuple[pd.DataFrame]:
    """Get dataframes containing all the info in the dataset.
    train_xy_base_df: The dataframe containing most features(x), and the Y value (sales). Base because other features should be merged into it.
    train_xy_base_df: The dataframe containing most features(x), and the Y value (sales). Base because other features should be merged into it. 
    Note that the only features that will be provided for prediction are the ones in train_xy_base, but since most entities should already exist, so you have the data, it should still improve predictions to use the other dfs.
    """
    train_xy_base_df = pd.read_csv('./dataset/train.csv', index_col='id')
    elements_to_predict_x_base_df = pd.read_csv('./dataset/test.csv', index_col='id')
    stores_df = pd.read_csv('./dataset/stores.csv', index_col='store_nbr')
    oil_df = pd.read_csv('./dataset/oil.csv') 
    transactions_df = pd.read_csv('./dataset/transactions.csv')
    holiday_events_df = pd.read_csv('./dataset/holidays_events.csv')
    sample_submission_df = pd.read_csv('./dataset/sample_submission.csv')
    return train_xy_base_df, elements_to_predict_x_base_df, stores_df, oil_df, transactions_df, holiday_events_df, sample_submission_df

def rename_raw_dfs_cols(train_xy_base_df, stores_df, oil_df, transactions_df, special_days_df) ->tuple:
    """Rename the columns of the raw dataframes so they are more easily understandable"""
    train_xy_base_df = train_xy_base_df.rename(columns={'family':'product_family', 'onpromotion':'products_of_family_on_promotion'})
    oil_df = oil_df.rename(columns={'dcoilwtico':'oil_price'})
    stores_df = stores_df.rename(columns={'type':'store_type', 'cluster':'store_cluster', 'city':'store_city', 'state':'store_state'})
    transactions_df = transactions_df.rename(columns={'transactions':'all_products_transactions'})
    special_days_df = special_days_df.rename(columns={'type':'day_type', 'locale':'special_day_locale_type', 'locale_name':'special_day_locale','description':'special_day_reason', 'transferred':'special_day_transferred'})
    return train_xy_base_df, stores_df, oil_df, transactions_df, special_days_df

def merge_data_sources(features_df:pd.DataFrame, stores_df:pd.DataFrame, oil_df:pd.DataFrame, transactions_df:pd.DataFrame, special_days_df:pd.DataFrame)->pd.DataFrame:
    """Add relevant columns from the auxiliary dataframes into a features dataset, be it the train set or the test set."""
    full_features_df = features_df.merge(oil_df, on='date',how='left')
    full_features_df = full_features_df.merge(stores_df, on=['store_nbr'], how='left')
    full_features_df = full_features_df.merge(special_days_df, on='date', how='left')
    full_features_df = full_features_df.merge(transactions_df, on=['date', 'store_nbr'], how='left')
    return full_features_df

def reorder_features_dataset(features_df):
    """Reorder the columns in the feature dataframe so the table becomes easier to understand and inspect. Does not affect the rows."""
    return features_df[[
                'store_nbr', 'store_city', 'store_state', 'store_type', 'store_cluster',
                'days_since_start', 'day_of_week','day_of_month', 'is_15th', 'is_last_day_of_month', 'day_of_year',  
                'day_type', 'special_day_reason', 'special_day_offset', 'special_day_transferred', 'special_day_reason_subtype', 'special_day_locale_type', 'special_day_locale',
                'oil_price', 'all_products_transactions', 
                'product_family', 'products_of_family_on_promotion'
            ]]

def one_hot_encode_necessary_features(features_df): #TODO change call to after the creation of the proper columns, and the columns to match it.
    """One hot encodes the columns, using their name as prefix, and adding them into the same place the original was, while removing the original """
    names_of_columns_to_ohe= [ 'store_nbr', 'product_family', 'store_city','store_state',
                      'store_type', 'day_type', 'special_day_locale_type', 'special_day_locale',
                       'special_day_reason', 'special_day_transferred']
                    #holiday_transferred may be better vectorized. Day type might be too. holiday_locale_type too.
                    #This is because their values might have meaning in the order.
    
    for name_of_column_to_ohe in names_of_columns_to_ohe:
        index_of_col_to_ohe = features_df.columns.get_loc(name_of_column_to_ohe)
        one_hot_encoded_column:pd.DataFrame = pd.get_dummies(features_df[name_of_column_to_ohe], dummy_na=True, prefix=name_of_column_to_ohe)
        
        # Split the dataframe into two parts: before and after the position of the original column
        columns_before_col_to_ohe = features_df.iloc[:, :index_of_col_to_ohe]
        columns_after_col_to_ohe = features_df.iloc[:, index_of_col_to_ohe+1:] #Exclude the current column

        # Concatenate the two parts with the one-hot encoded dataframe in between
        features_df = pd.concat([columns_before_col_to_ohe, one_hot_encoded_column, columns_after_col_to_ohe], axis=1)

    return features_df
    #TODO #return the dataset and the references

def process_numerical_features(features_df, normalizers, is_test_data): #Should take the normalizer as parameter.
    """Either normalize or standardize variables depending on their distribution. Also handle missing values where necessary. """
    print(features_df.columns)
    numerical_variable_names = ['oil_price', 'all_products_transactions']

    #Normalization is a scaling technique in which values are shifted and rescaled so that they end up ranging between 0 and 1. It is also known as Min-Max scaling.
    #Its more robust than standarization since it doesnt require the distribution to be gaussian.
    #MinMaxScaler is a scikitlearn normalizer.

    for variable_name in numerical_variable_names: #TODO MAKE IT CHECK IF ITS TEST AND TRANSFORM INSTEAD OF FIT TRANSFORM
        features_df[variable_name + '_standardized'] = normalizers[variable_name].fit_transform(features_df[variable_name])
        #features_df[variable_name + '_normalized'] = normalizers[variable_name].fit_transform(features_df[variable_name]) #hadd double brakcets


    return features_df
    #TODO #return the dataset and the normalizer

def fill_missing_oil_values(features_df):
    # Oil price highly varies over time, making average imputing inaddecuate.
    # While we could use the average between the previous day and the next day, that wouldnt work for forecasting.
    # Instead just use the last known price, sinceFill down
    features_df['oil_price'] = features_df['oil_price'].ffill()#.fillna(method='ffill')
    features_df['oil_price'] = features_df['oil_price'].bfill() #Make sure that if the first elements are blank they still get a price (the price after them).
    return features_df


def refine_special_day_reason(features_df):
    """Assign the same reason to special days that have the same reason but with a small variation, storing otherwise lost information in other columns"""

    # Create a special_day_reason subtype column. Extract the subtype of mundial de futbol brasil into a subtype column, since they all have a common reason.
    features_df['special_day_reason_subtype'] = features_df['special_day_reason'].apply(lambda x: x.replace('Mundial de futbol Brasil: ', '').replace('Mundial de futbol Brasil', '') if (type(x) == str) and (x.startswith('Mundial de futbol Brasil')) else '')
    
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
    
    #Use the function to create a special_day_offset_column
    features_df['special_day_offset'] = features_df['special_day_reason'].apply(get_special_day_offset)


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
    """"Adds many date_related_columns """

    features_df['date'] = pd.to_datetime(features_df['date'])

    #Calculate the absolute date (from unix epoch), which is a standard start date used by many siystems and libraries working with time series data.
    #While this would work its probably worth for the nn to learn.
    #UNIX_EPOCH = pd.Timestamp("1970-01-01")
    #features_df['absolute_day_number'] = (features_df['date'] - UNIX_EPOCH) // pd.Timedelta('1D')

    #Calculate the number of days since the first date in the dataset.
    features_df['days_since_start'] = (features_df['date'] - features_df['date'].min()) // pd.Timedelta('1D')

    features_df['day_of_year'] = features_df['date'].dt.dayofyear
    features_df['day_of_month'] = features_df['date'].dt.day
    features_df['day_of_week'] = features_df['date'].dt.dayofweek

    features_df['is_15th'] = (features_df['date'].dt.day == 15).astype(int)
    features_df['is_last_day_of_month'] = (features_df['date'] == features_df['date'] + MonthEnd(1)).astype(int)

    features_df = features_df.drop(columns=['date'])
    return features_df



def prepare_features_df(features_df:pd.DataFrame, stores_df, oil_df, transactions_df, special_days_df, normalizers):
    """Call all the preprocessing methods in order, and prepare a features dataframe for deep learning, be it the train set or the test set"""
    #TODO READ and rename columns of the dataset, and then process them here.
    features_df = merge_data_sources(features_df, stores_df, oil_df, transactions_df, special_days_df) #Careful, do not move this later, since the rest of the methods need access to all the columns.
    features_df = reorder_features_dataset(features_df) #Careful, do not move this later, the rest of the methods try to respect the order of the dataframe, so theyll end up in messy places otherwise.
    features_df = fill_missing_oil_values(features_df)     
    features_df = refine_special_day_reason(features_df)
    features_df = process_numerical_features(features_df) #Rename or include fill missing oil values here?
    features_df = one_hot_encode_necessary_features(features_df) #Careful moving this earlier since one hot encoding properly requires categorical variables to have been processed.
    features_df = replace_date_with_date_related_columns(features_df) #Careful, moving calling this earlier could be problematic since it eliminates date column.

    return features_df

def window_dataset():
    """Window the dataset so that it can be used for training a neural network."""
    #Aka create a window of days with values and then the unknown value for the next day. and then slide that window forward to create another data point.
    #TODO
    return

#perform fourier transform?
#Inpute missing values in other columns? 

#TODO train_val_split, probably the most sensible approach for a simple time series analysis is to use the last data for validation,
#though i can imagine other approaches which could lead to better peformance, for example masking like bert does for text data.
#Or creating crossval sets by skipping part of the time before prediction




from sklearn.model_selection import KFold
from sklearn.pipeline import FunctionTransformer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
def create_pipeline(stores_df, oil_df, transactions_df, special_days_df, verbose=False):
    """Create a pipeline for data processing
       Keep in mind that pipelines are extremely practical, and easy to debug.
       You can call parts of the pipeline for debugging purposes by adding a list slicer next to it, for example pipeline[:1].fit_transform(dataset) or by name of the step.
       You can also call the pipeline with 'verbose=True' to analyze the time taken by each step.
       You can use ipython.display to display a graph of the pipeline steps too. Though it seems functiontransformer isnt really named with this way of defining them, you can create a class extending function transformer instead.
    """
    cat_cols_to_ohe = [ 'store_nbr', 'store_cluster' 'product_family', 'store_city','store_state', 'store_type', 'day_type', 'special_day_locale_type', 'special_day_locale','special_day_reason', 'special_day_transferred']
    numerical_features_to_min_max_scale = ['oil_price', 'all_products_transactions']

    pipeline= Pipeline([
        ('rename_columns', FunctionTransformer(rename_raw_dfs_cols, kw_args={'stores_df': stores_df, 'oil_df': oil_df, 'transactions_df': transactions_df, 'special_days_df': special_days_df})),
        ('merge_dataframes', FunctionTransformer(merge_data_sources, kw_args={'stores_df': stores_df, 'oil_df': oil_df, 'transactions_df': transactions_df, 'special_days_df': special_days_df})),
        ('fill_missing_oil_values', FunctionTransformer(fill_missing_oil_values)), #Could be achievable with sklearn most likely
        ('refine_special_day_reason', FunctionTransformer(refine_special_day_reason)), #Isnt placed where the column came from.
        ('replace_date_with_date_related_columns', FunctionTransformer(replace_date_with_date_related_columns)), #Careful, moving calling this earlier could be problematic since it eliminates date column.
        ('reorder_features', FunctionTransformer(reorder_features_dataset)),
        ('prepare_features', ColumnTransformer([
            ('standardize_numerical_features', MinMaxScaler(), numerical_features_to_min_max_scale), #MinMaxScaler().set_output(transform='pandas')
            ('prepare_categorical_columns', FunctionTransformer(one_hot_encode_necessary_features), cat_cols_to_ohe)
        ], remainder='passthrough', sparse_threshold=0, n_jobs=1, verbose_feature_names_out=False).set_output(transform='pandas')),
    ], verbose=verbose)
    return pipeline

