from csv_sql_dataset_utils import CsvSqlDatasetProperties

properties_by_csv_sql_dataset:dict[str,CsvSqlDatasetProperties] = dict(
    stores = CsvSqlDatasetProperties(
        '../dataset/stores.csv',
        'stores',
        """
        CREATE TABLE IF NOT EXISTS stores
        (
            id SMALLINT PRIMARY KEY,
            city TEXT,
            state TEXT,
            type TEXT,
            cluster SMALLINT
        );
        COMMENT ON TABLE stores IS 'Charateristics of each store that can generate sales'
        """,
        dict(id='store_nbr', city='city', state='state', type='type', cluster='cluster')

        #['store_nbr']
    ),
    
    sales_agg_by_date_store_productfamily = CsvSqlDatasetProperties( #It could be called facts_by_date_store_productfamily
        '../dataset/train.csv',
        'sales_agg_by_date_store_productfamily',
        """
        CREATE TABLE IF NOT EXISTS sales_agg_by_date_store_productfamily
        (
            id INTEGER NOT NULL PRIMARY KEY,
            date DATE NOT NULL,
            store_id SMALLINT NOT NULL REFERENCES stores(id),
            product_family TEXT,
            sales_total DOUBLE PRECISION,
            products_of_family_on_promotion_count INTEGER
        );
        COMMENT ON TABLE sales_agg_by_date_store_productfamily IS 'The total sales income by date, store, and product family'
        """,
        dict(id='id', date='date', store_id='store_nbr', product_family='family', sales_total='sales', products_of_family_on_promotion_count='onpromotion')
        #['id']
    ),
    
    transactions_agg_by_date_store = CsvSqlDatasetProperties(
        '../dataset/transactions.csv',
        'transactions_agg_by_date_store',
        """
        CREATE TABLE IF NOT EXISTS transactions_agg_by_date_store
        (
            date date,
            store_id smallint REFERENCES stores(id),
            transactions_count integer,
            PRIMARY KEY(date, store_id)
        );
        COMMENT ON TABLE transactions_agg_by_date_store IS 'The number of transactions that took place on each date'
        """,
        dict(date='date', store_id='store_nbr', transactions_count='transactions')
       #['date', 'store_nbr']
    ),
    
    events_by_date = CsvSqlDatasetProperties(
        '../dataset/holidays_events.csv',
        'events_by_date',
        """
        CREATE TABLE IF NOT EXISTS events_by_date
        (
            date DATE,
            type TEXT,
            locale TEXT,
            locale_name TEXT,
            description TEXT,
            was_transferred BOOLEAN,
            PRIMARY KEY (date, type, locale, locale_name, description, was_transferred)
        );
        COMMENT ON TABLE events_by_date IS 'Holidays, and other events that took place on a certain date. Multiple events can exist per date.'
        """,
        dict(date='date', type='type', locale='locale', locale_name='locale_name', description='description', was_transferred='transferred')
        #['date', 'type', 'locale', 'locale_name', 'description', 'transferred']
    ),
    
    oil_price_by_date = CsvSqlDatasetProperties(
        '../dataset/oil.csv',
        'oil_price_by_date',
        """
        CREATE TABLE IF NOT EXISTS oil_price_by_date
        (
            date date PRIMARY KEY,
            oil_price double precision
        );
        COMMENT ON TABLE oil_price_by_date IS 'The price of oil, registered on dates where it was available'
        """,
        dict(date='date', oil_price='dcoilwtico')
        #['date']
    ),
)

def get_sql_table_name_of_dataset_of_name(dataset_name:str):
    return properties_by_csv_sql_dataset[dataset_name].sql_table_name