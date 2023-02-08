import os
from env import host, username, password

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from env import username, host, password

#------------------------------------------------------

def get_connection(db, user=username, host=host, password=password):
    
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

#------------------------------------------------------

def acquire_store():
    
    filename = 'store.csv'
    
    if os.path.exists(filename):
        
        return pd.read_csv(filename)
    
    else:
        
        query = '''
                SELECT sale_date, sale_amount,
                item_brand, item_name, item_price,
                store_address, store_zipcode
                FROM sales
                LEFT JOIN items USING(item_id)
                LEFT JOIN stores USING(store_id)
                '''
        
        url = get_connection(db='tsa_item_demand')
        
        df = pd.read_sql(query, url)
        
        df.to_csv(filename, index=False)
        
        return df
    
#------------------------------------------------------

def acquire_germany():
    
    if os.path.isfile('germany.csv'):
        
        df = pd.read_csv('germany.csv')

        return df
    
    else:
        
        df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
        
        df.to_csv('germany.csv')

        return df
    
#------------------------------------------------------

def wrangle_store_data():
    
    df = acquire.acquire_store()
    
    df['sale_date'] = pd.to_datetime(df['sale_date'], infer_datetime_format=True)
    df = df.set_index('sale_date').sort_index()
    
    df['month'] = df.index.month_name()
    df['day'] = df.index.day_name()
    
    df['sales_total'] = df['sale_amount'] * df['item_price']
    
    return df

#------------------------------------------------------

def wrangle_germany():
    
    df = acquire_germany()
    
    df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True)
    df = df.set_index('Date').sort_index()
    
    df['month'] = df.index.month_name()
    df['year'] = df.index.year_name()
    
    df['Wind'] = df['Wind'].fillna(value=0)
    df['Solar'] = df['Solar'].fillna(value=0)
    df['Wind+Solar'] = df['Wind+Solar'].fillna(value=0)
    
    return df

#------------------------------------------------------

def store_distributions(df):
    
    vars_to_plot = ['sale_amount', 'item_price', 'sales_total']

    for col in vars_to_plot:
    
        df.groupby('sale_date')[vars_to_plot].sum().plot()
        
        plt.show()
        
#------------------------------------------------------

def germany_distributions(df):
    
    vars_to_plot = ['Consumption', 'Wind', 'Solar', 'Wind+Solar']

    for col in vars_to_plot:
    
        df.groupby('Date')[vars_to_plot].sum().plot()
    
        plt.show()

#------------------------------------------------------


#------------------------------------------------------


#------------------------------------------------------


#------------------------------------------------------


#------------------------------------------------------
