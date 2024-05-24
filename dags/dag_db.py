from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import numpy as np
import os
import json
import pendulum
import pickle 
from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, String, Date, Boolean, SmallInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect
from sqlalchemy import create_engine, schema 
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import insert
import csv

# default_args={
#     'owner':'me',
#     'retries':1,
#     'retry_delay':timedelta(minutes=1)
# }

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bykers_taskflow"],
)
def bikers_taskflow():
    @task()
    def init_dag():
        pass


    @task()
    def Transactions_load():
        Transactions = pd.read_excel(
            os.path.join("/opt/airflow/dags/99Bikers_Raw_data.xlsx"),
            engine='openpyxl',
            sheet_name='Transactions'
        )
        Transactions = Transactions.dropna(axis=1, how='all').dropna(axis=0, how='all')
        Transactions.to_csv("/opt/airflow/dags/Transactions.csv")
    
    @task()
    def CustomerDemographic_load():
        CustomerDemographic = pd.read_excel(
            os.path.join("/opt/airflow/dags/99Bikers_Raw_data.xlsx"),
            engine='openpyxl',
            sheet_name='CustomerDemographic'
        )
        CustomerDemographic = CustomerDemographic.dropna(axis=1, how='all').dropna(axis=0, how='all')
        CustomerDemographic.to_csv("/opt/airflow/dags/CustomerDemographic.csv")
    
    @task()
    def NewCustomerList_load():
        NewCustomerList = pd.read_excel(
            os.path.join("/opt/airflow/dags/99Bikers_Raw_data.xlsx"),
            engine='openpyxl',
            sheet_name='NewCustomerList'
        )
        NewCustomerList = NewCustomerList.dropna(axis=1, how='all').dropna(axis=0, how='all')
        NewCustomerList.to_csv("/opt/airflow/dags/NewCustomerList.csv")
    
    @task()
    def CustomerAddress_load():
        CustomerAddress = pd.read_excel(
            os.path.join("/opt/airflow/dags/99Bikers_Raw_data.xlsx"),
            engine='openpyxl',
            sheet_name='CustomerAddress'
        )
        CustomerAddress = CustomerAddress.dropna(axis=1, how='all').dropna(axis=0, how='all')
        CustomerAddress.to_csv("/opt/airflow/dags/CustomerAddress.csv")

    @task()
    def transform():
        Transactions = pd.read_csv('/opt/airflow/dags/Transactions.csv')
        CustomerDemographic = pd.read_csv('/opt/airflow/dags/CustomerDemographic.csv')
        #NewCustomerList = pd.read_csv('/opt/airflow/dags/NewCustomerList.csv')
        CustomerAddress = pd.read_csv('/opt/airflow/dags/CustomerAddress.csv')

        Transactions['transaction_date'] = pd.to_datetime(Transactions['transaction_date'] , format='mixed')
        Transactions['transaction_date']  = Transactions['transaction_date'].dt.strftime('%Y-%m-%d')
        transaction_date = Transactions.transaction_date.unique()
        transaction_date_data = [{'transaction_date_id': key + 1, "transaction_date": cnt} for key, cnt in enumerate(transaction_date)]
        Transactions['transaction_date'].replace([i['transaction_date'] for i in transaction_date_data], [i['transaction_date_id'] for i in transaction_date_data], inplace=True)

        Transactions = Transactions.fillna(2)
        Transactions['online_order'] = Transactions['online_order'].astype(int)
        Transactions['transaction_id'] = Transactions['transaction_id'].astype(int)
        Transactions['product_id'] = Transactions['product_id'].astype(int)
        Transactions['customer_id'] = Transactions['customer_id'].astype(int)

        list_price = Transactions.list_price.unique()
        list_price_data = [{'id': key + 1, "name": cnt} for key, cnt in enumerate(list_price)]

        for list_item in list_price_data:
            Transactions.loc[Transactions['list_price'] == list_item['name'], 'product_id'] = list_item['id']

        Product = Transactions[[
            'product_id', 
            'brand',
            'product_line',
            'product_class',
            'product_size'
            ]]

        Product_unique = Product.drop_duplicates()
        product_table_data = Product_unique.to_dict('records')

        Customer = CustomerDemographic[[
            'customer_id',
            'first_name',
            'last_name',
            'gender',
            'past_3_years_bike_related_purchases',
            'DOB',
            'job_title',
            'job_industry_category',
            'wealth_segment',

            ]]
        Customer_join = Customer.join(CustomerAddress.set_index('customer_id'), on='customer_id')
        Customer_cleaned = Customer_join[[
            'customer_id',
            'first_name',
            'last_name',
            'gender',
            'past_3_years_bike_related_purchases',
            'DOB',
            'job_title',
            'job_industry_category',
            'wealth_segment',
            'address',
            'postcode',
            'state',
        ]]

        Customer_cleaned.loc[Customer_cleaned['gender'] == 'F', 'gender'] = 'Female'
        Customer_cleaned.loc[Customer_cleaned['gender'] == 'Femal', 'gender'] = 'Female'
        Customer_cleaned.loc[Customer_cleaned['gender'] == 'M', 'gender'] = 'Male'
        Customer_cleaned['DOB'] = pd.to_datetime(Customer_cleaned['DOB'] , format='mixed')
        Customer_cleaned['DOB']  = Customer_cleaned['DOB'].dt.strftime('%Y-%m-%d')
        Customer_cleaned = Customer_cleaned.fillna(0)
        Customer_cleaned['customer_id'] = Customer_cleaned['customer_id'].astype(int)
        Customer_cleaned['past_3_years_bike_related_purchases'] = Customer_cleaned['past_3_years_bike_related_purchases'].astype(int)
        Customer_cleaned['postcode'] = Customer_cleaned['postcode'].astype(int)
        customer_cleaned_data = Customer_cleaned.to_dict('records')

        transactions_data = Transactions[[
            'transaction_id',
            'product_id',
            'customer_id',
            'transaction_date',
            'online_order',
            'list_price',
            'standard_cost',
        ]]
        transactions_data = transactions_data.fillna(2)
        transactions_data['online_order'] = transactions_data['online_order'].astype(int)
        transactions_data = transactions_data.to_dict('records')

        with open('/opt/airflow/dags/transaction_date_data.pkl', 'wb') as f:
            pickle.dump(transaction_date_data, f)
        with open('/opt/airflow/dags/product_table_data.pkl', 'wb') as f:
            pickle.dump(product_table_data, f)
        with open('/opt/airflow/dags/customer_cleaned_data.pkl', 'wb') as f:
            pickle.dump(customer_cleaned_data, f)
        with open('/opt/airflow/dags/transactions_data.pkl', 'wb') as f:
            pickle.dump(transactions_data, f)

    @task()
    def init_db():
        # Настройка соединения с БД
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = 'airflow',
            password = 'airflow',
            host = '192.168.1.57',
            port = 5432,
            database = 'bikers',
        )

        engine = create_engine(engine_string)
        if not database_exists(engine.url):
            create_database(engine.url)

        def drop_table(table_name, engine=engine):
            Base = declarative_base()
            metadata = MetaData()
            metadata.reflect(bind=engine)
            table = metadata.tables[table_name]
            if table is not None:
                Base.metadata.drop_all(engine, [table], checkfirst=True)

        inspector = inspect(engine)
        for table_name in inspector.get_table_names():
            drop_table(table_name)

    @task()
    def load():
        # Настройка соединения с БД
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = 'airflow',
            password = 'airflow',
            host = '192.168.1.57',
            port = 5432,
            database = 'bikers',
        )
        engine = create_engine(engine_string)

        meta = MetaData()
        transaction_date_table = Table(
        'transaction_date', meta,
        Column('transaction_date_id', SmallInteger, primary_key = True),
        Column('transaction_date', String),
        )
        meta.create_all(engine)

        meta = MetaData()
        product_table = Table(
        'product_table', meta,
        Column('product_id', Integer, primary_key = True),
        Column('brand', String),
        Column('product_line', String),
        Column('product_class', String),
        Column('product_size', String),
        )
        meta.create_all(engine)

        meta = MetaData()
        customer_table = Table(
        'customer_table', meta,
        Column('customer_id', Integer, primary_key = True),
        Column('first_name', String),
        Column('last_name', String),
        Column('past_3_years_bike_related_purchases', SmallInteger),
        Column('DOB', String),
        Column('job_title', String),
        Column('job_industry_category', String),
        Column('wealth_segment', String),
        Column('address', String),
        Column('postcode', SmallInteger),
        Column('state', String),
        )
        meta.create_all(engine)

        meta = MetaData()
        transactions_data_table = Table(
        'transactions_data_table', meta,
        Column('transaction_id', Integer, primary_key = True),
        Column('product_id', Integer),
        Column('customer_id', Integer),
        Column('transaction_date', String),
        Column('online_order', SmallInteger),
        Column('list_price', Float),
        Column('standard_cost', Float),

        )
        meta.create_all(engine)


        with open('/opt/airflow/dags/transaction_date_data.pkl', 'rb') as f:
            transaction_date_data = pickle.load(f)
        with open('/opt/airflow/dags/product_table_data.pkl', 'rb') as f:
            product_table_data = pickle.load(f)
        with open('/opt/airflow/dags/customer_cleaned_data.pkl', 'rb') as f:
            customer_cleaned_data = pickle.load(f)
        with open('/opt/airflow/dags/transactions_data.pkl', 'rb') as f:
            transactions_data = pickle.load(f)

        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = 'airflow',
            password = 'airflow',
            host = '192.168.1.57',
            port = 5432,
            database = 'bikers',
        )
        engine = create_engine(engine_string)

        with engine.connect() as conn:
            conn.execute(
                insert(transaction_date_table),
                transaction_date_data
            )

        with engine.connect() as conn:
            conn.execute(
                insert(product_table),
                product_table_data
            )


        with engine.connect() as conn:
            conn.execute(
                insert(customer_table),
                customer_cleaned_data
            )

        with engine.connect() as conn:
            conn.execute(
                insert(transactions_data_table),
                transactions_data
            )
        with open('/opt/airflow/dags/transaction_date_data.csv','w') as f:
            writer = csv.DictWriter(f, fieldnames=transaction_date_data[0])
            writer.writeheader()
            writer.writerows(transaction_date_data)
        with open('/opt/airflow/dags/product_table_data.csv','w') as f:
            writer = csv.DictWriter(f, fieldnames=product_table_data[0])
            writer.writeheader()
            writer.writerows(product_table_data)
        with open('/opt/airflow/dags/transactions_data.csv','w') as f:
            writer = csv.DictWriter(f, fieldnames=transactions_data[0])
            writer.writeheader()
            writer.writerows(transactions_data)
        with open('/opt/airflow/dags/customer_cleaned_data.csv','w') as f:
            writer = csv.DictWriter(f, fieldnames=customer_cleaned_data[0])
            writer.writeheader()
            writer.writerows(customer_cleaned_data)
    init_dag() >> [Transactions_load() , CustomerDemographic_load() , NewCustomerList_load() , CustomerAddress_load()] >> transform() >> init_db() >>  [load()]

bikers_taskflow()




'''with open('saved_dictionary.pkl', 'wb') as f:
    pickle.dump(dictionary, f)
        
with open('saved_dictionary.pkl', 'rb') as f:
    loaded_dict = pickle.load(f)'''