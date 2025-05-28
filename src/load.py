from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
sys.path.append('/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/retail_sales_pipeline/src/utility/')
from logging_config import logger
from utility import write_to_local, write_to_database


## The following functions write the business metrics to path config.processed_folder_path
def local_monthly_sales_by_location(df):
    return write_to_local(df, 'monthly_sales_by_location')

def local_monthly_sales_by_stores(df):
    return write_to_local(df, 'monthly_sales_by_stores')

def local_monthly_sales_by_product(df):
    return write_to_local(df, 'monthly_sales_by_product')

def local_monthly_sales_by_category(df):
    return write_to_local(df, 'monthly_sales_by_category')

def local_sales_by_month(df):
    return write_to_local(df, 'monthly_sales')

def local_average_basket_size(df):
    return write_to_local(df, 'average_basket_size')


## The following function write the business metrics to database tables
def db_monthly_sales_by_location(df):
    return write_to_database(df, 'monthly_sales_by_location')

def db_monthly_sales_by_stores(df):
    return write_to_database(df, 'monthly_sales_by_stores')

def db_monthly_sales_by_product(df):
    return write_to_database(df, 'monthly_sales_by_product')

def db_monthly_sales_by_category(df):
    return write_to_database(df, 'monthly_sales_by_category')

def db_sales_by_month(df):
    return write_to_database(df, 'monthly_sales')

def db_average_basket_size(df):
    return write_to_database(df, 'average_basket_size')