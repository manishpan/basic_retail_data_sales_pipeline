import sys
sys.path.append('/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/retail_sales_pipeline/configs/')
import config
from utility.utility import read_csv_file

def extract_sales(spark):
    path = f'{config.raw_folder_path}/sales.csv'
    return read_csv_file(spark, path)
    
def extract_products(spark):
    path = f'{config.raw_folder_path}/products.csv'
    return read_csv_file(spark, path)
    
def extract_stores(spark):
    path = f'{config.raw_folder_path}/stores.csv'
    return read_csv_file(spark, path)