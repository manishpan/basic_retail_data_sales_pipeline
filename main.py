from src.extract import *
from src.transform import *
from src.load import *
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
                .appName('retail_sales_pipeline') \
                .config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-j-8.4.0.jar") \
                .getOrCreate()

    ## Reading all dataframes

    df_sales = extract_sales(spark)
    df_products = extract_products(spark)
    df_stores = extract_stores(spark)


    ## Validating columns
    validate_columns(df_sales, 'sales')
    validate_columns(df_stores, 'stores')
    validate_columns(df_products,'products')


    ## Calculating business metrics
    df_monthly_sales_by_location = monthly_sales_by_location(df_sales, df_stores)
    df_monthly_sales_by_stores = monthly_sales_by_stores(df_sales, df_stores)
    df_monthly_sales_by_products = monthly_sales_by_product(df_sales, df_products)
    df_monthly_sales_by_category = monthly_sales_by_category(df_sales, df_products)
    df_monthly_sales = sales_by_month(df_sales)
    df_average_basket_size = average_basket_size(df_sales)


    ### Writing dataframes calculated to local path
    local_monthly_sales_by_location(df_monthly_sales_by_location)
    local_monthly_sales_by_stores(df_monthly_sales_by_stores)
    local_monthly_sales_by_product(df_monthly_sales_by_products)
    local_monthly_sales_by_category(df_monthly_sales_by_category)
    local_sales_by_month(df_monthly_sales)
    local_average_basket_size(df_average_basket_size)


    ### Writing dataframes to databases
    db_monthly_sales_by_location(df_monthly_sales_by_location)
    db_monthly_sales_by_stores(df_monthly_sales_by_stores)
    db_monthly_sales_by_product(df_monthly_sales_by_products)
    db_monthly_sales_by_category(df_monthly_sales_by_category)
    db_sales_by_month(df_monthly_sales)
    db_average_basket_size(df_average_basket_size)


    logger.info("Application ran successfully")
    spark.stop()

if __name__ == '__main__':
    main()