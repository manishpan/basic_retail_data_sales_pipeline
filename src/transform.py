from utility.logging_config import logger
from extract import *
from utility.schemas import required_columns_maps, df_schemas
from utility.utility import missing_columns, check_schema, sales_join_products, sales_join_stores
from pyspark.sql.types import StructType, StructField, StringType, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import *

## checking for mandatory columns

# def check_mandatory_columns(df, domain=None):
#     if domain not in {'sales', 'products', 'stores'}:
#         logger.error(f"❌ Error! Domain must be one of sales, products or stores")
#         raise ValueError
#     logger.info(f"🧵 Validating {domain} columns")

#     if domain == 'products':
#         products_required_columns = ['product_id', 'name', 'cost_price']
#         validate_columns(df, products_required_columns)

#     elif domain == 'stores':
#         stores_required_columns = ['store_id', 'store_name', 'location']
#         validate_columns(df, stores_required_columns)

#     else:
#         sales_required_columns = ['transaction_id', 'date', 'store_id', 'product_id', 'quantity', 'unit_price']
#         validate_columns(df, sales_required_columns)

#     logger.info(f"✅ Validation of {domain} is successful")
#     return True

def validate_columns(df, domain=None):
    if domain not in {'sales', 'products', 'stores'}:
        logger.error(f"❌ Error! Domain must be one of sales, products or stores")
        raise ValueError

    ## Checking for mandatory columns    
    logger.info(f"🧵 Validating {domain} columns")
    missing_columns(df, required_columns_maps[domain])
    logger.info(f"✅ {domain} Column validation passed")

    ## Checking for schema
    logger.info(f"🧵 Validating {domain} schema")
    check_schema(df, df_schemas[domain])
    logger.info(f"✅ {domain} schema validation passed")

## The following functions implement business logic
def monthly_sales_by_location(df_sales, df_stores):
    df = sales_join_stores(df_sales, df_stores)
    logger.info(f"🧵 Calculating monthly sales by location")
    df = df.groupBy('location', month('date')) \
        .agg({'total_price': 'sum'}) \
        .withColumnRenamed('month(date)', 'month') \
        .withColumnRenamed('sum(total_price)', 'total_revenue') \
        .withColumn('total_revenue', round('total_revenue', 2)) \
        .orderBy('location', 'month')
    logger.info(f"✅ Done")
    return df

def monthly_sales_by_stores(df_sales, df_stores):
    df = sales_join_stores(df_sales, df_stores)
    logger.info(f"🧵 Calculating monthly sales by stores")
    df = df.groupBy('store_name', month('date')) \
        .agg({'total_price': 'sum'}) \
        .withColumnRenamed('month(date)', 'month') \
        .withColumnRenamed('sum(total_price)', 'total_revenue') \
        .withColumn('total_revenue', round('total_revenue', 2)) \
        .orderBy('store_name', 'month')

    logger.info(f"✅ Done")
    return df

def monthly_sales_by_product(df_sales, df_products):
    df = sales_join_products(df_sales, df_products)
    logger.info(f"🧵 Calculating monthly sales by product")
    df = df.groupBy('name', month('date')) \
        .agg({'total_price': 'sum', 'quantity': 'sum'}) \
        .withColumnRenamed('month(date)', 'month') \
        .withColumnRenamed('sum(total_price)', 'total_revenue') \
        .withColumnRenamed('sum(quantity)', 'units_sold') \
        .withColumn('total_revenue', round('total_revenue', 2)) \
        .orderBy('name', 'month')

    logger.info(f"✅ Done")
    return df

def monthly_sales_by_category(df_sales, df_products):
    df = sales_join_products(df_sales, df_products)
    logger.info(f"🧵 Calculating monthly sales by category")
    df = df.groupBy('category', month('date')) \
        .agg({'total_price': 'sum', 'quantity': 'sum'}) \
        .withColumnRenamed('month(date)', 'month') \
        .withColumnRenamed('sum(total_price)', 'total_revenue') \
        .withColumnRenamed('sum(quantity)', 'units_sold') \
        .withColumn('total_revenue', round('total_revenue', 2)) \
        .orderBy('category', 'month')    

    logger.info(f"✅ Done")
    return df

def sales_by_month(df_sales):
    logger.info(f"🧵 Calculating monthly sales")
    df = df_sales.groupBy(month(col('date'))) \
            .agg({'transaction_id': 'count', 'quantity': 'sum', 'total_price': 'sum'}) \
            .withColumnRenamed('month(date)', 'month') \
            .withColumnRenamed('count(transaction_id)', 'transaction_count') \
            .withColumn('quantity_per_transaction', round(col('sum(quantity)') / col('transaction_count'), 2)) \
            .withColumn('items_price_per_transaction', round(col('sum(total_price)') / col('transaction_count'), 2)) \
            .orderBy('month')

    logger.info(f"✅ Done")
    return df

def average_basket_size(df_sales):
    logger.info(f"🧵 Calculating average transaction size")
    df = df_sales.groupBy(month(col('date'))) \
                .agg({'total_price': 'avg'}) \
                .withColumnRenamed('month(date)', 'month') \
                .withColumnRenamed('avg(total_price)', 'avg_monthly_basket_value') \
                .withColumn('avg_monthly_basket_value', round('avg_monthly_basket_value', 2)) \
                .orderBy('month')

    logger.info(f"✅ Done")
    return df