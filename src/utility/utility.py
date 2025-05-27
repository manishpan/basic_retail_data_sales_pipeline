import os
from utility.logging_config import logger

""" 
    read_csv_file helper function
    Args:
        spark -> SparkSession
        path -> path of the file to be read
    
    Returns:
        dataframe containing data of the file present at path
"""
def read_csv_file(spark, path):
    """  This function read csv file given at the path"""
    if os.path.exists(path):
        df = spark.read.format('csv') \
                .option('header', 'true') \
                .option('inferSchema', 'true') \
                .load(path)
        filename = os.path.basename(path)
        logger.info(f"**** ✅ {filename} loaded successfully ****")

        return df
    else:
        filename = os.path.basename(path)
        logger.error(f'**** ❌ Error! {filename} does not exists ****')
        raise FileNotFoundError
    

"""
    validate_columns helper function
    Args:
        df -> dataframe of which columns needs to be check
        required_columns -> list of columns that needs to be in df
    Returns:
        True if all columns are present
        Raise exception if atleast one column is not present
"""
def missing_columns(df, required_columns):
    df_columns = [c.lower() for c in df.columns]
    required_columns = [c.lower() for c in required_columns]
    missing_columns = set(required_columns) - set(df_columns)

    if missing_columns:
        logger.error(f"❌ Error! Missing required columns: {', '.join(missing_columns)}")
        raise ValueError
    return True

"""
    check_schema function checks schema for dataframes
    Args:
        df -> dataframe for which schema is to be checked
        schema -> schema which df should be following
    
    Returns:
        Warns if df schema if not as per schema
"""

def check_schema(df, schema):
    if len(df.schema.fields) != len(schema.fields):
        logger.error(f"❌ Schema length mismatch")
        raise ValueError  
    
    for expected_field, actual_field in zip(schema.fields, df.schema.fields):
        if expected_field.name != actual_field.name:
            logger.warning(f"⚠️ Warning! {actual_field.name} is not equal to {expected_field.name}")
        if expected_field.dataType != actual_field.dataType:
            logger.warning(f"⚠️ Warning! {actual_field.name} dataType({actual_field.dataType}) is not {expected_field.dataType}")
        if expected_field.nullable != actual_field.nullable:
            logger.warning(f"⚠️ Nullability mismatch on {actual_field.name}")

    return True

"""
    sales_join_products joins sales dataframe and products dataframe
    return the inner join of sales dataframe and products dataframe based on product_id
"""
def sales_join_products(df_sales, df_products):
    return df_sales.join(df_products, on = ['product_id'], how = 'inner').dropDuplicates()


"""
    sales_join_stores joins sales dataframe and stores dataframe
    return the inner join of sales dataframe and stores dataframe based on store_id
"""
def sales_join_stores(df_sales, df_stores):
    return df_sales.join(df_stores, on = ['store_id'], how = 'inner').dropDuplicates()