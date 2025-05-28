import os
import sys
sys.path.append('/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/retail_sales_pipeline/configs/')
sys.path.append('/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/retail_sales_pipeline/src/utility/')
import config
from logging_config import logger
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

"""
    write_to_local(df, name) writes name.csv to config.processed_folder_path
    Args:
        df -> dataframe to be written
        name -> name of the file after writing df
"""
def write_to_local(df, filename = None):
    if not filename:
        logger.error(f"❌ Error! File must have a name")
        raise ValueError('Filename cannot be None')
    save_path = f"{config.processed_folder_path}/{filename}"

    logger.info(f"🧵 Writing {filename}.csv to {save_path}")
    df.write.format('csv').option('header', 'true').mode('OVERWRITE').save(save_path)
    logger.info(f"✅ {filename}.csv written successfully")

    return True

class DatabaseWrite:
    def __init__(self, url, properties):
        self.url = url
        self.properties = properties

    def write_dataframe(self, df, table_name):
        try:
            logger.info(f"🧵 Writing into {table_name}")
            df.write.jdbc(url = self.url,
                        table = table_name,
                        mode = 'append',
                        properties = self.properties)
            logger.info(f"✅ Data successfully written into {table_name}")
        except Exception as e:
            logger.error(f"❌ Error writing to {table_name}: {e}")
            raise e
        
def write_to_database(df, table_name = None):
    if table_name is None:
        logger.error(f"❌ Error! table_name must be provided")
        raise ValueError('table_name cannot be None')

    dbw = DatabaseWrite(config.url, config.properties)
    dbw.write_dataframe(df, table_name)
    return True