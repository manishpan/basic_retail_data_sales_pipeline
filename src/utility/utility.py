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
    missing_columns helper function
    Args:
        df -> dataframe of which columns needs to be check
        required_columns -> list of columns that needs to be in df
    Returns:
        True if all columns are present
        Raise exception if atleast one column is not present
"""
def validate_columns(df, required_columns):
    df_columns = [c.lower() for c in df.columns]
    required_columns = [c.lower() for c in required_columns]
    missing_columns = set(required_columns) - set(df_columns)

    if missing_columns:
        logger.error(f"❌ Error! Missing required columns: {', '.join(missing_columns)}")
        raise ValueError
    return True