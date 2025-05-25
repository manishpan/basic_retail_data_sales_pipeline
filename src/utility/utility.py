import os
from logging_config import logger

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