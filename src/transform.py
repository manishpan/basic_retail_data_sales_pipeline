from utility.logging_config import logger
from extract import *
from utility.utility import validate_columns

df_products = extract_products(spark)
df_stores = extract_stores(spark)
df_sales = extract_sales(spark)

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

def check_mandatory_columns(df, domain=None):
    if domain not in {'sales', 'products', 'stores'}:
        logger.error(f"❌ Error! Domain must be one of sales, products or stores")
        raise ValueError
    
    required_columns_maps = {'products': ['product_id', 'name', 'cost_price'], 
                             'stores': ['store_id', 'store_name', 'location'],
                             'sales': ['transaction_id', 'date', 'store_id', 'product_id', 'quantity', 'unit_price']}
    
    logger.info(f"🧵 Validating {domain} columns")

    validate_columns(df, required_columns_maps[domain])

    logger.info(f"✅ Validation of {domain} is successful")
    return True