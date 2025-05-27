from pyspark.sql.types import StructType, StructField, StringType, StringType, IntegerType, DoubleType, DateType

## Required column of dataframes
required_columns_maps = {'products': ['product_id', 'name', 'cost_price'], 
                            'stores': ['store_id', 'store_name', 'location'],
                            'sales': ['transaction_id', 'date', 'store_id', 'product_id', 'quantity', 'unit_price']}

## Schemas of dataframes
products_schema = StructType([
    StructField('product_id', IntegerType(), False),
    StructField('name', StringType(), False),
    StructField('cost_price', StringType(), False)
])

stores_schema = StructType([
    StructField('store_id', IntegerType(), False),
    StructField('store_name', StringType(), False),
    StructField('location', StringType(), False)
])

sales_schema = StructType([
    StructField('transaction_id', IntegerType(), False),
    StructField('date', DateType(), True),
    StructField('store_id', IntegerType(), False),
    StructField('product_id', IntegerType(), False),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DoubleType(), False),
    StructField('total_price', DoubleType(), True)
])

df_schemas = {'products': products_schema, 'stores': stores_schema, 'sales': sales_schema}