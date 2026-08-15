from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType
)

# Landing paths
stores_path = "abfs://landing@jadabalastorageaccount.dfs.core.windows.net/Stores"
orders_path = "abfs://landing@jadabalastorageaccount.dfs.core.windows.net/orders"
order_items_path = "abfs://landing@jadabalastorageaccount.dfs.core.windows.net/order_items"
products_path = "abfs://landing@jadabalastorageaccount.dfs.core.windows.net/products"
employees_path = "abfs://landing@jadabalastorageaccount.dfs.core.windows.net/employees"
customers_path = "abfs://landing@jadabalastorageaccount.dfs.core.windows.net/customers"

# Bronze paths
bronze_customers_path = "abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/customers"
bronze_orders_path = "abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/orders"
bronze_order_items_path = "abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/order_items"
bronze_products_path = "abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/products"
bronze_stores_path = "abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/stores"
bronze_employees_path = "abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/employees"

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True)
])

customers_df = spark.read.format("csv").option("header", "true").schema(customers_schema).load(customers_path)

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("order_status", StringType(), True)
])
orders_df = spark.read.format("csv").option("header", "true").schema(orders_schema).load(orders_path)

order_items_schema = StructType([
    StructField("order_item_id", IntegerType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("selling_price", IntegerType(), True)
])
order_items_df = spark.read.format("csv").option("header", "true").schema(order_items_schema).load(order_items_path)

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_price", IntegerType(), True),
    StructField("supplier", StringType(), True)
])
products_df = spark.read.format("csv").option("header", "true").schema(products_schema).load(products_path)

employees_schema = StructType([
    StructField("employee_id", StringType(), True),
    StructField("employee_name", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("joining_date", DateType(), True),
    StructField("salary", IntegerType(), True)
])
employees_df = spark.read.format("csv").option("header", "true").schema(employees_schema).load(employees_path)

stores_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("store_address", StringType(), True),
    StructField("phone", StringType(), True)
])
stores_df = spark.read.format("csv").option("header", "true").schema(stores_schema).load(stores_path)

# Write Delta data to Bronze
customers_df.write.format("delta").mode("overwrite").save(bronze_customers_path)
orders_df.write.format("delta").mode("overwrite").save(bronze_orders_path)
order_items_df.write.format("delta").mode("overwrite").save(bronze_order_items_path)
products_df.write.format("delta").mode("overwrite").save(bronze_products_path)
stores_df.write.format("delta").mode("overwrite").save(bronze_stores_path)
employees_df.write.format("delta").mode("overwrite").save(bronze_employees_path)

# Register Bronze Delta tables in Unity Catalog
spark.sql("CREATE SCHEMA retail_catalog.bronze")

spark.sql("""CREATE TABLE retail_catalog.bronze.customers USING DELTA LOCATION 'abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/customers'""")
spark.sql("""CREATE TABLE retail_catalog.bronze.orders USING DELTA LOCATION 'abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/orders'""")
spark.sql("""CREATE TABLE retail_catalog.bronze.order_items USING DELTA LOCATION 'abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/order_items'""")
spark.sql("""CREATE TABLE retail_catalog.bronze.products USING DELTA LOCATION 'abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/products'""")
spark.sql("""CREATE TABLE retail_catalog.bronze.employees USING DELTA LOCATION 'abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/employees'""")
spark.sql("""CREATE TABLE retail_catalog.bronze.stores USING DELTA LOCATION 'abfs://bronze@jadabalastorageaccount.dfs.core.windows.net/stores'""")
