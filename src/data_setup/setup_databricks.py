from data_generators.generators import (
    generate_customers,
    generate_products,
    generate_orders,
    generate_transactions,
)

# create base tables
customers = generate_customers()
products = generate_products()
orders = generate_orders(customers, products)
transactions = generate_transactions(orders)

spark.createDataFrame(customers).write.mode("overwrite").saveAsTable(
    "main.reneluijk_dlt_vs_dynamic_tables.customers"
)
spark.createDataFrame(products).write.mode("overwrite").saveAsTable(
    "main.reneluijk_dlt_vs_dynamic_tables.products"
)
spark.createDataFrame(orders).write.mode("overwrite").saveAsTable(
    "main.reneluijk_dlt_vs_dynamic_tables.orders"
)
spark.createDataFrame(transactions).write.mode("overwrite").saveAsTable(
    "main.reneluijk_dlt_vs_dynamic_tables.transactions"
)

# add more data to base tables
customers = generate_customers()
products = generate_products()
orders = generate_orders(customers, products)
transactions = generate_transactions(orders)

spark.createDataFrame(customers).write.mode("append").saveAsTable(
    "main.reneluijk_dlt_vs_dynamic_tables.customers"
)
spark.createDataFrame(products).write.mode("append").saveAsTable(
    "main.reneluijk_dlt_vs_dynamic_tables.products"
)
spark.createDataFrame(orders).write.mode("append").saveAsTable(
    "main.reneluijk_dlt_vs_dynamic_tables.orders"
)
spark.createDataFrame(transactions).write.mode("append").saveAsTable(
    "main.reneluijk_dlt_vs_dynamic_tables.transactions"
)
