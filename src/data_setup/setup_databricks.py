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
    "<catalog>.<schema>.customers"
)
spark.createDataFrame(products).write.mode("overwrite").saveAsTable(
    "<catalog>.<schema>.products"
)
spark.createDataFrame(orders).write.mode("overwrite").saveAsTable(
    "<catalog>.<schema>.orders"
)
spark.createDataFrame(transactions).write.mode("overwrite").saveAsTable(
    "<catalog>.<schema>.transactions"
)

# add more data to base tables
customers = generate_customers()
products = generate_products()
orders = generate_orders(customers, products)
transactions = generate_transactions(orders)

spark.createDataFrame(customers).write.mode("append").saveAsTable(
    "<catalog>.<schema>.customers"
)
spark.createDataFrame(products).write.mode("append").saveAsTable(
    "<catalog>.<schema>.products"
)
spark.createDataFrame(orders).write.mode("append").saveAsTable(
    "<catalog>.<schema>.orders"
)
spark.createDataFrame(transactions).write.mode("append").saveAsTable(
    "<catalog>.<schema>.transactions"
)
