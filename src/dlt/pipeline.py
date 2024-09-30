import dlt
import pyspark.sql.functions as F


@dlt.table
def successful_orders():
    orders = spark.readStream.table("<catalog>.<schema>.orders")
    transactions = spark.read.table("<catalog>.<schema>.transactions")
    filtered_orders = (
        orders.join(transactions, on=["order_id"])
        .where(F.col("transaction_status") == "Success")
        .select("order_id", "customer_id", "product_id", "order_date", "quantity")
    )
    return filtered_orders


@dlt.table
def customer_sales():
    orders = dlt.read("successful_orders")
    customers = spark.read.table("<catalog>.<schema>.customers")
    products = spark.read.table("<catalog>.<schema>.products")
    customer_product_orders = orders.join(customers, on=["customer_id"]).join(
        products, on=["product_id"]
    )
    customer_sales_report = customer_product_orders.groupBy("customer_id").agg(
        F.sum("price").alias("total_spent")
    )
    return customer_sales_report


@dlt.table
def product_sales_by_category():
    orders = dlt.read("successful_orders")
    products = spark.read.table("<catalog>.<schema>.products")
    product_orders = orders.join(products, on=["product_id"])
    product_sales_report = product_orders.groupBy(
        "product_id", "product_name", "category"
    ).agg(F.count(F.lit(1)).alias("product_category"), F.sum("price").alias("revenue"))
    return product_sales_report
