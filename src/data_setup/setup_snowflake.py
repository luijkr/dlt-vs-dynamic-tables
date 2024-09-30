from snowflake.snowpark.context import get_active_session

session = get_active_session()
session.use_role("IT_OPERATIONS")


# paste generators here

fake = Faker()


# create base tables
customers = generate_customers()
products = generate_products()
orders = generate_orders(customers, products)
transactions = generate_transactions(orders)

session.write_pandas(
    df=customers,
    table_name="customers",
    auto_create_table=True,
    quote_identifiers=False,
)
session.write_pandas(
    df=products, table_name="products", auto_create_table=True, quote_identifiers=False
)
session.write_pandas(
    df=orders, table_name="orders", auto_create_table=True, quote_identifiers=False
)
session.write_pandas(
    df=transactions,
    table_name="transactions",
    auto_create_table=True,
    quote_identifiers=False,
)

# add more data to base tables
customers = generate_customers()
products = generate_products()
orders = generate_orders(customers, products)
transactions = generate_transactions(orders)

session.write_pandas(df=customers, table_name="customers")
session.write_pandas(df=products, table_name="products")
session.write_pandas(df=orders, table_name="orders")
session.write_pandas(df=transactions, table_name="transactions")

# add more data to base tables
customers = generate_customers()
products = generate_products()
orders = generate_orders(customers, products)
transactions = generate_transactions(orders)
