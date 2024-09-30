from faker import Faker
import pandas as pd
import random

fake = Faker()


def generate_customers(num_customers=100):
    customers = []
    for _ in range(num_customers):
        customers.append(
            {
                "customer_id": fake.uuid4(),
                "name": fake.name(),
                "email": fake.email(),
                "address": fake.address(),
                "city": fake.city(),
                "account_created": fake.date_this_decade(),
            }
        )
    return pd.DataFrame(customers)


def generate_products(num_products=50):
    categories = ["Electronics", "Clothing", "Books", "Furniture", "Toys"]
    products = []
    for _ in range(num_products):
        products.append(
            {
                "product_id": fake.uuid4(),
                "product_name": fake.word().capitalize(),
                "category": random.choice(categories),
                "price": round(random.uniform(10, 1000), 2),
                "stock_quantity": random.randint(1, 100),
            }
        )
    return pd.DataFrame(products)


def generate_orders(customers, products, num_orders=200):
    orders = []
    for _ in range(num_orders):
        customer = random.choice(customers.to_dict("records"))
        product = random.choice(products.to_dict("records"))
        orders.append(
            {
                "order_id": fake.uuid4(),
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "order_date": fake.date_this_year(),
                "quantity": random.randint(1, 5),
            }
        )
    return pd.DataFrame(orders)


def generate_transactions(orders):
    transactions = []
    for order in orders.to_dict("records"):
        transactions.append(
            {
                "transaction_id": fake.uuid4(),
                "order_id": order["order_id"],
                "transaction_date": fake.date_this_year(),
                "payment_method": random.choice(
                    ["Credit Card", "PayPal", "Bank Transfer"]
                ),
                "transaction_status": random.choice(["Success", "Failed"]),
            }
        )
    return pd.DataFrame(transactions)
