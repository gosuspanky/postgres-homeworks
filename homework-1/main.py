"""Скрипт для заполнения данными таблиц в БД Postgres."""
import os

import pandas as pd
import psycopg2

data_path = os.path.dirname(os.path.dirname(__file__))

customers_file = os.path.join(data_path, "homework-1", "north_data", "customers_data.csv")
employees_file = os.path.join(data_path, "homework-1", "north_data", "employees_data.csv")
orders_file = os.path.join(data_path, "homework-1", "north_data", "orders_data.csv")

customers = pd.read_csv(customers_file)
employees = pd.read_csv(employees_file)
orders = pd.read_csv(orders_file)

with psycopg2.connect(host="localhost", database="north", user="postgres", password="8520") as connector:
    with connector.cursor() as cursor:
        for customer in customers.itertuples(index=False):
            cursor.execute("INSERT INTO customers VALUES (%s, %s, %s)", customer)

        for employee in employees.itertuples(index=False):
            cursor.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", employee)

        for order in orders.itertuples(index=False):
            cursor.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", order)