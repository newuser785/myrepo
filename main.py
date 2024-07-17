from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import random

spark = SparkSession.builder.appName("CreateSyntheticPurchaseData").getOrCreate()

# Самостоятельно определяете названия товаров (мин. 5)
products = ["товар1", "товар2", "товар3", "товар4", "товар5"]

def generate_random_data(products, num_rows):
    data = []
    for _ in range(num_rows):
        date = datetime.utcnow() - timedelta(days=random.randint(0, 365))
        user_id = random.randint(1, 1000)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        price = round(random.uniform(10.0, 100.0), 2)

        data.append((date, user_id, product, quantity, price))

    return data

num_rows = 1000
columns = ["Дата", "UserID", "Продукт", "Количество", "Цена"]
data = generate_random_data(products, num_rows)

df = spark.createDataFrame(data, columns)

# Сохранение DataFrame в один файл
df.coalesce(1).write.mode("overwrite").csv("output_data.csv", header=True)
