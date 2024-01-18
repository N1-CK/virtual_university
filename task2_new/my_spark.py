from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

spark = SparkSession.builder \
    .appName("HousePricesAnalysis") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.0.jar") \
    .getOrCreate()

csv_path = "/opt/bitnami/spark/data/house_prices.csv"
house_prices_df = spark.read.csv(csv_path, header=True)
house_prices_df.show()



jdbc_url = "jdbc:postgresql://postgres:5432/mydb"
connection_properties = {
    "user": "sonoii",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}
table_name = "house_prices"

house_prices_df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

house_prices_df2 = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

average_prices_df = house_prices_df2.groupBy("property_type", "location", "bedrooms") \
    .agg(round(avg("price"), 3).alias("avg_price"))


average_prices_df.show()

spark.stop()

