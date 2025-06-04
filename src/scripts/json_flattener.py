from pyspark.sql import SparkSession
from pyspark.sql.functions import col,concat_ws,explode
import pandas as pd

def create_spark_session():
    return SparkSession.builder\
        .appName("JSON Flattener")\
        .config("spark.sql.shuffle.partitions", 200)\
        .config("spark.default.parallelism", 200)\
        .getOrCreate()

def flatten_json(spark, input_path, output_path):
    # read the json file
    df = spark.read.option("multiline",True).json(input_path)
    flattened_df = df.select(\
        "transaction_id",
        "date",
        "product.id",
        "product.name",
        "product.category",
        "product.specifications.weight",
        concat_ws("x",\
    col("product.specifications.dimensions.length"),\
          col("product.specifications.dimensions.width"),\
          col("product.specifications.dimensions.height")).alias("dimensions"),
        col("customer.id").alias("customer_id"),
        "customer.location.country",
        "customer.location.city",
        "payment.method",
        "payment.amount",
        "payment.currency",
        "payment.status",
        "payment.details.transaction_fee",
        "payment.details.tax",
        col("shipping.method").alias("shipping_method"),
        "shipping.cost",
        "shipping.estimated_days"
)
    #flattened_df.write.mode("overwrite").parquet(output_path)
    return flattened_df.toPandas()

def main():

    spark = create_spark_session()
    input_path = "C:/Users/HP/PycharmProjects/Pyspark_Comprehensive/data/sales_data.json"
    output_path = "/output/"
    df = flatten_json(spark,input_path,output_path)
    df.to_parquet('sales_data.parquet', engine='pyarrow', compression='snappy')


if __name__ == "__main__":
    main()
