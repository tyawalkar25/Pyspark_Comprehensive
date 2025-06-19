from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, count_distinct, col, concat_ws, avg, round, rank, desc, percentile_approx, \
    when, expr
from pyspark.sql.types import DateType, LongType
from pyspark.sql.window import Window

class SalesAggregationPipeline:
    def __init__(self,spark):
        self.spark = spark

    def read_data(self,spark):
        df = spark.read.option("multiline", True).json("C:/Users/HP/PycharmProjects/Pyspark_Comprehensive/data/sales_data.json")
        flattend_df = df.select( \
            "transaction_id",
            "date",
            "product.id",
            "product.name",
            col("product.category").alias("category"),
            "product.specifications.weight",
            concat_ws("x", \
                      col("product.specifications.dimensions.length"), \
                      col("product.specifications.dimensions.width"), \
                      col("product.specifications.dimensions.height")).alias("dimensions"),
            col("customer.id").alias("customer_id"),
            col("customer.location.country").alias("country"),
            "customer.location.city",
            "payment.method",
            col("payment.amount").alias("amount"),
            "payment.currency",
            "payment.status",
            "payment.details.transaction_fee",
            "payment.details.tax",
            col("shipping.method").alias("shipping_method"),
            "shipping.cost",
            "shipping.estimated_days"
        )
        return flattend_df

    #other pipeline methods for business metrics
    def calculate_daily_metrics(self,df):
        return df.groupBy("date","country","category")\
                .agg(
                 sum("amount").alias("total_sales"),
                 count("transaction_id").alias("total_transactions"),
                 count_distinct("customer_id").alias("unique_customers"),
                 avg("amount").alias("average_transaction_value"),
                 sum("tax").alias("total_tax"),
                 sum("transaction_fee").alias("total_transaction_fees"))

    def calculate_moving_averages(self,daily_metrics):
        window_7d = Window.partitionBy("country","category").orderBy("date")\
        .rowsBetween(-6,0)

        return (daily_metrics\
                .withColumn("7_day_moving_average_sales",
                            avg("total_sales").over(window_7d))
                .withColumn("7_day_moving_average_transactions",
                            avg("total_transactions").over(window_7d)))

    def calculate_category_performance(self,df):
        window = Window.partitionBy()
        return (df.groupBy("category")
                .agg(sum("amount").cast(LongType()).alias("category_revenue"),
                count("transaction_id").alias("category_transactions"))
                .withColumn("revenue_percentage",
                            round(col("category_revenue")/sum("category_revenue").over(window),3)))

    def identify_top_products(self,df):
        return (df.groupBy("category","id","name")
                .agg(sum("amount").alias("product_revenue"),
                count("transaction_id").alias("product_transactions"))
                .withColumn("rank",
                rank().over(Window.partitionBy("category").orderBy(desc("product_revenue"))))
                .filter(col("rank") <=5 ))

    def calculate_customer_segments(self,df):
        customer_purchases = df.groupBy("customer_id")\
                .agg(sum("amount").alias("total_spent"))

        perc_90 = customer_purchases.agg(percentile_approx(col("total_spent"),0.9).alias("90th_percentile"))
        perc_70 = customer_purchases.agg(percentile_approx(col("total_spent"), 0.7).alias("70th_percentile"))
        perc_40 = customer_purchases.agg(percentile_approx(col("total_spent"), 0.4).alias("40th_percentile"))

        final_df = customer_purchases.crossJoin(perc_90).crossJoin(perc_70).crossJoin(perc_40)
        return (final_df.withColumn("segment",
                when(col("total_spent") >= col("90th_percentile"),"premium")
                .when(col("total_spent") >= col("70th_percentile"), "high_value")
                .when(col("total_spent") >= col("40th_percentile"), "mid_value")
                .otherwise("standard"))
                .select("customer_id", "total_spent", "segment"))



    def run_pipeline(self,spark,output_path):
        try:
            sales_df = self.read_data(spark)
            # Calculate all metrics
            daily_metrics = self.calculate_daily_metrics(sales_df)
            moving_averages = self.calculate_moving_averages(daily_metrics)
            category_metrics = self.calculate_category_performance(sales_df)
            category_top_products = self.identify_top_products(sales_df)
            customer_segments = self.calculate_customer_segments(sales_df)

            return {
                "daily_metrics": daily_metrics,
                "moving_averages": moving_averages,
                "category_metrics": category_metrics,
                "category_top_products": category_top_products,
                "customer_segments": customer_segments
            }

        except Exception as e:
            print("An error occured in Sales Aggregation Pipeline: ", str(e))
            raise

def entrypoint():
    #create spark session
    spark = SparkSession.builder.appName("Sales Aggregation Pipeline").getOrCreate()

    output_path = "C:/Users/HP/PycharmProjects/Pyspark_Comprehensive/output/aggregated_metrics"
    pipeline = SalesAggregationPipeline(spark)
    results = pipeline.run_pipeline(spark,output_path)
    for metric_name,df in results.items():
        print(f"Summary for metric: {metric_name}")
        df.show(truncate=False)

if __name__ == "__main__":
    entrypoint()
    a = input()
