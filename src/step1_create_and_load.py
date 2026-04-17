import os
from src.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    TimestampType,
    DecimalType,
    StringType,
)

def main():
    spark = get_spark_session("Iceberg-Step1")
    input_path = os.path.abspath("data/input/orders_day1.csv")
    print(input_path)

    schema = StructType([
        StructField("order_id", LongType(), False),
        StructField("customer_id", LongType(), False),
        StructField("order_ts", StringType(), False),
        StructField("order_amount", DecimalType(12, 2), False),
        StructField("order_status", StringType(), False),
        StructField("last_updated_ts", StringType(), False),
    ])

    try:
        # Step 1: Create namespace & drop table
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.demo")
        spark.sql("DROP TABLE IF EXISTS local.demo.orders")

        # Step 2: Read CSV
        orders_df = (
            spark.read
            .option("header", True)
            .schema(schema)
            .csv(input_path)
        )

        # Step 3: Convert to proper types
        orders_df = (
            orders_df
            .withColumn("order_ts", F.to_timestamp("order_ts"))
            .withColumn("last_updated_ts", F.to_timestamp("last_updated_ts"))
        )

        print("\n=== Source Data ===")
        orders_df.show(truncate=False)
        orders_df.printSchema()

        # Step 4: Write to Iceberg table
        (
            orders_df.writeTo("local.demo.orders")
            .using("iceberg")
            .partitionedBy(F.days("order_ts"))
            .create()
        )

        print("\n=== Iceberg Table Data ===")
        spark.sql("""
            SELECT * FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

        print("\n=== Snapshots ===")
        spark.sql("""
            SELECT committed_at, snapshot_id, operation
            FROM local.demo.orders.snapshots
            ORDER BY committed_at
        """).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()





