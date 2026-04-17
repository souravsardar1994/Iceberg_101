from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DecimalType,
    StringType,
)

from src.spark_session import get_spark_session


def get_orders_schema() -> StructType:
    return StructType([
        StructField("order_id", LongType(), False),
        StructField("customer_id", LongType(), False),
        StructField("order_ts", StringType(), False),
        StructField("order_amount", DecimalType(12, 2), False),
        StructField("order_status", StringType(), False),
        StructField("last_updated_ts", StringType(), False),
    ])


def read_orders_csv(spark, path: str):
    schema = get_orders_schema()

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )

    return (
        df
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("last_updated_ts", F.to_timestamp("last_updated_ts"))
    )


def main():
    spark = get_spark_session("Iceberg-Step2")

    new_orders_path = "data/input/orders_day2_new.csv"
    updates_path = "data/input/orders_day2_updates.csv"

    try:
        print("\n=== Current table before Step 2 ===")
        spark.sql("""
            SELECT * FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

        print("\n=== Reading day 2 new orders ===")
        new_orders_df = read_orders_csv(spark, new_orders_path)
        new_orders_df.show(truncate=False)

        print("\n=== Appending new day 2 orders ===")
        new_orders_df.writeTo("local.demo.orders").append()

        print("\n=== Table after append ===")
        spark.sql("""
            SELECT * FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

        print("\n=== Reading day 2 updates ===")
        updates_df = read_orders_csv(spark, updates_path)
        updates_df.show(truncate=False)

        updates_df.createOrReplaceTempView("orders_updates_stage")

        print("\n=== Merging updates into Iceberg table ===")
        spark.sql("""
            MERGE INTO local.demo.orders AS target
            USING orders_updates_stage AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET
                target.customer_id = source.customer_id,
                target.order_ts = source.order_ts,
                target.order_amount = source.order_amount,
                target.order_status = source.order_status,
                target.last_updated_ts = source.last_updated_ts
            WHEN NOT MATCHED THEN INSERT (
                order_id,
                customer_id,
                order_ts,
                order_amount,
                order_status,
                last_updated_ts
            )
            VALUES (
                source.order_id,
                source.customer_id,
                source.order_ts,
                source.order_amount,
                source.order_status,
                source.last_updated_ts
            )
        """)

        print("\n=== Final table after merge ===")
        spark.sql("""
            SELECT * FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

        print("\n=== Snapshot history after append + merge ===")
        spark.sql("""
            SELECT committed_at, snapshot_id, operation
            FROM local.demo.orders.snapshots
            ORDER BY committed_at
        """).show(truncate=False)

        print("\n=== History table ===")
        spark.sql("""
            SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
            FROM local.demo.orders.history
            ORDER BY made_current_at
        """).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()