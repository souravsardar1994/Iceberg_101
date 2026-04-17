from pyspark.sql import functions as F
from decimal import Decimal
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DecimalType,
    StringType,
)

from src.spark_session import get_spark_session


def main():
    spark = get_spark_session("Iceberg-Step4")

    try:
        print("\n=== Current Schema ===")
        spark.sql("DESCRIBE TABLE local.demo.orders").show(truncate=False)

        # --------------------------------------------------
        # 1. SCHEMA EVOLUTION (Add new column)
        # --------------------------------------------------
        print("\n=== Adding new column: payment_mode ===")
        spark.sql("""
            ALTER TABLE local.demo.orders
            ADD COLUMN payment_mode STRING
        """)

        print("\n=== Schema After Evolution ===")
        spark.sql("DESCRIBE TABLE local.demo.orders").show(truncate=False)

        # --------------------------------------------------
        # 2. INSERT DATA WITH NEW COLUMN
        # --------------------------------------------------
        print("\n=== Inserting new data with payment_mode ===")

        new_data = [
            (1009, 509, "2026-04-14 10:00:00", Decimal("1999.00"), "CREATED", "2026-04-14 10:00:00", "UPI"),
            (1010, 510, "2026-04-14 11:30:00", Decimal("2999.00"), "CREATED", "2026-04-14 11:30:00", "CARD"),
        ]

        schema = StructType([
            StructField("order_id", LongType(), False),
            StructField("customer_id", LongType(), False),
            StructField("order_ts", StringType(), False),
            StructField("order_amount", DecimalType(12, 2), False),
            StructField("order_status", StringType(), False),
            StructField("last_updated_ts", StringType(), False),
            StructField("payment_mode", StringType(), True),
        ])

        df = spark.createDataFrame(new_data, schema=schema)

        df = (
            df
            .withColumn("order_ts", F.to_timestamp("order_ts"))
            .withColumn("last_updated_ts", F.to_timestamp("last_updated_ts"))
        )

        df.writeTo("local.demo.orders").append()

        print("\n=== Data After Insert ===")
        spark.sql("""
            SELECT * FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

        # --------------------------------------------------
        # 🔥 IMPORTANT OBSERVATION
        # --------------------------------------------------
        print("\n=== Observe payment_mode column ===")
        print("Old records → NULL")
        print("New records → populated")

        # --------------------------------------------------
        # 3. PARTITION EVOLUTION
        # --------------------------------------------------
        print("\n=== Adding new partition field: bucket(customer_id) ===")

        spark.sql("""
            ALTER TABLE local.demo.orders
            ADD PARTITION FIELD bucket(4, customer_id)
        """)

        print("\n=== Partition Spec ===")
        spark.sql("""
            SELECT * FROM local.demo.orders.partitions
        """).show(truncate=False)

        # --------------------------------------------------
        # 4. INSERT AFTER PARTITION EVOLUTION
        # --------------------------------------------------
        print("\n=== Insert more data after partition evolution ===")

        new_data_2 = [
            (1011, 511, "2026-04-15 09:00:00", Decimal("499.00"), "CREATED", "2026-04-15 09:00:00", "UPI"),
            (1012, 512, "2026-04-15 10:30:00", Decimal("899.00"), "CREATED", "2026-04-15 10:30:00", "CARD"),
        ]

        df2 = spark.createDataFrame(new_data_2, schema=schema)

        df2 = (
            df2
            .withColumn("order_ts", F.to_timestamp("order_ts"))
            .withColumn("last_updated_ts", F.to_timestamp("last_updated_ts"))
        )

        df2.writeTo("local.demo.orders").append()

        print("\n=== Final Data ===")
        spark.sql("""
            SELECT * FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

        print("\n=== Files Table (after partition evolution) ===")
        spark.sql("""
            SELECT file_path, record_count
            FROM local.demo.orders.files
        """).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()