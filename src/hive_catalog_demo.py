from pyspark.sql import SparkSession
import os

def main():
    warehouse_path = os.path.abspath("data/hive_warehouse")

    spark = (
        SparkSession.builder
        .appName("Iceberg-Hive-Catalog-Demo")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive_prod.type", "hive")
        .config("spark.sql.catalog.hive_prod.uri", "thrift://localhost:9083")
        .config(
            "spark.sql.catalog.hive_prod.warehouse",
            "file:///tmp/iceberg-hive-warehouse"
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n=== Reset Objects ===")
        spark.sql("DROP TABLE IF EXISTS hive_prod.demo.orders_hive")
        spark.sql("DROP NAMESPACE IF EXISTS hive_prod.demo")

        print("\n=== Create Namespace ===")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_prod.demo")

        print("\n=== Create Table ===")
        spark.sql("""
            CREATE TABLE hive_prod.demo.orders_hive (
                order_id BIGINT,
                customer_id BIGINT,
                order_amount DOUBLE
            )
            USING iceberg
        """)

        print("\n=== Insert Data ===")
        spark.sql("""
            INSERT INTO hive_prod.demo.orders_hive VALUES
            (1, 101, 1000.0),
            (2, 102, 2000.0)
        """)

        print("\n=== Query Table ===")
        spark.sql("""
            SELECT * FROM hive_prod.demo.orders_hive
            ORDER BY order_id
        """).show(truncate=False)

        print("\n=== Show Tables ===")
        spark.sql("SHOW TABLES IN hive_prod.demo").show(truncate=False)

        print("\n=== Describe Extended ===")
        spark.sql("""
            DESCRIBE EXTENDED hive_prod.demo.orders_hive
        """).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()