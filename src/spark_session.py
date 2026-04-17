import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "IcebergPOC") -> SparkSession:
    warehouse_path = os.path.abspath("data/warehouse")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", f"file://{warehouse_path}")
        .config("spark.sql.defaultCatalog", "local")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark