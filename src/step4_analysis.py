from src.spark_session import get_spark_session


def main():
    spark = get_spark_session("Iceberg-Step4-Analysis")

    try:
        print("\n=== 1. Current Schema ===")
        spark.sql("DESCRIBE TABLE local.demo.orders").show(truncate=False)

        print("\n=== 2. Orders Data (focus on schema evolution) ===")
        spark.sql("""
            SELECT
                order_id,
                customer_id,
                order_ts,
                order_status,
                payment_mode
            FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

        print("\n=== 3. Schema Evolution Comparison ===")
        print("Old rows were written before payment_mode existed, so they show NULL.")
        print("New rows written after schema evolution have payment_mode populated.")

        spark.sql("""
            SELECT
                CASE
                    WHEN order_id <= 1008 THEN 'old_rows_before_schema_change'
                    ELSE 'new_rows_after_schema_change'
                END AS row_group,
                COUNT(*) AS row_count,
                SUM(CASE WHEN payment_mode IS NULL THEN 1 ELSE 0 END) AS null_payment_mode_count,
                SUM(CASE WHEN payment_mode IS NOT NULL THEN 1 ELSE 0 END) AS populated_payment_mode_count
            FROM local.demo.orders
            GROUP BY
                CASE
                    WHEN order_id <= 1008 THEN 'old_rows_before_schema_change'
                    ELSE 'new_rows_after_schema_change'
                END
            ORDER BY row_group
        """).show(truncate=False)

        print("\n=== 4. Partitions Table ===")
        spark.sql("""
            SELECT
                partition,
                spec_id,
                record_count,
                file_count,
                last_updated_at,
                last_updated_snapshot_id
            FROM local.demo.orders.partitions
            ORDER BY spec_id, partition
        """).show(truncate=False)

        print("\n=== 5. Partition Evolution Explanation ===")
        print("spec_id = 0 corresponds to the original partition spec.")
        print("After partition evolution, new writes use a newer partition spec.")
        print("Old data is not rewritten; old and new specs can coexist.")

        print("\n=== 6. Files Table ===")
        spark.sql("""
            SELECT
                file_path,
                record_count,
                file_size_in_bytes
            FROM local.demo.orders.files
            ORDER BY file_path
        """).show(truncate=False)

        print("\n=== 7. Files Layout Comparison ===")
        print("Old files have only order_ts_day in the path.")
        print("New files after partition evolution also include customer_id_bucket_4 in the path.")

        spark.sql("""
            SELECT
                CASE
                    WHEN file_path LIKE '%customer_id_bucket_4=%'
                        THEN 'new_partition_spec_files'
                    ELSE 'old_partition_spec_files'
                END AS file_group,
                COUNT(*) AS file_count,
                SUM(record_count) AS total_records
            FROM local.demo.orders.files
            GROUP BY
                CASE
                    WHEN file_path LIKE '%customer_id_bucket_4=%'
                        THEN 'new_partition_spec_files'
                    ELSE 'old_partition_spec_files'
                END
            ORDER BY file_group
        """).show(truncate=False)

        print("\n=== 8. Old vs New Data Written Under Different Layouts ===")
        spark.sql("""
            SELECT
                order_id,
                customer_id,
                order_ts,
                payment_mode,
                CASE
                    WHEN order_id <= 1010 THEN 'written_before_partition_evolution_or_before_new_spec_writes'
                    ELSE 'written_after_partition_evolution'
                END AS write_phase
            FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()