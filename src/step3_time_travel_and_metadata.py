from src.spark_session import get_spark_session


def main():
    spark = get_spark_session("Iceberg-Step3")

    try:
        print("\n=== Current Table State ===")
        spark.sql("""
            SELECT * FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

        # --------------------------------------------
        # 1. SNAPSHOT EXPLORATION
        # --------------------------------------------
        print("\n=== Snapshots ===")
        snapshots_df = spark.sql("""
            SELECT committed_at, snapshot_id, operation
            FROM local.demo.orders.snapshots
            ORDER BY committed_at
        """)
        snapshots_df.show(truncate=False)

        snapshot_list = snapshots_df.collect()

        # --------------------------------------------
        # 2. TIME TRAVEL (go back to first snapshot)
        # --------------------------------------------
        first_snapshot_id = snapshot_list[0]["snapshot_id"]

        print(f"\n=== Time Travel → First Snapshot ({first_snapshot_id}) ===")
        spark.sql(f"""
            SELECT * FROM local.demo.orders
            VERSION AS OF {first_snapshot_id}
            ORDER BY order_id
        """).show(truncate=False)

        # --------------------------------------------
        # 3. TIME TRAVEL → BEFORE MERGE (second snapshot)
        # --------------------------------------------
        second_snapshot_id = snapshot_list[1]["snapshot_id"]

        print(f"\n=== Time Travel → Second Snapshot ({second_snapshot_id}) ===")
        spark.sql(f"""
            SELECT * FROM local.demo.orders
            VERSION AS OF {second_snapshot_id}
            ORDER BY order_id
        """).show(truncate=False)

        # --------------------------------------------
        # 4. METADATA TABLES
        # --------------------------------------------
        print("\n=== History Table ===")
        spark.sql("""
            SELECT * FROM local.demo.orders.history
            ORDER BY made_current_at
        """).show(truncate=False)

        print("\n=== Files Table ===")
        spark.sql("""
            SELECT file_path, record_count, file_size_in_bytes
            FROM local.demo.orders.files
        """).show(truncate=False)

        # --------------------------------------------
        # 5. REAL DELETE (hard delete)
        # --------------------------------------------
        print("\n=== Performing DELETE (remove CANCELLED orders) ===")
        spark.sql("""
            DELETE FROM local.demo.orders
            WHERE order_status = 'CANCELLED'
        """)

        print("\n=== Table After DELETE ===")
        spark.sql("""
            SELECT * FROM local.demo.orders
            ORDER BY order_id
        """).show(truncate=False)

        # --------------------------------------------
        # 6. SNAPSHOT AFTER DELETE
        # --------------------------------------------
        print("\n=== Snapshots After DELETE ===")
        spark.sql("""
            SELECT committed_at, snapshot_id, operation
            FROM local.demo.orders.snapshots
            ORDER BY committed_at
        """).show(truncate=False)

        # --------------------------------------------
        # 7. TIME TRAVEL AFTER DELETE
        # --------------------------------------------
        print("\n=== Time Travel → Before DELETE (last snapshot before delete) ===")
        all_snapshots = spark.sql("""
            SELECT snapshot_id
            FROM local.demo.orders.snapshots
            ORDER BY committed_at
        """).collect()

        before_delete_snapshot = all_snapshots[-2]["snapshot_id"]

        spark.sql(f"""
            SELECT * FROM local.demo.orders
            VERSION AS OF {before_delete_snapshot}
            ORDER BY order_id
        """).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()