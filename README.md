# Apache Iceberg Lakehouse POC

This project demonstrates a hands-on implementation of Apache Iceberg using PySpark.

## Features Covered

- Table creation
- Incremental load
- Merge (upsert)
- Delete operations
- Snapshots & Time Travel
- Metadata tables
- Schema evolution
- Partition evolution
- Hadoop catalog vs Hive catalog

## Tech Stack

- PySpark 3.5
- Apache Iceberg 1.10
- Docker (Hive Metastore)
- Local filesystem

## Project Structure

- src/ → implementation scripts
- data/ → input data
- docker/ → Hive metastore setup

## How to Run

```bash
python -m src.step1_create_and_load