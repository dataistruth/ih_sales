"""
src/common/spark_session.py
===========================
Shared SparkSession for all pipeline layers.

Usage
-----
    from common.spark_session import get_spark

    spark = get_spark()
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "meridian-pipeline") -> SparkSession:
    """
    Return the active SparkSession or create one.
    Calling get_spark() multiple times is safe — Spark returns
    the existing session if one is already running.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark