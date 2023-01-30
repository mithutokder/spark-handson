from pyspark.sql import SparkSession


def create_or_get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()
