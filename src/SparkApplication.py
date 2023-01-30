from pyspark.sql import SparkSession

from src.core.SparkInitializer import create_or_get_spark_session
from src.practice.BasicPractice import DataFrameApiPractice, DataframeWithFiles

if __name__ == '__main__':
    spark: SparkSession = create_or_get_spark_session("spark-handson")
    data_frame_practice: DataFrameApiPractice = DataFrameApiPractice(spark)
    data_frame_file: DataframeWithFiles = DataframeWithFiles(spark)
    data_frame_practice.learn_dataframe_api()
    #data_frame_file.explore_csv_file()
