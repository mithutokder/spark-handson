from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, lit, dense_rank, count, explode, udf, upper, transform, collect_list
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType


def item_count(languages: []) -> int:
    return len(languages)


@udf(returnType=IntegerType())
def item_count_2(languages: []) -> int:
    return len(languages)


class DataFrameApiPractice:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def learn_dataframe_api(self) -> None:
        input_data: [] = [('Sachin', 'India', 45, 20000, True, ['English', 'Marathi', 'Hindi']),
                          ('Saurav', 'India', 45, 15000, True, ['English', 'Bengali', 'Hindi']),
                          ('Zaheer', 'India', 40, 5000, True, ['English', 'Hindi']),
                          ('Rohit', 'India', 34, 10000, False, ['English', 'Marathi', 'Hindi']),
                          ('Ponting', 'Australia', 38, 19000, True, ['English']),
                          ('Finch', 'Australia', 34, 7000, True, ['English'])]
        schema: StructType = StructType([StructField('name', StringType(), True),
                                         StructField('country', StringType(), True),
                                         StructField('age', IntegerType(), True),
                                         StructField('run', IntegerType(), True),
                                         StructField('retired', BooleanType(), True),
                                         StructField('languages', ArrayType(StringType()), True)])
        data_frame: DataFrame = self.spark.createDataFrame(input_data, schema=schema)
        # print the schema
        data_frame.printSchema()
        # fetch selective columns
        data_frame.select('name', 'run').show(truncate=False)
        # filter, withColumn, order by
        data_frame.filter(col('country') == 'India').withColumn('can bowl',
                                                                lit(True)).orderBy(col('run').desc()) \
            .show(truncate=False)
        # use window function
        data_frame.select('name', 'country', 'run').withColumn("index",
                                                               dense_rank().over(Window.partitionBy('country')
                                                                                 .orderBy(col('run').desc()))) \
            .show(truncate=False)
        # count using window function
        data_frame.select('country').withColumn("index",
                                                count('country').over(Window.partitionBy('country'))) \
            .distinct() \
            .show(truncate=False)
        # using spark sql
        data_frame.select('name', 'country', 'languages').createOrReplaceTempView("country")

        self.spark.sql("select country, count(*) as country_count from country group by country").show()

        # explode function
        data_frame.select('name', 'country', explode('languages')).show(truncate=False)
        data_frame.select('name', 'country', explode('languages')) \
            .select('name', 'country', collect_list('col').over(Window.partitionBy('name', 'country'))).show(
            truncate=False)

        # udf
        length_udf = udf(lambda x: item_count(x), IntegerType())
        data_frame.select(col('name'), length_udf(col('languages')).alias("language_count")).show(truncate=False)

        data_frame.select(col('name'), item_count_2(col('languages')).alias("language_count")).show(truncate=False)

        self.spark.udf.register("length_udf_3", item_count, IntegerType())
        self.spark.sql("select name, length_udf_3(languages) as language_count from country").show(truncate=False)

        # pivot
        data_frame.groupBy('country').pivot('name').sum('run').show(truncate=False)

        data_frame.select(upper(col('country'))).show()

        data_frame.select(transform("languages", lambda x: upper(x))).show()


class DataframeWithFiles:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def explore_csv_file(self) -> None:
        student_schema: StructType = StructType([StructField('id', IntegerType(), True),
                                                 StructField('name', StringType(), True),
                                                 StructField('address', StringType(), True)])

        score_schema: StructType = StructType([StructField('student_id', IntegerType(), True),
                                               StructField('subject_id', IntegerType(), True),
                                               StructField('score', IntegerType(), True)])

        subject_schema: StructType = StructType([StructField('id', IntegerType(), True),
                                               StructField('subject', StringType(), True)])

        student_df: DataFrame = self.spark.read.format("csv") \
            .options(header=True) \
            .schema(student_schema) \
            .load("resources/students.csv")

        score_df: DataFrame = self.spark.read.format("csv") \
            .options(header=True) \
            .schema(score_schema) \
            .load("resources/score.csv")

        subject_df: DataFrame = self.spark.read.format("csv") \
            .options(header=True) \
            .schema(subject_schema) \
            .load("resources/subject.csv")

        student_df.join(score_df, student_df['id'] == score_df['student_id'], 'left') \
            .join(subject_df, score_df['subject_id'] == subject_df['id'], 'left') \
            .select(student_df['name'], subject_df['subject'], score_df['score']) \
            .show()
