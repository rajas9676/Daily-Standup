from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, IntegerType, StringType, StructField, FloatType


def process_file(file_name):
    spark = SparkSession.builder.appName('MinTemp').getOrCreate()
    schema = StructType([StructField('stationId', IntegerType(), True),
                         StructField('date', IntegerType(), True),
                         StructField('measure_type', StringType(), True),
                         StructField('temperature', FloatType(), True)])

    # read the data as dataframe
    data = spark.read.schema(schema).csv(file_name)
    data.printSchema()
    # filter out TMIN entries
    minTemp = data.filter(data.measure_type=='TMIN')
    # select only stationid and temperature columns
    stationTemp = minTemp.select('stationId', 'temperature')
    mintempbystation = stationTemp.groupby('stationId').sum('temperature')
    mintempbystation.show()





