from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, FloatType,LongType
import codecs


def loadMovieNames():
    MovieNames = {}
    with codecs.open("C:/SparkCourse/movie_ratings/ml-100k/u.item",\
                     "r" , encoding = "ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split("|")
            MovieNames[int(fields[0])] = fields[1]
    return MovieNames
            

spark = SparkSession.builder.appName("MoviesBest").getOrCreate()
nameDict = spark.sparkContext.broadcast(loadMovieNames())


schema = StructType([\
                     StructField("UserID", IntegerType(),True),\
                     StructField("MovieID", IntegerType(), True),\
                     StructField("Rating", IntegerType(), True),\
                     StructField("TimeStamp", LongType(),True)
                    ])
    

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/movie_ratings/ml-100k/u.data")

movieCount = moviesDF.groupBy("MovieID").count()


def lookupName(MovieID):
    return nameDict.value[MovieID]


lookupNameUDF = func.udf(lookupName)

movieswithNames = movieCount.withColumn("MovieTitle", lookupNameUDF(func.col("MovieID")))

SortedMovieswithName = movieswithNames.orderBy(func.desc("count"))
SortedMovieswithName.show(10, False)
spark.stop()
