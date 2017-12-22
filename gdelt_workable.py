#import findspark
#findspark.init('/home/joadmin/spark-2.1.2-bin-hadoop2.7/')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#import spark with graphframes
spark = SparkSession.builder.appName('gdelt_1').config("spark.jars.packages", "graphframes:graphframes:0.2.0-spark2.0-s_2.11").getOrCreate()
# load CSV
df = spark.read.load("gs://dataproc-e3ed320f-9323-4292-82c5-3137e4648be9-asia-east1/gdelt_small_data.csv",format="csv", delimiter="\t", header=True)

#df.printSchema() #print out the schema of your dataset
# select Actor1 and Actor2 on CSV
df2 = df.select([c for c in df.columns if c in {'Actor1Code','Actor1Name','Actor1EthnicCode','Actor1CountryCode'}])
df3 = df.select([c for c in df.columns if c in {'Actor2Code','Actor2Name','Actor2EthnicCode','Actor2CountryCode'}])

# join Actor1(df2) and Actor2(df3) as vertex 
df2.join(df3)

#df2.filter(col('Actor1Code').isin(['MIL'])).show() # filter the row with a specific attribute values

# the content of vertex should contain a distinct row
vertex = df2.distinct() #get distinct row

# the first column should be named as id to initiate a graphframe
vertex = vertex.selectExpr("Actor1Code as id", "Actor1Name as Actor1Name", "Actor1EthnicCode as Actor1EthnicCode", "Actor1CountryCode as Actor1CountryCode" )
vertex.show() #print out the dataset 

# select a specific attribute to create the edge dataframe
# the column named src, dst and relationship cannot change to initiate a graphframe
df.createOrReplaceTempView("table1")
edge = spark.sql("SELECT Actor1Code AS src, Actor2Code as dst, EventCode as relationship from table1")
edge.collect()

edge.show() #print out the dataset

from graphframes import *

g = GraphFrame(vertex, edge)

# Query: Get in-degree of each vertex.
g.inDegrees.show()
# Query: Count the number of "039" connections in the graph.
g.edges.filter("EventCode = '039'").count()

