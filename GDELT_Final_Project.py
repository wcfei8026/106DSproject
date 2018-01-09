import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframe import *
import curses
import sys

#import pandas as pd
#import numpy as np
#import spark with graphframes
#--packages graphframes:graphframes:0.2.0-spark2.0-s_2.11
spark = SparkSession.builder.appName('gdelt_1').config("spark.jars.packages", "graphframes:graphframes:0.2.0-spark2.0-s_2.11").getOrCreate()
# load CSV
df = spark.read.load(["gs://nccu20171220_josephlin_gdelt_test/gdelt_201707.CSV","gs://nccu20171220_josephlin_gdelt_test/gdelt_201708.CSV","gs://nccu20171220_josephlin_gdelt_test/gdelt_201709.CSV","gs://nccu20171220_josephlin_gdelt_test/gdelt_201710.CSV","gs://nccu20171220_josephlin_gdelt_test/gdelt_201711.CSV","gs://nccu20171220_josephlin_gdelt_test/gdelt_201712.CSV"],format="csv", delimiter=",", header=True)

# shows the df count	

#df138 = df.filter("EventCode = '138'")



def Gdelt_input_proc():
	#event_code = raw_input(">>> Input EventCode : ")
	#print event_code
	#SQLDATE = raw_input(">>> SQLDATE(keyin 'ALL' with date between 20170601~20171231): ")
	#print SQLDATE
	#print 'Processing.....'
	print " Arguments(1) : " + str(sys.argv[1])
	print " Arguments(2) : " + str(sys.argv[2])
	df138 = df.filter("EventCode = " + str(sys.argv[1]))
	print 'AFTER FILTERING EventCode ..... DATAFRAME COUNT():'+str(df138.count())
	if str(sys.argv[2]) == 'ALL':
		print 'ALL'
	else :
		df138 = df138.filter("SQLDATE = "+str(sys.argv[2]))
		
	print 'AFTER FILTERING   SQLDATE ..... DATAFRAME COUNT():'+str(df138.count())

	print 'Processing Vertex.....(join Actor1 and Actor2)'
	#df.printSchema() #print out the schema of your dataset
	# select Actor1 and Actor2 on CSV
	df2 = df138.select([c for c in df138.columns if c in {'Actor1Code','Actor1Name','Actor1EthnicCode','Actor1CountryCode'}])
	df3 = df138.select([c for c in df138.columns if c in {'Actor2Code','Actor2Name','Actor2EthnicCode','Actor2CountryCode'}])
	
	# join Actor1(df2) and Actor2(df3) as vertex 
	df2.join(df3)

	#df2.filter(col('Actor1Code').isin(['MIL'])).show() # filter the row with a specific attribute values
	
	# the content of vertex should contain a distinct row
	vertex = df2.distinct() #get distinct row
	
	# the first column should be named as id to initiate a graphframe
	vertex = vertex.selectExpr("Actor1Code as id", "Actor1Name as Actor1Name", "Actor1EthnicCode as Actor1EthnicCode", "Actor1CountryCode as Actor1CountryCode" )
	vertex.show() #print out the dataset 

	print 'Processing Edge.....(v1=Actor1,v2=Actor2,relation=eventcode)'	
	# select a specific attribute to create the edge dataframe
	# the column named src, dst and relationship cannot change to initiate a graphframe
	df138.createOrReplaceTempView("table1")
	edge = spark.sql("SELECT Actor1Code AS src, Actor2Code as dst, EventCode as relationship from table1")
	edge.collect()
	
	edge.show() #print out the dataset
	
	g = GraphFrame(vertex, edge)

	print 'Processing InDegree TOP(10).....'		
	# Query: Get in-degree of each vertex.
	g.inDegrees.show()
	# Query: Count the number of "138" connections in the graph.
	#g.edges.filter("EventCode = '138'").count()
	g.inDegrees.orderBy("inDegree", ascending=False).show(10)
	
	print 'Processing PageRank.....'
	results = g.pageRank(resetProbability=0.15, maxIter=10)
	results.vertices.select("id", "pagerank").show()

#while (True):
print('<<The Military Events InDegree/PageRank>>')
print('Date Between 20170601~20171231')
print('Total DataRows:'+str(df.count()))
Gdelt_input_proc()
