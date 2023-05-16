# -*- coding: utf-8 -*-
"""
Created on Mon May 15 12:12:08 2023

@author: kulpr
"""

from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("MaxTempratures")
sc = SparkContext(conf = conf)


def parseLines(line):
    fields = line.split(",")
    StationID = fields[0]
    entrytype = fields[2]
    temprature = float(fields[3])*0.1*(9.0/5.0) + 32.0
    return (StationID, entrytype,temprature)


lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLines)
minTemps = parsedLines.filter(lambda x : "TMIN" in x[1])
stationTemps = minTemps.map(lambda x : (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x,y : min(x,y))
results = minTemps.collect();
for result in results : 
    print(result[0] + "\t{:.2f}F".format(result[1]))