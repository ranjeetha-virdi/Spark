# -*- coding: utf-8 -*-
"""
Created on Mon May 15 11:03:47 2023

@author: kulpr
"""

from pyspark import SparkConf, SparkContext
import nltk
nltk.download('punkt')
nltk.download('stopwords')

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import string

def normalizeWord(text):
  
    words =  word_tokenize(text)
    words=[word.lower() for word in words if word.isalpha() and len(word)> 2]
    word = " ".join(words).lower()
    
    
    return word.split()


conf = SparkConf().setMaster("local").setAppName("wordcount")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///c:/SparkCourse/book.txt")
words = lines.flatMap(normalizeWord)
wordcounts = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordcounts.map(lambda x : (x[1],x[0])).sortByKey()

results = wordCountsSorted.collect()



for result in results:
    count = str(result[0])
    
    word = result[1].encode("ascii", "ignore")
    if (word) : 
        print (word.decode() + ":\t\t " + count)
    
    



