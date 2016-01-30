"""Test of similar title website comparision"""
from pyspark import SparkContext,SparkConf
import re
import datetime as dt
import util
from operator import add, mul 
import math
from pyspark.mllib.feature import Word2Vec,Word2VecModel
import random


conf = SparkConf()
conf.setAppName("Simple App")
conf.set("spark.executor.memory","1g")
spark = SparkContext(conf = conf)
logFile = "./log.log"  # Should be some file on your system
logData = spark.textFile(logFile).cache()


#read data
uri_title_vecs = spark.pickleFile('mude/uri_vectors_full.pickle')
uri_title_vecs.cache()

count = uri_title_vecs.count()

num_test = 100

test_set = uri_title_vecs.take(num_test*10)
broadcast_vec = None


def calculate_similarity(uri_title_vec):
	cur_vec = uri_title_vec[2]
	candidate_vec = broadcast_vec.value
	dot_val = reduce(lambda a,b: a+b , map(mul, candidate_vec, cur_vec) )
	manitude = math.sqrt(reduce(lambda a,b: a+b,map(lambda a: a*a, cur_vec))) \
				  * math.sqrt(reduce(lambda a,b: a+b, map(lambda a: a*a, candidate_vec)))
	return (uri_title_vec[0],uri_title_vec[1],dot_val/manitude)

start = random.random() * num_test * 9
end = start + num_test
for doc_ind in range(start,end)
	uri_title_vec = test_set[doc_ind]
	candidate_vec = uri_title_vec[2]
	broadcast_vec = spark.broadcast(candidate_vec)
	uri_title_similarity = uri_title_vecs.map(calculate_similarity)
	top_10_similar = uri_title_similarity.takeOrdered(10, lambda x: -x[2])
	
	top_10_similar.append(uri_title_vec)

	save_path = 'mude/full/similary_test_%d.txt' % doc_ind
	spark.parallelize(top_10_similar).saveAsTextFile(save_path)



