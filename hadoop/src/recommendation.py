from pyspark import SparkContext,SparkConf
from pyspark.mllib.feature import Word2Vec,Word2VecModel
import re
import datetime as dt
import pickle

#from py4j.java_gateway import JavaGateway
#gateway = JavaGateway()

conf = SparkConf()
conf.setAppName("Simple App")
conf.set("spark.executor.memory","2g")
sc = SparkContext(conf = conf)
logFile = "./log.log"  # Should be some file on your system
logData = sc.textFile(logFile).cache()


models = Word2VecModel.load(sc,'mude/title_word2vec.model')

java_word_vec_map = models.getVectors()
key_iter = java_word_vec_map.keySet().iterator()
word_vec = dict()
while key_iter.hasNext():
	key = key_iter.next()
	value = models.transform(key)
	word_vec[key] = value
	
print len(word_vec)

broadcast_map = sc.broadcast(word_vec)



def gen_title_vector(uri_title):
	title_line = uri_title[1]

	title_line = filter(lambda ch: ch.isalpha() or ch.isspace(),
					 str(title_line.strip().encode('utf-8')))
	words = title_line.split(' ')
	
	word_vec_map = broadcast_map.value
	title_vec = None
	word_count = 0
	for word in words:
		word_vec = word_vec_map.get(word)
		if word_vec == None:
			continue
		if title_vec == None:
			title_vec = word_vec
		else:
			title_vec = map(add, title_vec, word_vec)
		word_count += 1

	if title_vec == None:
		title_vec = word_vec_map.get('MS')
	else:
		title_vec = map(lambda a: a/float(word_count), title_vec)
	return (uri_title[0], uri_title[1], title_vec)

total_uri_title_vecs = None
for seg_id in segments:
	fn = 'data/warc/seg_%d.warc' % seg_id	
	#fn = 'data/test_samples.warc'
	html_rdd = util.readFileWithDelimiter(spark, fn, 'WARC-Type: request')
	uri_title = html_rdd.map(util.extract_url_title)
	
	uri_title_vecs = uri_title.map(gen_title_vector)
	if total_uri_title_vecs == None:
		total_uri_title_vecs = uri_title_vecs
	else:
		total_uri_title_vecs = total_uri_title_vecs.union(uri_title_vecs)


save_dir = 'mude/uri_vectors_full.pickle'
total_uri_title_vecs.saveAsPickleFile(save_dir)


start_time = dt.datetime.now()