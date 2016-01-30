

""" Similarity documents based on title"""
from pyspark import SparkContext,SparkConf
import re
import datetime as dt
import util
from pyspark.mllib.feature import Word2Vec

conf = SparkConf()
conf.setAppName("Similar Title Train")
conf.set("spark.executor.memory","2g")
spark = SparkContext(conf = conf)
logFile = "./log.log"  # Should be some file on your system
logData = spark.textFile(logFile).cache()

# read data
#txt_lines = spark.textFile("data/warc/seg_7.warc")

kSegments = 1

warc_list = spark.textFile('mude/warc_list.txt').collect()

kWarcNum = len(warc_list)
if (kSegments > kWarcNum):
	kSegments = kWarcNum


corpos = None
for seg_id in range(0,kSegments):
	
	fn = warc_list[seg_id]

	txt_lines = spark.textFile(fn)
	#print 'Origin txt line count: ',txt_lines.count()

	# filter link line 
	title_lines =  txt_lines.filter(lambda line: '<title>' in line.strip() and '</title>' in line.strip())
	cur_corpos = title_lines.flatMap(util.extract_title).map(lambda line: line.split(' '))

	if corpos == None:
		corpos = cur_corpos
	else:
		corpos = corpos.union(cur_corpos)			

word2vec = Word2Vec()
word_vec_model = word2vec.fit(corpos)

word_vec_model.save(spark,'mude/title_word2vec.model')

java_word_vec_map = word_vec_model.getVectors()
key_iter = java_word_vec_map.keySet().iterator()
word_vec_map = dict()
while key_iter.hasNext():
	key = key_iter.next()
	value = word_vec_model.transform(key)
	word_vec_map[key] = value

broadcast_map = spark.broadcast(word_vec_map)

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
for seg_id in range(0,kSegments):
	fn = warc_list[seg_id]

	html_rdd = util.readFileWithDelimiter(spark, fn, 'WARC-Type: request')
	uri_title = html_rdd.map(util.extract_url_title)
	
	uri_title_vecs = uri_title.map(gen_title_vector)
	if total_uri_title_vecs == None:
		total_uri_title_vecs = uri_title_vecs
	else:
		total_uri_title_vecs = total_uri_title_vecs.union(uri_title_vecs)


save_dir = 'mude/uri_vectors_full.pickle'
total_uri_title_vecs.saveAsPickleFile(save_dir)

