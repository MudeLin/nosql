from pyspark import SparkContext,SparkConf
from pyspark.mllib.feature import Word2Vec
import re
import datetime as dt
import pickle

conf = SparkConf()
conf.setAppName("Simple App")
conf.set("spark.executor.memory","2g")
sc = SparkContext(conf = conf)
logFile = "./log.log"  # Should be some file on your system
logData = sc.textFile(logFile).cache()

DELIMETER = "WARC/1.0"
FILE_NAME = "warc_data/2015/CC-MAIN-20150417045713-00003-ip-10-235-10-82.ec2.internal.warc.gz"
text_file= sc.newAPIHadoopFile(FILE_NAME,"org.apache.hadoop.mapreduce.lib.input.TextInputFormat", "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text", conf = {"textinputformat.record.delimiter":DELIMETER}).map(lambda l:l[1])

def find_uri_and_title(block):
	uri = None
	title = None
	uri_pattern = re.compile("WARC-Target-URI: (.*)")
	title_pattern = re.compile("<title>(.*)</title>")
	for lin in block.split('\n'):
		if uri == None:
			match = uri_pattern.findall(lin) 
			if len(match) > 0:
				uri = match[0]
		if title == None:
			match = title_pattern.findall(lin)
			if len(match) > 0:
				title = match[0].strip().lower()
		if uri != None and title != None :
			break
	if uri == None or title == None:
		return []
	title = filter(lambda ch: ch.isalpha() or ch.isspace(),title)
	return [(uri, title)]

# extract uri and titles
uri_titles = text_file.flatMap(find_uri_and_title)

corpus =  uri_titles.map(lambda uri_title: uri_title[1].split(' '))

word2vec = Word2Vec().setVectorSize(100)
model = word2vec.fit(corpus)
model.save(sc, 'mude/title_word2vec.model')
#word_vecs = sc.parallelize( model.getVectors() )
#word_vecs.saveAsPickleFile('mude/word_vecs.pickle')

# save models here 


