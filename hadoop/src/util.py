
from pyspark import SparkContext
import re
import xml.etree.cElementTree as ET

def extract_title(line):
		title_lines = re.findall('<title>(.+)</title>',line)	
		title_lines = map(lambda line: filter( lambda ch: ch.isalpha() or ch.isspace(), str(line.encode('utf-8'))) ,title_lines)
		return title_lines	


def readFileWithDelimiter(spark_context, filename, delimiter = ','):
	return spark_context.newAPIHadoopFile(filename, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
            "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
            conf={"textinputformat.record.delimiter": delimiter}).map(lambda l:l[1])

def extract_url_title(warc_item):
	warc_lines = warc_item.split('\n')
	uris = []
	
	for line in warc_lines:
		uris = re.findall('WARC-Target-URI:(.*)',line)
		if len(uris) > 0:
			break
	title_pattern = re.compile('<html.*<head.*<title>(.+)</title>.*/head>.*',re.DOTALL)
	titles = title_pattern.findall(warc_item)
	
	if len(uris) == 0:
		uris = ['http://default.com']
	if len(titles) == 0:
		titles = ['default, hello word']
	return (uris[0], titles[0])