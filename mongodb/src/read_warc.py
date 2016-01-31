import warc
from pymongo import MongoClient
import os
import bson
client = MongoClient(host="Master.Hadoop",port=21871)

db = client.common_crawl


warc_folder = '/home/hadoop/Downloads/warc_data/'
SAMPLE = True


count = 1
for c_,p_,fns in os.walk(warc_folder):
	for fn in fns:
		if '.gz' not in fn:
			continue	
		abs_path = os.path.join(c_,fn)
		print 'processing file' ,abs_path		
		warc_file = warc.open(abs_path)

		for record in warc_file:
			mongo_record = dict()
			if 'warc-target-uri' not in record.header.keys():
				continue
			for header_key in  record.header.keys():
				mongo_record[header_key] = record.header[header_key]
			
			content = record.payload.read(3*1000*1000)
			content = bson.Binary(str(content))	
			mongo_record['content'] = content

			if SAMPLE:
				db.warc_data_sample.insert_one(mongo_record)
			else:
				db.warc_data.insert_one(mongo_record)
			count += 1
			if count % 1000 == 0:
				print 'processed ', count 
		if count > 1 and SAMPLE:
			break
	if count > 1 and SAMPLE:
		break






