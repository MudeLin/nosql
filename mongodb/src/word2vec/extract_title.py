class MySentence(object):
	def __iter__(self):
		for docs in db.title.find():
			title = docs['title']
			yield title.lower().split(' ')