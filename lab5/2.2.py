import nltk
import re

p = re.compile('''.*(director|partner|analyst|governor|producer|manager|
comissioner|executive|writer|chief|professor|chair(wo)?man|editor|librarian|
lawyer|economist|researcher|counsel|president|spokes(wo)?man|leader).*''')
for doc in nltk.corpus.ieer.parsed_docs('NYT_19980315'):
  for rel in nltk.sem.extract_rels('PER', 'ORG', doc, corpus='ieer', pattern = p):
    print(nltk.sem.rtuple(rel))

