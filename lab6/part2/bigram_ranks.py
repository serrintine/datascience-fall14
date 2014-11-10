from __future__ import print_function
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("bigram").setMaster("local")
sc = SparkContext(conf=conf)
text = sc.textFile("bible+shakes.nopunc")

text = text.map(lambda x: x.split())
bigrams = text.flatMap(lambda x: [((x[i], x[i+1]), 1) for i in range(0, len(x)-1)])
counts = bigrams.reduceByKey(lambda x, y: x + y)
pairs = counts.flatMap(lambda x: [(x[0][0], (x[0], x[1])), (x[0][1], (x[0], x[1]))])
ranks = pairs.combineByKey((lambda x: [x]), (lambda x, y: x + [y]), (lambda x, y: x + y))
top = ranks.map(lambda x: (x[0], sorted(x[1], key=lambda x: x[1])[-5:][::-1]))

with open('bigram_ranks.txt', 'w+') as f:
  sample = top.take(100)
  for result in sample:
    word = result[0]
    bigrams = result[1]
    for tup in bigrams:
      bigram = tup[0][0] + " " + tup[0][1] + " " + str(tup[1])
      print (word + ':' + '\t' + bigram, file=f)
