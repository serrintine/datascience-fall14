import nltk

files = ["ebola" + str(i) + ".md" for i in range(1, 11)]
for f in files:
  with open(f, "r") as article:
    article = article.read()
    sents = nltk.sent_tokenize(article)
    tokenized_sents = [nltk.word_tokenize(sent) for sent in sents]
    tagged_sents = [nltk.pos_tag(sent) for sent in tokenized_sents]
    chunked_sents = nltk.ne_chunk_sents(tagged_sents)
    for sent in chunked_sents:
      for chunk in sent:
        if type(chunk) is nltk.tree.Tree:
          entity = "" 
          for name in chunk.leaves():
            entity += (" " + name[0])
          print entity + ", " + chunk.label()
