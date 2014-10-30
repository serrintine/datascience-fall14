import pandas as pd

relevant = pd.read_csv('product_mapping.csv')
retrieved = pd.read_csv('products_out.csv')

# Only keep Amazon-Google clusters
# since ground truth doesn't have Google-Google or Amazon-Amazon clusters
retrieved = pd.merge(retrieved[retrieved.source == 'amazon'],
                     retrieved[retrieved.source == 'google'],
                     on = 'Cluster ID')
retrieved = retrieved[['id_x', 'id_y']]
retrieved.columns = ['idAmazon', 'idGoogleBase']

# According to Wikipedia
# precision = |relevant ^ retrieved| / |retrieved|
# recall = |relevant ^ retrieved| / |relevant|
intersect = pd.merge(relevant, retrieved, on = ['idAmazon', 'idGoogleBase'])
precision = float(len(intersect)) / len(retrieved)
recall = float(len(intersect)) / len(relevant)

print "Precision:", precision
print "Recall:", recall
