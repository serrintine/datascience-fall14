import sys
import pandas as pd

worldcup = pd.read_csv(sys.stdin, header=0, names=['Country', 'Year', 'Title'])
pivoted = worldcup.pivot(index='Country', columns='Year', values='Title')
print pivoted.to_string(na_rep='-', index_names=False)
