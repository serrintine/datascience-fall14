import pandas as pd

worldcup = pd.read_csv('worldcup.csv')
print worldcup.pivot(index='Country', columns='Year', values='Placement')
