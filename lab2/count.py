from avro.datafile import DataFileReader
from avro.io import DatumReader
import pandas as pd

reader = DataFileReader(open("countries.avro", "r"), DatumReader())

countries = pd.DataFrame.from_records(reader)
big_countries = countries[countries.population > 10000000]

print(big_countries['name'].count())

reader.close()
