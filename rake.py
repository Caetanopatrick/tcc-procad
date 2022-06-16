# Adapted from: github.com/aneesha/RAKE/rake.py
from __future__ import division
import operator
import nltk
import string
import pandas as pd
import os
import sqlite3


nltk.download('punkt')

d = os.path.dirname(os.getcwd())

print(d)

# Read sqlite query results into a pandas DataFrame
con = sqlite3.connect(f"{d}/tcc-procad/42.userdata/msgstore.db")
messages = pd.read_sql_query("SELECT * from messages", con)

# Verify that result of SQL query is stored in the dataframe
print(messages.head())

con.close()



from rake_nltk import Rake

# Uses stopwords for english from NLTK, and all puntuation characters by
# default
nltk.download('stopwords')
nltk.download('words')

def test():
  texto = ''
  
  for index, row in messages.iterrows():
      word_list = []
      if row['key_remote_jid'] == 'jean_barddal.pdf':
          word_list = row['data'].split()
          if len(word_list) <= 5:
              texto = texto + row['data'] + '. '
  

  words = set(nltk.corpus.words.words())

    
  " ".join(w for w in nltk.wordpunct_tokenize(texto) \
         if w.lower() in words or not w.isalpha())
  r = Rake()

  # Extraction given the text.
  r.extract_keywords_from_text(texto)

  # To get keyword phrases ranked highest to lowest.
  r.get_ranked_phrases()

  # To get keyword phrases ranked highest to lowest with scores.
  print(r.get_ranked_phrases_with_scores()[0:50])
  
  
if __name__ == "__main__":
  test()
