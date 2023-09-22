#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
from mrjob.job import MRJob
class inverted_index(MRJob):
    
    
    def mapper(self, _, line):
        document_id,text=line.split(':',1)
        words=[]
        #word_re=re.compile(r'\b\w+\b')
        words_only = re.findall(r'\w+',text)
        for word in words_only:
            words.append(word.lower())
        for word in words:
            yield word,document_id
    
    def reducer(self, word, doc_ids):
        docs=set(doc_ids)
        yield word,str(docs)
if __name__ == '__main__':
    inverted_index.run()







# In[ ]:




