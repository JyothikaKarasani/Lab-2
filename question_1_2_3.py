#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import re
from mrjob.job import MRJob
class unique_words(MRJob):
    
    
    def mapper(self, _, line):
        lines=line.split('\n')
        for lin in lines:
            words=[]
            words_only = re.findall(r'\w+', lin.strip())
            for word in words_only:
                words.append(word.lower())
            for word in words:
                yield word, 1
    
    def reducer(self, word, counts):
        count=sum(counts)
        if count==1:
            yield word,count
            
            
            
            
class non_stop_words(MRJob):
    
    
    def mapper(self, _, line):
        lines=line.split('\n')
        for lin in lines:
            words=[]
            words_only = re.findall(r'\w+', lin.strip())
            for word in words_only:
                words.append(word.lower())
            for word in words:
                yield word, 1
    
    def reducer(self, word, counts):
        count=sum(counts)
        stop_words=["he","and","of","a","to","in","is","it"]
        if word not in stop_words:
            yield word,count  
            
class bigrams(MRJob):
    
    
    def mapper(self, _, line):
        bigram=[]
        lines=line.split('\n')
        for lin in lines:
            words=[]
            words_only = re.findall(r'\w+', lin.strip())
            for word in words_only:
                words.append(word.lower())
            for i in range(len(words)-1):
                bigram.append(f"{words[i]},{words[i+1]}")
            for word in bigram:
                yield word, 1
    
    def reducer(self, word, counts):
        count=sum(counts)
        yield word,count            


if __name__ == '__main__':
    print("Folowing are the unique words in the given input file:\n")
    unique_words.run()
    print("\n \n \n \n \n")
    print("Folowing are the words in the given input file after removing the stop words:\n")
    non_stop_words.run()
    print("\n \n \n \n \n")
    print("Folowing are all the bigrams:\n")
    bigrams.run()
    
