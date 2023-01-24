#########################################################################
#
# ourembeddings.py
#
# @author: Phil Mui
# @email: thephilmui@gmail.com
# @date: Mon Jan 23 16:45:56 PST 2023
#
#########################################################################

import numpy as np
from sentence_transformers import SentenceTransformer
from nltk.tokenize import sent_tokenize

EMBEDDING_MODEL_NAME = 'all-MiniLM-L6-v2'
__sentence_transformer = SentenceTransformer(EMBEDDING_MODEL_NAME)

def getDocEmbeddings(doc_list):
    ''' 
    Return documents' embeddings from those docs in the input list
    '''
    docvecs = []

    for doc in doc_list:
        sentences = sent_tokenize(doc)
        sentence_embeddings = __sentence_transformer.encode(sentences)
        docvecs.append(np.mean(sentence_embeddings, axis=0))

    return docvecs