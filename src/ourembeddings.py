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
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import sent_tokenize, word_tokenize

EMBEDDING_MODEL_NAME = 'all-MiniLM-L6-v2'

def getTFIDFDocEmbeddings(doc_list):
    ''' 
    Return documents' embeddings from those docs in the input list.

    Each document is weighted average of the sentence embedding -- weighted
    by the TFIDF of each word relative to other sentences in the document
    '''
    docvecs = []

    for doc in doc_list:
        sentences = sent_tokenize(doc)
        sentence_transformer = SentenceTransformer(EMBEDDING_MODEL_NAME)
        sentence_embeddings = sentence_transformer.encode(sentences)

        tfidf = TfidfVectorizer()
        tfidf.fit_transform(sentences)

        # Get the tf-idf values for each sentence
        tfidf_matrix = tfidf.transform(sentences)

        weighted_sentence_embeddings = []
        for i, sentence_embedding in enumerate(sentence_embeddings):

            weighted_sentence_embedding = np.zeros(len(sentence_embedding))
            # Iterate through each word in the sentence
            words = word_tokenize(sentences[i])
            tfidf_weight = 0.0
            for j, word in enumerate(words):
                if word in tfidf.vocabulary_:
                    # Get the tf-idf value for the word at index tfidf.vocabulary_[word]
                    tfidf_weight += tfidf_matrix[i, tfidf.vocabulary_[word]]

            # Append the weighted sentence embedding to the list
            weighted_sentence_embeddings.append(tfidf_weight * sentence_embedding)

        # Cdocument vector = the weighted average of the sentence embeddings
        docvecs.append(np.mean(weighted_sentence_embeddings, axis=0))

    return docvecs

def getDocEmbeddings(doc_list):
    ''' 
    Return documents' embeddings from those docs in the input list.

    The document's embedding is the mean of the sentence embeddings.
    '''
    docvecs = []
    for doc in doc_list:
        sentences = sent_tokenize(doc)
        sentence_transformer = SentenceTransformer(EMBEDDING_MODEL_NAME)
        sentence_embeddings = sentence_transformer.encode(sentences)
        docvecs.append(np.mean(sentence_embeddings, axis=0))

    return docvecs