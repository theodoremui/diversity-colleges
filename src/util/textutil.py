#########################################################################
#
# textutil.py
#
# @author: Phil Mui
# @email: thephilmui@gmail.com
# @date: Mon Jan 23 16:45:56 PST 2023
#
#########################################################################

import re
import numpy as np
from scipy.spatial import distance

import nltk
# nltk.download('stopwords')
# nltk.download('omw-1.4')
# nltk.download('wordnet')
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
__lemmatizer = WordNetLemmatizer()

__diversity_words = [
    'diversity', 
    'equity',
    'inclusion',
    'inclusive',
    'inclusivity',
    'equality', 
    'equal opportunity',
    'social justice',
    'racial justice', 
    'multi-cultural', 
    'multicultural',
    'intercultural',
    'intersectional',
    'intersectionality',
    'anti-discrimination'
]
__diversity_lemmas = [__lemmatizer.lemmatize(w) for w in __diversity_words]
__diversity_pattern = "\b" + "|".join(__diversity_lemmas) + "\b"

__english_stopwords = stopwords.words('english')

def filterText(text_series):
    '''Clean & prep text (numpy.Series): lowercase, lemmatize, stopword'''
    text_series = text_series.apply(
        lambda x: " ".join(__lemmatizer.lemmatize(w.lower()) for w in x.split() 
                           if w not in __english_stopwords))
    text_series = text_series.apply(lambda x: x if len(x) > 100 else None)
    text_series = text_series.dropna()
    total_diversity_words = sum([len(re.findall(__diversity_pattern, text)) 
                                 for text in text_series])
    return text_series, total_diversity_words

def filterTextWithYears(text_series, year_series):
    '''Clean & prep text (numpy.Series): lowercase, lemmatize, stopword'''
    text_series = text_series.apply(
        lambda x: " ".join(__lemmatizer.lemmatize(w.lower()) for w in x.split() 
                           if w not in __english_stopwords))
    result_pairs = []
    for t,y in zip(text_series, year_series):
        if len(t) > 100:
            result_pairs.append((t, y))
    return zip(*result_pairs)

def getCovDispersion(wv):
    N = len(wv)
    cov = np.cov(wv)
    return (N, 
            np.trace(cov)/N, 
            np.linalg.norm(cov, ord=1)/N, 
            np.linalg.norm(cov, ord=2)/N, 
            np.linalg.norm(cov, ord=np.inf)/N, 
            )

def cosine_distance(a, b):
    """Calculate the cosine distance between two numpy arrays.
    
    Parameters:
    a (numpy array): First input array.
    b (numpy array): Second input array.
    
    Returns:
    float: Cosine distance between a and b.
    """
    # Calculate dot product and magnitudes of the input arrays
    dot   = np.dot(a, b)
    a_mag = np.linalg.norm(a)
    b_mag = np.linalg.norm(b)
    
    if np.isclose(a_mag, 0, rtol=1e-9, atol=1e-12):
        print(f"a_mag is very small: {a_mag}")
    if np.isclose(b_mag, 0, rtol=1e-9, atol=1e-12):
        print(f"b_mag is very small: {b_mag}")
    
    # Calculate and return the cosine distance
    return 1.0 - (dot / (a_mag * b_mag))

def getPairwiseCosineDistances(wv):
    if len(wv) <= 1: return 0.0
    darray = distance.cdist([wv[0]], wv[1:], 'cosine')
    return darray[0].sum() + getPairwiseCosineDistances(wv[1:])

def getNormalizedPairwiseDispersion(wv):
    '''Normalize the dispersion by (N-Choose-2) number of pairs'''
    N = len(wv)
    return getPairwiseCosineDistances(wv) / (N * (N-1)/2.)


if __name__ == "__main__":
    a1 = [1, 0]
    a2 = [2, 0]
    a3 = [1, 1]

    wv = [a1, a2, a3]
    print(getPairwiseCosineDistances(wv[:1]))
    print(getPairwiseCosineDistances(wv[:2]))
    print(getPairwiseCosineDistances(wv[:3]))
