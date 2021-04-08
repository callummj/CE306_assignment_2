import csv

# Please import via pip3 with command pip3 install elasticsearch
from elasticsearch import Elasticsearch


# Default port for localhosted elasticsearch : 9200
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])



'''
S Y S T E M    S E T T I N G S

Please set your settings for the system here

'''

stemmerLanguage = "English" # Default to english, but you can change to these languages:
useStemmer = True # True -> Use stemmer on analysis, False -> Do not use stemmer

includeStopwords = False # False -> Remove stopwords, True -> Keep them
stopWordsLanguage = "_english_" # Please include underscores ( '_' ), either side of the language string (Available languages defined below


maxResults = 50 # Maximum results which can be shown in the command line
'''


STEMMER LANGUAGES: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-snowball-tokenfilter.html
"Arabic", "Armenian", "Basque", "Catalan", "Danish", "Dutch", "English", "Estonian", "Finnish", "French", "German",
"German2",
"Hungarian", "Italian", "Irish", "Kp", "Lithuanian", "Lovins", "Norwegian", "Porter", "Portuguese", "Romanian",
"Russian", "Spanish",
"Swedish", "Turkish",


STOPWORDS LANGUAGES: https://www.elastic.co/guide/en/elasticsearch/reference/6.8/analysis-stop-tokenfilter.html

_arabic_, _armenian_, _basque_, _bengali_, _brazilian_, _bulgarian_, _catalan_, 
_czech_, _danish_, _dutch_, _english_, _finnish_, _french_, _galician_, _german_, 
_greek_, _hindi_, _hungarian_, _indonesian_, _irish_, _italian_, _latvian_, 
_norwegian_, _persian_, _portuguese_, _romanian_, _russian_, _sorani_,
 _spanish_, _swedish_, _thai_, _turkish_.

'''


# https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-termvectors.html

# Deletes the existing index under 'movies' (if it exists) and then applies my mapping
def createIndex():
    es.indices.delete(index='movies', ignore=[400, 404])  # ignore 400/404 errors if index doesn't exist

    #Depending on the settings defined above the code, this will set the filter settings.
    filter = []
    if useStemmer:
        filter.append("movie_snowball_filter")
    if (includeStopwords == False):
        filter.append("stopfilter")


    map = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            },
            "analysis": {
                "analyzer": {
                    "movie_analyser": {
                        "type": "custom",
                        "tokenizer": "lowercase", # Tokenizer: also makes text lowercase for normal form
                        "filter": filter, # Filter settings as defined above
                    }
                },
                "filter": {
                    "movie_snowball_filter": { # Stemmer, uses snowball stemmer and language is defined above in settings section
                        "type": "snowball",
                        "language": stemmerLanguage,
                    },
                    "stopfilter": { # Stopwords filter, removes stopwords from text, settings are defined above
                        "type": "stop",
                        "stopwords": stopWordsLanguage
                    }
                },
            },
            "similarity": { # Similarity module built into Elasticsearch. Using ES provided script you can reimplement TFIDF following its deprecation in 7.0. (Although this has been made using version 6, in the interest of reliability)
                "tfidf": {
                    # extracted from elasticsearch docs https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-similarity.html#scripted_similarity
                    "type": "scripted",
                    "weight_script": {
                        "source": "double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; return query.boost * idf;"
                    },
                    "script": {
                        "source": "double tf = Math.sqrt(doc.freq); double norm = 1/Math.sqrt(doc.length); return weight * tf * norm;"
                    }
                }

            },
        },
        "mappings": { # Mappings for the movie index: All have an analyzer/filter on except The Release Year. This makes all other fields searchable,as ES retains end user data integrity with these filters
            'movie': {
                "properties": {
                    "Release Year": {
                        "type": "integer"
                    },
                    "Title": {
                        "type": "text",
                        "similarity": "tfidf",
                    },
                    "Origin/Ethnicity": {
                        "type": "text",
                        "analyzer": "movie_analyser",
                        "similarity": "tfidf",
                    },
                    "Director": {
                        "type": "text",
                        "analyzer": "movie_analyser",
                        "similarity": "tfidf",
                    },
                    "Cast": {
                        "type": "text",
                        "analyzer": "movie_analyser",
                        "similarity": "tfidf",
                    },
                    "Genre": {
                        "type": "text",
                        "analyzer": "movie_analyser",
                        "similarity": "tfidf",
                    },
                    "Wiki Page": {
                        "type": "text"
                    },
                     "Plot": {
                        "type": "text",
                        "analyzer": "movie_analyser",
                        "similarity": "tfidf",
                        # https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-similarity.html#scripted_similarity

                    },

                }
            },
        },
    }

    es.indices.create(index='movies', ignore=400, body=map) # Creates the index: 'movies', using my index mapping





#Extracts the first 1000 movies and uploads them directly to Elasticsearch.
def extractAndUpload():
    with open("wiki_movie_plots_deduped.csv", encoding='utf-8') as moviesCSV:
        csvReader = csv.DictReader(moviesCSV)
        movies = []
        id = 0
        for row in csvReader:

            upload(row, id)
            id += 1
            if id > 999:
                break




# Release Year,Title,Origin/Ethnicity,Director,Cast,Genre,Wiki Page,Plot
#Uploads to Elasticsearch
def upload(document, id):
    plot = document['Plot']

    # source: https://careerkarma.com/blog/python-remove-punctuation/

    punctionation = ( "?", ".", ";", ":", "!", "(", ")", "{", "}", "@", "#", "+", "-", "[", "]", "\"")
    alnumPlot = "".join(char for char in plot if
                             char not in (
                            punctionation))

    es.index(index='movies', doc_type='movie', ignore=400, id=id, body={
        'Release Year': document['Release Year'],
        'Title': document['Title'],
        'Origin/Ethnicity': document['Origin/Ethnicity'],
        'Director': document['Director'],
        'Cast': [document['Cast']],
        'Genre': document['Genre'],
        'Wiki Page': document['Wiki Page'],
        'Plot': alnumPlot,
    })




