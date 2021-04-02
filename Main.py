from elasticsearch import Elasticsearch
import Assignment1Index as assignment1
import Assignment2Index as assignment2
import json


# Default port for localhosted elasticsearch : 9200
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Create a Movie object so it's neater to store in the test collection dictionary
class Movie:
    def __init__(self, movie):
        self.title = movie['_source']['Title']
        self.genre = movie['_source']['Genre']
        self.origin = movie['_source']['Origin/Ethnicity']
        self.releaseYear = movie['_source']['Release Year']
        self.director = movie['_source']['Director']
        self.cast = movie['_source']['Cast']
        self.plot = movie['_source']['Plot']
        self.id = movie['_id']

    def toString(self):
        print("----------------------------------------------------------------------------")
        print("Title: ", self.title, "\nRelease Year: ", self.releaseYear, "\nGenre: ", str.capitalize(self.genre), "\nID: ", self.id, "\nOrigin: ", self.origin)
        print(
            "----------------------------------------------------------------------------")


def getResults(terms, filter):

    # Search the Database for western movies, this is what
    # will make up my testCollection

    '''
    matchQuery = {
        "query":
            {
                "match": {
                    field: {
                        "query": query,
                        "fuzziness": 1
                    }
                }
            }
    }
    '''


    queryM = {
        "query":{
            "range": {
                  "Release Year": {
                    "gte": 1900,
                    "lte": 1909,
                    "boost": 1.0
                  }
            },
        }
    }




    fieldQuery = {}
    for term in terms:
        field = term[0]
        searchTerm = term[1]
        fieldQuery[field] = {"query": field + ":", "fuzziness": 1}



    indexOneSettings = {
        "analyzer": {
            "movie_analyser": {
                "type": "custom",
                "tokenizer": "lowercase",
                # Tokenizer: also makes text lowercase for normal form
                "filter": filter,
                # Filter settings as defined above
            }
        },
        "filter": {
            "movie_snowball_filter": {
                # Stemmer, uses Porter stemmer and language is defined above in settings section
                "type": "porter_stem",
                "language": "English", # This can be changed -> See top of Assignment1Index for more details
            },
            "stopfilter": {
                # Stopwords filter, removes stopwords from text, settings are defined above
                "type": "stop",
                "stopwords": "_english_"
            }
        },
    }



    if (filter != False):


        # Combine the queries
        # Source: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-filter-context.html#query-filter-context-ex

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match":
                            {
                            field: searchTerm,

                           }},
                    ],
                    "filter": [
                        {
                            # Gets movies with a filterLower >= Release Year <= filterHigher
                            # Source: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html#range-query-ex-request
                            "range":
                                {
                                    "Release Year": {
                                        "gte": filter[0],
                                        "lte": filter[1],
                                    },

                                }
                        }
                    ]
                }
            }

        }
    else:
        query = {
            "query":
                {
                    "match_phrase": {
                        field: {
                            "query": searchTerm,
                            "analyzer": "movie_analyser",
                            #"fuzziness": "AUTO" # Source: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html#query-dsl-match-query-fuzziness
                        }
                    }
                }
        }




    result = es.search(index='movies', body=query,
                       size=50)


    # Dictionary of Movie objects which make up the test
    # collection
    results = []

    for movie in result['hits']['hits']:
        #thisMovie = Movie(movie['_source']['Title'], movie['_source']['Genre'], movie['_source']['_id'] )

        results.append(Movie(movie)) # create object

       # print(movie)
        #testCollection[movie['_source']['Title']] = thisMovie


    return results


def createTestCollection():
    testCollection = {}

    '''
    Queries for test colleciton:
    
    query1 -> Movies About world war 1
    query2 -> Movies about the wild west between 1910-1920
    query3 -> Movies about robbery
      
      
    Covers:
    '''



    query = 0

    #                       Field       Query           Date range
    result = getResults([ ["Plot", " world war 1"] ], False)
    if len(result)==0:
        result = []

    testCollection[query] = result
    query+=1

    result = getResults([["Plot", "The wild west"]], [1910, 1920])
    if len(result) == 0:
        result = []

    testCollection[query] = result
    query+=1


    result = getResults([["Plot", "robbery"]], False)
    if len(result) == 0:
        result = []


    testCollection[query] = result


    return testCollection


'''



1. create a test collection
    Dictionary testCollecton = {
        string      array
        `query` : [results]
        `query2`: [results]
        `queryn`: [results]
    
    }

'''


if __name__ == '__main__':


    testCollection = {}


    # Create the indexes, peform the test queries on each
    assignment1.createIndex() # This function deletes the previous index as well
    assignment1.extractAndUpload()
    testCollection['Index1'] = createTestCollection()



    assignment2.createIndex()
    assignment2.extractAndUpload()
    testCollection['Index2'] = createTestCollection()


    # Peforming Test set on index 1


    print("Top result for each query in each index")
    print("Query 1")
    try:
        testCollection['Index1'][0][0].toString()
    except IndexError:
        print("Query 1 is empty")

    try:
        testCollection['Index2'][0][0].toString()
    except IndexError:
        print("Query 1 is empty")



    print("Query 2")

    try:
        testCollection['Index1'][1][0].toString()
    except IndexError:
        print("Query 2 is empty")

    try:
        testCollection['Index2'][1][0].toString()
    except IndexError:
        print("Query 2 is empty")



    print("Query 3")
    try:
        testCollection['Index1'][2][0].toString()
    except IndexError:
        print("Query 3 is empty")

    try:
        testCollection['Index2'][2][0].toString()
    except IndexError:
        print("Query 3 is empty")

