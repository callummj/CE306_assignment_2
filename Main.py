from elasticsearch import Elasticsearch
import Assignment1Index as assignment1
import Assignment2Index as assignment2
import nltk
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.porter import PorterStemmer

# Default port for localhosted elasticsearch : 9200
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

MAX_RESULTS = 10

# Create a Movie object so it's neater to store in the test collection dictionary
class Movie:
    def __init__(self, movie):
        self.title = movie['_source']['Title']
        self.genre = movie['_source']['Genre']
        self.origin = movie['_source']['Origin/Ethnicity']
        self.releaseYear = movie['_source']['Release Year']
        self.director = movie['_source']['Director']
        self.cast = movie['_source']['Cast']
        self.plot = nltk.sent_tokenize(movie['_source']['Plot'])
        self.id = movie['_id']
        self.score = movie['_score']


    def toString(self):
        print("----------------------------------------------------------------------------")
        print("Title: ", self.title, "\nRelease Year: ", self.releaseYear, "\nGenre: ", str.capitalize(self.genre), "\nID: ", self.id, "\nOrigin: ", self.origin, "\nScore :", self.score)
        print(
            "----------------------------------------------------------------------------")


    def stemPlot(self):
        snowballStemmer = SnowballStemmer(language='english')
        stemmedPlot = []
        for sentence in self.plot:
            print("sent:" , sentence)
            stemmedSentece = []

            sentence = snowballStemmer.stem(sentence)
            '''
            for word in sentence:
                print("word:" , word)
                word =  snowballStemmer.stem(word)
                stemmedSentece.append(word) 
            '''
            stemmedPlot.append(sentence)

        self.plot = stemmedPlot

def getResults(terms, filter):

    # Search the Database for western movies, this is what
    # will make up my testCollection







    fieldQuery = {}
    for term in terms:
        field = term[0]
        searchTerm = term[1]
        fieldQuery[field] = {"query": field + ":", "fuzziness": 1}





    if (filter != False):


        # Combine the queries
        # Source: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-filter-context.html#query-filter-context-ex

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase":
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
                },
            "sort": [
                "_score"
                    
            ]
        }




    result = es.search(index='movies', body=query,
                       size=50)


    # Dictionary of Movie objects which make up the test
    # collection
    results = []

    i = 1
    for movie in result['hits']['hits']:
        #thisMovie = Movie(movie['_source']['Title'], movie['_source']['Genre'], movie['_source']['_id'] )
        if i > MAX_RESULTS:
            break
        else:
            results.append(Movie(movie)) # create object

        i+=1

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
    result = getResults([ ["Plot", "World war 1 soldiers in the trenches"] ], False)
    if len(result)==0:
        result = []

    testCollection[query] = result
    query+=1

    result = getResults([["Plot", "New York"]], False)
    if len(result) == 0:
        result = []

    testCollection[query] = result
    query+=1


    result = getResults([["Plot", "racing"]], False)
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

    relevantDocs = []

    # Create the indexes, peform the test queries on each
    assignment1.createIndex() # This function deletes the previous index as well
    assignment1.extractAndUpload()
    testCollection['Index1'] = createTestCollection()



    assignment2.createIndex()
    assignment2.extractAndUpload()
    testCollection['Index2'] = createTestCollection()


    # Peforming Test set on index 1

    uniqueDocs = []


    print("Top result for each query in each index")

    for queryNumber in range (0, 3):
        print("Query: ", queryNumber)
        for index in range(1, 3):
            index = "Index" + str(index)

            try:
                testCollection[index][queryNumber][0].toString()
                fname = "query"+str(queryNumber) + str(index) + ".txt"
                f = open(fname, "w")

                res = []

                for i in testCollection[index][queryNumber]:
                    #f.write("Title: " + i.title + " | " + " ID: " + i.id + "\n")
                    f.write(i.title + "\n")

                f.close()
                print("size: ", len(testCollection['Index1'][queryNumber]))
            except IndexError:
                print("Query ", queryNumber," is empty")



    print("----Number of unique documents----")

    for queryNumber in range (0, 3):
        print("Query: ", queryNumber)
        res = []
        for index in range(1, 3):
            index = "Index" + str(index)
            for i in testCollection[index][queryNumber]:
                res.append(i.id)
        print("Unique documents in: ", queryNumber, ": ", len(set(res)))


    print(testCollection['Index1'][1])
    print(testCollection['Index2'][1])

    '''
    

try:
        testCollection[index][queryNumber][0].toString()
        print("size: ", len(testCollection['Index2'][queryNumber]))
    except IndexError:
        print("Query 1 is empty")

    snowballStemmer = SnowballStemmer(
        language='english')
    porterStemmer = PorterStemmer(
       )




    query = 'World War 1'
    query = snowballStemmer.stem(query)

    print("INDEX 1")
    for i in range(0, len(testCollection[index][0])):
        print(testCollection[index][queryNumber][i].plot)
        uniqueDocs.append(
            testCollection[index][queryNumber][i].title)

        plot = str(testCollection[index][queryNumber][i].plot)

        plot = snowballStemmer.stem(plot)

        if query in plot:
            relevantDocs.append(testCollection[index][queryNumber][i])


    #print("ID: ", testCollection['Index1'][queryNumber][i].id)

    index = 'Index2'

    query = 'World War 1'
    query = porterStemmer.stem(query)

    print("INDEX 2")
    for i in range (0, len(testCollection[index][0])):
        print(testCollection[index][queryNumber][i].id)
        uniqueDocs.append(testCollection[index][queryNumber][i].title)
        plot = str(testCollection[index][queryNumber][i].plot)

        plot = porterStemmer.stem(plot)

        if query in plot:
            relevantDocs.append(
                testCollection[index][queryNumber][i])



    uniqueDocs = set(uniqueDocs)
    print(uniqueDocs)
    print(len(uniqueDocs))

    #relevantDocs = set(relevantDocs)
    print("Relevant docs:\n", relevantDocs)


    '''









    '''
    try:
        testCollection['Index2'][0][0].toString()
        print("size: ", len(testCollection['Index2'][0]))
    except IndexError:
        print("Query 1 is empty")




    print("Query 2")

    try:
        testCollection['Index1'][1][0].toString()
        print("size: ", len(testCollection['Index1'][1]))
    except IndexError:
        print("Query 2 is empty")

    try:
        testCollection['Index2'][1][0].toString()
        print("size: ", len(testCollection['Index2'][1]))
    except IndexError:
        print("Query 2 is empty")




    print("Query 3")
    try:
        testCollection['Index1'][2][0].toString()
        print("size: ", len(testCollection['Index1'][2]))
    except IndexError:
        print("Query 3 is empty")

    try:
        testCollection['Index2'][2][0].toString()
        print("size: ", len(testCollection['Index2'][2]))
    except IndexError:
        print("Query 3 is empty")

    print("\n\n")
    '''