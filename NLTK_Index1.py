import csv
import json

import nltk
from elasticsearch import Elasticsearch
from sklearn.feature_extraction.text import TfidfVectorizer

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
Lemmatizer = nltk.wordnet.WordNetLemmatizer()  # Define this here so we do not need to re-define each time preProcess is called

plots = []


def init():
    es.indices.delete(index='movies', ignore=[400,
                                              404])  # ignore 400/404 errors if index doesn't exist
    es.indices.create(index='movies', ignore=400)

    # https://kb.objectrocket.com/elasticsearch/when-to-use-the-keyword-type-vs-text-datatype-in-elasticsearch
    map = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "refresh_interval": "1m"},
        "properties": {
            "Release Year": {
                "type": "integer"
            },
            "Title": {
                "type": "keyword",
            },
            "Origin/Ethnicity": {
                "type": "text"
            },
            "Director": {
                "type": "text",
            },
            "Cast": {
                "type": "array",
            },
            "Genre": {
                "type": "text",
            },
            "Wiki Page": {
                "type": "text"
            },
            "Plot": {
                "type": "text"
            },
            "Keywords": {
                "type": "keyword"
            },
        }
    }
    es.indices.put_mapping(
        index='movies',
        doc_type='movie',
        ignore=400,
        # Release Year,Title,Origin/Ethnicity,Director,Cast,Genre,Wiki Page,Plot

        body=map)
    print("Created Index")


def readAndConvertData():
    print("Reading from CSV file")
    with open("wiki_movie_plots_deduped.csv",
              encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        movies = []

        id = 0
        for row in csvReader:
            movies.append(
                row)  # will have same position as its index into the elasticsearch DB

            id += 1
            if id > 999:
                break

    return movies


# Returns a movie with a preprocessed plot
def preProcessMovie(movie):
    plot = movie['Plot']
    plot = nltk.word_tokenize(
        plot)  # Tokenizes the dataset so each word can be addressed individually
    for word in range(0, len(plot)):
        plot[word] = Lemmatizer.lemmatize(plot[
                                              word])  # Lemetizer applied to the word. I have chosen a lemmetizer over a stemmer as it is a less aggressive form of normalisation, and is generally more accurate

    plotKeywords = []
    for word in plot:
        #if word in keywords and not word not in plotKeywords:  # To prevent adding duplicates of words
        if word in keywords:
            plotKeywords.append(word)

    movie[
        'Keywords'] = plotKeywords  # Updates the keywords to the preprocessed version of the plot
    return movie


# Peform Tf-Idf to get keywords
# Docs: https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html
def getKeywords(plots):
    TFIDFVectoriser = TfidfVectorizer(stop_words='english',
                                      token_pattern=r'\b[^\W]+\b')  # I have decided to keep digits in the plot, in case a plot may mention a date, such as a war film in the 1940s, may then be linked to other 1940s films

    TFIDFVectoriser.fit_transform(plots).toarray()
    res = TFIDFVectoriser.get_feature_names()
    # print(res)
    return res

def getKeyPhrases(comment):
    # Define an array of comments to test.



    # Print POS tags of all sentences.

    for i in range(0, 3):
        print(nltk.pos_tag(nltk.word_tokenize(comment[i])))
        print('n')

    # Extract only company name from all sentences

    for i in range(0, 3):
        token_comment = nltk.word_tokenize(comment[i])
        tagged_comment = nltk.pos_tag(token_comment)
        print(
            [(word, tag) for word, tag in tagged_comment if
             (tag == 'NNP')])

    # Extract company name and model no. both.

    # Function to extract two pattern tags
    def match2(token_pos, pos1, pos2):
        for subsen in token_pos:
            # avoid index error and catch last three elements
            end = len(subsen) - 1
            for ind, (a, b) in enumerate(subsen, 1):
                if ind == end:
                    break
                if b == pos1 and subsen[ind][1] == pos2:
                    yield ("{} {}".format(a, subsen[ind][0],
                                          subsen[ind + 1][
                                              0]))

    # Print company and model no for each sentence

    for i in range(0, 3):
        tokens = nltk.word_tokenize(
            comment[i])  # Generate list of tokens
        tokens_pos = nltk.pos_tag(tokens)
        a = [tokens_pos]
        print(list(match2(a, 'NNP', 'NN')))

# Release Year,Title,Origin/Ethnicity,Director,Cast,Genre,Wiki Page,Plot
def upload(document, id):
    es.index(index='movies', doc_type='movie', ignore=400,
             id=id, body={
            'Release Year': document['Release Year'],
            'Title': document['Title'],
            'Origin/Ethnicity': document[
                'Origin/Ethnicity'],
            'Director': document['Director'],
            'Cast': [document['Cast']],
            'Genre': document['Genre'],
            'Wiki Page': document['Wiki Page'],
            'Plot': document['Plot'],
            'Keywords': document['Keywords']
        })


# Query the ES DB using a search phrase provided by the user, return false if not results are found
def search(searchPhrase):
    query = {
        "query":
            {
                "match": {
                    "Keywords": {
                        "query": searchPhrase,
                        "fuzziness": 1
                    }
                }
            }
    }
    result = es.search(index='movies', body=query)
    if len(result) > 0:
        return result
    else:
        return False


def preProcessPlots():
    for plot in plots:
        for word in plot:
            if word.isalnum():
                word = Lemmatizer.lemmatize(word)
                word.lower()
    return plots


if __name__ == '__main__':

    init()

    movies = readAndConvertData()  # Read and convert the data from its CSV values and store it in an array

    id = 0
    # Iterate over the movies and pre-process the plots
    for movie in movies:
        plots.append(movie["Plot"])
        # movie['Keywords'] = getKeywords(movie['Plot'])
        # movie = preProcess(movie)
        # upload(movie, id)
        id += 1

    processedPlots = preProcessPlots()
    keywords = getKeywords(plots)
    print("key phrases:\n", getKeyPhrases(processedPlots))
    print("keywords:\n", keywords)

    id = 0
    for movie in movies:
        movie = preProcessMovie(movie)
        #print("movie keywords: ", movie['Title'], " ", movie['Keywords'])

        upload(movie, id)
        id += 1
        if (id > 999):
            break

    # TODO Sentence splitting

    '''

    M A I N     M E N U

    '''

    runSearch = True

    print("CE306 Assignment 1: Movie searcher")
    while (runSearch):
        searchPhrase = input("Please enter ")
        result = search(searchPhrase)
        if (result != False):
            for movie in result['hits']['hits']:
                print("Title: ", movie['_source']['Title'],
                      "\nWiki: ",
                      movie['_source']['Wiki Page'], "\n")

