import MeCab
import psycopg2
from config import config
from tqdm import tqdm


#this function connects to the postgresdb and gets a movies IMDBid, the subtitle filename,
#and the encoding by which to open the contents of the file.
def getmoviedetails():
    connection = None
    try:
        params = config()
        print("Connecting to the postgresql database...")
        connection = psycopg2.connect(**params)
        crsr = connection.cursor()
        crsr.execute('SELECT movieimdbid, idsubtitlefile, fileencoding FROM movies')
        global moviedetails
        moviedetails = crsr.fetchall()
        crsr.close()
        return moviedetails   
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)   
    finally:
        if connection is not None:
            connection.close()

#open file using the parameters we pulled from the db, read the appropriate script
def openMoviescript(subsfolder, moviedetail):
    with open(subsfolder + moviedetail[1], "r", encoding= moviedetail[2]) as f:
            moviescript = f.read()
    return moviescript
    

#This function uses mecab to analyse every word in the script, and after that 
# places each word's dictionary form into a subdictionary by part of speech
def analyzeMoviescript(moviescript):
    t = MeCab.Tagger()
    MovieWordsDictionary = {
    "nouns" : {},
    "pronouns" : {},
    "verbs" : {},
    "adverbs" : {},
    "adjectives" : {},
    "conjunctions" : {},
    "interjections" : {},
    "particles" : {},
}
    node = t.parseToNode(moviescript)
    while node:
        f = node.feature
        featuresFull = f.split(',')[0:11]
        partOfSpeech = featuresFull[0]
        dictionaryForm = featuresFull[-1]
        if partOfSpeech == '助詞':
            if dictionaryForm not in MovieWordsDictionary['particles']:
                MovieWordsDictionary['particles'][dictionaryForm] = 1
            else:
                MovieWordsDictionary['particles'][dictionaryForm] += 1
        if partOfSpeech == '連体詞' or partOfSpeech == '形容詞' or partOfSpeech == '形容動詞':
            if dictionaryForm not in MovieWordsDictionary['adjectives']:
                MovieWordsDictionary['adjectives'][dictionaryForm] = 1
            else:
                MovieWordsDictionary['adjectives'][dictionaryForm] += 1
        if partOfSpeech == '名詞':
            if dictionaryForm == "*": #I think sometimes MeCab treats 空白 or some 記号 as nouns?
                pass
            elif dictionaryForm not in MovieWordsDictionary['nouns']:
                MovieWordsDictionary['nouns'][dictionaryForm] = 1
            else:
                MovieWordsDictionary['nouns'][dictionaryForm] += 1
        if partOfSpeech == '代名詞':
            if dictionaryForm not in MovieWordsDictionary['pronouns']:
                MovieWordsDictionary['pronouns'][dictionaryForm] = 1
            else:
                MovieWordsDictionary['pronouns'][dictionaryForm] += 1
        if partOfSpeech == '動詞':
            if dictionaryForm not in MovieWordsDictionary['verbs']:
                MovieWordsDictionary['verbs'][dictionaryForm] = 1
            else:
                MovieWordsDictionary['verbs'][dictionaryForm] += 1
        if partOfSpeech == '副詞':
            if dictionaryForm not in MovieWordsDictionary['adverbs']:
                MovieWordsDictionary['adverbs'][dictionaryForm] = 1
            else:
                MovieWordsDictionary['adverbs'][dictionaryForm] += 1
        if partOfSpeech == '接続詞':
            if dictionaryForm not in MovieWordsDictionary['conjunctions']:
                MovieWordsDictionary['conjunctions'][dictionaryForm] = 1
            else:
                MovieWordsDictionary['conjunctions'][dictionaryForm] += 1
        if partOfSpeech == '感動詞':
            if dictionaryForm not in MovieWordsDictionary['interjections']:
                MovieWordsDictionary['interjections'][dictionaryForm] = 1
            else:
                MovieWordsDictionary['interjections'][dictionaryForm] += 1
        
        node = node.next
    return MovieWordsDictionary
    

#this function compiles every word in the script, in dictionary-form, into an iterable moviewordlist_iterable
#this iterable moviewordlist_iterable serves as a coding workaroud to get the exact parts of speech of words
#later in the python script we will be creating tuples to be inserted in to the postgresdb
def createMovieWordList(nested_dict):
    moviewordlist_iterable = []
    for inner_dict in nested_dict.values():
        moviewordlist_iterable.extend(inner_dict.keys())
    return moviewordlist_iterable
#this function is a companion to the previous one. it creates a moviewordfrequency_iterable which is
#how often a word occured within the movie it appeared it. It will be the same length as moviewordlist_iterable
#so we can rest assured they will be able to be paired together when making our tuple for postgres insertion
def createMovieWordFrequencyList(nested_dict):
    moviewordfrequency_iterable = []
    for inner_dict in nested_dict.values():
        moviewordfrequency_iterable.extend(inner_dict.values())
    return moviewordfrequency_iterable


#this function takes a word and checks the moviewordlist_iterable to find what the words part of speech is
#by checking every key (all possible words) in every sub dictionary (all part of speech dics)
#we run our MovieWordsDictionary_iterable/complete_movie_vocab through this
def getWordPartOfSpeech(nested_dict, word_to_check):
    for sub_dict_key, sub_dict_value in nested_dict.items():
        if word_to_check in sub_dict_value:
            part_of_speech = sub_dict_key[0:-1]
            return part_of_speech  # Return the key of the sub-dictionary containing the searched key
    return None  # Return None if the key is not found in any sub-dictionary


#this function is where tables are created, and data is entered into our postgresdb
#counterintuitive, but the many to many table is created first because
#we already have access to the movieimdbid which is a primary key
def db_operations_createMovieWordsTable():
    connection = None
    try:
        params = config()
        connection = psycopg2.connect(**params)
        crsr = connection.cursor()
        
        crsr.execute(createMovie_Words) #CREATE TABLE HERE 'movie_words'
        connection.commit()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            

def db_operations_insertToMovieWordsTable():
    connection = None
    try:
        params = config()
        connection = psycopg2.connect(**params)
        crsr = connection.cursor()
        
        crsr.executemany(insertMovie_Words, result_list) #ADD WORDS TO TABLE HERE
        connection.commit()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            








#moviedetails = [('283832', '46119', 'SHIFT_JIS')]

subsfolder = r"C:/Users/pc/Documents/Subtitles/files/"
ErrorLog = []
createMovie_Words = '''CREATE TABLE movie_words ( 
                                    movieimdbid INTEGER NOT NULL,
                                    wordID SERIAL NOT NULL,
                                    word VARCHAR(255) NOT NULL,
                                    part_of_speech VARCHAR(255) NOT NULL,
                                    word_frequency INTEGER NOT NULL);'''
insertMovie_Words = '''INSERT INTO movie_words (movieimdbid,  
                                       word, 
                                       part_of_speech, 
                                       word_frequency) 
                                       VALUES(%s,%s,%s,%s);'''


#_____________________________________________________________________________________________#
#loop through every word, in every movie. then send those analyses to our
#postgres database
#the executemany() function relies on tuples for input and tuple needs 4 values.
getmoviedetails()
db_operations_createMovieWordsTable()
for moviedetail in tqdm(moviedetails):
    try: 
        moviescript = openMoviescript(subsfolder, moviedetail)
        MovieWordsDictionary = analyzeMoviescript(moviescript)
        movie_vocab_iterable = createMovieWordList(MovieWordsDictionary)
        movie_vocabfrequency_iterable = createMovieWordFrequencyList(MovieWordsDictionary)

        
        #zip the word_frequency_iterable of words into a tuple to be inserted into the db
        movieimdbid_forinsert = [moviedetail[0]]
        partofspeech_forinsert = []
        for item in movie_vocab_iterable:
            partofspeech_forinsert.append(getWordPartOfSpeech(MovieWordsDictionary, item))
        '''movie_vocab_iterable is the "word_word_frequency_iterable" to be put in the tuple for insertion'''
        '''word_frequency_iterable is the "frequency_word_frequency_iterable" to put in the tuple for insertion'''

        #zip() time!
        max_length = max(len(movieimdbid_forinsert), len(partofspeech_forinsert), len(movie_vocab_iterable), len(movie_vocabfrequency_iterable))
        
        movieimdbid_forinsert_extended = movieimdbid_forinsert * (max_length - len(movieimdbid_forinsert))
        partofspeech_forinsert_extended = partofspeech_forinsert + [""] * (max_length - len(partofspeech_forinsert))
        movie_vocab_iterable_extended = movie_vocab_iterable + [""] * (max_length - len(movie_vocab_iterable))
        movie_vocabfrequency_iterable_extended = movie_vocabfrequency_iterable + [""] * (max_length - len(movie_vocabfrequency_iterable))

        result_list = list(zip(movieimdbid_forinsert_extended, movie_vocab_iterable_extended, partofspeech_forinsert_extended, movie_vocabfrequency_iterable_extended))
        #print(result_list)
        #print(movieimdbid_forinsert)

        db_operations_insertToMovieWordsTable()

    #write ErrorLog to a file that we can read after everything is done. We'll wanna see
    #What went awry with trying to open a file.
    except(Exception, UnicodeError) as error:
        print(error)
        ErrorLog.append(moviedetail)
        ErrorLog.append(error)
    finally:
        continue

ErrorString = str(ErrorLog)
with open("C:/Users/pc/Documents/SerifuDBerrorLog", "w", encoding= "UTF8") as f:
    f.write(ErrorString)
    
    
        