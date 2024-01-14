import requests
import json
import psycopg2
from config import config
from tqdm import tqdm
from time import sleep

#get imdbid's as they are stored in our postgres db
def getmoviedetails():
    connection = None
    try:
        params = config()
        print(":::Retrieving IMDB ids:::")
        connection = psycopg2.connect(**params)
        crsr = connection.cursor()
        crsr.execute('SELECT movieimdbid FROM movies')
        global movieids
        movieids = crsr.fetchall()
        crsr.close()
        print(":::IDs retrieved successfully:::")
        return movieids
    except(Exception, psycopg2.DatabaseError) as error:
            print(error)   
    finally:
        if connection is not None:
            connection.close()

#this function will be used to affix the *tt0* to the imdbmovieid
#so that we can search TMDB API with it, int to string conversion
def ttaffix(idnumber):
    idstring = str(idnumber)
    if len(idstring) >= 7:
        idstring = "tt" + idstring   
    elif len(idstring) < 7:
        idstring = "tt" + (7 - len(idstring)) * "0" + idstring
    return idstring

def Insert_MovieGenres(inserttuple):
    connection = None
    try:
        params = config()
        connection = psycopg2.connect(**params)
        crsr = connection.cursor()
        print(f"Inserting genres for {externalid} to database")        
        crsr.executemany(insertcommand, inserttuple)
        connection.commit()
        crsr.close()
    except(Exception, BaseException) as error:
        print(error)
        ErrorLog.append(movieid)
        ErrorLog.append(error)
    finally:
        if connection is not None:
            connection.close()

insertcommand = 'INSERT INTO movie_genres (movieimdbid, genrename) VALUES (%s,%s)'
ErrorLog = []

# #use the API to query TMDB using the IMDBID and get the genres
# def API_query(movieid):
getmoviedetails()
for movieid in tqdm(movieids):
    try:
        sleep(0.333)
        externalid = ttaffix(movieid[0])
        base = "http://www.omdbapi.com/?i="
        pathparam = externalid
        apikey = ""
        url = base + pathparam + apikey
        response = requests.get(url)
        print(response)
        movieinfo = json.loads(response.text)

       
        
        #print(response.text)
        respective_genres = (movieinfo["Genre"].split(","))
        #print(respective_genres)
        
        #unzip then rezip for inserting into the db
        movieimdbid_forinsert = [movieid[0]]
        movieimdbid_forinsert_extended = movieimdbid_forinsert * len(respective_genres)
        inserttuple = list(zip(movieimdbid_forinsert_extended,respective_genres))

        Insert_MovieGenres(inserttuple)
    except(Exception, BaseException) as error:
        print(error)
        ErrorLog.append(movieid)
        ErrorLog.append(error)
    finally:
        continue
    
ErrorString = str(ErrorLog)
with open("C:/Users/pc/Documents/TMDB-API-query-ErrorLog", "w", encoding= "UTF8") as f:
    f.write(ErrorString)



