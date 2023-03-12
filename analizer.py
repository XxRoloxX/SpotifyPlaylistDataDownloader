import requests
import sqlite3
import urllib.parse 
from urllib.parse import unquote

#Not used
USER_ID = ''

CLIENT_ID='94a74909f0ef48848644eaf00cdaa99d'
redirect_uri='https://www.spotify.com/pl/'
SCOPE='user-read-private user-top-read playlist-read-collaborative'

#Your access token
#SPOTIFY_ACCESS_TOKEN='BQAMR6Jl0Ls8VUdIYNSYy6JVW675E-MbrXXu4AFMPoaBlrRc5FgrfeH0nVkEeZzsvpYLZqyLn9mjifw-7e-9JCHOfDDZFl0fkBLeGfEC6iViImG3Vom9Qu2Z9dRH8v7m4oDg-PfHBcXnIFcSuLwKQ8pahKw1ybPV_4DMYULJ_eBRn2Q40dhh'

#Not used
SPOTIFY_GET_PLAYLIST_URL=f"https://api.spotify.com/v1/users/{USER_ID}/playlists?offset=0&limit=50"

SPOTIFY_GET_CURRENT_USER_PLAYLIST_URL="https://api.spotify.com/v1/me/playlists?limit=50"
SPOTIFY_GET_TRACKS_URL="https://api.spotify.com/v1/playlists/"
SPOTIFY_GET_TRACK_URL="https://api.spotify.com/v1/tracks/"
SPOTIFY_GET_ARTIST_URL="https://api.spotify.com/v1/artists/"
SPOTIFY_GET_TRACK_FEATURES_URL="https://api.spotify.com/v1/audio-features/"
SPOTIFY_GET_TOP_TRACKS_URL="https://api.spotify.com/v1/me/top/tracks?time_range=medium_term&limit=500&offset=0"

def auth_get(url,access_token):

    response = requests.get(url,
        headers={
            "Authorization": f"Bearer {access_token}"
        }
        )
    resp_json = response.json()

    return resp_json

def getPlaylist(access_token):

    resp_json = auth_get(SPOTIFY_GET_CURRENT_USER_PLAYLIST_URL,access_token)
    return resp_json


def getTracks(playlist_id, access_token):
    resp_json = auth_get(SPOTIFY_GET_TRACKS_URL+playlist_id+"/tracks?offset=0",access_token)
    return resp_json

def getTrack(track_id, access_token):
    resp_json = auth_get(SPOTIFY_GET_TRACK_URL+track_id,access_token)
    return resp_json
def getTrackFeatures(track_id, access_token):
    resp_json=auth_get(SPOTIFY_GET_TRACK_FEATURES_URL+track_id,access_token)
    return resp_json

def getArtistGenres(artist_id,access_token):
    resp_json = auth_get(SPOTIFY_GET_ARTIST_URL+artist_id,access_token)
    return resp_json["genres"]
def getTopTracks(access_token):
    resp_json=auth_get(SPOTIFY_GET_TOP_TRACKS_URL, access_token)
    return resp_json
def prepareUrl():
    url='https://accounts.spotify.com/authorize'
    url+='?response_type=token&'
    url+=urllib.parse.urlencode({'client_id':CLIENT_ID,'scope': SCOPE,'redirect_uri':redirect_uri  })
    return url


def CreateTables(nazwa):
    con=sqlite3.connect(nazwa)
    cur=con.cursor()

    try:
        cur.execute('''CREATE TABLE tracks (id text PRIMARY KEY, name text,acousticness real, danceability real,duration_ms int, energy real, 
            instrumentalness real, key int, liveness real, loudness real, speechiness real, tempo real, valence real, popularity real)''')

        cur.execute('''CREATE TABLE artists (id text PRIMARY KEY, name text)''')
        cur.execute('''CREATE TABLE genres (name text PRIMARY KEY)''')
        cur.execute('''CREATE TABLE artistsTracks(artistID text, trackID text, CONSTRAINT id PRIMARY KEY (artistID,trackID))''')
        cur.execute('''CREATE TABLE artistsGenres (artistID text, genreName text, CONSTRAINT id PRIMARY KEY (artistID,genreName))''')
        con.commit()
    except:
        cur.execute('''DROP TABLE tracks''')
        cur.execute('''DROP TABLE artists''')
        cur.execute('''DROP TABLE genres''')
        cur.execute('''DROP TABLE artistsTracks''')
        cur.execute('''DROP TABLE artistsGenres''')

        cur.execute('''CREATE TABLE tracks (id text PRIMARY KEY, name text,acousticness real, danceability real,duration_ms int, energy real, 
            instrumentalness real, key int, liveness real, loudness real, speechiness real, tempo real, valence real, popularity real)''')
        cur.execute('''CREATE TABLE artists (id text PRIMARY KEY, name text)''')
        cur.execute('''CREATE TABLE genres (name text PRIMARY KEY)''')
        cur.execute('''CREATE TABLE artistsTracks(artistID text, trackID text, CONSTRAINT id PRIMARY KEY (artistID,trackID))''')
        cur.execute('''CREATE TABLE artistsGenres (artistID text, genreName text, CONSTRAINT id PRIMARY KEY (artistID,genreName))''')
        con.commit()

    con.close()

def getFavouiteTracks(name,access_token):

    con=sqlite3.connect(name)
    cur=con.cursor()

    playlist=getPlaylist(access_token)
    track_number=1
    
    #print(playlist)
    for item in playlist['items']:

        if not item['name'].startswith('Spotilizer'):
            continue

        print(item['id'])
        print(item['name'])
        tracks=getTracks(item["id"],access_token)
        #print(tracks)
        for track in tracks["items"]:

            track_info = getTrack(track["track"]['id'],access_token)
            track_features = getTrackFeatures(track_info['id'], access_token)

            cur.execute("INSERT INTO tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (track_info['id'],
             track_info['name'],track_features['acousticness'], track_features['danceability'], 
             track_features['duration_ms'], track_features['energy'], track_features['instrumentalness'], 
             track_features['key'], track_features['liveness'], track_features['loudness'], 
             track_features['speechiness'], track_features['tempo'], track_features['valence'],track_info['popularity']))

            artists = track_info["artists"]
            print(f"{track_number}, {track_info['name']}")
            track_number+=1
            artists = track_info["artists"]

            for artist in artists:
                genres = getArtistGenres(artist["id"],access_token)
                try:
                     cur.execute("INSERT INTO artists VALUES (?,?)",(artist['id'],artist['name']))
                except sqlite3.IntegrityError:
                    pass
                cur.execute("INSERT INTO artistsTracks VALUES (?,?)",(artist['id'],track_info['id']))
                
                for genre in genres:
                    try:
                        cur.execute("INSERT INTO genres VALUES (?)",(genre,))

                    except sqlite3.IntegrityError:
                        pass 
                    try:
                        cur.execute("INSERT INTO artistsGenres VALUES (?,?)",(artist['id'],genre))
                    except sqlite3.IntegrityError:
                        pass
                    con.commit()

    con.close()

    

def genresOfAllTracks(nazwa):
    con=sqlite3.connect(nazwa)
    cur=con.cursor()
    trackCur=con.cursor()
    
    try:
         cur.execute('''CREATE TABLE tracksGenres (trackID text, genreName text, CONSTRAINT id PRIMARY KEY (trackID,genreName))''' )

    except sqlite3.OperationalError:
        cur.execute("DROP TABLE tracksGenres")
        con.commit()
        cur.execute('''CREATE TABLE tracksGenres (trackID text, genreName text, CONSTRAINT id PRIMARY KEY (trackID,genreName))''' )

    con.commit()

    rows = cur.execute("SELECT genres.name, tracks.id FROM genres INNER JOIN artistsGenres ON genres.name=artistsGenres.genreName INNER JOIN artists ON artistsGenres.artistID=artists.id INNER JOIN artistsTracks ON artists.id=artistsTracks.artistID INNER JOIN tracks ON artistsTracks.trackID=tracks.id")

    for row in rows:
        try:
            trackCur.execute("INSERT INTO tracksGenres VALUES (?,?)", (row[1],row[0]))
        except sqlite3.IntegrityError:
            pass

        con.commit()
    with open("GatunkiWyniki.txt",'w') as filehandler:
        filehandler.write("Gatunek;Wystąpienia\n")
        for row in cur.execute("SELECT genreName, COUNT(trackID) AS genreTimes FROM tracksGenres GROUP BY genreName ORDER BY genreTimes DESC"):
            filehandler.write(f"{row[0]}; {row[1]}\n")

    cur.execute("DROP TABLE tracksGenres")
    con.commit()

   
    con.close()
def getArtistCount(nazwa):
    con=sqlite3.connect(nazwa)
    cur=con.cursor()
    cur2=con.cursor()

    try:
      cur.execute('''CREATE TABLE allArtistsGenres (artistID text PRIMARY KEY, genre text)''')
    except sqlite3.OperationalError:
        cur.execute('''DROP TABLE allArtistsGenres''')
        cur.execute('''CREATE TABLE allArtistsGenres (artistID text PRIMARY KEY, genre text)''')

    con.commit()


    rows = cur.execute('''SELECT artistsGenres.artistID, artistsGenres.genreName FROM artistsGenres''')

    for row in rows:

        try:
            cur2.execute('''INSERT INTO allArtistsGenres VALUES (?,?)''', (row[0],row[1]+", "))

        except sqlite3.IntegrityError:
            currentGenre=cur2.execute('''SELECT genre FROM allArtistsGenres WHERE artistID=(?)''',(row[0],)).fetchone()
            cur2.execute('''UPDATE allArtistsGenres SET genre=? WHERE artistID=?''',(currentGenre[0]+row[1]+", ",row[0]))

        con.commit()

    rows=cur.execute('''SELECT artists.id, artists.name,allArtistsGenres.genre, COUNT(tracks.id) AS artistTimes
     FROM tracks INNER JOIN artistsTracks ON tracks.id = artistsTracks.trackID 
     INNER JOIN artists ON artistsTracks.artistID=artists.id 
     INNER JOIN allArtistsGenres ON artists.id=allArtistsGenres.artistID 
     GROUP BY artists.name ORDER BY artistTimes DESC''')

    with open('artysciWyniki.txt','w') as filehandler:
        filehandler.write("Identyfikator artysty; Nazwa artysty; Gatunki artysty; Wystąpienia artysty\n")
        for row in rows:
            filehandler.write(f"{row[0]}; {row[1]}; {row[2]}; {row[3]}\n")

    cur.execute('''DROP TABLE allArtistsGenres''')
    con.close()


def getYourTop(nazwa, access_token):
    con=sqlite3.connect(nazwa)
    cur=con.cursor()
    

    tracks=getTopTracks(access_token)
    
    track_number=1
    for track in tracks['items']:
        track_info = getTrack(track['id'],access_token)
        track_features = getTrackFeatures(track_info['id'], access_token)

        cur.execute("INSERT INTO tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (track_info['id'],
             track_info['name'],track_features['acousticness'], track_features['danceability'], 
             track_features['duration_ms'], track_features['energy'], track_features['instrumentalness'], 
             track_features['key'], track_features['liveness'], track_features['loudness'], 
             track_features['speechiness'], track_features['tempo'], track_features['valence'],track_info['popularity']))

        artists = track_info["artists"]
        print(f"{track_number}, {track_info['name']}")
        track_number+=1
        artists = track_info["artists"]

        for artist in artists:
            genres = getArtistGenres(artist["id"],access_token)
            try:
                 cur.execute("INSERT INTO artists VALUES (?,?)",(artist['id'],artist['name']))
            except sqlite3.IntegrityError:
                pass
            cur.execute("INSERT INTO artistsTracks VALUES (?,?)",(artist['id'],track_info['id']))
            
            for genre in genres:
                try:
                    cur.execute("INSERT INTO genres VALUES (?)",(genre,))

                except sqlite3.IntegrityError:
                    pass 
                try:
                    cur.execute("INSERT INTO artistsGenres VALUES (?,?)",(artist['id'],genre))
                except sqlite3.IntegrityError:
                    pass

                con.commit()

    con.close()        
        
def main():

    prepareUrl()
    while True:

        print("Wybierz co zrobić:")
        print("1) Utwórz bazę danych")
        print("2) Zapisz dane do bazy danych")
        print("3) Wykorzystaj dane z obecnej bazy danych")
        print("4) Zakończ działanie programu")

        choice=input()
        if choice=='1':
            nazwa=input("Podaj nazwę nowej bazy danych (bez rozszerzenia): ")
            CreateTables(nazwa+'.db')
        elif choice=='2':
            print("Otwórz ten link i wpisz poniżej access token, z linku na który cię przekieruje")
            print(prepareUrl())
            tmp=input()

            access_token=unquote(tmp)
            print("Jakie dane zapisać do bazy danych ?")
            print("1) Dane związane z playlistą ulubionych utworów")
            print("2) Dane związane z twoimi topowymi utworami")
            choice2=input()

            nazwa=input("Podaj nazwę bazy danych do której zapisać dane (bez rozszerzenia): ")

            if choice2=='1':
                getFavouiteTracks(nazwa+'.db',access_token)
            elif choice2=='2':
                getYourTop(nazwa+'.db',access_token)
            else:
                print("Niepoprawna opcja")

        elif choice=='3':
            nazwa=input("Podaj nazwę bazy danych z której mają być pobierane dane (bez rozszerzenia): ")
            print("Co zrobić z danymi z tej bazy danych ?")
            print("1) Sporządź zestawienie z ilością utworów w poszczególnych gatunków w tej bazie danych")
            print("2) Sporządź zestawienie z ilością utowrów napisanych przez poszczególnego artystę i gatunki z nim powiązane")
            choice3=input()

            if choice3=='1':
                genresOfAllTracks(nazwa+'.db')
                print("Gotowe!")
                print("Wyniki zostały zapisane do GatunkiWyniki.txt")
            elif choice3=='2':
                getArtistCount(nazwa+'.db')
                print("Gotowe!")
                print("Wyniki zostały zapisane do ArtysciWyniki.txt")
            else:
                print("Niepoprawna opcja")
        elif choice=='4':
            break
        else:
            print("Niepoprawna opcja")


    


   
if(__name__=='__main__'):
    main()
