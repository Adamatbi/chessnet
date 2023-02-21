import requests
import sqlite3
from tqdm import tqdm
from time import sleep
from contextlib import closing

def backoff_request(url,Verbose=False):
    backoff_time = 0.5
    for _ in range(5):
        try:
            response = requests.get(url)
            if response.status_code == 404:
                #avoid backoff delay if user is not found
                break
            if response:
                return response
            else:
                if Verbose:
                    print("sleeping: {}".format(str(backoff_time)))
                sleep(backoff_time)
                backoff_time*=2
        except:
            if Verbose:
                print("sleeping: {}".format(str(backoff_time)))
            sleep(backoff_time)
            backoff_time*=2
    if Verbose:
        print("backoff failed on {}".format(url))
    raise Exception("backoff failed")

def get_player_info(username,Verbose=False):
    if Verbose:
        print("getting player info for {}".format(username))
    player_response = backoff_request("https://api.chess.com/pub/player/{}".format(username),Verbose).json()
    if "name" in player_response.keys():
        name = player_response['name']
    else:
        name = ""
    return (player_response['username'],
    name,
    player_response['country'].split('/')[-1],
    player_response['player_id'],
    player_response['joined'],
    player_response['last_online'],
    player_response['followers']
    )
    

def get_player_games(username,Verbose=False):
    archives_response = backoff_request("https://api.chess.com/pub/player/{}/games/archives".format(username),Verbose)
    archives = archives_response.json()['archives']
    games = []
    if Verbose:
        print("fetching {} archives for {}".format(str(len(archives)),username))
    for archive in archives:
        archive_response = backoff_request(archive,Verbose).json()
        for game in archive_response['games']:
            games.append((game['time_control'],
            game['end_time'],
            game['rated'],
            game['time_class'],
            game['rules'],
            game['white']['username'],
            game['white']['rating'],
            game['white']['result'],
            game['black']['username'],
            game['black']['rating'],
            game['black']['result'],
            ))
    return games


def get_player(username,con,cur,Verbose=False):
    if Verbose:
        print("getting data for {}".format(username))
    player = get_player_info(username,Verbose)
    games = get_player_games(username,Verbose)
    cur.execute("insert into players values {}".format(str(player)))
    cur.execute("insert into games values {}".format(str(games)[1:-1]))
    con.commit()

def get_target_usernames(con,cur):
    usernames = cur.execute("select distinct games.white_player from games left outer join players on games.white_player = players.username where players.username is null order by random() limit 10000")
    rough_usernames = usernames.fetchall()
    return [x[0] for x in rough_usernames]



def run(Verbose=False):
    target_usernames = set()
    with closing(sqlite3.connect("chess.db")) as con:
        with closing(con.cursor()) as cur:
            while True:
                if Verbose:
                    print("parsing target usernames from database")
                else:
                    target_usernames = get_target_usernames(con,cur)
                for username in tqdm(target_usernames,disable=not Verbose):
                    try:
                        get_player(username,con,cur,Verbose)
                    except Exception as e:
                        print("ERROR: "+e)
                        with open("failed.txt",'a') as f:
                            f.write(username+"\n")



if __name__=="__main__":
    run(True)