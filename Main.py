from pymongo import MongoClient #import pymongo to connect MongoDB
import re
import json
import pandas as pd
from datetime import datetime,timedelta
from database import database
#client = MongoClient()  # create connection
client = MongoClient('193.255.161.41',27017) #create remote connection
db = client.backup_20150523 # connect to database called "backup_201505"
coll = db.Response  # select collection called "Response"

# function to return true if text contains word, false otherwise
def word_in_text(word, text):
    word = word.lower()
    text = text.lower()
    match = re.search(word, text)
    if match:
        return True
    return False

def computeTime(createdAt, utcOffset):
    nw = datetime.strptime(createdAt, '%a %b %m %H:%M:%S +0000 %Y')
    if utcOffset != None:
        currentDateTime = nw + timedelta(hours=(utcOffset/3600))
        currentTime = datetime.time(currentDateTime)
    else:
        currentTime = datetime.time(nw)
    currentDate = datetime.date(currentDateTime)
    return currentTime,currentDate

def computeTime2(createdAt, utcOffset):
    nw = datetime.strptime(createdAt, '%a %b %d %H:%M:%S +0000 %Y')
    if utcOffset != None:
        currentDateTime = nw + timedelta(hours=(utcOffset/3600))
        currentTime = datetime.time(currentDateTime)
    else:
        currentTime = datetime.time(nw)
    currentDate = datetime.date(currentDateTime)
    return currentTime,currentDate

daba = database()

a=1
for i in coll.find({}, {"JsonResponse": 1}):
     obj = json.loads(i['JsonResponse'])    # obj now contains a dict of the data (all info about tweet in json format)
     print(a)
     a = a+1
     try:
        if "Foursquare" in obj['source']:   # check-ins
            if "I'm at" in obj['user']:
                text_split = obj['text'].split("I'm at ",1) # text_split[1] holds the text after "I'm at "
                #print(text_split)
                placeAndLink = text_split[1].split(" in ")  # placeAndLink[0] holds name place name and placeName[1] holds the city
                if "http" not in placeAndLink[0]:
                    place = placeAndLink[0]    #---------PLACE-----------
                else:   # no place information, there is a link instead
                    continue
                try:
                    if " w/ " in placeAndLink[1]:
                        cityWithAndLink = placeAndLink[1].split(" w/ ")
                        city = cityWithAndLink[0]
                    else:
                        cityAndLink = placeAndLink[1].split(" http")   # cityAndLink[0] holds city
                        city = cityAndLink[0]   #-----------CITY-----------
                except IndexError:
                    city = "-"
                if obj['user']['utc_offset'] == None:
                    [currentTime,currentDate] = computeTime(obj['created_at'], 0)
                else:
                    [currentTime,currentDate] = computeTime(obj['created_at'], obj['user']['utc_offset'])
                tweetID = obj['id']
                dayOfWeek = obj['created_at'].split(" ",1)[0]
                userID = obj['user']['id']
                try:
                    coordinates = str(obj['coordinates']['coordinates'])
                    if coordinates == None:
                        coordinates = "-"
                except Exception:
                    coordinates = "-"
            elif "I'm at" in obj['text']:
                text_split = obj['text'].split("I'm at ", 1)  # text_split[1] holds the text after "I'm at "
                # print(text_split)
                placeAndLink = text_split[1].split(
                    " in ")  # placeAndLink[0] holds name place name and placeName[1] holds the city
                if "http" not in placeAndLink[0]:
                    place = placeAndLink[0]  # ---------PLACE-----------
                else:  # no place information, there is a link instead
                    continue
                try:
                    if " w/ " in placeAndLink[1]:
                        cityWithAndLink = placeAndLink[1].split(" w/ ")
                        city = cityWithAndLink[0]
                    else:
                        cityAndLink = placeAndLink[1].split(" http")  # cityAndLink[0] holds city
                        city = cityAndLink[0]  # -----------CITY-----------
                except IndexError:
                    city = "-"
                if obj['user']['utc_offset'] == None:
                    [currentTime,currentDate] = computeTime2(obj['created_at'], 0)
                else:
                    [currentTime,currentDate] = computeTime2(obj['created_at'], obj['user']['utc_offset'])
                tweetID = obj['id']
                dayOfWeek = obj['created_at'].split(" ",1)[0]
                userID = obj['user']['id']
                try:
                    coordinates = str(obj['geo']['coordinates'])
                    if coordinates == None:
                        coordinates = "-"
                except Exception:
                    coordinates = "-"
            else:
                if "(@" in obj['text']:
                    text_split = obj['text'].split("(@ ",1) #text_split[1] holds the text after personal text and "(@ "
                    try:
                        placeAndCityTogether = text_split[1].split(")")  # placeAndCityTogether[0] holds name place name and city
                        if " w/" in placeAndCityTogether[0]:
                            placeAndCityTogether = placeAndCityTogether[0].split(" w/ ")    # placeAndCity[0] holds place name
                        placeAndCity = placeAndCityTogether[0].split(" in ")   # placeAndCity[0] holds place name and placeAndCity[1] holds city
                        place = placeAndCity[0]
                        try:
                            city = placeAndCity[1]
                        except IndexError:
                            city = "-"
                    except IndexError:
                        continue
                else:
                    text_split = obj['text'].split("(at @",1)
                    try:
                        placeAndCityTogether = text_split[1].split(")")  # placeAndCityTogether[0] holds name place name and city
                        if " w/" in placeAndCityTogether[0]:
                            placeAndCityTogether = placeAndCityTogether[0].split(" w/ ")    # placeAndCity[0] holds place name
                        placeAndCity = placeAndCityTogether[0].split(" in ")   # placeAndCity[0] holds place name and placeAndCity[1] holds city
                        place = placeAndCity[0]
                        try:
                            city = placeAndCity[1]
                        except IndexError:
                            city = "-"
                    except IndexError:
                        continue
                if obj['user']['utc_offset'] == None:
                    [currentTime,currentDate] = computeTime(obj['created_at'], 0)
                else:
                    [currentTime,currentDate] = computeTime(obj['created_at'], obj['user']['utc_offset'])
                tweetID = obj['id']
                dayOfWeek = obj['created_at'].split(" ",1)[0]
                userID = obj['user']['id']
                try:
                    coordinates = str(obj['coordinates']['coordinates'])
                    if coordinates == None:
                        coordinates = "-"
                except Exception:
                    coordinates = "-"

            data = {
                'tweetID': tweetID,
                'userID' : userID,
                'date': currentDate,
                'time': currentTime,
                'dayOfWeek': dayOfWeek,
                'place': place,
                'city': city,
                'coordinates': coordinates,
            }
            if word_in_text("istanbul",city) or word_in_text("İstanbul",city):
                daba.insert(data)
            else:
                print(city," ---------------------- not inserted")
     except Exception:
         print("######################")
         continue


daba.close()
