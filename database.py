import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from mysql.connector import errorcode

class database:
    config = {
      'user': 'root',
      'password': '1234',
      'host': 'localhost',
      'database': 'tweetResearch',
      'raise_on_warnings': True,
      'use_pure': False,
    }

    add_sql = ("INSERT IGNORE INTO bdata "
                  "(tweetID,userID,date,time,dayOfWeek,place,city,coordinates) "
                  "VALUES (%(tweetID)s, %(userID)s, %(date)s, %(time)s, %(dayOfWeek)s, %(place)s, %(city)s, %(coordinates)s)")

    addToUser_sql = ("INSERT IGNORE INTO user "
               "(userID,time,dayOfWeek,place,city,coordinates) "
               "VALUES (%(userID)s, %(currentWork)s, %(time)s, %(dayOfWeek)s, %(place)s, %(city)s, %(coordinates)s)")

    totalNumberOfCheckins = ("select count(tweetID) from `bdata`")
    totalNumberOfUsers = ("select count(distinct userID) from `bdata`")
    totalNumberOfPlaces = ("select count(distinct place) from `bdata`")
    totalNumberOfCheckinsPerUser = ("select userID, count(*) as 'numberOfCheckins' from `bdata` group by userID order by numberOfCheckins desc")
    averageNumberOfCheckinsPerUser = ("select count(tweetID)/(select count(DISTINCT userID) from `bdata`) from `bdata` ")
    averageNumberOfCheckinsPerPlace = ("select count(tweetID)/(select count(DISTINCT place) from `bdata`) from `bdata` ")
    standardDeviationOfCheckinsPerUser = ("select STD(numberOfCheckins) as 'standardDeviation' from (select userID, count(*) as 'numberOfCheckins' from `bdata` as Y group by userID) as T order by standardDeviation desc ")
    totalNumberOfCheckinsPerPlace = ("select place, count(*) as 'numberOfCheckins' from `bdata` group by place order by numberOfCheckins desc")
    numberOfChackinsPerEachDay = ("select dayOfWeek, count(*) as 'numberOfCheckins' from `bdata` group by dayOfWeek order by field(dayOfWeek,'Mon','Tue','Wed','Thu','Fri','Sat','Sun')")
    numberOfCheckinsPerHour = "SELECT hour(time), count(*) as 'numberOfCheckins' FROM `bdata` GROUP BY hour(time)"
    numberOfDistinctUsersPerPlace = ("select place, count(distinct userID) as 'numberOfUsers' from `bdata` group by place order by numberOfUsers desc")
    numberOfDistinctUserCheckedinPerHour = ("SELECT hour(time), count(distinct userID) as 'numberOfDistinctUsers' FROM `bdata` GROUP BY hour(time)")
    dateOfFirstAndLastCheckin = ("select min(date) as 'firstCheckin', max(date) as 'lastCheckin' from `bdata`")
    numberOfCheckinsPerDate = ("SELECT date, COUNT(*) as 'numberOfCheckins' FROM `bdata` GROUP BY date ORDER BY date ")
    numberOfCheckinsInWeekdays = ("SELECT bdata.date, count(*) FROM `bdata` WHERE dayname(date) IN ('Monday','Tuesday', 'Wednesday', 'Thursday', 'Friday') GROUP BY bdata.date ")
    numberOfCheckinsInEachWeekdayDateBetween7To11 = ("SELECT bdata.date, count(*) as 'numberOfCheckins' FROM `bdata` WHERE dayname(date) IN ('Monday','Tuesday', 'Wednesday', 'Thursday', 'Friday') and time<'11:00:00' AND time>'07:00:00' GROUP BY bdata.date")
    averageNumberOfCheckinsInWeekdays = ("SELECT avg(numberOfCheckins) FROM (SELECT date, COUNT(*) as 'numberOfCheckins' FROM `bdata` as q WHERE dayname(date) IN ('Monday','Tuesday', 'Wednesday', 'Thursday', 'Friday') GROUP BY date ) as p")
    numberOfCheckinsPerEachWeekdayIn7To11 = ("select dayOfWeek, count(*) as 'numberOfCheckins' from `bdata` where dayOfWeek in ('Mon','Tue','Wed','Thu','Fri') AND time<'11:00:00' AND time>'07:00:00' group by dayOfWeek ORDER BY FIELD(dayOfWeek,'Mon','Tue','Wed','Thu','Fri')")

    # user-place pairs that user checkedin the place in weekdays between 07:00-11:00 more on different dates more than 5 times on total
    userPlacePairs = ("SELECT userID,place,count(*) as 'noc' FROM `bdata` b WHERE b.dayOfWeek in ('Mon','Tue','Wed','Thu','Fri') AND b.time<'11:00:00' AND b.time>'07:00:00' GROUP BY userID, place HAVING COUNT(DISTINCT date)>5 order by noc DESC")

    selectWorkCheckins = ("select userID, place, COUNT(*) as 'numberOfCheckins' from `bdata` WHERE userID in (SELECT userID FROM `bdata` b WHERE b.dayOfWeek in ('Mon','Tue','Wed','Thu','Fri') AND b.time<'11:00:00' AND b.time>'07:00:00' GROUP BY userID HAVING COUNT(*)>15) GROUP BY userID, place HAVING numberOfCheckins>15 ORDER BY numberOfCheckins")

    getTweetStr = ("select * from `bdata`")


    cursor=None
    cnx=None

    def __init__(self):

            self.cnx = mysql.connector.connect(**self.config)
            self.cursor = self.cnx.cursor()

    def insert(self,data):
        print(data)
        self.cursor.execute(self.add_sql,data)
        #self.cursor.execute(self.add_sql, data)
        self.cnx.commit()

    def tNOC(self):
        dt = pd.read_sql(self.totalNumberOfCheckins,self.cnx)
        dt.cumsum
        print(dt)
        #dt.plot()

    def tNOU(self):
        dt = pd.read_sql(self.totalNumberOfUsers,self.cnx)
        print(dt)
        #dt.plot()

    def tNOP(self):
        dt = pd.read_sql(self.totalNumberOfPlaces,self.cnx)
        print(dt)
        #dt.plot()

    def tNOCPU(self):
        dt = pd.read_sql(self.totalNumberOfCheckinsPerUser,self.cnx)
        print(dt.head(5000))
        dt.cumsum
        dt['numberOfCheckins'].head(1000).plot(title='Total Number Of Checkins Per User')


    def aNOCPU(self):
        dt = pd.read_sql(self.averageNumberOfCheckinsPerUser,self.cnx)
        dt.cumsum
        print(dt)
        #dt.plot()

    def aNOCPP(self):
        dt = pd.read_sql(self.averageNumberOfCheckinsPerPlace,self.cnx)
        dt.cumsum
        print(dt)
        #dt.plot()

    def sDOCPU(self):
        dt = pd.read_sql(self.standardDeviationOfCheckinsPerUser,self.cnx)
        dt.cumsum
        print(dt)
        #dt.plot()

    def tNOCPP(self):
        dt = pd.read_sql(self.totalNumberOfCheckinsPerPlace,self.cnx)
        dt.cumsum
        print(dt)
        dt.head(1000).plot(title='Number Of Checkins Per Place (first 1000)')

    def nOCPED(self):
        dt = pd.read_sql(self.numberOfChackinsPerEachDay,self.cnx)
        dt.cumsum
        print(dt)
        dt.plot.bar(title='Number Of Checkins Per Each Day', x='dayOfWeek')


    def nOCPH(self):
        dt = pd.read_sql(self.numberOfCheckinsPerHour,self.cnx)
        dt.cumsum
        print(dt)
        dt['numberOfCheckins'].plot.bar(title='Number Of Checkins Per Hour')

    def nODUPP(self):
        dt = pd.read_sql(self.numberOfDistinctUsersPerPlace,self.cnx)
        dt.cumsum
        print(dt)
        dt['numberOfUsers'].head(1000).plot(title='Number Of Distinct Users Per Place')

    def nODUCPH(self):
        dt = pd.read_sql(self.numberOfDistinctUserCheckedinPerHour,self.cnx)
        dt.cumsum
        print(dt)
        dt['numberOfDistinctUsers'].plot.bar(title='Number Of Distinct Users Per Hour')

    def dOFALC(self):
        dt = pd.read_sql(self.dateOfFirstAndLastCheckin,self.cnx)
        dt.cumsum
        print(dt)

    def nOCPD(self):
        dt = pd.read_sql(self.numberOfCheckinsPerDate,self.cnx)
        #dt.cumsum
        print(dt)
        dt.plot(title='Number Of Checkins Per Date')

    def nOCIW(self):
        dt = pd.read_sql(self.numberOfCheckinsInWeekdays,self.cnx)
        #dt.cumsum
        print(dt)
        dt.plot(title='Number Of Checkins Per Each Weekday Dates')

    def nOCIWB7to11(self):
        dt = pd.read_sql(self.numberOfCheckinsInEachWeekdayDateBetween7To11,self.cnx)
        #dt.cumsum
        print(dt)
        dt.plot(title='Number Of Checkins Per Each Weekday Dates Bw 07:00-11:00')

    def aNOCIW(self):
        dt = pd.read_sql(self.averageNumberOfCheckinsInWeekdays,self.cnx)
        #dt.cumsum
        print(dt)

    def getTweet(self):
        dt = pd.read_sql(self.getTweetStr, self.cnx)
        return dt

    def addToUser(self,data):
        self.cursor.execute(self.addToUser_sql, data)
        # self.cursor.execute(self.add_sql, data)
        self.cnx.commit()

    def getWork(self):
        dt = pd.read_sql(self.getWorkStr, self.cnx)
        return dt

    def userPlaceWorkPairs(self):
        dt = pd.read_sql(self.userPlacePairs, self.cnx)
        print(dt)
        return dt

    def nOCPEWIB(self):
        dt = pd.read_sql(self.numberOfCheckinsPerEachWeekdayIn7To11, self.cnx)
        dt.plot.bar(title='Number Of Checkins Per Each Weekday Between 07:00-11:00',x='dayOfWeek')
        return dt

    def workCheckins(self):
        dt = pd.read_sql(self.selectWorkCheckins, self.cnx)
        print(dt)
        return dt

    def close(self):
        self.cursor.close()
        self.cnx.close()


