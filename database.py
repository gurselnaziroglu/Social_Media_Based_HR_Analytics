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

    totalNumberOfCheckins = ("select count(tweetID) from `bdata`")
    totalNumberOfUsers = ("select count(distinct userID) from `bdata`")
    totalNumberOfPlaces = ("select count(distinct place) from `bdata`")
    totalNumberOfCheckinsPerUser = ("select userID, count(*) as 'numberOfCheckins' from `bdata` group by userID order by numberOfCheckins desc")
    averageNumberOfCheckinsPerUser = ("select count(tweetID)/(select count(DISTINCT userID) from `bdata`) from `bdata` ")
    averageNumberOfCheckinsPerPlace = ("select count(tweetID)/(select count(DISTINCT place) from `bdata`) from `bdata` ")
    standardDeviationOfCheckinsPerUser = ("select STD(numberOfCheckins) as 'standardDeviation' from (select userID, count(*) as 'numberOfCheckins' from `bdata` as Y group by userID) as T order by standardDeviation desc ")
    totalNumberOfCheckinsPerPlace = ("select place, count(*) as 'numberOfCheckins' from `bdata` group by place order by numberOfCheckins desc")
    numberOfChackinsPerEachDay = ("select dayOfWeek, count(*) as 'numberOfCheckins' from `bdata` group by dayOfWeek order by numberOfCheckins desc")
    numberOfCheckinsPerHour = "SELECT hour(time), count(*) as 'numberOfCheckins' FROM `bdata` GROUP BY hour(time)"
    numberOfDistinctUsersPerPlace = ("select place, count(distinct userID) as 'numberOfUsers' from `bdata` group by place order by numberOfUsers desc")
    numberOfDistinctUserCheckedinPerHour = "SELECT hour(time), count(distinct userID) as 'numberOfDistinctUsers' FROM `bdata` GROUP BY hour(time)"

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
        dt['numberOfCheckins'].head(1000).plot()


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
        dt.head(1000).plot()

    def nOCPED(self):
        dt = pd.read_sql(self.numberOfChackinsPerEachDay,self.cnx)
        dt.cumsum
        print(dt)
        dt.plot.bar()


    def nOCPH(self):
        dt = pd.read_sql(self.numberOfCheckinsPerHour,self.cnx)
        dt.cumsum
        print(dt)
        dt['numberOfCheckins'].plot.bar()

    def nODUPP(self):
        dt = pd.read_sql(self.numberOfDistinctUsersPerPlace,self.cnx)
        dt.cumsum
        print(dt)
        dt['numberOfUsers'].head(1000).plot()

    def nODUCPH(self):
        dt = pd.read_sql(self.numberOfDistinctUserCheckedinPerHour,self.cnx)
        dt.cumsum
        print(dt)
        dt['numberOfDistinctUsers'].plot.bar()

    def close(self):
        self.cursor.close()
        self.cnx.close()


