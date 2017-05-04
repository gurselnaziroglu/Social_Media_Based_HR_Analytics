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
    totalNumberOfUsers = ("select count(userID) from `bdata` group by userID")
    totalNumberOfCheckinsPerUser = ("select userID, count(*) as 'numberOfCheckins' from `bdata` group by userID order by numberOfCheckins desc")
    averageNumberOfCheckinsPerUser = ("select count(tweetID)/(select count(userID) from `bdata` group by userID) from `bdata`")
    standardDeviationOfCheckinsPerUser = ("select STDEV(numberOfCheckins) as 'standardDeviation' from (select userID, count(*) as 'numberOfCheckins' from `bdata` group by userID) order by standardDeviation desc")
    totalNumberOfCheckinsPerPlace = ("select place, count(*) as 'numberOfCheckins' from `bdata` group by place order by numberOfCheckins desc")
    numberOfChackinsPerEachDay = ("select dayOfWeek, count(*) as 'numberOfCheckins' from `bdata` group by dayOfWeek order by numberOfCheckins desc")
    numberOfCheckinsPerHour = ("""SELECT  SUM(CASE WHEN time BETWEEN ('00:00:00' AND '01:00:00') THEN 1 ELSE 0) as '00:00-01:00',
                                SUM(CASE WHEN time BETWEEN ('01:00:01' AND '02:00:00') THEN 1 ELSE 0) as '01:00-02:00',
                                SUM(CASE WHEN time BETWEEN ('02:00:01' AND '03:00:00') THEN 1 ELSE 0) as '02:00-03:00',
                                SUM(CASE WHEN time BETWEEN ('03:00:01' AND '04:00:00') THEN 1 ELSE 0) as '03:00-04:00',
                                SUM(CASE WHEN time BETWEEN ('04:00:01' AND '05:00:00') THEN 1 ELSE 0) as '04:00-05:00',
                                SUM(CASE WHEN time BETWEEN ('05:00:01' AND '06:00:00') THEN 1 ELSE 0) as '05:00-06:00',
                                SUM(CASE WHEN time BETWEEN ('06:00:01' AND '07:00:00') THEN 1 ELSE 0) as '06:00-07:00',
                                SUM(CASE WHEN time BETWEEN ('07:00:01' AND '08:00:00') THEN 1 ELSE 0) as '07:00-08:00',
                                SUM(CASE WHEN time BETWEEN ('08:00:01' AND '09:00:00') THEN 1 ELSE 0) as '08:00-09:00',
                                SUM(CASE WHEN time BETWEEN ('09:00:01' AND '10:00:00') THEN 1 ELSE 0) as '09:00-10:00',
                                SUM(CASE WHEN time BETWEEN ('10:00:01' AND '11:00:00') THEN 1 ELSE 0) as '10:00-11:00',
                                SUM(CASE WHEN time BETWEEN ('11:00:01' AND '12:00:00') THEN 1 ELSE 0) as '11:00-12:00',
                                SUM(CASE WHEN time BETWEEN ('12:00:01' AND '13:00:00') THEN 1 ELSE 0) as '12:00-13:00',
                                SUM(CASE WHEN time BETWEEN ('13:00:01' AND '14:00:00') THEN 1 ELSE 0) as '13:00-14:00',
                                SUM(CASE WHEN time BETWEEN ('14:00:01' AND '15:00:00') THEN 1 ELSE 0) as '14:00-15:00',
                                SUM(CASE WHEN time BETWEEN ('15:00:01' AND '16:00:00') THEN 1 ELSE 0) as '15:00-16:00',
                                SUM(CASE WHEN time BETWEEN ('16:00:01' AND '17:00:00') THEN 1 ELSE 0) as '16:00-17:00',
                                SUM(CASE WHEN time BETWEEN ('17:00:01' AND '18:00:00') THEN 1 ELSE 0) as '17:00-18:00',
                                SUM(CASE WHEN time BETWEEN ('18:00:01' AND '19:00:00') THEN 1 ELSE 0) as '18:00-19:00',
                                SUM(CASE WHEN time BETWEEN ('19:00:01' AND '20:00:00') THEN 1 ELSE 0) as '19:00-20:00',
                                SUM(CASE WHEN time BETWEEN ('20:00:01' AND '21:00:00') THEN 1 ELSE 0) as '20:00-21:00',
                                SUM(CASE WHEN time BETWEEN ('21:00:01' AND '22:00:00') THEN 1 ELSE 0) as '21:00-22:00',
                                SUM(CASE WHEN time BETWEEN ('22:00:01' AND '23:00:00') THEN 1 ELSE 0) as '22:00-23:00',
                                SUM(CASE WHEN time BETWEEN ('23:00:01' AND '00:00:00') THEN 1 ELSE 0) as '23:00-00:00',
                            FROM `bdata`""")
    numberOfDistinctUsersPerPlace = ("select place, count(distinct userID) as 'numberOfUsers' from `bdata` group by place order by numberOfUsers desc")
    numberOfDistinctUserCheckedinPerHour = ("""SELECT  count(distinct (CASE WHEN time BETWEEN ('00:00:00' AND '01:00:00'))) as '00:00-01:00',
                                                count(distinct (CASE WHEN time BETWEEN ('01:00:01' AND '02:00:00'))) as '01:00-02:00',
                                                count(distinct (CASE WHEN time BETWEEN ('02:00:01' AND '03:00:00'))) as '02:00-03:00',
                                                count(distinct (CASE WHEN time BETWEEN ('03:00:01' AND '04:00:00'))) as '03:00-04:00',
                                                count(distinct (CASE WHEN time BETWEEN ('04:00:01' AND '05:00:00'))) as '04:00-05:00',
                                                count(distinct (CASE WHEN time BETWEEN ('05:00:01' AND '06:00:00'))) as '05:00-06:00',
                                                count(distinct (CASE WHEN time BETWEEN ('06:00:01' AND '07:00:00'))) as '06:00-07:00',
                                                count(distinct (CASE WHEN time BETWEEN ('07:00:01' AND '08:00:00'))) as '07:00-08:00',
                                                count(distinct (CASE WHEN time BETWEEN ('08:00:01' AND '09:00:00'))) as '08:00-09:00',
                                                count(distinct (CASE WHEN time BETWEEN ('09:00:01' AND '10:00:00'))) as '09:00-10:00',
                                                count(distinct (CASE WHEN time BETWEEN ('10:00:01' AND '11:00:00'))) as '10:00-11:00',
                                                count(distinct (CASE WHEN time BETWEEN ('11:00:01' AND '12:00:00'))) as '11:00-12:00',
                                                count(distinct (CASE WHEN time BETWEEN ('12:00:01' AND '13:00:00'))) as '12:00-13:00',
                                                count(distinct (CASE WHEN time BETWEEN ('13:00:01' AND '14:00:00'))) as '13:00-14:00',
                                                count(distinct (CASE WHEN time BETWEEN ('14:00:01' AND '15:00:00'))) as '14:00-15:00',
                                                count(distinct (CASE WHEN time BETWEEN ('15:00:01' AND '16:00:00'))) as '15:00-16:00',
                                                count(distinct (CASE WHEN time BETWEEN ('16:00:01' AND '17:00:00'))) as '16:00-17:00',
                                                count(distinct (CASE WHEN time BETWEEN ('17:00:01' AND '18:00:00'))) as '17:00-18:00',
                                                count(distinct (CASE WHEN time BETWEEN ('18:00:01' AND '19:00:00'))) as '18:00-19:00',
                                                count(distinct (CASE WHEN time BETWEEN ('19:00:01' AND '20:00:00'))) as '19:00-20:00',
                                                count(distinct (CASE WHEN time BETWEEN ('20:00:01' AND '21:00:00'))) as '20:00-21:00',
                                                count(distinct (CASE WHEN time BETWEEN ('21:00:01' AND '22:00:00'))) as '21:00-22:00',
                                                count(distinct (CASE WHEN time BETWEEN ('22:00:01' AND '23:00:00'))) as '22:00-23:00',
                                                count(distinct (CASE WHEN time BETWEEN ('23:00:01' AND '00:00:00'))) as '23:00-00:00',
                                            FROM `bdata`""")

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

    def tNOCPU(self):
        dt = pd.read_sql(self.totalNumberOfCheckinsPerUser,self.cnx)
        print(dt)
        dt.cumsum
        dt.plot()

    def aNOCPU(self):
        dt = pd.read_sql(self.averageNumberOfCheckinsPerUser,self.cnx)
        dt.cumsum
        print(dt)

    def sDOCPU(self):
        dt = pd.read_sql(self.standardDeviationOfCheckinsPerUser,self.cnx)
        dt.cumsum
        print(dt)

    def tNOCPP(self):
        dt = pd.read_sql(self.totalNumberOfCheckinsPerPlace,self.cnx)
        dt.cumsum
        print(dt)

    def nOCPED(self):
        dt = pd.read_sql(self.numberOfChackinsPerEachDay,self.cnx)
        dt.cumsum
        print(dt)

    def nOCPH(self):
        dt = pd.read_sql(self.numberOfCheckinsPerHour,self.cnx)
        dt.cumsum
        print(dt)

    def nODUPP(self):
        dt = pd.read_sql(self.numberOfDistinctUsersPerPlace,self.cnx)
        dt.cumsum
        print(dt)

    def nODUCPH(self):
        dt = pd.read_sql(self.numberOfDistinctUserCheckedinPerHour,self.cnx)
        dt.cumsum
        print(dt)

    def close(self):
        self.cursor.close()
        self.cnx.close()


