from datetime import date
from Work import Work

class User:

    def __init__(self):
        __userID = None
        __checkinDate = None
        __work = None

    # add work to the user
    def addWork(self,work):
        self.work.append(work)
