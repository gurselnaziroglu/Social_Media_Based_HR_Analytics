from User import User
import datetime

class Checkin:

    def __init__(self):

        __user = User()
        __id = None
        __date = None
        __time = None
        __dayOfWeek = None
        __place = None
        __city = None
        __latitude = None
        __longitude = None

    # return True if the day is one of the weekdays
    def isWeekDay(self):
        if self.dayOfWeek in ['Mon','Tue','Wed','Thu','Fri']:
            return True
        else:
            return False

    # return True if the time is morning (07:00 - 10:00)
    def isMorning(self):
        if self.time < 10 and self.time > 7:
            return True
        else:
            return False

    # Return True if the month is not July of August
    def inNotSummer(self):
        if self.date.month != 7 or self.date.month != 8:
            return True
        else:
            return False
