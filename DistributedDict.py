


class DistributedDict:
    def __init__(self, clientport, nodeid, nodemap, host="127.0.0.1"):
        self.HOST = host
        self.port = clientport
        self.nodeid = nodeid
        self.map = nodemap
        self.calendar = {}

    def displayCalendar(self):
        for key, value in self.calendar.items():
            print(key, ' : ', value)

    def addAppointment(self, message):
        pass

    def cancelAppointment(self, message):
        pass

    def updateDict(self, partial_log):
        pass

    