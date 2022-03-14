from threading import Lock

import numpy as np

from DistributedLog import Event, Operation


class DistributedDict:
    def __init__(self, clientport, nodeid, nodemap, host="127.0.0.1", eventClass=None):
        self.HOST = host
        self.port = clientport
        self.nodeid = nodeid
        self.map = nodemap
        self.calendar = {}
        self.mutex = Lock()
        self.noOfNodes = len(nodemap)
        self.events = set()
        self.matrix = np.zeros((self.noOfNodes, self.noOfNodes), dtype=np.int32)
        if eventClass is None:
            self.eventClass = Event
        else:
            self.eventClass = eventClass

    # Low level methods
    def insert(self, x):
        # x should be a key value pair
        self.mutex.acquire()
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(self.nodeid, (nodeid, lamptime))
            event._op = Operation.INSERT
            event._m = x
            self.events.add(event)

            self.calendar[x[0]] = [x[1]]
        finally:
            self.mutex.release()
        pass

    def appendValue(self, x):
        # x should be a key value pair
        self.mutex.acquire()
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(self.nodeid, (nodeid, lamptime))
            event._op = Operation.INSERT
            event._m = x
            self.events.add(event)

            # appended the value instead inserting
            self.calendar[x[0]].append(x[1])
        finally:
            self.mutex.release()

    def delete(self, x):
        self.mutex.acquire()
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(self.nodeid, (nodeid, lamptime))
            event._op = Operation.DELETE
            event._m = x
            self.events.add(event)

            r = self.calendar.pop(x[0], None)
            # print(r)
            pass
        finally:
            self.mutex.release()

    def deleteValue(self, x):
        self.mutex.acquire()
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(self.nodeid, (nodeid, lamptime))
            event._op = Operation.DELETE
            event._m = x
            self.events.add(event)

            r = self.calendar[x[0]].remove(x[1])
            # print(r)
            pass
        finally:
            self.mutex.release()

    def sendMessage(self, k):
        NP = self.calculatePartialLog(k)
        return NP, self.matrix, self.nodeid

    def receiveMessage(self, m):
        # let m be <NP_k, T_k>
        pl = m[0]
        matrix = m[1]
        k = m[2]
        NE = self.calculateNE(pl)

        tempCreateKeyList = {}
        tempDeleteKeyLsit = {}
        for e in NE:
            if e._op == Operation.INSERT:
                if e._m[0] in tempCreateKeyList:
                    tempCreateKeyList[e._m[0]].append(e._m[1])
                else:
                    tempCreateKeyList[e._m[0]] = [e._m[1]]
            elif e._op == Operation.DELETE:
                if e._m[0] in tempDeleteKeyLsit:
                    tempDeleteKeyLsit[e._m[0]].append(e._m[1])
                else:
                    tempDeleteKeyLsit[e._m[0]] = [e._m[1]]

        # go through the events again one by one
        for e in NE:
            if e._op == Operation.INSERT:
                if e._m[1] not in (tempDeleteKeyLsit[e._m[0]] if e._m[
                                                                     0] in tempDeleteKeyLsit else []):  # checking insert appointment is not in delete list
                    if e._m[0] in self.calendar:
                        self.calendar[e._m[0]].append(e._m[1])
                    else:
                        self.calendar[e._m[0]] = [e._m[1]]
                else:
                    print(
                        "Appointment {0} is in create and delete list, therefore it will not be added to the dictionary".format(
                            e._m[1]))
            elif e._op == Operation.DELETE:
                if e._m[1] not in (tempCreateKeyList[e._m[0]] if e._m[0] in tempCreateKeyList else []):
                    # we need to delete this value from the dictionary
                    if e._m[0] in self.calendar:
                        self.calendar[e._m[0]].remove(e._m[1])
                        # make sure to write logic to remove the key if list is empty after this
                        if not self.calendar[e._m[0]]:
                            print("No value for key {0} in the dictionary. Removing the key".format(e._m[0]))
                            self.calendar.pop(e._m[0])
                    else:
                        print("Delete key not in calendar Error {0}".format(e._m[0]))

        # for ck in tempCreateKeyList:
        #     if ck not in tempDeleteKeyLsit:
        #         if ck in self.calendar:
        #             self.calendar[ck].append(tempCreateKeyList[ck]._m[1])
        #         else:
        #             self.calendar[ck] = [tempCreateKeyList[ck]._m[1]]
        #     else:
        #         deleteevents = tempDeleteKeyLsit[ck]
        #
        # for dk in tempDeleteKeyLsit:
        #     if dk not in tempCreateKeyList:
        #         if dk not in self.calendar:
        #             print("Delete key not in calandar Error {0}".format(dk))
        #         self.calendar.pop(dk, None)

        # merge the partial logs
        self.updateMatrixFromReceivedMatrix(matrix, k)
        self.unionevents(pl)
        for ev in self.events.copy():
            needArecord = False
            for j in range(1, self.noOfNodes + 1):
                if not self.hasRecord(self.matrix, ev, j):
                    needArecord = True
                    break
            if needArecord:
                pass
            else:
                self.events.remove(ev)

    def unionevents(self, pl):
        for e in pl:
            if e not in self.events:
                # add to the events
                self.events.add(e)
                # log the event

    def updateMatrixFromReceivedMatrix(self, receivedMatrix, k):
        i = self.nodeid
        size = self.noOfNodes
        for j in range(size):
            self.matrix[i - 1][j] = max(receivedMatrix[k - 1][j], self.matrix[i - 1][j])

        for m in range(size):
            for n in range(size):
                self.matrix[m][n] = max(receivedMatrix[m][n], self.matrix[m][n])

    def calculateNE(self, pl):
        NE = []
        for e in pl:
            if not self.hasRecord(self.matrix, e, self.nodeid):
                NE.append(e)
        return NE

    def calculatePartialLog(self, k):
        pl = []
        for e in self.events:
            if not self.hasRecord(self.matrix, e, k):
                pl.append(e)
        return pl

    def hasRecord(self, matrix, eR, k):
        return matrix[k - 1][eR.nodeId - 1] >= eR.ts[1]

    def getNewLamportTimestamp(self):
        self.matrix[self.nodeid - 1][self.nodeid - 1] = self.matrix[self.nodeid - 1][self.nodeid - 1] + 1
        return self.getLamportTime()

    def getLamportTime(self):
        return self.nodeid, self.matrix[self.nodeid - 1][self.nodeid - 1]

    # High level methods
    def displayCalendar(self):
        for key, value in self.calendar.items():
            print(key, ' : ', value)

    def addAppointment(self, message):
        # Todo: implement the add appointment logic here
        # create the appointment

        timeslot = message[0]
        scheduler = message[1]
        participants = message[2]
        appnmnt = Appointment()
        appnmnt.timeslot = timeslot
        appnmnt.scheduler = scheduler
        appnmnt.participants = participants

        # check whether scheduler is a participant
        if not (scheduler in participants):
            print("Scheduler is not a participant. Scheduler must be a participant")
            return False

        # check for internal conflicts

        if self.isInternalConflicts(timeslot, scheduler, participants, self.calendar):
            print("Appointment conflict for timeslot {0} scheduler {1} participants {2} ".format(timeslot, scheduler,
                                                                                                 participants))
            return False
        else:
            print("No Internal conflict detected. Moving on to do the schedule the appointment")
            # if timeslot is not used then we need to insert the timeslot and send messages
            if timeslot not in self.calendar:
                print('Timeslot is not used in the local calendar')
                self.insert((timeslot, appnmnt))
            else:
                print("Timeslot already exist. Moving on to adding non conflicting appointment to the local calendar")
                self.appendValue((timeslot, appnmnt))
            return True

    def cancelAppointment(self, message):
        timeslot = message[0]
        appnmnt = message[1]

        scheduler = appnmnt.scheduler
        participants = appnmnt.participants
        # check whether canceler is a participant
        if not (scheduler in participants):
            print("Canceler is not an participant of the appointment")
            return False
        else:
            # check if it is delete or deletevalue
            if timeslot not in self.calendar:
                print("Error! trying to delete appintment at a timeslot that doesn't exist")
                return False
            else:
                if len(self.calendar[timeslot]) > 1:
                    self.deleteValue((timeslot, appnmnt))
                elif len(self.calendar[timeslot]) == 1:
                    self.delete((timeslot, appnmnt))
                return True
        pass

    def updateDict(self, partial_log):
        pass

    def isInternalConflicts(self, timeslot, scheduler, participants, calendar):
        if timeslot not in self.calendar:
            return False
        else:
            appnmnts = self.calendar[timeslot]
            for a in appnmnts:
                for p in participants:
                    if a.isParticipant(p):
                        return True
        return False


class Appointment:
    def __init__(self):
        self.timeslot = None
        self.scheduler = None
        self.participants = []

    def isParticipant(self, participant):
        return participant in self.participants

    def isScheduler(self, schedulerperson):
        return self.scheduler == schedulerperson

    def __str__(self):
        return "Timeslot: {0} scheduled by: {1} participants {2}".format(self.timeslot, self.scheduler,
                                                                         self.participants)
