from threading import Lock

import hashlib
import random
import time
import numpy as np


class DistributedDict:
    def __init__(self, clientport, nodeid, nodemap, host="127.0.0.1", eventClass=None):
        self.HOST = host
        self.port = clientport
        self.nodeid = nodeid
        self.map = nodemap
        self.log = set()
        self.calendar = {}
        self.mutex = Lock()
        self.noOfNodes = len(nodemap)
        self.matrix = np.zeros((self.noOfNodes, self.noOfNodes), dtype=np.int32)
        if eventClass is None:
            self.eventClass = Event
        else:
            self.eventClass = eventClass

    # Low level methods
    def insert(self, nodes, slot):
        # x should be a key value pair
        self.mutex.acquire()
        nodes = [int(item) for item in nodes]
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(nodes, self.nodeid, (nodeid, lamptime))
            event._op = 1
            event._m = slot
            self.log.add(event)
            # self.calendar[slot] = nodes
        finally:
            self.mutex.release()
        pass

    def delete(self, nodes, slot):
        self.mutex.acquire()
        nodes = [int(item) for item in nodes]
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(nodes, self.nodeid, (nodeid, lamptime))
            event._op = 2
            event._m = slot
            self.log.add(event)
            # r = self.calendar.pop(slot, None)
            pass
        finally:
            self.mutex.release()

    def sendMessage(self, k):
        NP = self.calculatePartialLog(k)
        return NP, self.matrix

    def receiveMessage(self, m):
        # let m be <NP_k, T_k>
        pl = m[0]
        matrix = m[1]
        k = m[2]
        NE = self.calculateNE(pl)

        tempCreateKeyList = {}
        tempDeleteKeyLsit = {}
        for e in NE:
            if e._op == 1:
                tempCreateKeyList[e._m[0]] = e
            elif e._op == 2:
                tempDeleteKeyLsit[e._m[0]] = e

        for ck in tempCreateKeyList:
            if ck not in tempDeleteKeyLsit:
                self.calendar[ck] = tempCreateKeyList[ck]._m[1]

        for dk in tempDeleteKeyLsit:
            if dk not in tempCreateKeyList:
                if dk not in self.calendar:
                    print("Delete key not in calandar Error {0}".format(dk))
                self.calendar.pop(dk, None)

        # merge the partial logs
        self.updateMatrixFromReceivedMatrix(matrix, k)
        self.unionevents(pl)
        for ev in self.log.copy():
            needArecord = False
            for j in range(1, self.noOfNodes + 1):
                if not self.hasRecord(self.matrix, ev, j):
                    needArecord = True
                    break
            if needArecord:
                pass
            else:
                self.log.remove(ev)

    def unionevents(self, pl):
        for e in pl:
            if e not in self.log:
                # add to the events
                self.log.add(e)
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
        for e in self.log:
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

        # check for conflicts
        # if any of the participants are already in one of the appointments in that timesolt return false

        pass

    def cancelAppointment(self, message):
        pass

    def updateDict(self, partial_log):
        pass


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
        return "Timeslot: {0} scheduled by: {1} participants {2)".format(self.timeslot, self.scheduler,
                                                                         self.participants)


class Event:

    def __init__(self, nodes, nodeid=1, timestamp=(0, 0)):
        hash = hashlib.sha1()
        hash.update(str(time.time_ns()).encode('utf-8') + str(random.randint(1, 1000000)).encode('utf-8'))
        self.id = hash.hexdigest()
        self.nodeId = nodeid
        self.nodes = nodes
        self.ts = timestamp
        self._op = None
        self._m = None

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, Event):
            return hash(self.id) == hash(other)
        return False

    def __str__(self):
        return "Event Hash: {0} Nodes:{1} Timestamp: {2} Op: {3} Slot:{4}".format(self.id, self.nodes, self.ts, self._op, self._m)

