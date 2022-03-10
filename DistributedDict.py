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

            self.calendar[x[0]] = x[1]
        finally:
            self.mutex.release()
        pass

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
            if e._op == Operation.INSERT:
                tempCreateKeyList[e._m[0]] = e
            elif e._op == Operation.DELETE:
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
        for ev in self.events.copy():
            needArecord = False
            for j in range(self.noOfNodes):
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
        pass

    def cancelAppointment(self, message):
        pass

    def updateDict(self, partial_log):
        pass
