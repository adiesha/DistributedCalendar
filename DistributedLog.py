import hashlib
import pickle
import random
import socket
import threading
import time
from enum import Enum
from threading import Lock

import chardet
import numpy as np


class DistributedLog:

    def threadFunc(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("started listening to port {0}".format(self.port))
            s.bind((self.HOST, self.port))
            while (True):
                s.listen()
                conn, addr = s.accept()
                with conn:
                    self.mutex.acquire()
                    try:
                        print(f"Connected by {addr}")
                        while True:
                            data = self.receiveWhole(conn)
                            if data == b'':
                                break
                            unpickledRequest = pickle.loads(data)
                            print(unpickledRequest)
                            if isinstance(unpickledRequest, dict):
                                # join the partial log
                                pl = unpickledRequest['pl']
                                self.unionevents(pl)
                                # make sure logging is done when events unioned

                                # update the lamport time and create receive event
                                receivedTs = unpickledRequest['ts']
                                receivedMatrix = unpickledRequest['T']
                                self.addReceiveEvent(receivedTs)
                                self.updateMatrixFromReceivedMatrix(receivedMatrix, receivedTs[0])

                                # create message receive event
                                # send the message success request
                                response = {"response": "Success"}
                                the_encoding = chardet.detect(pickle.dumps(response))['encoding']
                                response['encoding'] = the_encoding
                                pickledMessage = pickle.dumps(response)
                                conn.sendall(pickledMessage)

                            else:
                                response = {"response": "Failed", "error": "Request should be a dictionary"}
                                the_encoding = chardet.detect(pickle.dumps(response))['encoding']
                                response['encoding'] = the_encoding
                                pickledMessage = pickle.dumps(response)
                                conn.sendall(pickledMessage)
                    finally:
                        self.mutex.release()

    def __init__(self, clientport, nodeid, nodemap, eventClass=None, host="127.0.0.1"):
        self.HOST = host
        self.port = clientport
        self.nodeid = nodeid
        self.map = nodemap
        self.events = set()
        self.mutex = Lock()
        self.noOfNodes = len(nodemap)
        self.matrix = np.zeros((self.noOfNodes, self.noOfNodes), dtype=np.int32)
        self._terminate = False
        # logging.basicConfig(level=logging.DEBUG, format='%(message)s')
        # logging.info("haha")
        if eventClass is None:
            self.eventClass = Event
        else:
            self.eventClass = eventClass

    def runThread(self):
        th = threading.Thread(target=self.threadFunc)
        print("Join the thread when you call runThread")
        th.start()
        return th

    def getLamportTime(self):
        return self.nodeid, self.matrix[self.nodeid - 1][self.nodeid - 1]

    def getNewLamportTimestamp(self):
        self.matrix[self.nodeid - 1][self.nodeid - 1] = self.matrix[self.nodeid - 1][self.nodeid - 1] + 1
        return self.getLamportTime()

    def addLocalEvent(self, event):
        self.mutex.acquire()
        try:
            # update lamport
            lamptime = self.getNewLamportTimestamp()
            event.ts = lamptime
            event.nodeId = self.nodeid
            self.events.add(event)
            # log it
        finally:
            self.mutex.release()
        return lamptime, event

    def update(self, pl, receivedTs, receivedMatrix):
        self.mutex.acquire()
        try:
            self.unionevents(pl)
            # make sure logging is done when events unioned

            # update the lamport time and create receive event
            # self.addReceiveEvent(receivedTs)
            self.updateMatrixFromReceivedMatrix(receivedMatrix, receivedTs[0])
        finally:
            self.mutex.release()

    def receiveWhole(self, s):
        BUFF_SIZE = 4096  # 4 KiB
        data = b''
        while True:
            part = s.recv(BUFF_SIZE)
            data += part
            if len(part) < BUFF_SIZE:
                # either 0 or end of data
                break
        return data

    def printEvents(self):
        for s in self.events:
            print(str(s))

    def calculatePartialLog(self, j):
        pl = []
        for e in self.events:
            if not self.hasRecord(self.matrix, e, j):
                pl.append(e)
        return pl

    def hasRecord(self, matrix, eR, k):
        return matrix[k - 1][eR.nodeId - 1] >= eR.ts[1]

    def sendMsg(self, nodeId, port, event):
        lamptime, event = self.addLocalEvent(event)
        # calculate the partial log
        pl = self.calculatePartialLog(nodeId)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, port))
            # strReq = self.createJSONReq(4)
            strReq = {}
            strReq['ts'] = lamptime
            strReq['pl'] = pl
            strReq['T'] = self.matrix
            the_encoding = chardet.detect(pickle.dumps(self.events))['encoding']
            print(the_encoding)
            strReq['encoding'] = the_encoding
            strReq['msg'] = self.events

            pickledMessage = pickle.dumps(strReq)
            s.sendall(pickledMessage)

            data = self.receiveWhole(s)
            resp = pickle.loads(data)

            print(resp['response'])
            s.close()

    def unionevents(self, pl):
        for e in pl:
            if e not in self.events:
                # add to the events
                self.events.add(e)
                # log the event

    def setTerminateFlag(self):
        self._terminate = True

    def addReceiveEvent(self, receivedTs):
        if receivedTs[1] > self.getLamportTime()[1]:
            eReceived = Event(self.nodeid, (self.nodeid, receivedTs[1] + 1))
            self.matrix[self.nodeid - 1][self.nodeid - 1] = receivedTs[1] + 1
            self.events.add(eReceived)

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


class Event:

    def __init__(self, nodeid=1, timestamp=(0, 0)):
        hash = hashlib.sha1()
        hash.update(str(time.time_ns()).encode('utf-8') + str(random.randint(1, 1000000)).encode('utf-8'))
        self.id = hash.hexdigest()
        self.nodeId = nodeid
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
        return "Event Hash: {0} Node:{1} Timestamp: {2} Op: {3} message: {4}".format(self.id, self.nodeId, self.ts,
                                                                                     self._op, self._m[1])


class Operation(Enum):
    INSERT = 1
    DELETE = 2


# test class for hashing in set
class Apple:
    def __init__(self, i):
        self.app = i

    def __eq__(self, other):
        if isinstance(other, Apple):
            return self.app == other.app
        return False

    def __hash__(self):
        return hash(2)

    def __str__(self):
        return "ID: {0}".format(self.app)


def main():
    a1 = Apple(1)
    a2 = Apple(2)
    se = set()
    se.add(a1)
    se.add(a2)
    print(se)
    e1 = Event(1, (1, 1))
    e2 = Event(1, (1, 2))
    print(e1 == e2)
    se.add(e1)
    se.add(e2)
    print(se)
    print(e1.__hash__())
    print(e2.__hash__())
    print(e2.__hash__())
    li = [e1, e2]
    print(e1 in li)

    import chardet

    the_encoding = chardet.detect(pickle.dumps(se))['encoding']
    print(the_encoding)

    dl = DistributedLog(62222, 1, {"1": 62222, "2": 62223}, Event, host="127.0.0.1")
    th = dl.runThread()
    dl2 = DistributedLog(62223, 2, {"1": 62222, "2": 62223}, Event, host="127.0.0.1")
    th2 = dl2.runThread()
    dl.addLocalEvent(Event(1))
    dl.addLocalEvent(Event(1))
    dl.addLocalEvent(Event(1))
    dl.addLocalEvent(Event(1))
    dl.addLocalEvent(e1)

    dl2.addLocalEvent(Event(2))
    dl2.addLocalEvent(Event(2))

    print(dl.hasRecord(dl.matrix, e1, 2))
    print((dl.calculatePartialLog(2)))
    for i in dl.calculatePartialLog(2):
        print(i)
    dl.matrix[1][0] = 1
    print("_______________________")
    for i in dl.calculatePartialLog(2):
        print(i)

    print("_______________________")
    dl2.events.update(dl.calculatePartialLog(2))
    dl2.printEvents()
    th.join()
    th2.join()
    pass


if __name__ == '__main__':
    main()
