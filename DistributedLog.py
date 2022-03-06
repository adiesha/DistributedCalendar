import hashlib
import random
import socket
import threading
import time
import json
from threading import Lock


class DistributedLog:

    def threadFunc(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("started listening to port {0}".format(self.port))
            s.bind((self.HOST, self.port))
            while (True):
                s.listen()
                conn, addr = s.accept()
                with conn:
                    print(f"Connected by {addr}")
                    while True:
                        data = self.receiveWhole(conn)
                        if data == b'':
                            break
                        jsonreq = self.getJsonObj(data)
                        reqType = jsonreq["req"]
                        if reqType == "4":  # receive message
                            # print the received message
                            print(jsonreq['msg'])
                            # update lamport timestamp makesure to get the mutex

                            # send success
                            x = {"response": "success"}
                            y = json.dumps(x)
                            print(y)
                            print(self.map)
                            conn.sendall(str.encode(y))


    def __init__(self, clientport, nodeid, nodemap, eventClass=None, host="127.0.0.1"):
        self.HOST = host
        self.port = clientport
        self.nodeid = nodeid
        self.map = nodemap
        self.events = set()
        self.lamport = 0
        self.mutex = Lock()
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
        return (self.nodeid, self.lamport)

    def getNewLamportTimestamp(self):
        self.lamport = self.lamport + 1
        return self.getLamportTime()

    def addLocalEvent(self, event):
        self.mutex.acquire()
        try:
            # update lamport
            lamptime = self.getNewLamportTimestamp()
            self.events.add(event)
        finally:
            self.mutex.release()
        return lamptime, event

    def receiveMessage(self):
        self.mutex.acquire()
        try:
            print("somethig")
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

    def getJsonObj(self, input):
        jr = json.loads(input)
        return jr

    def createJSONReq(self, typeReq):
        if typeReq == 4:
            request = {"req": "4", "msg": "Not implemented"}
            return request

        else:
            return ""

    def sendMsg(self, nodeId, port, event):
        lamptime, event = self.addLocalEvent(event)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, port))
            strReq = self.createJSONReq(4)
            jsonReq = json.dumps(strReq)
            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            print(resp['response'])
            s.close()


class Event:

    def __init__(self, timestamp):
        hash = hashlib.sha1()
        hash.update(str(time.time_ns()).encode('utf-8') + str(random.randint(1, 1000000)).encode('utf-8'))
        self.id = hash.hexdigest()
        print(self.id)
        print(type(self.id))
        self.ts = timestamp

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, Event):
            return hash(self.id) == hash(other)
        return False


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
    e1 = Event((1, 1))
    e2 = Event((1, 2))
    print(e1 == e2)
    se.add(e1)
    se.add(e2)
    print(se)
    print(e1.__hash__())
    print(e2.__hash__())
    print(e2.__hash__())

    dl = DistributedLog(62222, 1, {}, Event, host="127.0.0.1")
    th = dl.runThread()

    th.join()
    pass


if __name__ == '__main__':
    main()
