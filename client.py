# echo-client.py
import datetime
import json
import logging
import os
import pickle
import random
import socket
import string
import sys
import threading
import time

from DistributedDict import DistributedDict


class Client():

    def __init__(self, host="127.0.0.1", serverport=65431, hb=20, toggleHB=False):
        self.HOST = host  # The server's hostname or IP address
        self.SERVER_PORT = serverport  # The port used by the server
        self.clientPort = None
        self.seq = None
        self.map = None
        self.dd = None
        self.heartbeatInterval = hb
        self.togglehb = toggleHB
        self.clientmutex = threading.Lock()

    def createJSONReq(self, typeReq, nodes=None, slot=None):
        # Initialize node
        if typeReq == 1:
            request = {"req": "1"}
            return request
        # Send port info
        elif typeReq == 2:
            request = {"req": "2", "seq": str(self.seq), "port": str(self.clientPort)}
            return request
        # Get map data
        elif typeReq == 3:
            request = {"req": "3", "seq": str(self.seq)}
            return request
        # Make Appointment
        elif typeReq == 4:
            request = {"req": "4", "seq": str(self.seq), "node": str(nodes), "slot": str(slot)}
            return request
        # Cancel Appointment
        elif typeReq == 5:
            request = {"req": "5", "seq": str(self.seq), "node": str(nodes), "slot": str(slot)}
            return request
        else:
            return ""

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

    def initializeTheNode(self):
        # establish connection with server and give info about the client port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(1)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            self.seq = int(resp['seq'])
            print("sequence: " + str(self.seq))
            s.close()
        currrent_dir = os.getcwd()
        finallogdir = os.path.join(currrent_dir, 'log')
        if not os.path.exists(finallogdir):
            os.mkdir(finallogdir)
        logging.basicConfig(filename="log/{0}.log".format(self.seq), level=logging.DEBUG, filemode='w')

    def sendNodePort(self):
        # establish connection with server and give info about the client port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(2)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            print(resp['response'])
            s.close()

    def downloadNodeMap(self):
        # establish connection with server and give info about the client port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(3)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            print(resp)
            s.close()
            return resp

    def getMapData(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(3)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))
            resp2 = {}
            for k, v in resp.items():
                resp2[int(k)] = (v[0], int(v[1]))

            print(resp2)
            s.close()
            return resp2

    def process(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST, int(self.clientPort)))
            while (True):
                s.listen()
                conn, addr = s.accept()
                with conn:
                    print(f"Connected by {addr}")
                    time.sleep(2)
                    while True:
                        data = self.receiveWhole(conn)
                        if data == b'':
                            break
                        message = self.getJsonObj(data.decode("utf-8"))
                        if list(message.keys())[0] == "req":
                            event = message
                            print("Message received: ", message)
                        else:
                            self.map = message
                            print("Updated Map: ", self.map)

    def createThreadToListen(self):
        thread = threading.Thread(target=self.ReceiveMessageFunct)
        thread.daemon = True
        thread.start()
        return thread

    def broadcast(self, event, nodes, slot):
        for node in nodes:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.HOST, int(self.map[node])))
                strReq = self.createJSONReq(event, node, slot)
                jsonReq = json.dumps(strReq)
                s.sendall(str.encode(jsonReq))
                s.close()

    def createThreadToBroadcast(self, event, nodes, slot):
        thread = threading.Thread(target=self.broadcast(event, nodes, slot))
        thread.daemon = True
        thread.start()

    def createHeartBeatThread(self):
        thread = threading.Thread(target=self.heartbeat)
        thread.daemon = True
        thread.start()

    def ReceiveMessageFunct(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("started listening to port {0}".format(self.clientPort))
            s.bind((self.HOST, self.clientPort))
            while (True):
                s.listen()
                conn, addr = s.accept()
                with conn:
                    # print(f"Connected by {addr}")
                    logging.debug(f"Connected by {addr}")
                    while True:
                        data = self.receiveWhole(conn)
                        self.clientmutex.acquire()
                        if data == b'':
                            break
                        unpickledRequest = pickle.loads(data)
                        # print(unpickledRequest)
                        logging.debug(unpickledRequest)
                        if isinstance(unpickledRequest, dict):
                            # join the partial log
                            np = unpickledRequest['pl']
                            mtx = unpickledRequest['mtx']
                            nid = unpickledRequest['nodeid']

                            self.dd.receiveMessage((np, mtx, nid))
                            conflicts = self.dd.checkConflictingAppnmts()
                            for c in conflicts:
                                self.dd.cancelAppointment((c.timeslot, c))

                            # create message receive event
                            # send the message success request
                            response = {"response": "Success"}
                            # the_encoding = chardet.detect(pickle.dumps(response))['encoding']
                            # response['encoding'] = the_encoding
                            pickledMessage = pickle.dumps(response)
                            try:
                                conn.sendall(pickledMessage)
                            except:
                                print("Problem occurred while sending the reply to node {0}".format(nid))
                        else:
                            response = {"response": "Failed", "error": "Request should be a dictionary"}
                            # the_encoding = chardet.detect(pickle.dumps(response))['encoding']
                            # response['encoding'] = the_encoding
                            pickledMessage = pickle.dumps(response)
                            conn.sendall(pickledMessage)
                        self.clientmutex.release()
                        break

    def heartbeat(self):
        while (True):
            time.sleep(random.randint(10, self.heartbeatInterval))
            self.clientmutex.acquire()
            if self.togglehb:
                # print("heartbeating")
                logging.debug("heartbeating")
                # do the hearbeat
                nodes = sorted(self.map.keys())
                # remove the self node
                nodes.remove(self.seq)
                for n in nodes:
                    self.dd.sendViaSocket(self.dd.sendMessage(n), n)
            self.clientmutex.release()

    def menu(self, d):
        while True:
            print("Display Calender\t[d]")
            # Make an appoint with node 2 for slot 2: m 2 2
            # Make an appoint with node 1,2 and 3 for slot 2: m 1,2,3 2
            print("Make Appointment\t[m <node/s> <slot> <start time> <end time> <name>]")
            print("Cancel Appointment\t[c <node/s> <slot>]")
            print("Press i for interactive appointments")
            print("Press t to Toggle heartbeats service")
            print("Press h to send heartbeat manually")
            print("Press e to print diagnostics")
            print("Quit    \t[q]")

            resp = input("Choice: ").lower().split()
            if len(resp) < 1:
                print("Not a correct input")
                continue
            if resp[0] == 'd':
                print("Display Calender")
                self.clientmutex.acquire()
                d.displayCalendar()
                self.clientmutex.release()
                # d.displayCalendar()
            elif resp[0] == 'm':
                self.clientmutex.acquire()
                nodes = resp[1].split(",")
                participants = [int(i) for i in nodes]
                scheduler = int(d.nodeid)
                timeslot = int(resp[2])
                print(participants)
                starthour, startminute = int(resp[3].split('.')[0]), int(resp[3].split('.')[1])
                print("Start time {0}".format(datetime.time(hour=starthour, minute=startminute)))
                endhour, endminute = int(resp[4].split('.')[0]), int(resp[4].split('.')[1])
                print("End time {0}".format(datetime.time(hour=endhour, minute=endminute)))
                # Assigning random name
                letters = string.ascii_lowercase
                name = "apt:" + "".join(random.choice(letters) for i in range(10))
                if len(resp) > 5:
                    name = "apt:" + ' '.join(str(item) for item in resp[5:len(resp)])
                    print("Appointment Name: {0}".format(name))
                success = d.addAppointment(
                    (timeslot, scheduler, participants, name, (starthour, startminute), (endhour, endminute)))
                if not success:
                    print("Conflict Detected. Cannot continue with the appointment")
                else:
                    # remove itself from the participant list
                    tempParticipants = participants.copy()
                    tempParticipants.remove(d.nodeid)
                    # for each participant send the partial log
                    for p in tempParticipants:
                        (NP, mtx, nid) = d.sendMessage(p)
                        # send the message
                # self.createThreadToBroadcast(4, nodes, resp[2])
                self.clientmutex.release()
            elif resp[0] == 'c':

                # nodes = resp[1].split(",")
                # participants = [int(i) for i in nodes]
                scheduler = int(d.nodeid)
                timeslot = int(resp[1])
                appntment = int(resp[2])
                if (timeslot not in d.calendar) or (len(d.calendar[timeslot]) < appntment):
                    print("Timeslot or Appointment does not exist")
                    print("Appointment cancellation was aborted")
                    continue
                self.clientmutex.acquire()
                success = d.cancelAppointment((timeslot, d.calendar[timeslot][appntment - 1]))
                print("Cancel appointment was {0}".format("successful" if success else "Unsuccessful"))
                # self.createThreadToBroadcast(4, nodes, resp[2])
                self.clientmutex.release()
            elif resp[0] == 'q':
                print("Quitting")
                break
            elif resp[0] == 'h':
                self.clientmutex.acquire()
                # do the hearbeat
                if not self.togglehb:
                    print("Heartbeating is off. Toggle it on by pressing t")
                    self.clientmutex.release()
                    continue
                print("Manual heartbeat")
                logging.debug("Manual heartbeat")
                nodes = sorted(self.map.keys())
                # remove the self node
                nodes.remove(self.seq)
                for n in nodes:
                    self.dd.sendViaSocket(self.dd.sendMessage(n), n)
                self.clientmutex.release()
            elif resp[0] == 't':
                self.clientmutex.acquire()
                self.togglehb = not self.togglehb
                print("Heartbeat toggled to {0}".format("ON" if self.togglehb else "OFF"))
                logging.debug("Heartbeat toggled to {0}".format("ON" if self.togglehb else "OFF"))
                self.clientmutex.release()
                pass
            elif resp[0] == 'e':
                self.clientmutex.acquire()
                print("Printing Diagnostics")
                print("Node Id: {0}".format(self.seq))
                print("Node Map: {0}".format(self.map))
                print("Server IP: {0}".format(self.HOST))
                print("Node Port: {0}".format(self.SERVER_PORT))
                print("Client Port: {0}".format(self.clientPort))
                print("mtx : \n{0}".format(self.dd.matrix))
                print("Heartbeat Toggle status is {0}".format("ON" if self.togglehb else "OFF"))
                self.clientmutex.release()
            elif resp[0] == 'i':
                print("Interactive mode initiated")
                m = input("Input m to make appointment c to cancel appointment: ")
                if m == 'm':
                    nodes = input("input participants: ")
                    tempnodes = nodes.split(",")
                    participants = [int(i) for i in tempnodes]
                    print(participants)
                    name = input("Input apt Name: ")
                    day = int(input("Input Day [1-7] [Monday to Sunday]: "))

                    time = input("Input start Time. Use 24 hour standard: Ex: 12.30: ")
                    t = time.split('.')
                    starthour = int(t[0])
                    startminute = int(t[1])
                    print(datetime.time(hour=starthour, minute=startminute))
                    timeslot = day

                    endtime = input("Input end time. Use 24 hour standard. Ex: 14.30: ")
                    et = endtime.split('.')
                    endhour = int(et[0])
                    endminute = int(et[1])
                    print("End time {0}".format(datetime.time(hour=endhour, minute=endminute)))

                    self.clientmutex.acquire()
                    scheduler = int(d.nodeid)
                    name = "apt:" + name
                    success = d.addAppointment(
                        (timeslot, scheduler, participants, name, (starthour, startminute), (endhour, endminute)))
                    if not success:
                        print("Conflict Detected. Cannot continue with the appointment")
                    self.clientmutex.release()
                elif m == 'c':
                    print("Interactive mode for cancelling")
                    day = int(input("Input Appointment Day [1-7] [Monday to Sunday]: "))
                    timeslot = day
                    aptn = int(input("Input appointment number: "))

                    self.clientmutex.acquire()

                    appntment = aptn
                    success = d.cancelAppointment((timeslot, d.calendar[timeslot][appntment - 1]))
                    print("Cancel appointment was {0}".format("successful" if success else "Unsuccessful"))
                    # self.createThreadToBroadcast(4, nodes, resp[2])
                    self.clientmutex.release()

                else:
                    print("Incorrect input")

    def main(self):
        print('Number of arguments:', len(sys.argv), 'arguments.')
        print('Argument List:', str(sys.argv))

        if len(sys.argv) > 1:
            print("Server ip is {0}".format(sys.argv[1]))
            self.HOST = sys.argv[1]
            print("Server Ip updated")

        if len(sys.argv) > 2:
            print("Client's listening port {0}".format(sys.argv[2]))
            self.clientPort = int(sys.argv[2])

        else:
            print("User did not choose a port for the node. Random port between 55000-63000 will be selected")
            port = random.randint(55000, 63000)
            print("Random port {0} selected".format(port))
            self.clientPort = port

        self.initializeTheNode()
        self.sendNodePort()
        # need to put following inside the menu
        # self.createThreadToListen()
        print("Ready to start the Calendar. Please wait until all the nodes are ready to continue. Then press Enter")
        if input() == "":
            print("Started Creating the Distributed Calendar")
            self.map = self.getMapData()
            d = DistributedDict(self.clientPort, self.seq, self.map)
            self.dd = d
            self.createThreadToListen()
            self.createHeartBeatThread()
            self.menu(d)


if __name__ == '__main__':
    client = Client()
    client.main()
