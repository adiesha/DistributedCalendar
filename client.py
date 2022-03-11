# echo-client.py
import json
import socket
import sys
import time
import threading

from DistributedDict import DistributedDict


class Client():

    def __init__(self, clientPort=62344):
        self.HOST = "127.0.0.1"  # The server's hostname or IP address
        self.SERVER_PORT = 65431  # The port used by the server
        self.clientPort = clientPort
        self.seq = None
        self.map = None

    def createJSONReq(self, typeReq, nodes = None, slot = None):
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
        data = s.recv(1024)
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

            self.seq = resp['seq']
            print("Node ID: " + self.seq)
            s.close()

    def sendNodePort(self):
        # establish connection with server and give info about the client port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(2)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            # print(resp['response'])
            s.close()

    def getMapData(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(3)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            s.close()
            return resp


    def process(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST,int(self.clientPort)))
            while (True):
                s.listen()
                conn, addr = s.accept()
                with conn:
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
                            self.map =  message
                            print("Updated Map: ", self.map)

    def createThreadToListen(self):
        thread = threading.Thread(target=self.process)
        thread.daemon = True
        thread.start()

    def send(self, event, nodes, slot):
        for node in nodes:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.HOST, int(self.map[node])))
                strReq = self.createJSONReq(event, node, slot)
                jsonReq = json.dumps(strReq)
                s.sendall(str.encode(jsonReq))
                s.close()

    def createThreadToSend(self, event, nodes, slot):
        thread = threading.Thread(target=self.send(event, nodes, slot))
        thread.daemon = True
        thread.start()

    def menu(self, d):
        while True:
            print ("Display Calender\t[d]")
            # Make an appoint with node 2 for slot 2: m 2 2
            # Make an appoint with node 1,2 and 3 for slot 2: m 1,2,3 2
            print ("Make Appointment\t[m <node(s)> <slot>]")
            print ("Cancel Appointment\t[c <node(s)> <slot>]")
            print ("Quit    \t[q]")

            resp = input("Choice: ").lower().split()
            if int(resp[2]) > 10:
                print("Please enter a slot between 1 & 10")
            else:
                if resp[0] == 'd':
                    print("Display Calender")
                    # d.displayCalendar()
                elif resp[0] == 'm':
                    nodes = resp[1].split(",")
                    d.insert(nodes, resp[2])
                    partiallog, matrix = d.sendMessage()
                    self.createThreadToSend(4, nodes, int(resp[2]))
                elif resp[0] == 'c':
                    nodes = resp[1].split(",") 
                    d.delete(nodes, resp[2])
                    partiallog, matrix = d.sendMessage()             
                    self.createThreadToSend(5, nodes, int(resp[2]))
                elif resp == 'q':
                    print("Quitting")
                    break

    def main(self):
        if len(sys.argv) > 1:
            print("Client's listening port {0}".format(sys.argv[1]))
            self.clientPort = sys.argv[1]
        
        self.initializeTheNode()
        self.sendNodePort()
        # need to put following inside the menu
        self.createThreadToListen()
        self.map = self.getMapData()
        d = DistributedDict(self.clientPort, int(self.seq), self.map)
        self.menu(d)


if __name__ == '__main__':
    client = Client()
    client.main()
