# echo-client.py
import json
import socket
import sys
import time
import threading
import pickle

from DistributedDict import DistributedDict


class Client():

    def __init__(self, clientPort=62344):
        self.HOST = "127.0.0.1"  # The server's hostname or IP address
        self.SERVER_PORT = 65431  # The port used by the server
        self.clientPort = clientPort
        self.seq = None
        self.map = None
        self.dict_obj = None

    def createJSONReq(self, typeReq, nodes = None, message= None):
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
                        map = True
                        if data == b'':
                            break
                        try:
                            message = pickle.loads(data[10:])
                            print("Message received: ", message)
                            self.dict_obj.receiveMessage(message)
                        except pickle.UnpicklingError:
                            json = self.getJsonObj(data.decode("utf-8"))
                            self.map =  json
                            print("Updated Map: ", self.map)
                            self.dict_obj.update_matrix(len(self.map))

    def createThreadToListen(self):
        thread = threading.Thread(target=self.process)
        thread.daemon = True
        thread.start()

    def send(self, event, nodes):
        for node in nodes:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.HOST, int(self.map[node])))
                partiallog, matrix = self.dict_obj.sendMessage(int(node))
                message = pickle.dumps([partiallog, matrix, int(node)])
                message = bytes(f"{len(message):<{10}}", 'utf-8')+message
                s.sendall(message)
                s.close()

    def createThreadToSend(self, event, nodes):
        thread = threading.Thread(target=self.send(event,nodes))
        thread.daemon = True
        thread.start()

    def menu(self):
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
                    self.dict_obj.insert(nodes, resp[2])
                    self.createThreadToSend(4, nodes)
                elif resp[0] == 'c':
                    nodes = resp[1].split(",") 
                    self.dict_obj.delete(nodes, resp[2])        
                    self.createThreadToSend(5, nodes)
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
        self.dict_obj = DistributedDict(int(self.clientPort), int(self.seq), self.map)
        self.menu()


if __name__ == '__main__':
    client = Client()
    client.main()
