# echo-client.py
import json
from re import S
import select
import socket
import sys
import threading


class Client():

    def __init__(self, clientPort=62344):
        self.HOST = "127.0.0.1"  # The server's hostname or IP address
        self.SERVER_PORT = 65431  # The port used by the server
        self.clientPort = clientPort
        self.seq = None

    def createJSONReq(self, typeReq):
        if typeReq == 1:
            request = {"req": "1"}
            return request
        elif typeReq == 2:
            request = {"req": "2", "seq": str(self.seq), "port": str(self.clientPort)}
            return request
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
            print("sequence: " + self.seq)
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

            print(resp['response'])
            s.close()

    def getMapData(self):
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


    def process(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST,int(self.clientPort)))
            s.listen(1)
            rxset = [s]
            txset = []

            while 1:
                rxfds, txfds, exfds = select.select(rxset, txset, rxset)
                for sock in rxfds:
                    if sock is s:
                        conn, addr = s.accept()
                        conn.setblocking(0)
                        rxset.append(conn)
                        print ('Connection from address:', addr)
                    else:
                        try:
                            data = sock.recv(1024)
                            if data == ";" :
                                print("Received all the data")
                                for x in param:
                                    print(x)
                                param = []
                                rxset.remove(sock)
                                sock.close()
                            else:
                                print ("received data: ", data)
                                param.append(data)
                        except:
                            print ("Connection closed by remote end")
                            param = []
                            rxset.remove(sock)
                            sock.close()

    def createThreadToListen(self):
        thread = threading.Thread(target=self.process)
        thread.daemon = True
        thread.start()


    def menu(self):
        while True:
            print ("Display Calender\t[d]")
            print ("Make appointment\t[m]")
            print ("Cancel Appointment\t[c]")
            print ("Quit[q]")

            resp = input("Choice: ").lower()
            if resp == 'd':
                pass
            elif resp == 'm':
                pass
            elif resp == 'c':
                pass
            elif resp == 'q':
                pass

    def main(self):
        print('Number of arguments:', len(sys.argv), 'arguments.')
        print('Argument List:', str(sys.argv))

        if len(sys.argv) > 1:
            print("Client's listening port {0}".format(sys.argv[1]))
            self.clientPort = sys.argv[1]

        self.initializeTheNode()
        self.sendNodePort()
        # need to put following inside the menu
        map = self.getMapData()
        sockets = list(map.values())
        self.createThreadToListen()
        self.menu()


if __name__ == '__main__':
    client = Client()
    client.main()
