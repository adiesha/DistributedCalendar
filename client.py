# echo-client.py
import json
import socket
import sys


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

    def createThreadToListen(self):
        pass

    def menu(self):
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
        self.getMapData()
        self.createThreadToListen()
        self.menu()


if __name__ == '__main__':
    client = Client()
    client.main()
