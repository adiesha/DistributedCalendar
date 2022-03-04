# echo-client.py
import json
import socket


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
        else:
            return ""

    def receiveWhole(self, s):
        data = s.recv(1024)
        return data

    def getJsonObj(self, input):
        jr = json.loads(input)
        return jr

    def main(self):
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


if __name__ == '__main__':
    client = Client()
    client.main()
