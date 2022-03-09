import json
import socket
import time
import threading


class Server():

    def __init__(self):
        self.HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
        self.PORT = 65431  # Port to listen on (non-privileged ports are > 1023)
        self.mutex = threading.Lock()
        self.seq = 0
        self.map = {}

    def getNewSeq(self):
        self.mutex.acquire()
        try:
            self.seq = self.seq + 1
            temp = self.seq
        finally:
            self.mutex.release()
        return temp

    def receiveWhole(self, conn):
        data = conn.recv(1024)
        return data

    def getJsonObj(self, bytestr):
        jr = json.loads(bytestr.decode("utf-8"))
        return jr

    def sendNewMap(self):
        thread = threading.Thread(target=self.sendMap)
        thread.daemon = True
        thread.start()

    def sendMap(self,):        
        for key, value in self.map.items():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.HOST, int(value)))
                y = json.dumps(self.map)
                s.sendall(str.encode(y))
                s.close()

    def main(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST, self.PORT))
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
                        jsonreq = self.getJsonObj(data)
                        reqType = jsonreq["req"]
                        if reqType == "1":
                            temp = self.getNewSeq()
                            x = {"seq": str(temp)}
                            y = json.dumps(x)
                            print(y)
                            conn.sendall(str.encode(y))
                        if reqType == "2":
                            self.map[jsonreq['seq']] = jsonreq['port']

                            # create response
                            x = {"response": "success"}
                            y = json.dumps(x)
                            print(y)
                            print(self.map)
                            conn.sendall(str.encode(y))
                        if reqType == "3":
                            # print("Request from {0} for node data".format(jsonreq['seq']))
                            y = json.dumps(self.map)
                            conn.sendall(str.encode(y))
                            print("New node {0} added".format(jsonreq['seq']))
                            self.sendNewMap()


if __name__ == '__main__':
    serv = Server()
    serv.main()
