import json
import socket
from threading import Lock


class Server():

    def __init__(self):
        self.HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
        self.PORT = 65431  # Port to listen on (non-privileged ports are > 1023)
        self.mutex = Lock()
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

    def main(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST, self.PORT))
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
                        if reqType == "1":
                            temp = self.getNewSeq()
                            x = {"seq": str(temp)}
                            y = json.dumps(x)
                            print(y)
                            conn.sendall(str.encode(y))


if __name__ == '__main__':
    serv = Server()
    serv.main()
