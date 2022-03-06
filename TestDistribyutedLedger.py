from DistributedLog import Event, DistributedLog
import threading


def main():
    dl1 = DistributedLog(62222, 1, {}, Event, host="127.0.0.1")
    dl2 = DistributedLog(62223, 2, {}, Event, host="127.0.0.1")
    t1 = dl1.runThread()
    t2 = dl2.runThread()

    while (True):
        x = input()
        if x == "1":
            t1.kill()
            t2.kill()
            t1.join()
            t2.join()
            break
        elif x == "2":
            e1 = Event((1,2))
            dl1.addLocalEvent(e1)
            print(dl1.events)
        elif x == "3":
            e2 = Event((1,3))
            dl1.sendMsg(2, 62223, e2)

    pass


if __name__ == '__main__':
    main()
