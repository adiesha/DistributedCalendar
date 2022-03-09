from DistributedLog import Event, DistributedLog


def main():
    dict1 = {"1": ("127.0.0.1", 62222),
             "2": ("127.0.0.1", 62223)}
    dl1 = DistributedLog(62222, 1, dict1, Event, host="127.0.0.1")
    dl2 = DistributedLog(62223, 2, dict1, Event, host="127.0.0.1")
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
            e1 = Event(1)
            dl1.addLocalEvent(e1)
            dl1.printEvents()
            print("lamport time of dl1 is {0}".format(dl1.getLamportTime()))
        elif x == "3":
            e2 = Event((1, 3))
            dl1.sendMsg(2, 62223, e2)
        elif x == "4":
            e3 = Event(2)
            dl2.addLocalEvent(e3)
            dl2.printEvents()
            print("lamport time of dl2 is {0}".format(dl2.getLamportTime()))
    pass


if __name__ == '__main__':
    main()
