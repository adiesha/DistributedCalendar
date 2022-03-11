from DistributedDict import DistributedDict


def main():
    dl1 = DistributedDict(65555, 1, {"1": 65556, "2": 56555}, host="127.0.0.1", eventClass=None)
    dl2 = DistributedDict(56555, 2, {"1": 65556, "2": 56555}, host="127.0.0.1", eventClass=None)

    dl1.insert([2], 2)
    dl1.insert([2,3], 5)
    print("calender")
    for e in dl1.calendar:
        print(str(e))
    print("events")
    for e in dl1.log:
        print(str(e))
    print("-------------------")

    # dl2.insert((1, "haha"))
    # dl2.insert((4, "meow"))
    # dl2.insert((3, "moo"))
    # print("calender")
    # for e in dl2.calendar:
    #     print(str(e))
    # print("events")
    # for e in dl2.log:
    #     print(str(e))
    # print("-------------------")

    # plfrom1to2, dl1matrix = dl1.sendMessage(2)

    # print("Martix")
    # print(dl1matrix )

    # print("Partial Log")
    # print(plfrom1to2)
    # for e in plfrom1to2:
    #     print(str(e))

    # dl2.receiveMessage((plfrom1to2, dl1matrix, 1))

    # for e in dl2.calendar:
    #     print(dl2.calendar[e])

    # for e in dl2.log:
    #     print(str(e))
    # print("-------------------")

    # dl1.delete((1, dl1.calendar[1]))
    # plfrom1to2, dl1matrix = dl1.sendMessage(2)
    # dl2.receiveMessage((plfrom1to2, dl1matrix, 1))
    print("calendar")
    for e in dl2.calendar:
        print(dl2.calendar[e])

    # for e in dl2.log:
    #     print(str(e))
    # print("-------------------")


if __name__ == '__main__':
    main()
