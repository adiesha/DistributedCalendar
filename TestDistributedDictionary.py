from DistributedDict import DistributedDict, Appointment


def main():
    # dl1 = DistributedDict(65555, 1, {"1": 65556, "2": 56555}, host="127.0.0.1", eventClass=None)
    # dl2 = DistributedDict(56555, 2, {"1": 65556, "2": 56555}, host="127.0.0.1", eventClass=None)
    #
    # dl1.insert((1, "adiesha"))
    # dl1.insert((2, "wow"))
    # dl1.insert((3, "great"))
    # dl1.insert((4, "where"))
    #
    # for e in dl1.calendar:
    #     print(str(e))
    #
    # for e in dl1.events:
    #     print(str(e))
    # print("-------------------")
    #
    # dl2.insert((1, "haha"))
    # dl2.insert((4, "meow"))
    # dl2.insert((3, "moo"))
    #
    # for e in dl2.calendar:
    #     print(str(e))
    # for e in dl2.events:
    #     print(str(e))
    # print("-------------------")
    #
    # plfrom1to2, dl1matrix, nodeidd = dl1.sendMessage(2)
    #
    # dl2.receiveMessage((plfrom1to2, dl1matrix, 1))
    #
    # for e in dl2.calendar:
    #     print(dl2.calendar[e])
    #
    # for e in dl2.events:
    #     print(str(e))
    # print("-------------------")
    #
    # dl1.delete((1, dl1.calendar[1][0]))
    # plfrom1to2, dl1matrix, nodeid = dl1.sendMessage(2)
    # dl2.receiveMessage((plfrom1to2, dl1matrix, 1))
    # for e in dl2.calendar:
    #     print(dl2.calendar[e])
    #
    # for e in dl2.events:
    #     print(str(e))
    # print("-------------------")
    # # testInternalConflicts()

    # testAddAppointment()
    # testSendMessageWhenSuccessAppointment()
    #
    # testCancelAppointments()

    # testConflictsInaTimeSlot()
    testAddAppointmentWithConflicts()


def testInternalConflicts():
    dl1 = DistributedDict(65555, 1, {"1": 65556, "2": 56555}, host="127.0.0.1", eventClass=None)
    a1 = Appointment()
    a1.timeslot = 1
    a1.participants = [1, 2, 3]
    a1.scheduler = 1

    a2 = Appointment()
    a2.timeslot = 1
    a2.participants = [4, 5]
    a2.scheduler = 5

    dl1.insert((1, a1))
    dl1.calendar[1].append(a2)

    for e in dl1.calendar:
        print(str(dl1.calendar[e]))

    print(dl1.isInternalConflicts(1, 1, [2, 3], dl1.calendar))
    print(dl1.isInternalConflicts(1, 1, [6, 7], dl1.calendar))
    print(dl1.isInternalConflicts(2, 1, [6, 7], dl1.calendar))


def testAddAppointment():
    dl1 = DistributedDict(65555, 1, {"1": 65556, "2": 56555}, host="127.0.0.1", eventClass=None)

    dl1.addAppointment((1, 1, [1, 2, 3]))
    for e in dl1.calendar:
        for a in dl1.calendar[e]:
            print(a)

    # inserting a conflicting appointment
    dl1.addAppointment((1, 1, [3, 4, 5]))

    dl1.addAppointment((1, 4, [4, 5]))
    print(dl1)

    # trying to make an appointment for someone else
    dl1.addAppointment((2, 1, [2, 3]))
    print(dl1)


def testSendMessageWhenSuccessAppointment():
    dl1 = DistributedDict(65555, 1, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)
    dl2 = DistributedDict(65555, 2, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)
    dl3 = DistributedDict(65555, 3, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)

    dl1.addAppointment((1, 1, [1, 2, 3]))
    dl1.addAppointment((1, 4, [4, 5]))
    dl1.addAppointment((1, 6, [6, 7]))

    dl1.addAppointment((2, 1, [1, 2]))
    dl1.addAppointment((3, 1, [1, 2, 3]))
    dl1.delete((2, dl1.calendar[2][0]))
    participants = [1, 2, 3]
    for p in participants:
        NP, matrix, nodeid = dl1.sendMessage(p)

        for e in NP:
            print(e)
        print(matrix)
        print("-------------------------------")

    dl2.receiveMessage(dl1.sendMessage(2))
    dl3.receiveMessage(dl1.sendMessage(3))

    for e in dl2.calendar:
        for a in dl2.calendar[e]:
            print(a)

    print("++++++++++++++++++++++++++++")

    for e in dl3.calendar:
        for a in dl3.calendar[e]:
            print(a)

    print("+++++++++++++++++++++++++++")

    # test case for deletion after we updated the node 2
    dl1.delete((3, dl1.calendar[3][0]))
    dl1.deleteValue((1, dl1.calendar[1][0]))
    dl2.receiveMessage(dl1.sendMessage(2))
    for e in dl2.calendar:
        for a in dl2.calendar[e]:
            print(a)

    print("++++++++++++++++++++++++++++")


def testCancelAppointments():
    dl1 = DistributedDict(65555, 1, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)
    dl2 = DistributedDict(65555, 2, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)
    dl3 = DistributedDict(65555, 3, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)

    dl1.addAppointment((1, 1, [1, 2, 3]))
    dl1.addAppointment((1, 4, [4, 5]))
    dl1.addAppointment((1, 6, [6, 7]))

    dl1.addAppointment((2, 1, [1, 2]))
    dl1.addAppointment((3, 1, [1, 2, 3]))

    dl2.receiveMessage(dl1.sendMessage(2))
    dl3.receiveMessage(dl1.sendMessage(3))

    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    for e in dl1.calendar:
        for a in dl1.calendar[e]:
            print(a)

    print("++++++++++++++++++++++++++++")

    dl1.cancelAppointment((2, dl1.calendar[2][0]))
    dl1.cancelAppointment((1, dl1.calendar[1][1]))

    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    for e in dl1.calendar:
        for a in dl1.calendar[e]:
            print(a)

    print("++++++++++++++++++++++++++++")


def testConflictsInaTimeSlot():
    dl1 = DistributedDict(65555, 1, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)
    dl2 = DistributedDict(65555, 2, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)

    # Add several appnts to dl1
    dl1.addAppointment((1, 1, [1, 2, 3]))
    dl1.addAppointment((1, 4, [4, 5]))
    dl1.addAppointment((1, 6, [6, 7]))
    dl1.addAppointment((2, 1, [1, 2, 3]))

    print("+++++++++++++++++")
    # add several appts to  dl2 that could conflict with the dl1 appts
    dl2.addAppointment((1, 2, [2, 3]))
    dl2.addAppointment((1, 4, [4, 6]))

    print("+++++++++++++++++")
    # let dl2 receive message from dl1
    dl2.receiveMessage(dl1.sendMessage(2))

    for e in dl2.calendar:
        for a in dl2.calendar[e]:
            print(a)

    print("+++++++++++++++++")

    # check conflicts after updating the calendar
    dl2.checkConflictsInaTimeSlot(1, dl2.calendar[1])
    conflicts = dl2.checkConflictingAppnmts()
    for c in conflicts:
        print(c)

    print("+++++++++++++++++")

    print("+++++++++++++++++")
    for e in dl2.calendar:
        for a in dl2.calendar[e]:
            print(a)

    print("+++++++++++++++++")
    # cancel the conflicting apptmnts
    for c in conflicts:
        dl2.cancelAppointment((c.timeslot, c))

    print("+++++++++++++++++")
    for e in dl2.calendar:
        for a in dl2.calendar[e]:
            print(a)

    print("+++++++++++++++++")
    for e in dl1.calendar:
        for a in dl1.calendar[e]:
            print(a)

    # snd msg to dl1
    dl1.receiveMessage(dl2.sendMessage(1))

    print("+++++++++++++++++")
    for e in dl1.calendar:
        for a in dl1.calendar[e]:
            print(a)


def testAddAppointmentWithConflicts():
    dl1 = DistributedDict(65555, 1, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)
    dl2 = DistributedDict(65555, 2, {"1": 65556, "2": 56555, "3": 51236}, host="127.0.0.1", eventClass=None)

    # Add several appnts to dl1
    dl1.addAppointment((1, 1, [1, 2, 3]))
    dl1.addAppointment((1, 4, [4, 5]))
    dl1.addAppointment((1, 6, [6, 7]))
    dl1.addAppointment((2, 1, [1, 2, 3]))

    print("+++++++++++++++++")
    # add several appts to  dl2 that could conflict with the dl1 appts
    dl2.addAppointment((1, 2, [2, 3]))
    dl2.addAppointment((1, 4, [4, 6]))
    dl2.displayCalendar()
    dl1.displayCalendar()


if __name__ == '__main__':
    main()
