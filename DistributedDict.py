import datetime
import logging
import pickle
import socket
from threading import Lock

import numpy as np

from DistributedLog import Event, Operation


class DistributedDict:
    def __init__(self, clientport, nodeid, nodemap, host="127.0.0.1", eventClass=None):
        self.HOST = host
        self.port = clientport
        self.nodeid = nodeid
        self.map = nodemap
        self.calendar = {}
        self.mutex = Lock()
        self.noOfNodes = len(nodemap)
        self.events = set()
        self.matrix = np.zeros((self.noOfNodes, self.noOfNodes), dtype=np.int32)
        if eventClass is None:
            self.eventClass = Event
        else:
            self.eventClass = eventClass
        self.dlogfileName = "Node-{0}-dlog.txt".format(self.nodeid)
        self.ddfileName = "Node-{0}-dclog.txt".format(self.nodeid)

        self.initializeLogFiles()

    # Low level methods
    def insert(self, x):
        # x should be a key value pair
        self.mutex.acquire()
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(self.nodeid, (nodeid, lamptime))
            event._op = Operation.INSERT
            event._m = x
            self.events.add(event)
            self.appendToLog(self.dlogfileName, str(event))

            # update the appointment with the timestamp
            x[1].ts = (lamptime, nodeid)

            self.calendar[x[0]] = [x[1]]
            # log
            self.appendToLog(self.ddfileName, self.displayCalendarstr(), "w+")
        finally:
            self.mutex.release()
        pass

    def appendValue(self, x):
        # x should be a key value pair
        self.mutex.acquire()
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(self.nodeid, (nodeid, lamptime))
            event._op = Operation.INSERT
            event._m = x
            self.events.add(event)
            # log
            self.appendToLog(self.dlogfileName, str(event))

            # update the appointment with the timestamp
            x[1].ts = (lamptime, nodeid)

            # appended the value instead inserting
            self.calendar[x[0]].append(x[1])
            # log
            self.appendToLog(self.ddfileName, self.displayCalendarstr(), "w+")
        finally:
            self.mutex.release()

    def delete(self, x):
        self.mutex.acquire()
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(self.nodeid, (nodeid, lamptime))
            event._op = Operation.DELETE
            event._m = x
            self.events.add(event)
            # log
            self.appendToLog(self.dlogfileName, str(event))

            r = self.calendar.pop(x[0], None)
            # print(r)
            # log
            self.appendToLog(self.ddfileName, self.displayCalendarstr(), "w+")
            pass
        finally:
            self.mutex.release()

    def deleteValue(self, x):
        self.mutex.acquire()
        try:
            nodeid, lamptime = self.getNewLamportTimestamp()
            event = Event(self.nodeid, (nodeid, lamptime))
            event._op = Operation.DELETE
            event._m = x
            self.events.add(event)
            # log
            self.appendToLog(self.dlogfileName, str(event))

            r = self.calendar[x[0]].remove(x[1])
            # print(r)
            # log
            self.appendToLog(self.ddfileName, self.displayCalendarstr(), "w+")
            pass
        finally:
            self.mutex.release()

    def sendMessage(self, k):
        NP = self.calculatePartialLog(k)
        return NP, self.matrix, self.nodeid

    def receiveMessage(self, m):
        # let m be <NP_k, T_k>
        pl = m[0]
        matrix = m[1]
        k = m[2]
        NE = self.calculateNE(pl)

        tempCreateKeyList = {}
        tempDeleteKeyLsit = {}
        for e in NE:
            if e._op == Operation.INSERT:
                if e._m[0] in tempCreateKeyList:
                    tempCreateKeyList[e._m[0]].append(e._m[1])
                else:
                    tempCreateKeyList[e._m[0]] = [e._m[1]]
            elif e._op == Operation.DELETE:
                if e._m[0] in tempDeleteKeyLsit:
                    tempDeleteKeyLsit[e._m[0]].append(e._m[1])
                else:
                    tempDeleteKeyLsit[e._m[0]] = [e._m[1]]

        # go through the events again one by one
        for e in NE:
            if e._op == Operation.INSERT:
                if e._m[1] not in (tempDeleteKeyLsit[e._m[0]] if e._m[
                                                                     0] in tempDeleteKeyLsit else []):  # checking insert appointment is not in delete list
                    if e._m[0] in self.calendar:
                        self.calendar[e._m[0]].append(e._m[1])
                    else:
                        self.calendar[e._m[0]] = [e._m[1]]
                else:
                    print(
                        "Appointment {0} is in create and delete list, therefore it will not be added to the dictionary".format(
                            e._m[1]))
                    logging.debug(
                        "Appointment {0} is in create and delete list, therefore it will not be added to the dictionary".format(
                            e._m[1]))
            elif e._op == Operation.DELETE:
                if e._m[1] not in (tempCreateKeyList[e._m[0]] if e._m[0] in tempCreateKeyList else []):
                    # we need to delete this value from the dictionary
                    if e._m[0] in self.calendar:
                        el = next((x for x in self.calendar[e._m[0]] if x.ts == e._m[1].ts), None)
                        # we have to do this since when pickling and unpickling hash changes unless we override the hash function
                        if el is not None:
                            self.calendar[e._m[0]].remove(el)
                        else:
                            print("Couldn't find the appointment to delete")
                        # make sure to write logic to remove the key if list is empty after this
                        if not self.calendar[e._m[0]]:
                            # print("No values for key {0} in the dictionary. Removing the key".format(e._m[0]))
                            logging.debug("No values for key {0} in the dictionary. Removing the key".format(e._m[0]))
                            self.calendar.pop(e._m[0])
                    else:
                        logging.debug("Delete key not in calendar Error {0}".format(e._m[0]))

        # for ck in tempCreateKeyList:
        #     if ck not in tempDeleteKeyLsit:
        #         if ck in self.calendar:
        #             self.calendar[ck].append(tempCreateKeyList[ck]._m[1])
        #         else:
        #             self.calendar[ck] = [tempCreateKeyList[ck]._m[1]]
        #     else:
        #         deleteevents = tempDeleteKeyLsit[ck]
        #
        # for dk in tempDeleteKeyLsit:
        #     if dk not in tempCreateKeyList:
        #         if dk not in self.calendar:
        #             print("Delete key not in calandar Error {0}".format(dk))
        #         self.calendar.pop(dk, None)

        # update the dictionary log
        self.appendToLog(self.ddfileName, self.displayCalendarstr(), "w+")

        # merge the partial logs
        self.updateMatrixFromReceivedMatrix(matrix, k)

        # self.unionevents(NE)
        copyEvents = self.events.copy()
        copyEvents.update(NE)
        for ev in copyEvents.copy():
            needArecord = False
            for j in range(1, self.noOfNodes + 1):
                if not self.hasRecord(self.matrix, ev, j):
                    needArecord = True
            if needArecord:  # if record is needed and in NE, then we need to add to events and log it
                if ev in NE:
                    self.events.add(ev)
                    self.appendToLog(self.dlogfileName, str(ev))
                    pass
            else:
                if ev in self.events:  # record is not needed but is in events then remove it from the events
                    self.events.remove(ev)
            # else:
            #     logging.debug("Record not needed and not in events")

    def unionevents(self, pl):
        for e in pl:
            if e not in self.events:
                # add to the events
                self.events.add(e)
                # log the event
                self.appendToLog(self.dlogfileName, str(e))

    def updateMatrixFromReceivedMatrix(self, receivedMatrix, k):
        i = self.nodeid
        size = self.noOfNodes
        for j in range(size):
            self.matrix[i - 1][j] = max(receivedMatrix[k - 1][j], self.matrix[i - 1][j])

        for m in range(size):
            for n in range(size):
                self.matrix[m][n] = max(receivedMatrix[m][n], self.matrix[m][n])

    def calculateNE(self, pl):
        NE = []
        for e in pl:
            if not self.hasRecord(self.matrix, e, self.nodeid):
                NE.append(e)
        return NE

    def calculatePartialLog(self, k):
        pl = []
        for e in self.events:
            if not self.hasRecord(self.matrix, e, k):
                pl.append(e)
        return pl

    def hasRecord(self, matrix, eR, k):
        return matrix[k - 1][eR.nodeId - 1] >= eR.ts[1]

    def getNewLamportTimestamp(self):
        self.matrix[self.nodeid - 1][self.nodeid - 1] = max(self.matrix[self.nodeid - 1]) + 1
        return self.getLamportTime()

    def getLamportTime(self):
        return self.nodeid, self.matrix[self.nodeid - 1][self.nodeid - 1]

    def initializeLogFiles(self):
        with open(self.dlogfileName, "w+") as dl:
            dl.write("This is the beginning of the Distributed log of node {0}\n".format(self.nodeid))

        with open(self.ddfileName, "w+") as dc:
            dc.write("This is the beginning of the Distributed Calendar log of node {0}\n".format(self.nodeid))

    # High level methods
    def displayCalendarstr(self):
        weekDays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
        keys = sorted(self.calendar.keys())
        appendstr = ""
        for k in keys:
            appendstr = appendstr + "Day:Timeslot: {0}:{1} -> ".format(weekDays[k - 1], k)
            for a in self.calendar[k]:
                appendstr = appendstr + str(a) + "\t||\t"
            appendstr = appendstr + "\n"
        return appendstr

    def addAppointment(self, message):
        # Todo: implement the add appointment logic here
        # create the appointment

        timeslot = message[0]
        scheduler = message[1]
        participants = message[2]
        name = message[3]
        startt = (message[4])
        endt = (message[5])

        appnmnt = Appointment()
        appnmnt.timeslot = timeslot
        appnmnt.scheduler = scheduler
        appnmnt.participants = participants
        appnmnt.name = name
        appnmnt.starttime = appnmnt.starttime.replace(hour=startt[0], minute=startt[1])
        appnmnt.endtime = appnmnt.starttime.replace(hour=endt[0], minute=endt[1])

        # check whether scheduler is a participant
        if not (scheduler in participants):
            print("Scheduler is not a participant. Scheduler must be a participant")
            logging.debug("Scheduler is not a participant. Scheduler must be a participant")
            return False

        # check for internal conflicts

        if self.isInternalConflicts(timeslot, scheduler, participants, self.calendar, appnmnt.starttime,
                                    appnmnt.endtime):
            print("Appointment conflict for timeslot {0} scheduler {1} participants {2} ".format(timeslot, scheduler,
                                                                                                 participants))
            logging.debug(
                "Appointment conflict for timeslot {0} scheduler {1} participants {2} ".format(timeslot, scheduler,
                                                                                               participants))
            return False
        else:
            print("No Internal conflict detected. Moving on to do the schedule the appointment")
            logging.debug("No Internal conflict detected. Moving on to do the schedule the appointment")
            # if timeslot is not used then we need to insert the timeslot and send messages
            if timeslot not in self.calendar:
                print('Timeslot is not used in the local calendar')
                self.insert((timeslot, appnmnt))
            else:
                print("Timeslot already exist. Moving on to adding non conflicting appointment to the local calendar")
                logging.debug(
                    "Timeslot already exist. Moving on to adding non conflicting appointment to the local calendar")
                self.appendValue((timeslot, appnmnt))

            tempParticipants = participants.copy()
            tempParticipants.remove(self.nodeid)

            # for each participant send the partial log
            for p in tempParticipants:
                (NP, mtx, nid) = self.sendMessage(p)
                self.sendViaSocket((NP, mtx, nid), p)
            return True

    def cancelAppointment(self, message):
        timeslot = message[0]
        appnmnt = message[1]

        scheduler = appnmnt.scheduler
        canceler = self.nodeid
        participants = appnmnt.participants
        # check whether canceler is a participant
        if not (canceler in participants):
            print("Canceler is not an participant of the appointment")
            logging.debug("Canceler is not an participant of the appointment")
            return False
        else:
            # check if it is delete or deletevalue
            if timeslot not in self.calendar:
                print("Error! trying to delete appointment at a timeslot that doesn't exist")
                logging.debug("Error! trying to delete appointment at a timeslot that doesn't exist")
                return False
            else:
                if len(self.calendar[timeslot]) > 1:
                    self.deleteValue((timeslot, appnmnt))
                elif len(self.calendar[timeslot]) == 1:
                    self.delete((timeslot, appnmnt))

                tempParticipants = participants.copy()
                tempParticipants.remove(self.nodeid)

                # for each participant send the partial log
                for p in tempParticipants:
                    (NP, mtx, nid) = self.sendMessage(p)
                    self.sendViaSocket((NP, mtx, nid), p)

                return True
        pass

    def updateDict(self, partial_log):
        pass

    def sendViaSocket(self, m, p):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((self.map[p][0], int(self.map[p][1])))

                strReq = {}
                strReq['pl'] = m[0]
                strReq['mtx'] = m[1]
                strReq['nodeid'] = m[2]
                # the_encoding = chardet.detect(pickle.dumps(self.events))['encoding']
                # print(the_encoding)
                # strReq['encoding'] = the_encoding
                strReq['msg'] = self.events

                pickledMessage = pickle.dumps(strReq)
                s.sendall(pickledMessage)
            except ConnectionRefusedError:
                print("Connection cannot be established to node {0}".format(p))
                logging.error("Connection cannot be established to node {0}".format(p))

    def isInternalConflicts(self, timeslot, scheduler, participants, calendar, st, et):
        if timeslot not in self.calendar:
            return False
        else:
            appnmnts = self.calendar[timeslot]
            for a in appnmnts:
                for p in participants:
                    if a.isParticipant(p) and a.checkappttimeconflicts(st, et):
                        return True
        return False

    def checkConflictingAppnmts(self):
        conflicts = []
        for k, v in self.calendar.items():
            conflicts.extend(self.checkConflictsInaTimeSlot(k, v))
        return conflicts

    def checkConflictsInaTimeSlot(self, k, appnmts):
        apps = appnmts.copy()
        apps.sort(key=lambda x: x.ts)
        conflictingAppointments = []
        for i in range(len(apps)):
            # print(apps[i].participants)
            if apps[i] in conflictingAppointments:
                continue
            for j in range(i + 1, len(apps)):
                # print(apps[j].participants)
                if apps[j] not in conflictingAppointments:
                    if any(item in apps[i].participants for item in apps[j].participants) and apps[
                        i].checkappttimeconflicts(apps[j].starttime, apps[j].endtime):
                        conflictingAppointments.append(apps[j])

        # for c in conflictingAppointments:
        #     print(c)
        return conflictingAppointments

    def appendToLog(self, logname, line, format="a+"):
        with open(logname, format) as f:
            try:
                f.write(line + "\n")
            except:
                print("Error writing to the logfile {0} line {1}".format(logname, line))
                logging.error("Error writing to the logfile {0} line {1}".format(logname, line))

    def displayCalendar(self):
        weekDays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
        print(
            "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print("Displaying the calendar of Node {0}".format(self.nodeid))
        keys = sorted(self.calendar.keys())
        for k in keys:
            print("Day:Timeslot: {0}:{1} -> ".format(weekDays[k - 1], k), end='')
            for a in self.calendar[k]:
                print(str(a) + "\t||\t", end='')
            print("")

        print(
            "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


class Appointment:
    def __init__(self):
        self.timeslot = None
        self.scheduler = None
        self.participants = []
        self.ts = None
        self.name = ""
        self.dateoftheappointment = None
        self.starttime = datetime.time(0, 0, 0)
        self.endtime = datetime.time(0, 0, 0)

    def isParticipant(self, participant):
        return participant in self.participants

    def isScheduler(self, schedulerperson):
        return self.scheduler == schedulerperson

    def calculatedatestartandend(self):
        weekDays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
        day = self.timeslot - 1
        self.dateoftheappointment = weekDays[day]

    def checkappttimeconflicts(self, st, et):
        if (self.starttime <= st <= self.endtime) or (self.starttime <= et <= self.endtime):
            print("Appointment time conflict (intersection) detected")
            logging.debug("Appointment time conflict (intersection) detected")
            return True
        elif (st < self.starttime) and (self.endtime < et):
            print("Appointment time conflict (overlap) detected")
            logging.debug("Appointment time conflict (overlap) detected")
            return True
        return False

    def __str__(self):
        self.calculatedatestartandend()
        return "Timeslot: {0} scheduled by: {1}  Name: {2} participants {3}, Day-Time: {4} at {5} to {6}".format(
            self.timeslot,
            self.scheduler,
            self.name,
            self.participants,
            self.dateoftheappointment,
            self.starttime, self.endtime)
