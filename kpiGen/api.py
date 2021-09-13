
import numpy as np
import pandas as pd
import re
from datetime import datetime as dt
import xlrd
import datetime

import json

class DataExtractor:

    MISSEDCALLNAMES = ["Missed", "Voicemail"]
    INCOMINGCALLNAMES = ["Accepted"]
    INCOMINGCALLNAMES.extend(MISSEDCALLNAMES)
    OUTGOINGCALLNAMES = ["Call connected", "Hang Up", "No Answer"]

    INTERNALCALLNAMES = [
            "ABEL SANCHEZ",
            "ASIA THOMAS",
            "JOE BRAGA",
            "LOU CANCINO",
            "MATTHEW RASCO",
            "MIKE CARLITO",
            "MURPHY LIMON",
            "SCOTT STOLTENKAMP",
            "TENISHA OLIVER"]

    NEEDEDCOLUMNS = ['Phone Number', 'Name', 'Date', 'Time', 'Action Result', 'Duration']

    DATAFRAMENAMES = ['Number', 'Name', "Datetime", "Result", 'Duration']

    XMLNAMES = ['number', 'time', 'type', 'name', 'dur']

    INDIVIDUALCOLUMNNAMES = ['Name',
                   'Total Calls',
                   'Missed Calls',
                   'Missed Percentage',
                   'Callbacks',
                   'Missed While Ongoing',
                   'Total Duration(Hours)',
                   'Incoming Duration(Hours)',
                   'Outgoing Duration(Hours)',
                   'Unique Missed',
                   'Unique Callbacks',
                   'Unique Callback Percentage',
                   'Internal Calls',
                   'Internal Calls Duration(Hours)']

    DEPARTMENTCOLUMNNAMES = ['Department Name',
                   'Unique Missed',
                   'Unique Callbacks']

    def __init__(self, user_set, department_set, timeTableInstance):
        self.timeTableExcelInstance = None

        #if bool(timeTableInstance.timeTable):
        #    self.timeTableExcelInstance = xlrd.open_workbook(timeTableInstance.timeTable.path, on_demand=True).sheet_by_name('Details')

        self.user_set = user_set
        self.department_set = department_set
        self.nameList = [user.name for user in user_set]
        self.departmentNameList = [department.name for department in department_set]
        self.departmentNumberList = [department.lineNumber for department in department_set]

        self.individualDF = []
        self.departmentDF = []
        self.dataframeSet = []

        self.individualDict = {}
        self.departmentDict = {}
        self.updateCallData()


    def updateCallData(self):
        for user in self.user_set.all():
            self.individualDF += [self.updateUserCallData(user)]

        self.individualDict = dict(zip(self.nameList, self.individualDF))
        for department in self.department_set.all():
            self.departmentDF += [self.updateDepartmentCallData(department)]
        self.departmentDict = dict(zip(self.departmentNameList, self.departmentDF))

    def updateUserCallData(self, user):
        totalData = pd.DataFrame(columns=self.DATAFRAMENAMES)

        try:
            phoneDataSheet = pd.read_pickle("./kpiGen/uploads/pickles/" + str(user.name) + "/phone.pkl")
            totalData = totalData.append(phoneDataSheet, ignore_index=True)
        except:
            pass

        try:
            rcDatasheet = pd.read_pickle("./kpiGen/uploads/pickles/" + str(user.name) + "/rc.pkl")
            totalData = totalData.append(rcDatasheet, ignore_index=True)
        except:
            pass

        indexList = []
        for index, data in enumerate(totalData["Datetime"]):
            if not self.ifInWorkHours(data, self.getTimeTables(user.name)):
                indexList += [index]

        totalData = totalData.drop(i for i in indexList)

        totalData = totalData[totalData.Result != -1]

        totalData = totalData.sort_values('Datetime', ascending=True, ignore_index=True)
        pd.set_option("display.max_rows", None, "display.max_columns", None, 'display.expand_frame_repr', False)

        return totalData

    def getTimeTables(self, name):
        timeTable = []

        #timeTableSheet = pd.read_excel
        #timeTableSheet = pd.read_excel( timeTableModel.timeTable.open(), sheet_name=0).copy(deep=True)
        if (bool(self.timeTableExcelInstance)):
            row = -1
            for rowIndex in range(self.timeTableExcelInstance.nrows):

                if name.lower() in self.timeTableExcelInstance.cell_value(rowIndex, 0).lower():
                    row = rowIndex

            if row == -1:
                return []

            for columnIndex in range(1, self.timeTableExcelInstance.ncols, 3):
                date = self.timeTableExcelInstance.cell_value(1, columnIndex)
                time = []
                try:
                    if self.timeTableExcelInstance.cell_type(row, columnIndex + 0) != xlrd.XL_CELL_EMPTY:
                        time += [date + ' ' + self.timeTableExcelInstance.cell_value(row, columnIndex + 0)]
                except:
                    pass

                try:
                    if self.timeTableExcelInstance.cell_type(row, columnIndex + 1) != xlrd.XL_CELL_EMPTY:
                        value = self.timeTableExcelInstance.cell_value(row, columnIndex + 1)
                        index = value.find('-')
                        time += [date + ' ' + value[0:index]]
                        time += [date + ' ' + value[index + 1:]]

                    if self.timeTableExcelInstance.cell_type(row, columnIndex + 2) != xlrd.XL_CELL_EMPTY:
                        time += [date + ' ' + self.timeTableExcelInstance.cell_value(row, columnIndex + 2)]
                except:
                    pass

                try:
                    for data in time:
                        timeTable += [dt.strptime(data, '%a, %d %b %Y %I:%M %p')]
                except:
                    pass

        return timeTable

    def updateDepartmentCallData(self, department):
        userList = [user.name for user in department.users.all()]

        totalData = pd.DataFrame(columns=self.DATAFRAMENAMES)
        for user in userList:
            totalData = totalData.append(self.individualDict.get(str(user)), ignore_index=True)

        totalData = totalData.sort_values('Datetime', ascending=True, ignore_index=True)

        return totalData

    def getTotalCalls(self):
        return [dataSet.shape[0] for dataSet in self.individualDF]

    def getMissedCalls(self):
        missedCallSet = []
        for dataSet in self.individualDF:
            missedCalls = dataSet['Result'].value_counts().get(3)
            if missedCalls is None:
                missedCallSet += [0]
            else:
                missedCallSet += [missedCalls]

        return missedCallSet

    def getTotalDur(self):
        totalDurSet = []
        for dataSet in self.individualDF:
            duration = 0
            for value in dataSet['Duration']:
                duration += value
            totalDurSet += [round(duration/3600, 2)]
        return totalDurSet

    def getIncomingDur(self):
        totalDurSet = []
        for dataSet in self.individualDF:
            duration = 0
            for currentResult, currentDur in zip(dataSet['Result'], dataSet['Duration']):
                if currentResult == 1:
                    duration += currentDur
            totalDurSet += [round(duration/3600, 2)]
        return totalDurSet

    def getOutgoingDur(self):
        totalDurSet = []
        for dataSet in self.individualDF:
            duration = 0
            for currentResult, currentDur in zip(dataSet['Result'], dataSet['Duration']):
                if currentResult == 2:
                    duration += currentDur
            totalDurSet += [round(duration/3600, 2)]
        return totalDurSet

    def getMissedPercentage(self):
        missedPercentageSet = []
        for missedCalls, totalCalls in zip(self.getMissedCalls(), self.getTotalCalls()):
            try:
                missedPercentageSet += ["{:.2%}".format((missedCalls / totalCalls))]
            except:
                missedPercentageSet += ['0.00%']

        return missedPercentageSet

    def getCallbackPercentage(self):
        missedPercentageSet = []
        for missedCalls, totalCalls in zip(self.getUniqueCallbacks(), self.getUniqueMissedCalls()):
            try:
                missedPercentageSet += ["{:.2%}".format((missedCalls / totalCalls))]
            except:
                missedPercentageSet += ['0.00%']

        return missedPercentageSet

    def getCallsWhileOngoing(self):
        numberOfMissedWhileOngoingSet = []
        for dataSet in self.individualDF:
            numberOfMissedWhileOngoing = 0
            for currentTime, currentDur in zip(dataSet['Datetime'], dataSet['Duration']):

                if currentDur > 0:
                    startTime = currentTime
                    endTime = currentTime + datetime.timedelta(seconds=currentDur)

                if currentDur == 0:
                    try:
                        if startTime < currentTime < endTime:
                            numberOfMissedWhileOngoing += 1
                    except:
                        pass
            numberOfMissedWhileOngoingSet += [numberOfMissedWhileOngoing]

        return numberOfMissedWhileOngoingSet

    def getCallbacks(self):
        numberOfCallbacksSet = []

        for dataSet in self.individualDF:
            missedCallSet = []
            numberOfCallbacks = 0
            for currentNumber, currentResult in zip(dataSet['Number'], dataSet['Result']):
                if currentResult == 3:
                    missedCallSet += [currentNumber]
                elif currentResult == 2:
                    if currentNumber in missedCallSet:
                        numberOfCallbacks += 1
            numberOfCallbacksSet += [numberOfCallbacks]
        return numberOfCallbacksSet

    def getUniqueMissedCalls(self):
        numberOfUniqueMissedCallSet = []

        for dataSet in self.individualDF:
            missedCallSet = []
            callBacksSet = []
            numberToRemove = 0
            for currentNumber, currentResult in zip(dataSet['Number'], dataSet['Result']):
                if currentResult == 3:
                    missedCallSet += [currentNumber]

                elif currentResult == 2:
                    if currentNumber in missedCallSet:
                        callBacksSet += [currentNumber]
                elif currentResult == 1:
                    if currentNumber in missedCallSet and currentNumber not in callBacksSet:
                        numberToRemove += 1

            numberOfUniqueMissedCallSet += [len(set(missedCallSet)) - numberToRemove]
        return numberOfUniqueMissedCallSet

    def getUniqueCallbacks(self):
        numberOfUniqueCallBackSet = []

        for dataSet in self.individualDF:
            missedCallSet = []
            callBacksSet = []
            numberToRemove = 0
            for currentNumber, currentResult in zip(dataSet['Number'], dataSet['Result']):
                if currentResult == 3:
                    missedCallSet += [currentNumber]

                elif currentResult == 2:
                    if currentNumber in missedCallSet:
                        callBacksSet += [currentNumber]
                elif currentResult == 1:
                    if currentNumber in missedCallSet and currentNumber not in callBacksSet:
                        try:
                            while True:
                                missedCallSet.remove(currentNumber)
                        except ValueError:
                            pass

            numberOfUniqueCallBackSet += [len(set(callBacksSet))]
        return numberOfUniqueCallBackSet

    def getInternalCalls(self):
        numberOfInternalCallsSet = []

        for dataSet in self.individualDF:
            numberOfInternalCalls = 0
            for currentName in dataSet['Name']:
                for name in self.INTERNALCALLNAMES:
                    try:
                        if name.lower() in currentName.lower():
                             numberOfInternalCalls += 1
                    except:
                        pass
            numberOfInternalCallsSet += [numberOfInternalCalls]
        return numberOfInternalCallsSet

    def getUniqueDepartmentMissedCalls(self):
        numberOfUniqueMissedSet = []
        
        for dataSet, departmentNumber in zip(self.departmentDF, self.departmentNumberList):
            missedCallSet = []
            callBacksSet = []
            numberToRemove = 0
            for currentNumber, currentResult in zip(dataSet['Number'], dataSet['Result']):
                for currentNumber, currentResult, currentName in zip(dataSet['Number'], dataSet['Result'], dataSet['Name']):
                    try:
                        if currentResult == 3 and ('Line ' + str(departmentNumber)) in currentName:
                            missedCallSet += [currentNumber]
                        elif currentResult == 2:
                            if currentNumber in missedCallSet:
                                callBacksSet += [currentNumber]
                        elif currentResult == 1:
                            if currentNumber in missedCallSet and currentNumber not in callBacksSet:
                                numberToRemove += 1    
                    except:
                        pass

               
            numberOfUniqueMissedSet += [len(set(missedCallSet))]


        return numberOfUniqueMissedSet

    def getUniqueDepartmentCallbacks(self):
        numberOfUniqueCallbacksSet = []
        for dataSet, departmentNumber in zip(self.departmentDF, self.departmentNumberList):
            missedCallSet = []
            callBacksSet = []

            for currentNumber, currentResult, currentName in zip(dataSet['Number'], dataSet['Result'], dataSet['Name']):
                if currentResult == 3 and (currentName == currentName and ('Line ' + str(departmentNumber)) in currentName):
                    missedCallSet += [currentNumber]
                elif currentResult == 2:
                    if currentNumber in missedCallSet:
                        callBacksSet += [currentNumber]
                elif currentResult == 1:
                    if currentNumber in missedCallSet and currentNumber not in callBacksSet:
                        try:
                            while True:
                                missedCallSet.remove(currentNumber)
                        except ValueError:
                            pass
                        
            numberOfUniqueCallbacksSet += [len(set(callBacksSet))]
        return numberOfUniqueCallbacksSet


    def getInternalCallsDur(self):
        numberOfInternalCallsSet = []


        for dataSet in self.individualDF:
            internalDuration = 0
            for currentName, duration in zip(dataSet['Name'], dataSet['Duration']):
                for name in self.INTERNALCALLNAMES:
                    try:
                        if name.lower() in currentName.lower():
                            internalDuration += duration
                    except:
                        pass
            numberOfInternalCallsSet += [round(internalDuration/3600, 2)]
        return numberOfInternalCallsSet

    def getIndividualAllData(self):
        return pd.DataFrame(np.column_stack([self.nameList,
                                             self.getTotalCalls(),
                                             self.getMissedCalls(),
                                             self.getMissedPercentage(),
                                             self.getCallbacks(),
                                             self.getCallsWhileOngoing(),
                                             self.getTotalDur(),
                                             self.getIncomingDur(),
                                             self.getOutgoingDur(),
                                             self.getUniqueMissedCalls(),
                                             self.getUniqueCallbacks(),
                                             self.getCallbackPercentage(),
                                             self.getInternalCalls(),
                                             self.getInternalCallsDur()]),
                            columns=self.INDIVIDUALCOLUMNNAMES)

    def getDepartmentAllData(self):
        return pd.DataFrame(np.column_stack([self.departmentNameList,
                                             self.getUniqueDepartmentMissedCalls(),
                                             self.getUniqueDepartmentCallbacks()]),
                            columns=[self.DEPARTMENTCOLUMNNAMES])

    def getDepartmentList(self):
        return self.getDepartmentAllData().values.tolist()

    def getIndividualList(self):
        return self.getIndividualAllData().values.tolist()

    def getResult(self, result):
        if result == "Accepted":
            return 1
        if result in self.OUTGOINGCALLNAMES:
            return 2
        if result in self.MISSEDCALLNAMES:
            return 3

        return -1

    def ifInWorkHours(self, currentDate, dateTable):
        if len(dateTable) == 0:
            return True
        for start, end in zip(*[iter(dateTable)]*2):
            if start <= currentDate <= end:
                return True
        return False

    def convertToDuration(self, durationStr):
        duration = 0
        hoursInSecs = int(durationStr[0:1]) * 3600
        minInSecs = int(durationStr[2:4]) * 60
        secs = int(durationStr[5:7])
        duration += (hoursInSecs + minInSecs + secs)
        return duration