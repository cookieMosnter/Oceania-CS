from django.core.exceptions import ValidationError
from django.core.validators import FileExtensionValidator
from django.db import models
import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime as dt
import re

DATALABELS = ['Missed Call Rate']
PERCENTAGELABLES = ['Missed Call Rate']



# Create your models here.
class User(models.Model):
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
                             'Total Duration(Hours)',
                             'Incoming Duration(Hours)',
                             'Outgoing Duration(Hours)',
                             'Missed Call Percentage',
                             'Missed While Ongoing',
                             'Callbacks',
                             'Unique Missed',
                             'Unique Callbacks',
                             'Internal Calls',
                             'Internal Calls Duration(Hours)']

    DEPARTMENTCOLUMNNAMES = ['Department Name',
                             'Unique Missed',
                             'Unique Callbacks']

    name = models.CharField(max_length=30, default="")
    phoneLogFile = models.FileField(upload_to='kpiGen/uploads/individual/',
                                validators=[FileExtensionValidator(allowed_extensions=['xml'])],
                                blank=True)
    ringCentralFile = models.FileField(upload_to='kpiGen/uploads/individual/',
                                validators=[FileExtensionValidator(allowed_extensions=['csv'])],
                                blank=True)

    def xmlToDataframe(self):

        phoneLogData = pd.DataFrame(columns=self.XMLNAMES)

        if self.phoneLogFile:
            phoneLogXMLRoot = ET.parse(self.phoneLogFile.path).getroot()

            for element in phoneLogXMLRoot:
                if element.attrib.get('name') not in ['Scam Likely'] and element.attrib.get('type') in ['1', '2', '3']:
                    phoneLogData = phoneLogData.append(element.attrib, ignore_index=True)

            phoneLogData["time"] = phoneLogData.apply(lambda row: dt.strptime(row.time, '%b %d, %Y %I:%M:%S %p'), axis=1)
            phoneLogData["dur"] = phoneLogData["dur"].astype('int')
            phoneLogData["type"] = phoneLogData["type"].astype('int')
            phoneLogData["number"] = phoneLogData.apply(lambda row: re.sub('\\D+', "", str(row.number)),
                                                    axis=1)

        phoneLogData.rename(
            columns={"number": "Number", "time": "Datetime", "type": "Result", "name": "Name", "dur": "Duration"},
            inplace=True)
        phoneLogData = phoneLogData[self.DATAFRAMENAMES]
        phoneLogData.to_pickle("./kpiGen/uploads/pickles/" + str(self.name) + "/phone.pkl")

    def csvToDataframe(self):

        phoneLogData = pd.read_csv(self.ringCentralFile.open(), usecols=self.NEEDEDCOLUMNS).copy(deep=True)
        phoneLogData.columns = [c.replace(' ', '_') for c in phoneLogData.columns]
        phoneLogData["Datetime"] = phoneLogData.apply(
            lambda row: dt.strptime(row.Date + " " + row.Time, '%a %m/%d/%Y %I:%M %p'), axis=1)
        phoneLogData["Phone_Number"] = phoneLogData.apply(lambda row: re.sub('\\D+', "", str(row.Phone_Number)),
                                                          axis=1)

        for i, result in enumerate(phoneLogData["Action_Result"]):
            if result in ["In progress", "Wrong Number"]:
                phoneLogData.drop(i, inplace=True )

            if result in ["Hang Up", "No Answer"]:
                phoneLogData.at[i, 'Duration'] = '0:00:00'

        phoneLogData["Action_Result"] = phoneLogData.apply(lambda row: self.getResult(row.Action_Result), axis=1)
        phoneLogData["Duration"] = phoneLogData.apply(lambda row: self.convertToDuration(row.Duration), axis=1)

        for i, result in enumerate(phoneLogData["Action_Result"]):
            if result == 3:
                phoneLogData.at[i, 'Duration'] = 0

        phoneLogData.rename(columns={"Phone_Number": "Number", "Action_Result": "Result"}, inplace=True)
        phoneLogData = phoneLogData[self.DATAFRAMENAMES]
        phoneLogData.to_pickle("./kpiGen/uploads/pickles/" + str(self.name) + "/rc.pkl")

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
        for start, end in zip(*[iter(dateTable)] * 2):
            if start <= currentDate <= end:
                return True
        return False

    def convertToDuration(self, durationStr):
        try:
            duration = 0
            hoursInSecs = int(durationStr[0:1]) * 3600
            minInSecs = int(durationStr[2:4]) * 60
            secs = int(durationStr[5:7])
            duration += (hoursInSecs + minInSecs + secs)
            return duration
        except:
            return 0

    def save(self, *args, **kwargs):
        super(User, self).save(*args, **kwargs)
        import shutil
        dir = './kpiGen/uploads/pickles/' + str(self.name) + '/'
        if os.path.exists(dir):
            shutil.rmtree(dir)
        os.makedirs(dir)

        if self.phoneLogFile:
            self.xmlToDataframe()
        if self.ringCentralFile:
            self.csvToDataframe()


   #def save(self, *args, **kwargs):
   #    super(User, self).save(*args, **kwargs)
   #    for lable in DATALABELS:
   #        if self.data_set.all().count() < DATALABELS.__len__():
   #            self.data_set.create(data_text=lable, value=-1)

    def __str__(self):
        return self.name
class Department(models.Model):
    name = models.CharField(max_length=30, default="")
    lineNumber = models.IntegerField(default=-1)
    users = models.ManyToManyField(User)

    def __str__(self):
        return 'Line ' + str(self.lineNumber) + ' ' + str(self.name)

class TimeTable(models.Model):
    timeTable = models.FileField(upload_to='kpiGen/uploads/timeTable/',
                                   validators=[FileExtensionValidator(allowed_extensions=['xls'])],
                                   blank=True)

    def __str__(self):
        return 'Time Table'
    def save(self, *args, **kwargs):
        if not self.pk and TimeTable.objects.exists():
            # if you'll not check for self.pk
            # then error will also raised in update of exists model
            raise ValidationError('There is can be only one JuicerBaseSettings instance')
        return super(TimeTable, self).save(*args, **kwargs)