import datetime as dt
import blpapi
import logging
from BbgRefDataService import BbgRefDataService
import pandas as pd
import numpy as np
import BbgLogger

logger = BbgLogger.logger

SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")



class BbgIntradayTick(BbgRefDataService):
    def __init__(self, fields, securities, startTime, endTime, overrides = None):
        self.fields = list(fields) if type(fields) is not list else fields
        self.securities = list(securities) if type(securities) is not list else securities
        self.startTime = startTime
        self.endTime = endTime
        self.overrides = overrides

    def constructDf(self):
        BbgRefDataService.__init__(self)
        self.bbgRefData = pd.DataFrame()
        
        for sec in self.securities:
            self.request = self.createIntradayRequest(security = sec, requestType = "IntradayTickRequest", fields = self.fields,
                                                 startTime = self.startTime, endTime = self.endTime)
            self.cid = self.session.sendRequest(self.request)
            for response in self.parseResponse(self.cid, False):
                self.bbgRefData = self.bbgRefData.append(self.refDataContentToDf(response, sec))
        BbgRefDataService.__del__(self)
        return self.bbgRefData.set_index(['Security', 'time'])

    def appendHistoricalOverrides(self, request, startDate, endDate, perAdjustment, perSelection):
        request.set("periodicityAdjustment", perAdjustment)
        request.set("periodicitySelection", perSelection)
        request.set("startDate", startDate)
        request.set("endDate", endDate)

        return request

    def refDataContentToDf(self, response, security):
        securityData = response['content']['IntradayTickResponse']['tickData']
        tickData = securityData['tickData']
        returnDf = pd.DataFrame()
        for snapShot in tickData:
            fieldData = snapShot['tickData']
            returnDf = returnDf.append(pd.DataFrame(fieldData.items(), columns=['Field', 'Values']).set_index('Field').transpose().reset_index(drop=True))#, index = [fieldData['time'] for i in range(0, len(fieldData))])[1:])
        returnDf.index.names = ['time']
        returnDf['Security'] = security
        return returnDf