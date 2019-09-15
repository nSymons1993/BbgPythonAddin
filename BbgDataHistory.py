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



class BbgDataHistory(BbgRefDataService):
    def __init__(self, fields, securities, startDate, endDate, perAdjustment = "ACTUAL", perSelection = "MONTHLY", overrides = None):
        self.fields = fields
        self.securities = securities
        self.startDate = startDate
        self.endDate = endDate
        self.perAdjustment = perAdjustment
        self.perSelection = perSelection
        self.overrides = overrides

    def constructDf(self):
        BbgRefDataService.__init__(self)
        self.request = self.createRequest(securities = self.securities, fields = self.fields, requestType = "HistoricalDataRequest")
        self.appendRequestOverrides(request = self.request, overrides = self.overrides)
        self.appendHistoricalOverrides(request = self.request, startDate = self.startDate, endDate = self.endDate, perAdjustment = self.perAdjustment, perSelection = self.perSelection)
        self.cid = self.session.sendRequest(self.request)
        self.bbgRefData = pd.DataFrame()

        for response in self.parseResponse(self.cid):
            self.bbgRefData = self.bbgRefData.append(self.refDataContentToDf(response))
        
        bbgRefData = bbgRefData.set_index('Security', append = True).pivot(columns='Field').unstack('Security')
        bbgRefData.columns = bbgRefData.columns.droplevel(0).swaplevel()
        
        return self.bbgRefData

    def appendHistoricalOverrides(self, request, startDate, endDate, perAdjustment, perSelection):
        request.set("periodicityAdjustment", perAdjustment)
        request.set("periodicitySelection", perSelection)
        request.set("startDate", startDate)
        request.set("endDate", endDate)

        return request

    def refDataContentToDf(self, response):
        securityData = response['content']['HistoricalDataResponse']['securityData']
        historicalData = securityData['fieldData']
        returnDf = pd.DataFrame()
        for snapShot in historicalData:
            fieldData = snapShot['fieldData']
            returnDf = returnDf.append(pd.DataFrame(fieldData.items(), columns=['Field', 'Values'], index = [fieldData['date'] for i in range(0, len(fieldData))])[1:])
        returnDf.index.names = ['Date']
        returnDf['Security'] = securityData['security']
        return returnDf