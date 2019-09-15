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
    def __init__(self):
        BbgRefDataService.__init__(self)

    def dataHistory(self, fields, securities, startDate, endDate, perAdjustment = "ACTUAL", perSelection = "MONTHLY", requestType = "HistoricalDataRequest"):
        self.request = self.createRequest(securities = securities, fields = fields, startDate = startDate, endDate = endDate, perAdjustment = perAdjustment, perSelection = perSelection, requestType = requestType)
        cid = self.session.sendRequest(self.request)

        outData = pd.DataFrame(data=None, columns = ['Security','Field','Value'])

        bbgRefData = pd.DataFrame()

        for response in self.parseResponse(cid):
            # Parse responses and append content to dataframe
            #tempDf = self.refDataContentToDf(response['content'])
            bbgRefData = bbgRefData.append(self.refDataContentToDf(response))
            #logger.info(response)
            #logger.info(self.refDataContentToDf(response['content']))
            #bbgRefData = bbgRefData.append(self.refDataContentToDf(response['content']))
        #return response
        return bbgRefData
    
    def createRequest(self, securities, fields, startDate, endDate, perAdjustment, perSelection, requestType):
        logger.info("Creating refdata request...")
        request = self.service.createRequest(requestType)

        if type(securities) is not list:
            list(securities)
        if type(fields) is not list:
            list(fields)

        for security in securities:
            request.append("securities", security)
        
        for field in fields:
            request.append("fields", field)

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
            returnDf = returnDf.append(pd.DataFrame(fieldData.items(), columns=['Fields', 'Values'], index = [fieldData['date'] for i in range(0, len(fieldData))])[1:])
        returnDf.index.columns = ''
        returnDf['securities'] = securityData['security']
        returnDf = returnDf.set_index('securities', append = True).pivot(columns='Fields')
        returnDf.columns = returnDf.columns.droplevel(0)
        returnDf.index.names = ['Date', 'BB_TICKER']
        return returnDf

test = BbgDataHistory()
test.dataHistory(fields = ["FUT_CUR_GEN_TICKER", "PX_LAST"], securities = ["YM1 COMB Comdty", "XM1 COMB Comdty"], startDate = "20090101", endDate = "20181231", perAdjustment = "ACTUAL", perSelection = "MONTHLY", requestType = "HistoricalDataRequest")