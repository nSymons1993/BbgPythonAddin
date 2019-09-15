import blpapi
import logging
from BbgRefDataService import BbgRefDataService
import pandas as pd
import numpy as np
import BbgLogger

SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")

logger = BbgLogger.logger

class BbgDataPoint(BbgRefDataService):
    def __init__(self):
        BbgRefDataService.__init__(self)

    def dataPoint(self, fields, securities, requestType = "ReferenceDataRequest", overrides = None):
        self.request = self.createRequest(securities = securities, fields = fields, overrides = overrides, requestType = requestType)
        cid = self.session.sendRequest(self.request)

        outData = pd.DataFrame(data=None, columns = ['Security','Field','Value'])

        bbgRefData = pd.DataFrame()

        for response in self.parseResponse(cid):
            # Parse responses and append content to dataframe
            #tempDf = self.refDataContentToDf(response['content'])
            logger.info(self.refDataContentToDf(response['content']))
            bbgRefData = bbgRefData.append(self.refDataContentToDf(response['content']))
        
        return bbgRefData

    def createRequest(self, requestType, securities, fields, overrides = None):

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

        if overrides is not None:
            eOverrides = request.getElement("overrides")
            overrideList = []
            for k, v in overrides.items():
                overrideList.append(eOverrides.appendElement())
                overrideList[len(overrideList) - 1].setElement("fieldId", k)
                overrideList[len(overrideList) - 1].setElement("value", v)
        
        return request

    def refDataContentToDf(self, responseContent):
        returnDf = pd.DataFrame()
        for item in responseContent['ReferenceDataResponse']:
            tempDf = pd.DataFrame(item['securityData']['fieldData']['fieldData'].items(), columns=['Fields', 'Values'])
            tempDf['securities'] = item['securityData']['security']
            returnDf = returnDf.append(tempDf)
        return returnDf.pivot(index = 'securities', columns = 'Fields', values = 'Values')