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

class BbgDataService(BbgRefDataService):
    def __init__(self):
        BbgRefDataService.__init__(self)

    def dataService(self, fields, securities, requestType = "ReferenceDataRequest", overrides = None):
        self.request = self.createRequest(securities = securities, fields = fields, overrides = overrides, requestType = requestType)
        cid = self.session.sendRequest(self.request)

        outData = pd.DataFrame(data=None, columns = ['Security','Field','Value'])

        bbgRefData = pd.DataFrame()

        for response in self.parseResponse(cid):
            # Parse responses and append content to dataframe
            #tempDf = self.refDataContentToDf(response['content'])
            testResponse = response
        
        return response

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

    def refDataContentToDf(self, response):
        responseData = response['content']['ReferenceDataResponse']
        returnDf = pd.DataFrame()
        tempDf = pd.DataFrame()
        
        for security in responseData:
            securityData = security['securityData']
            fieldData = securityData['fieldData']['fieldData']
            
            for fieldK, fieldV in fieldData.items():
                for val in fieldV:
                    tempDf = tempDf.append(pd.DataFrame(val.values()),sort=True)
                    
            tempDf['BB_TICKER'] = securityData['security']
            returnDf = returnDf.append(tempDf,sort=True)
            
        return returnDf.set_index("BB_TICKER")