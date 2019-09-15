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
    def __init__(self, fields, securities, overrides = None):
        self.fields = fields
        self.securities = securities
        self.overrides = overrides
        
    def constructDf(self):
        BbgRefDataService.__init__(self)
        self.request = self.createRequest(securities = self.securities, fields = self.fields, requestType = "ReferenceDataRequest")
        self.request = self.appendRequestOverrides(self.request, self.overrides)
        self.cid = self.session.sendRequest(self.request)
        self.bbgRefData = pd.DataFrame()
        for response in self.parseResponse(self.cid):
            logger.info(self.refDataContentToDf(response['content']))
            self.bbgRefData = self.bbgRefData.append(self.refDataContentToDf(response['content']))
        return self.bbgRefData

    def refDataContentToDf(self, responseContent):
        returnDf = pd.DataFrame()
        for item in responseContent['ReferenceDataResponse']:
            tempDf = pd.DataFrame(item['securityData']['fieldData']['fieldData'].items(), columns=['Fields', 'Values'])
            tempDf['securities'] = item['securityData']['security']
            returnDf = returnDf.append(tempDf)
        return returnDf.pivot(index = 'securities', columns = 'Fields', values = 'Values')

    

    