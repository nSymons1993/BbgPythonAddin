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
    def __init__(self, fields, securities, overrides = None):
        if len(fields) > 1:
            raise TypeError("BbgDataService is only designed to handle a single bulk field per request.")
        self.fields = fields
        self.securities = securities
        self.overrides = overrides

    def constructDf(self):
        BbgRefDataService.__init__(self)
        self.request = self.createRequest(securities = self.securities, fields = self.fields, requestType = "ReferenceDataRequest")
        self.request = self.appendRequestOverrides(request = self.request, overrides = self.overrides)
        self.cid = self.session.sendRequest(request = self.request)
        self.bbgRefData = pd.DataFrame()

        for response in self.parseResponse(self.cid):
            self.bbgRefData = self.bbgRefData.append(self.refDataContentToDf(response))
        
        return self.bbgRefData

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