import blpapi
from BbgSession import BbgSession
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

class BbgRefDataService(BbgSession):

    def __init__(self):
        BbgSession.__init__(self)
        self.startSession()
        self.service = self.openService(serviceUrl = "//blp/refdata")
        self.request = None
        self.bbgRefData = None
    
    def createRequest(self, requestType, securities, fields):

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
        
        return request
    
    def createIntradayBarRequest(self, requestType, security, fields, startTime, endTime, event, barInterval, gapFillInitialBar):
        logger.info("Creating refdata intraday request...")
        request = self.service.createRequest(requestType)
        request.set("security", security)
        request.set("startDateTime", startTime)
        request.set("endDateTime", endTime)
        request.set("eventType", event)
        request.set("interval", barInterval)
        if gapFillInitialBar:
            request.set("gapFillInitialBar", True)
        return request

    def createIntradayRequest(self, requestType, security, fields, startTime, endTime):
        logger.info("Creating refdata intraday request...")
        request = self.service.createRequest(requestType)
        request.set("security", security)
        request.set("startDateTime", startTime)
        request.set("endDateTime", endTime)
        for field in fields:
            request.getElement("eventTypes").appendValue(field)
        request.set("includeConditionCodes", True)
        return request

    def appendRequestOverrides(self, request, overrides):
        if overrides is not None:
            eOverrides = request.getElement("overrides")
            overrideList = []
            for k, v in overrides.items():
                overrideList.append(eOverrides.appendElement())
                overrideList[len(overrideList) - 1].setElement("fieldId", k)
                overrideList[len(overrideList) - 1].setElement("value", v)
        return request

    def parseResponse(self, cid, stopSession = True):
        try:
            while(True):
                ev = self.session.nextEvent(500)

                for msg in ev:
                    if cid in msg.correlationIds() and ev.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                        logger.info(msg)
                        yield(self.parseResponseMsg(msg))
                    
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
        finally:
            # Stop the session
            if stopSession == True:
                self.closeSession()
    
    def parseResponseMsg(self, msg):
        return {
            "messageType" : "{}".format(msg.messageType()),
            "corrIDs" : ["{}".format(corrID) for corrID in msg.correlationIds()],
            "topicName" : "{}".format(msg.topicName()),
            "content" : self.parseElementData(msg.asElement())
        }
    
    def parseElementData(self, element):
        if element.datatype() == blpapi.DataType.CHOICE:
            logger.info("Parsing CHOICE element {}".format(element.name()))
            return {str(element.name()): self.parseElementData(element.getChoice())}
        elif element.isArray():
            logger.info("Parsing ARRAY element {}".format(element.name()))
            return [self.parseElementData(val) for val in element.values()]
        elif element.datatype() == blpapi.DataType.SEQUENCE:
            logger.info("Parsing SEQUENCE element {}".format(element.name()))
            return {str(element.name()): {str(subElement.name()): self.parseElementData(subElement) for subElement in element.elements()}}
        elif element.isNull():
            logger.info("Parsing NULL element {}".format(element.name()))
            return None
        else:
            logger.info("Parsing VALUE element {}".format(element.name()))
            try:
                returnValue = element.getValue()
            except:
                returnValue = None
            finally:
                return returnValue
    
    def __del__(self):
        self.closeSession()
        
 # partial lookup table for events used from blpapi.Event
eDict = {
    blpapi.Event.SESSION_STATUS: 'SESSION_STATUS',
    blpapi.Event.RESPONSE: 'RESPONSE',
    blpapi.Event.PARTIAL_RESPONSE: 'PARTIAL_RESPONSE',
    blpapi.Event.SERVICE_STATUS: 'SERVICE_STATUS',
    blpapi.Event.TIMEOUT: 'TIMEOUT',
    blpapi.Event.REQUEST: 'REQUEST'
}       
    

    


#test = BbgRefData()
#test.dataPoint(securities = ["IBM US Equity", "MSFT US Equity"], fields = ["PX_LAST", "DS002", "EQY_WEIGHTED_AVG_PX"])

