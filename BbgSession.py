import blpapi
import BbgLogger

logger = BbgLogger.logger

class BbgSession:
    def __init__(self, host='localhost', port=8194, session = None, timeout = 500):
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)
        
        logger.info('Trying to connect to {!s}:{!s}'.format(host, port))

        self.session = blpapi.Session(sessionOptions)
        self.timeout = timeout        

    def startSession(self):
        logger.info('Initializing connection to BLP API: Starting BLP Session')
        if not self.session.start():
            logger.exception("Failed to start BLP API session")
            raise ConnectionError("Failed to start BLP API session")
        else:
            e = self.session.nextEvent(self.timeout)
            eLog = self.readEventTypes(e)
            if eLog['msgType'] in ['SessionConnectionUp']:
                logger.info('Successfully started blpapi session with {} response of type {}'.format(eLog['eventName'], eLog['msgType']))
                logger.info('{!r} response returned with content:\n{}'.format(eLog['eventName'], eLog['msgType']))
                return 0
            else:
                logger.exception('Failed to start blpapi session with {} response of type {}'.format(eLog['eventName'], eLog['msgType']))
                raise RuntimeError('Falied to start blpapi session with {} response of type {}'.format(eLog['eventName'], eLog['msgType']))

    def openService(self, serviceUrl):
        logger.info('Initializing connection to BLP API: Opening BLP Service')
        if not self.session.openService(serviceUrl):
            logger.exception("Failed to open BLP API service: {!s}".format(serviceUrl))
            raise ConnectionError("Failed to open BLP API service: {!s}".format(serviceUrl))
        else:
            e = self.session.nextEvent(self.timeout)
            eLog = self.readEventTypes(e)
            if eLog['msgType'] in ['SessionStarted']:
                logger.info('Successfully opened {} service with {} response of type {}'.format(serviceUrl, eLog['eventName'], eLog['msgType']))
                logger.info('{!r} response returned with content:\n{}'.format(eLog['eventName'], eLog['msgType']))
                return self.session.getService(serviceUrl)
            else:
                logger.exception('Failed to open {} service with {} response of type {}'.format(serviceUrl, eLog['eventName'], eLog['msgType']))
                raise RuntimeError('Failed to open {} service with {} response of type {}'.format(serviceUrl, eLog['eventName'], eLog['msgType']))

    def readEventTypes(self, blpEvent):
        eType = blpEvent.eventType()
        eName = eDict[blpEvent.eventType()]
        for msg in blpEvent:
            mType = msg.messageType()
        return {'eventType' : eType, 'eventName' : eName, 'msgType' : mType}

    def createRequest(self):
        raise NotImplementedError("Subclass must implement this abstract method")

    def closeSession(self):
        self.session.stop()
        e = self.session.nextEvent(self.timeout)
        eLog = self.readEventTypes(e)
        logger.info('Session stopped with {!r} event of type {!s}'.format(eLog['eventName'], eLog['msgType']))

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