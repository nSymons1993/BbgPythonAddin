import datetime as dt
import blpapi
import logging
from .BbgRefDataService import BbgRefDataService
import pandas as pd
import numpy as np
import BbgLogger
import pytz
from tzlocal import get_localzone

logger = BbgLogger.logger

SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")

BAR_DATA = blpapi.Name("barData")
BAR_TICK_DATA = blpapi.Name("barTickData")
OPEN = blpapi.Name("open")
HIGH = blpapi.Name("high")
LOW = blpapi.Name("low")
CLOSE = blpapi.Name("close")
VOLUME = blpapi.Name("volume")
NUM_EVENTS = blpapi.Name("numEvents")
TIME = blpapi.Name("time")

class BbgIntradayBar(BbgRefDataService):
    def __init__(self, securities, startTime, endTime, event = "TRADE", barInterval = 60, timeZone = str(get_localzone()), gapFillInitialBar = False, adjustmentSplit = True, adjustmentAbnormal = False, adjustmentNormal = False, adjustmentFollowDPDF = True):
        '''
            Bloomberg Historical Data query object.  Allows user to input a list of securities and fields for retrieval over a specified time period with the ability to override certain field (as specified in FLDS <GO>) if required.

        Parameters
        ----------
        fields : tuple, list, or ndarray
            The list of fields to be retrieved, field names and data types can be determined by typing FLDS <GO> and using the search box.
        securities : tuple, list, or ndarray
            List of Bloomberg tickers to retrieve data for.  If one item is passed this can be input as a string, otherwise inputs must be passed as a list or array-like.
        startTime : datetime.datetime
            The start date and time at which to retrieving data from.  Must be passed as a datetime.  If no timezone is passed, will default to co-ordinated UTC.
        endTime : datetime.datetime
            The end date and time at which to retrieving data from.  Must be passed as a datetime.  If no timezone is passed, will default to co-ordinated UTC.
        perAdjustment : string, default ACTUAL
            Determines the frequency and calendar type of the output. To be used in conjunction with Period Selection.  Inputs include:
            1. ACTUAL : These revert to the actual date from today (if the end date is left blank) or from the end date.
            2. CALENDAR : For pricing fields, these revert to the last business day of the specified calendar period. Calendar Quarterly (CQ), Calendar Semi-Annually (CS) or Calendar Yearly (CY).
            3. FISCAL : These periods revert to the fiscal period end for the company: Fiscal Quarterly (FQ), Fiscal SemiAnnual (FS) and Fiscal Yearly (FY) only.
        perSelection : string, default MONTHLY Determines the frequency of the output. To be used in conjunction with Period Adjustment.  Inputs include DAILY, WEEKLY, MONTHLY, QUARTERLY, SEMI_ANNUAL and YEARLY.
        overrides : dictionary, optional
            A dictionary containing key, value pairs of fields and override values to input.        
        overrides : dictionary, optional
            A dictionary containing key, value pairs of fields and override values to input.
        
        See Also
        --------
        BbgDataPoint.constructDf : Constructor method, retrieves data associated with a BbgDataPoint query object and generates a dataframe from it.
        BbgDataPoint : Retrieve single point static, calculated or other reference data.
        BbgIntradayTick : Retrieve historic tick-level data for a given security.
        BbgIntradayBar : Retrieve historic bar level data for a given security (open, high, low and close) for a specified time interval given in minutes.

        Examples
        --------
        Retrieve last, high and low price data for a basket of stocks in the current session.

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> stockPriceData = bbg.BbgDataPoint(securities = ['MSFT US Equity', 'IBM US Equity', 'AMP AU Equity'],fields =               ['PX_LAST', 'PX_HIGH', 'PX_LOW'])
        
        >>> stockPriceData.constructDf()
            Fields	            PX_HIGH	    PX_LAST	    PX_LOW
            securities			
            AMP AU Equity	    1.84	    1.825	    1.815
            IBM US Equity	    144.05	    143.730	    140.790
            MSFT US Equity	    172.40	    170.230	    169.580

        Retrieve bid and ask forward yield to convention for the 3-Year bond futures basket @ the futures settlement date

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> basketBondA = bbg.BbgDataPoint(securities = ['AP364296 Corp'],fields = ['YLD CNV BID', 'YLD CNV ASK'], overrides =         {'SETTLE_DT': "20200615", "PX_BID": 104.0189, "PX_ASK": 104.0339})
        >>> basketBondB = bbg.BbgDataPoint(securities = ['AP364296 Corp'],fields = ['YLD CNV BID', 'YLD CNV ASK'], overrides =         {'SETTLE_DT': "20200615", "PX_BID": 113.8049, "PX_ASK": 113.8199})
        >>> basketBondC = bbg.BbgDataPoint(securities = ['AP364296 Corp'],fields = ['YLD CNV BID', 'YLD CNV ASK'], overrides =         {'SETTLE_DT': "20200615", "PX_BID": 107.8918, "PX_ASK": 107.9968})

        >>> outData = pd.DataFrame()
        >>> outData = outData.append(basketBondA.constructDf())
        >>> outData = outData.append(basketBondB.constructDf())
        >>> outData = outData.append(basketBondC.constructDf())

        >>> outData
            Fields	            YLD CNV ASK     YLD CNV BID
            securities		
            AP364296 Corp   	0.577168	    0.583240
            AP364296 Corp   	-3.170604	    -3.165165
            AP364296 Corp   	-0.990407	    -0.949785
        '''
        self.securities = list(securities) if type(securities) is not list else securities
        self.startTime = startTime
        self.endTime = endTime
        self.event = event
        self.barInterval = barInterval
        self.timeZone = timeZone
        self.gapFillInitialBar = gapFillInitialBar
        self.adjustmentSplit = adjustmentSplit
        self.adjustmentAbnormal = adjustmentAbnormal
        self.adjustmentNormal = adjustmentNormal
        self.adjustmentFollowDPDF = adjustmentFollowDPDF

    def constructDf(self):
        BbgRefDataService.__init__(self)
        self.bbgRefData = pd.DataFrame()

        UTCStartTime = self.__convertFromTimezoneToUTC(self.startTime, self.timeZone)
        UTCEndTime = self.__convertFromTimezoneToUTC(self.endTime, self.timeZone)


        for sec in self.securities:
            self.request = self.createIntradayBarRequest(security = sec, requestType = "IntradayBarRequest", startTime = UTCStartTime, endTime = UTCEndTime, event = self.event, barInterval = self.barInterval, gapFillInitialBar = self.gapFillInitialBar, adjustmentSplit = self.adjustmentSplit, adjustmentAbnormal = self.adjustmentAbnormal, adjustmentNormal = self.adjustmentNormal, adjustmentFollowDPDF = self.adjustmentFollowDPDF)
            self.cid = self.session.sendRequest(self.request)
            for response in self.parseResponse(self.cid, False):
                self.bbgRefData = self.bbgRefData.append(self.refDataContentToDf(response, sec))
        BbgRefDataService.__del__(self)
        self.bbgRefData['time'] = self.bbgRefData['time'].apply(lambda x: self.__convertFromUTCToTimezone(x, self.timeZone))
        return self.bbgRefData.set_index(['Security', 'time'])

    def refDataContentToDf(self, response, security):
        securityData = response['content']['IntradayBarResponse']['barData']
        barData = securityData['barTickData']
        returnDf = pd.DataFrame()
        for snapShot in barData:
            fieldData = snapShot['barTickData']
            returnDf = returnDf.append(pd.DataFrame(fieldData.items(), columns=['Field', 'Values']).set_index('Field').transpose().reset_index(drop=True))#, index = [fieldData['time'] for i in range(0, len(fieldData))])[1:])
        returnDf.index.names = ['time']
        returnDf['Security'] = security
        return returnDf

    def __convertFromUTCToTimezone(self, fromDt, toTimeZone):
        return pytz.utc.localize(fromDt).astimezone(pytz.timezone(toTimeZone))

    def __convertFromTimezoneToUTC(self, fromDt, fromTimeZone):
        return pytz.timezone(fromTimeZone).localize(fromDt).astimezone(pytz.utc)